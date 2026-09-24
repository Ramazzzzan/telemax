#!/usr/bin/env python3
"""Telemax 3.5.2 — MAX <-> Telegram bridge.

Python 3.10+; maxapi-python==2.4.1; curl; Linux.
Configure constants.json, run --init once, then start normally.
Existing schema 3 (tm_*) / 350 upgrades in place via --upgrade-db, after stopping
service and backup. Never reimport legacy queue_v2 rows. Session and routes retained.
MAX receipts enter an atomic filesystem inbox before SQLite admission. Finite
storage limits still apply; no guarantee for events not durably written or unseen.

Commands: /status, /dlq, /retry_dlq [ID], /retry_dlq ID force,
          /clear_dlq confirm, /alias Name, /bind MAX_CHAT_ID, /help,
          /chat MAX_USER_ID|+PHONE [Name], /mute [TOPIC_ID], /unmute [TOPIC_ID], /muted.
An interrupted external send is held as uncertain until an explicit forced retry.
Exactly-once delivery and replay of MAX events missed while offline are not promised.

Checks: python telemax.py --check-config; python telemax.py --check
Tests:  python -m unittest -v telemax_test
"""
from __future__ import annotations

import argparse
import base64
import asyncio
import concurrent.futures
import contextlib
import dataclasses
import enum
import fcntl
import hashlib
import heapq
import importlib.metadata
import ipaddress
import json
import logging
import logging.handlers
import math
import os
import random
import re
import shutil
import signal
import socket
import sqlite3
import sys
import time
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.parse import quote, urljoin, urlsplit

VERSION = "3.5.2"
SCHEMA_VERSION = "351"  # Storage format, not application release number.
UPGRADE_SCHEMAS = {"3", "350"}
SDK_VERSION = "2.4.1"
LOG = logging.getLogger("telemax")
ACTIVE = ("pending", "running", "dead", "uncertain")
MEDIA_TYPES = {"PHOTO", "VIDEO", "FILE", "AUDIO", "VOICE", "STICKER"}


class Retry(Exception):
    def __init__(self, reason: str, delay: float = 30, *, count: bool = True):
        super().__init__(reason)
        self.delay, self.count = delay, count


class Permanent(Exception):
    pass


class Uncertain(Exception):
    pass


class Suppressed(Exception):
    """A deliberately muted forwarding job; not a delivery error."""


class CapacityError(RuntimeError):
    pass


def jdump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def payload_object(raw: str) -> dict:
    """Diagnostics must also work for malformed/unexpected saved payloads."""
    try:
        value = json.loads(raw)
        if isinstance(value, dict):
            return value
    except (ValueError, TypeError):
        pass
    return {"unparsed_payload": raw}


def message_json(value: Any, seen: set[int] | None = None) -> Any:
    """Lossless JSON-compatible SDK *message* snapshot, including bytes as base64.

    This is the public model seen by the callback, not the original transport frame.
    SDK-private attributes (bound API/client/session) are deliberately not traversed.
    """
    if isinstance(value, enum.Enum):
        return message_json(value.value, seen)
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else {"__float__": str(value)}
    if isinstance(value, (bytes, bytearray, memoryview)):
        return {"__encoding__": "base64", "data": base64.b64encode(bytes(value)).decode("ascii")}
    if isinstance(value, datetime):
        return {"__datetime__": value.isoformat()}
    seen = set() if seen is None else seen
    if id(value) in seen:
        return {"__circular_reference__": type(value).__name__}
    seen.add(id(value))
    try:
        if isinstance(value, dict):
            return {str(k): message_json(v, seen) for k, v in value.items()}
        if isinstance(value, (list, tuple, set)):
            return [message_json(v, seen) for v in value]
        if callable(getattr(value, "model_dump", None)):
            try:
                snapshot = value.model_dump(mode="python", by_alias=True)
            except Exception as exc:
                snapshot = {k: v for k, v in vars(value).items() if not k.startswith("_") and not callable(v)}
                snapshot["__snapshot_error__"] = type(exc).__name__
            return message_json(snapshot, seen)
        if hasattr(value, "__dict__"):
            return {k: message_json(v, seen) for k, v in vars(value).items()
                    if not k.startswith("_") and not callable(v)}
        # Do not stringify unknown SDK objects: repr can expose client credentials.
        return {"__unserializable_type__": type(value).__name__}
    finally:
        seen.remove(id(value))


def display_time(stamp: Any, *, milliseconds=False) -> str:
    """Timestamp with an explicit UTC offset, in the server's local timezone."""
    try:
        if isinstance(stamp, bool) or stamp is None:
            raise ValueError()
        value = float(stamp)
        if not math.isfinite(value) or value <= 0:
            raise ValueError()
        if milliseconds:
            value /= 1000
        return datetime.fromtimestamp(value, timezone.utc).astimezone().isoformat(timespec="seconds")
    except (TypeError, ValueError, OverflowError, OSError):
        return "неизвестно"


def user_name(user: Any, fallback: str) -> str:
    name = " ".join(str(get(user, x) or "") for x in ("first_name", "last_name")).strip()
    names = get(user, "names") or []
    return str(name or (get(names[0], "name") if names else None) or get(user, "name") or fallback)


def control_text(attachment: dict) -> str:
    # ControlAttachment in maxapi-python 2.4.1 has event and optional title.
    # Unknown event codes are displayed literally, never guessed or downloaded.
    event = str(attachment.get("event") or "CONTROL")
    title = str(attachment.get("title") or "").strip()
    return f"[Событие MAX: {event}]" + (f" {title}" if title else "")


def attachment_text(attachment: dict) -> str:
    """Render known non-file MAX attachments without pretending to sync native widgets."""
    a = attachment.get("raw") if isinstance(attachment.get("raw"), dict) else attachment
    typ = kind(attachment.get("type") or attachment.get("unsupported") or get(a, "_type"))
    if typ == "CONTROL":
        return control_text({**a, **attachment})
    if typ == "SHARE":
        parts = [str(get(a, k)) for k in ("title", "url", "description") if get(a, k)]
        return "[Превью ссылки]" + ("\n" + "\n".join(parts) if parts else "")
    if typ == "CONTACT":
        name = get(a, "name") or " ".join(str(get(a, k) or "") for k in ("first_name", "last_name")).strip()
        parts = [str(name)] if name else []
        for field, label in (("contact_id", "MAX user ID"), ("phone", "Телефон")):
            if get(a, field) is not None:
                parts.append(f"{label}: {get(a, field)}")
        return "[Контакт MAX]" + ("\n" + "\n".join(parts) if parts else "")
    if typ == "POLL":
        parts = ["[Опрос MAX — текстовый снимок, голосование не синхронизируется]"]
        if get(a, "title"):
            parts.append(str(get(a, "title")))
        answers = get(a, "answers") or []
        if isinstance(answers, list):
            parts.extend(f"{i}. {get(answer, 'text') or ''}" for i, answer in enumerate(answers, 1))
        total = get(get(a, "state"), "total")
        if total is not None:
            parts.append(f"Голосов в снимке: {total}")
        return "\n".join(parts)
    if typ == "CALL":
        parts = ["[Звонок MAX]"]
        # Units of duration are not asserted: show the actual SDK value.
        for key in ("call_type", "hangup_type", "duration", "conversation_id", "contact_ids"):
            v = get(a, key)
            if v is not None and v != []:
                parts.append(f"{key}: {v}")
        return "\n".join(parts)
    return f"[Вложение MAX {typ or 'UNKNOWN'}: отображается только описание; полный объект сохранён в исходной задаче]"


def remove_controls(data: dict) -> tuple[dict, bool]:
    """Compatibility name: now removes ALL non-file descriptors, including old DLQ jobs."""
    files, rendered, changed = [], [], False
    text = str(data.get("text") or "")
    for f in data.get("files") or []:
        if not isinstance(f, dict):
            rendered.append("[Нераспознанное вложение MAX; исходный объект сохранён]")
            changed = True
            continue
        typ = kind(f.get("type") or f.get("unsupported"))
        # Telegram/legacy files need no MAX type; keep their existing descriptors.
        if f.get("source") in {"telegram", "legacy"} or (typ in MEDIA_TYPES and not f.get("unsupported")):
            files.append(f)
            continue
        if not typ and not f.get("unsupported") and (
                f.get("source") != "max" or (f.get("path") and f.get("kind") in {"photo", "video", "voice", "document"})):
            files.append(f)
            continue
        changed = True
        text = text.replace(f"\n[Вложение {typ or 'UNKNOWN'}: формат не поддерживается; сохранено в DLQ]", "")
        rendered.append(attachment_text(f))
    if not changed:
        return data, False
    result = dict(data)
    result["files"] = files
    result["text"] = (text + "\n" + "\n\n".join(rendered)).strip()
    return result, True


def independent_tg_parts(text: str, files: list[dict]) -> list[dict]:
    """Text never shares a delivery task with a download that can fail."""
    return tg_parts(text, []) + (tg_parts("", files) if files else [])


def get(obj: Any, key: str, default: Any = None) -> Any:
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        camel = re.sub(r"_([a-z])", lambda m: m[1].upper(), key)
        return obj.get(camel, default)
    return getattr(obj, key, default)


def kind(value: Any) -> str:
    return str(value.value if isinstance(value, enum.Enum) else value or "").upper()


def number(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    s = str(value)
    return int(s) if re.fullmatch(r"-?\d+", s) else None


def utf16len(text: str) -> int:
    return len(text.encode("utf-16-le")) // 2


def split_text(text: str, limit: int = 4000) -> list[str]:
    """Never cut a Unicode code point or exceed the conservative UTF-16 budget."""
    if limit < 2:
        raise ValueError("Text limit must be >= 2")
    text = text.replace("\x00", "").encode("utf-8", "replace").decode("utf-8")
    result, start, used = [], 0, 0
    for index, char in enumerate(text):
        cost = 2 if ord(char) > 0xFFFF else 1
        if used + cost > limit:
            result.append(text[start:index])
            start, used = index, 0
        used += cost
    if start < len(text):
        result.append(text[start:])
    return result


def safe_name(name: str, fallback: str = "attachment.bin") -> str:
    name = str(name or "").replace("\\", "/").split("/")[-1]
    name = re.sub(r"[^\w.() -]", "_", name, flags=re.UNICODE).strip(" .")
    if not name:
        return fallback
    suffix = Path(name).suffix[:16]
    return name if len(name.encode("utf-8")) <= 150 else (
        name[:30].rstrip(" .") + "_" + hashlib.sha256(name.encode()).hexdigest()[:12] + suffix
    )


def tg_parts(text: str, files: list[dict]) -> list[dict]:
    """Each returned part corresponds to exactly one external send operation."""
    result: list[dict] = []
    caption = text
    if not files or utf16len(text) > 900:
        result.extend({"text": p, "files": []} for p in split_text(text))
        caption = ""
    index = 0
    while index < len(files):
        group = [files[index]]
        if group[0].get("kind") in {"photo", "video"}:
            while (index + len(group) < len(files) and len(group) < 10
                   and files[index + len(group)].get("kind") in {"photo", "video"}):
                group.append(files[index + len(group)])
        result.append({"text": caption, "files": group})
        caption = ""
        index += len(group)
    return result


@dataclasses.dataclass(frozen=True)
class Config:
    root: Path
    phone: str
    token: str
    chat_id: int
    allowed: frozenset[int] = frozenset()
    admins: frozenset[int] = frozenset()
    my_max_id: int | None = None
    proxy: str = "socks5h://127.0.0.1:10808"
    ntfy: str = ""
    max_file_bytes: int = 49 * 1024 * 1024
    max_tg_download_bytes: int = 20 * 1024 * 1024
    disk_limit: int = 2 * 1024**3
    min_free: int = 256 * 1024**2
    queue_limit: int = 50000
    history_days: int = 30
    max_attempts: int = 10
    error_dump_limit: int = 256 * 1024**2
    inbox_limit: int = 512 * 1024**2
    workers: int = 4
    download_workers: int = 3
    retry_window: int = 86400
    history_max_jobs: int = 10000
    dedup_days: int = 90
    dedup_max_keys: int = 200000
    media_ipv4_only: bool = True
    blocked_ipv6_prefixes: tuple[str, ...] = ()

    KEYS = frozenset({
        "MAX_PHONE", "TG_BOT_TOKEN", "TG_CHAT_ID", "TG_ALLOWED_USER_IDS",
        "TG_ADMIN_USER_IDS", "MY_MAX_ID", "TG_PROXY", "NTFY_URL", "MAX_MEDIA_MB",
        "TG_DOWNLOAD_MB", "MEDIA_DISK_LIMIT_MB", "MIN_FREE_MB", "QUEUE_LIMIT",
        "HISTORY_DAYS", "MAX_ATTEMPTS", "ERROR_DUMP_LIMIT_MB",
        "INBOX_LIMIT_MB", "WORKERS", "DOWNLOAD_WORKERS", "RETRY_WINDOW_SECONDS",
        "HISTORY_MAX_JOBS", "DEDUP_DAYS", "DEDUP_MAX_KEYS", "MEDIA_IPV4_ONLY",
        "BLOCKED_IPV6_PREFIXES",
    })

    @property
    def media(self) -> Path:
        return self.root / "media_queue"

    @property
    def errors(self) -> Path:
        return self.root / "dumps" / "errors"

    @property
    def inbox(self) -> Path:
        return self.root / "inbox"

    @classmethod
    def load(cls, path: Path) -> Config:
        def unique_keys(pairs):
            obj = {}
            for key, value in pairs:
                if key in obj:
                    raise ValueError(f"Duplicate configuration key: {key}")
                obj[key] = value
            return obj

        data = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=unique_keys)
        if not isinstance(data, dict):
            raise ValueError("constants.json must contain an object")
        unknown = sorted(set(data) - cls.KEYS)
        if unknown:
            raise ValueError("Unknown configuration setting(s): " + ", ".join(unknown))
        for key in ("MAX_PHONE", "TG_BOT_TOKEN", "TG_CHAT_ID"):
            if data.get(key) is None or str(data[key]).strip() in {"", "None", "null"}:
                raise ValueError(f"Missing required setting: {key}")
        phone, token = str(data["MAX_PHONE"]).strip(), str(data["TG_BOT_TOKEN"]).strip()
        chat_id = number(data["TG_CHAT_ID"])
        if not re.fullmatch(r"\+\d{7,16}", phone):
            raise ValueError("MAX_PHONE must be a phone number with country code")
        if not re.fullmatch(r"[1-9]\d*:[A-Za-z0-9_-]+", token):
            raise ValueError("Invalid TG_BOT_TOKEN format")
        if chat_id is None or chat_id >= 0:
            raise ValueError("TG_CHAT_ID must be a negative Telegram supergroup ID")

        def ids(key):
            raw = data.get(key, [])
            if not isinstance(raw, list):
                raise ValueError(f"{key} must be a JSON list of positive integers")
            values = [number(x) for x in raw]
            if any(x is None or x <= 0 for x in values):
                raise ValueError(f"Invalid user ID in {key}")
            return frozenset(values)

        def positive(key, default):
            value = number(data.get(key, default))
            if value is None or value <= 0:
                raise ValueError(f"{key} must be a positive integer")
            return value

        own = number(data.get("MY_MAX_ID"))
        if data.get("MY_MAX_ID") is not None and (own is None or own <= 0):
            raise ValueError("MY_MAX_ID must be a positive integer or null")
        proxy = str(data.get("TG_PROXY", "socks5h://127.0.0.1:10808"))
        if any(ord(c) < 33 for c in proxy):
            raise ValueError("TG_PROXY must not contain whitespace/control characters")
        u = urlsplit(proxy)
        if u.scheme != "socks5h" or not u.hostname or not u.port or u.path or u.query or u.fragment:
            raise ValueError("TG_PROXY must be socks5h://host:port; direct Telegram is disabled")
        ntfy = data.get("NTFY_URL") or ""
        if not isinstance(ntfy, str) or (ntfy and (
                any(ord(c) < 33 for c in ntfy)
                or urlsplit(ntfy).scheme not in {"https", "http"}
                or not urlsplit(ntfy).hostname)):
            raise ValueError("Invalid NTFY_URL")
        ipv4_only = data.get("MEDIA_IPV4_ONLY", True)
        if not isinstance(ipv4_only, bool):
            raise ValueError("MEDIA_IPV4_ONLY must be true or false")
        networks = data.get("BLOCKED_IPV6_PREFIXES", [])
        if not isinstance(networks, list) or not all(isinstance(n, str) for n in networks):
            raise ValueError("BLOCKED_IPV6_PREFIXES must be a JSON array of IPv6 CIDRs")
        networks = tuple(str(ipaddress.IPv6Network(n)) for n in networks)
        workers, downloads = positive("WORKERS", 4), positive("DOWNLOAD_WORKERS", 3)
        if workers > 16 or downloads > 16:
            raise ValueError("WORKERS and DOWNLOAD_WORKERS must be between 1 and 16")
        return cls(path.resolve().parent, phone, token, chat_id,
                   ids("TG_ALLOWED_USER_IDS"), ids("TG_ADMIN_USER_IDS"), own, proxy, ntfy,
                   positive("MAX_MEDIA_MB", 49) * 1024**2,
                   min(20, positive("TG_DOWNLOAD_MB", 20)) * 1024**2,
                   positive("MEDIA_DISK_LIMIT_MB", 2048) * 1024**2,
                   positive("MIN_FREE_MB", 256) * 1024**2,
                   positive("QUEUE_LIMIT", 50000), positive("HISTORY_DAYS", 30),
                   positive("MAX_ATTEMPTS", 10), positive("ERROR_DUMP_LIMIT_MB", 256) * 1024**2,
                   positive("INBOX_LIMIT_MB", 512) * 1024**2, workers, downloads,
                   positive("RETRY_WINDOW_SECONDS", 86400), positive("HISTORY_MAX_JOBS", 10000),
                   positive("DEDUP_DAYS", 90), positive("DEDUP_MAX_KEYS", 200000),
                   ipv4_only, networks)

    def authorized(self, msg: dict, *, admin: bool = False) -> bool:
        sender = msg.get("from") or {}
        return (number((msg.get("chat") or {}).get("id")) == self.chat_id
                and not msg.get("sender_chat") and not sender.get("is_bot")
                and not msg.get("is_automatic_forward")
                and number(sender.get("id")) in (self.admins if admin else self.allowed | self.admins))


class RedactedFormatter(logging.Formatter):
    def __init__(self, secrets: list[str]):
        super().__init__("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
        self.secrets = [s for s in secrets if s]

    def format(self, record):
        text = super().format(record)
        for secret in self.secrets:
            text = text.replace(secret, "[redacted]")
        return re.sub(r"https?://\S+", "[URL redacted]", text)


def configure_logging(cfg: Config):
    handler = logging.handlers.RotatingFileHandler(
        cfg.root / "telemax.log", maxBytes=5 * 1024**2, backupCount=3, encoding="utf-8")
    fmt = RedactedFormatter([cfg.token, cfg.phone, cfg.ntfy, cfg.proxy])
    stream = logging.StreamHandler()
    for h in (handler, stream):
        h.setFormatter(fmt)
    logging.basicConfig(level=logging.INFO, handlers=[handler, stream], force=True)
    # The SDK must not emit raw message payloads or tokens to its own handlers.
    logging.getLogger("pymax").setLevel(logging.WARNING)
    os.chmod(cfg.root / "telemax.log", 0o600)


class Store:
    """One DB thread. Transactions include all state changes, not individual queries."""
    TABLES = {
        "tm_meta": {"key", "value"},
        "tm_routes": {"max_id", "thread_id", "name", "type", "state"},
        "tm_jobs": {"id", "key", "kind", "route", "payload", "state", "phase",
                    "attempts", "next_at", "error", "result", "created", "updated", "retry_since"},
        "tm_seen": {"key", "expires"},
    }
    ARCHIVE_TABLES = {"tm_aliases", "queue_v2", "queue_dead_letter", "topics", "contacts", "settings"}

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.path = cfg.root / "telegram_queue.db"
        self.conn = self.open_existing(cfg)
        self.extra_tables = {r[0] for r in self.conn.execute("SELECT name FROM sqlite_master WHERE type='table'")} - set(self.TABLES)
        self.db_error = ""
        self.last_db_log = 0.0
        self.pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="telemax-db")
        try:
            self.validate(self.conn, cfg)
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA synchronous=FULL")
            self.conn.execute("PRAGMA busy_timeout=1000")
            self.conn.execute("BEGIN IMMEDIATE")
            self.conn.execute(
                "UPDATE tm_jobs SET state='uncertain',error='Restart during external send; check delivery',"
                "updated=? WHERE state='running' AND phase='send'", (time.time(),))
            self.conn.execute(
                "UPDATE tm_jobs SET state='pending',phase='',next_at=0 WHERE state='running'")
            self.conn.execute("UPDATE tm_routes SET state='uncertain' WHERE state='creating'")
            self.conn.commit()
            os.chmod(self.path, 0o600)
        except BaseException:
            self.conn.rollback()
            self.conn.close()
            self.pool.shutdown(wait=True)
            raise

    @staticmethod
    def open_existing(cfg: Config, *, readonly=False):
        path = cfg.root / "telegram_queue.db"
        if path.is_symlink() or not path.is_file():
            raise RuntimeError("State database is missing or is a symlink; run --init in a fresh directory")
        uri = path.resolve().as_uri() + ("?mode=ro" if readonly else "?mode=rw")
        conn = sqlite3.connect(uri, uri=True, timeout=30, check_same_thread=False,
                               isolation_level=None)
        conn.row_factory = sqlite3.Row
        return conn

    @classmethod
    def validate(cls, conn, cfg: Config, *, for_upgrade=False, confirm_legacy_bot=False):
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")}
        if "tm_meta" not in tables:
            raise RuntimeError("Unsupported database: expected tm_* state, not queue_v2-only/bridge_jobs. No data changed.")
        meta = dict(conn.execute("SELECT key,value FROM tm_meta"))
        schema = meta.get("schema")
        supported = {SCHEMA_VERSION} | (UPGRADE_SCHEMAS if for_upgrade else set())
        if schema not in supported:
            raise RuntimeError(f"Database format {schema!r} needs --upgrade-db or is unsupported; do not delete/reset it")
        columns_by_table = {k: set(v) for k, v in cls.TABLES.items()}
        if schema in UPGRADE_SCHEMAS:
            columns_by_table.pop("tm_seen")
            columns_by_table["tm_jobs"].remove("retry_since")
        if not set(columns_by_table) <= tables or tables - set(columns_by_table) - cls.ARCHIVE_TABLES:
            raise RuntimeError("Unrecognized database layout; no tables are imported or discarded")
        for table, columns in columns_by_table.items():
            actual = {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}
            if actual != columns:
                raise RuntimeError(f"Invalid database layout: {table}")
        if meta.get("tg_chat_id") != str(cfg.chat_id):
            raise RuntimeError("TG_CHAT_ID differs from this database; saved messages will not be rerouted")
        bot_id = meta.get("tg_bot_id")
        if schema == "3" and not bot_id:
            if not confirm_legacy_bot:
                raise RuntimeError("Legacy v3 did not save bot identity: keep the SAME bot token and add --confirm-legacy-bot")
        elif bot_id != cfg.token.split(":", 1)[0]:
            raise RuntimeError("Telegram bot differs from this database; refusing to reuse Telegram offset")
        if cfg.my_max_id and meta.get("max_account_id") not in {None, str(cfg.my_max_id)}:
            raise RuntimeError("MY_MAX_ID differs from this database")
        return meta

    @classmethod
    def check(cls, cfg: Config):
        conn = cls.open_existing(cfg, readonly=True)
        try:
            cls.validate(conn, cfg)
            if conn.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                raise RuntimeError("SQLite quick_check failed")
        finally:
            conn.close()

    @classmethod
    def create(cls, cfg: Config):
        """Create one empty database. Never reuse, import, or reset an existing file."""
        path = cfg.root / "telegram_queue.db"
        for name in (str(path) + "-wal", str(path) + "-shm", str(path) + "-journal"):
            if os.path.lexists(name):
                raise RuntimeError("SQLite sidecar exists; use an empty installation directory")
        try:
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError as exc:
            raise RuntimeError("Database already exists; --init never overwrites state") from exc
        os.close(fd)
        conn = None
        try:
            conn = sqlite3.connect(path, isolation_level=None)
            conn.execute("PRAGMA auto_vacuum=INCREMENTAL")
            conn.execute("PRAGMA synchronous=FULL")
            conn.execute("BEGIN IMMEDIATE")
            statements = (
                "CREATE TABLE tm_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL)",
                "CREATE TABLE tm_routes(max_id TEXT PRIMARY KEY,thread_id INTEGER UNIQUE,"
                "name TEXT NOT NULL,type TEXT NOT NULL DEFAULT 'group',"
                "state TEXT NOT NULL DEFAULT 'new' CHECK(state IN ('new','creating','ready','uncertain')))",
                "CREATE TABLE tm_jobs(id INTEGER PRIMARY KEY AUTOINCREMENT,key TEXT NOT NULL UNIQUE,"
                "kind TEXT NOT NULL,route TEXT NOT NULL,payload TEXT NOT NULL,"
                "state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN "
                "('pending','running','done','dead','uncertain','cancelled')),phase TEXT NOT NULL DEFAULT '',"
                "attempts INTEGER NOT NULL DEFAULT 0,next_at REAL NOT NULL DEFAULT 0,"
                "error TEXT NOT NULL DEFAULT '',result TEXT NOT NULL DEFAULT '{}',"
                "created REAL NOT NULL,updated REAL NOT NULL,retry_since REAL NOT NULL DEFAULT 0)",
                "CREATE TABLE tm_seen(key TEXT PRIMARY KEY,expires REAL NOT NULL)",
                "CREATE INDEX tm_seen_expiry ON tm_seen(expires)",
                "CREATE INDEX tm_job_due ON tm_jobs(kind,state,next_at,id)",
                "CREATE INDEX tm_job_route ON tm_jobs(kind,route,state,id)",
                "CREATE INDEX tm_job_state ON tm_jobs(state,updated)",
            )
            for statement in statements:
                conn.execute(statement)
            conn.executemany("INSERT INTO tm_meta VALUES(?,?)", (
                ("schema", SCHEMA_VERSION), ("created_by", VERSION),
                ("tg_chat_id", str(cfg.chat_id)), ("tg_bot_id", cfg.token.split(":", 1)[0]),
            ))
            conn.commit()
            if conn.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                raise RuntimeError("SQLite initialization verification failed")
            conn.close()
            conn = None
            fsync_dir(cfg.root)
        except BaseException:
            if conn is not None:
                conn.close()
            path.unlink(missing_ok=True)
            raise

    @classmethod
    def backup(cls, cfg: Config, conn) -> Path:
        directory = cfg.root / "backups"
        if directory.is_symlink():
            raise RuntimeError("Backup directory must not be a symlink")
        directory.mkdir(mode=0o700, exist_ok=True)
        snapshot = directory / ("pre-" + VERSION + "-" + time.strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:8])
        snapshot.mkdir(mode=0o700)
        path = snapshot / "telegram_queue.db"
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        os.close(fd)
        dest = sqlite3.connect(path)
        try:
            conn.backup(dest)
            if dest.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                raise RuntimeError("Backup verification failed; state not upgraded")
        finally:
            dest.close()
        for name in ("constants.json", "telemax.py"):
            source = cfg.root / name
            if source.is_file() and not source.is_symlink():
                shutil.copy2(source, snapshot / name)
                os.chmod(snapshot / name, 0o600)
        fsync_dir(snapshot)
        fsync_dir(directory)
        return snapshot

    @classmethod
    def upgrade(cls, cfg: Config, *, confirm_legacy_bot=False) -> Path | None:
        """Offline, additive and transactional. Never reimport archived queues or rewrite jobs.

        Supported sources: single-file v3 tm_* schema 3; 3.5.0/1 schema 350.
        Caller MUST hold instance_lock; the deployed service must be stopped.
        """
        conn = cls.open_existing(cfg)
        try:
            meta = cls.validate(conn, cfg, for_upgrade=True, confirm_legacy_bot=confirm_legacy_bot)
            if conn.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                raise RuntimeError("Database integrity check failed; refusing upgrade")
            if meta["schema"] == SCHEMA_VERSION:
                return None
            snapshot = cls.backup(cfg, conn)
            conn.execute("PRAGMA synchronous=FULL")
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute("ALTER TABLE tm_jobs ADD COLUMN retry_since REAL NOT NULL DEFAULT 0")
                conn.execute("CREATE TABLE tm_seen(key TEXT PRIMARY KEY,expires REAL NOT NULL)")
                conn.execute("CREATE INDEX tm_seen_expiry ON tm_seen(expires)")
                conn.execute("CREATE INDEX IF NOT EXISTS tm_job_state ON tm_jobs(state,updated)")
                # Do not alter tg_offset/max_account_id/mute policies or jobs/routes at all.
                conn.executemany("INSERT OR REPLACE INTO tm_meta VALUES(?,?)", (
                    ("schema", SCHEMA_VERSION), ("upgraded_from", meta["schema"]),
                    ("upgraded_by", VERSION), ("tg_bot_id", cfg.token.split(":", 1)[0]),))
                cls.validate(conn, cfg)
                conn.commit()
            except BaseException:
                conn.rollback()
                raise
            fsync_dir(cfg.root)
            return snapshot
        finally:
            conn.close()

    @classmethod
    def compact(cls, cfg: Config) -> Path:
        """Offline physical compaction, with a fresh verified backup. Does not delete jobs."""
        conn = cls.open_existing(cfg)
        try:
            cls.validate(conn, cfg)
            size = (cfg.root / "telegram_queue.db").stat().st_size
            if shutil.disk_usage(cfg.root).free < 3 * size + cfg.min_free:
                raise RuntimeError("Need free space for backup and VACUUM temporary database")
            snapshot = cls.backup(cfg, conn)
            if conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()[0]:
                raise RuntimeError("Database busy; stop all readers/writers before compaction")
            conn.execute("PRAGMA auto_vacuum=INCREMENTAL")
            conn.execute("VACUUM")
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            if conn.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                raise RuntimeError("Post-compaction integrity check failed")
            return snapshot
        finally:
            conn.close()

    @staticmethod
    def insert(c, key, job_kind, route, payload, state="pending", error=""):
        now = time.time()
        if c.execute("SELECT 1 FROM tm_seen WHERE key=? AND expires>?", (key, now)).fetchone():
            return None
        cur = c.execute("""INSERT INTO tm_jobs(key,kind,route,payload,state,error,created,updated)
                         VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(key) DO NOTHING""",
                        (key, job_kind, str(route), jdump(payload), state, error, now, now))
        return cur.lastrowid if cur.rowcount else None

    async def db_call(self, fn):
        """Retry only storage/unavailability errors. Never wrap/retry an external send."""
        while True:
            future = asyncio.get_running_loop().run_in_executor(self.pool, fn)
            try:
                result = await asyncio.shield(future)
                self.db_error = ""
                return result
            except asyncio.CancelledError:
                # The SQLite thread cannot be cancelled. Wait before releasing the connection.
                with contextlib.suppress(Exception):
                    await future
                raise
            except sqlite3.Error as exc:
                code = getattr(exc, "sqlite_errorcode", 0) & 255
                recoverable = code in {5, 6, 10, 11, 13, 14, 26} or any(
                    word in str(exc).lower() for word in ("locked", "busy", "disk i/o", "disk is full", "readonly"))
                if not recoverable:
                    raise
                self.db_error = type(exc).__name__
                if time.monotonic() - self.last_db_log > 30:
                    LOG.error("SQLite unavailable (%s); inbound MAX events remain in inbox", self.db_error)
                    self.last_db_log = time.monotonic()
                await asyncio.sleep(2)

    async def tx(self, fn: Callable):
        def run():
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                result = fn(self.conn)
                self.conn.commit()
                return result
            except BaseException:
                self.conn.rollback()
                raise
        return await self.db_call(run)

    async def read(self, sql: str, params: tuple = ()) -> list[dict]:
        def run():
            return [dict(r) for r in self.conn.execute(sql, params).fetchall()]
        return await self.db_call(run)

    async def meta(self, key: str, default="") -> str:
        rows = await self.read("SELECT value FROM tm_meta WHERE key=?", (key,))
        return rows[0]["value"] if rows else default

    async def set_meta(self, key: str, value: Any):
        await self.tx(lambda c: c.execute("INSERT OR REPLACE INTO tm_meta VALUES(?,?)", (key, str(value))).rowcount)

    def capacity(self, c):
        count = c.execute("SELECT COUNT(*) FROM tm_jobs WHERE state IN ('pending','running')").fetchone()[0]
        if count >= self.cfg.queue_limit:
            raise CapacityError("Durable queue limit reached; no new events acknowledged")

    @staticmethod
    def muted_in_tx(c, route: str) -> bool:
        row = c.execute("SELECT value FROM tm_meta WHERE key=?", ("mute:" + str(route),)).fetchone()
        if not row:
            return False
        try:
            value = json.loads(row[0])
            return isinstance(value, dict) and value.get("muted") is True
        except (ValueError, TypeError):
            # Corrupt policy must not accidentally enable forwarding.
            raise RuntimeError(f"Invalid mute policy for route {route}")

    async def muted(self, route: str) -> bool:
        return await self.tx(lambda c: self.muted_in_tx(c, route))

    @staticmethod
    def cancel_muted_in_tx(c, job_id: int):
        c.execute("UPDATE tm_jobs SET state='cancelled',phase='',error='',result=?,updated=? "
                  "WHERE id=? AND state NOT IN ('done','uncertain')",
                  (jdump({"suppressed": "muted"}), time.time(), job_id))

    async def forward_allowed(self, job: dict, *, sending=False) -> bool:
        def op(c):
            row = c.execute("SELECT state FROM tm_jobs WHERE id=?", (job["id"],)).fetchone()
            if not row or row[0] == "cancelled" or self.muted_in_tx(c, job["route"]):
                self.cancel_muted_in_tx(c, job["id"])
                return False
            if sending:
                c.execute("UPDATE tm_jobs SET phase='send',updated=? WHERE id=?",
                          (time.time(), job["id"]))
            return True
        allowed = await self.tx(op)
        if allowed and sending:
            job["phase"] = "send"
        return allowed

    async def add(self, key, job_kind, route, payload):
        def op(c):
            if (c.execute("SELECT 1 FROM tm_jobs WHERE key=?", (key,)).fetchone()
                    or c.execute("SELECT 1 FROM tm_seen WHERE key=? AND expires>?", (key, time.time())).fetchone()):
                return False
            self.capacity(c)
            jid = self.insert(c, key, job_kind, route, payload)
            if job_kind in {"max_in", "to_tg"}:
                policy_row = c.execute("SELECT value FROM tm_meta WHERE key=?", ("mute:" + str(route),)).fetchone()
                policy = payload_object(policy_row[0]) if policy_row else {}
                received = payload.get("received_at")
                stale_muted = (job_kind == "max_in" and isinstance(received, (float, int))
                               and received <= float(policy.get("discard_before") or 0))
                if self.muted_in_tx(c, str(route)) or stale_muted:
                    self.cancel_muted_in_tx(c, jid)
            return True
        return await self.tx(op)

    async def accept_updates(self, updates: list[dict]):
        """Freeze the target and save the Telegram offset in the same transaction."""
        def op(c):
            row = c.execute("SELECT value FROM tm_meta WHERE key='tg_offset'").fetchone()
            offset = int(row[0]) if row else 0
            for update in sorted(updates, key=lambda u: int(u["update_id"])):
                uid = int(update["update_id"])
                if uid < offset:
                    continue
                msg = update.get("message")
                if msg and self.cfg.authorized(msg):
                    text = str(msg.get("text") or msg.get("caption") or "")
                    is_command = text.startswith("/") and bool(msg.get("text"))
                    thread = msg.get("message_thread_id")
                    has_content = text or any(msg.get(k) for k in
                        ("photo", "document", "video", "voice", "audio", "animation", "sticker", "video_note", "contact", "location", "poll", "venue", "dice"))
                    if is_command or (thread and has_content):
                        if not is_command:
                            self.capacity(c)
                        route = c.execute("SELECT max_id FROM tm_routes WHERE thread_id=?", (thread,)).fetchone()
                        payload = {"message": msg, "target": route[0] if route else None, "update": update}
                        self.insert(c, f"tg:{uid}", "command" if is_command else "tg_in",
                                    route[0] if route else f"thread:{thread}", payload)
                offset = max(offset, uid + 1)
            c.execute("INSERT OR REPLACE INTO tm_meta VALUES('tg_offset',?)", (str(offset),))
            c.execute("INSERT OR REPLACE INTO tm_meta VALUES('tg_poll_ok',?)", (str(time.time()),))
            return offset
        return await self.tx(op)

    async def claim(self, job_kind: str) -> dict | None:
        def op(c):
            now = time.time()
            # A count=False retry is still finite in wall-clock time.
            c.execute("UPDATE tm_jobs SET state='dead',phase='',error='Retry window exhausted; manual review required',updated=? "
                      "WHERE kind=? AND state='pending' AND retry_since>0 AND retry_since<=?",
                      (now, job_kind, now - self.cfg.retry_window))
            row = c.execute("""SELECT j.* FROM tm_jobs j WHERE j.kind=? AND j.state='pending'
                AND j.next_at<=? AND NOT EXISTS(SELECT 1 FROM tm_jobs p
                    WHERE p.kind=j.kind AND p.route=j.route AND
                    (p.state='running' OR (p.id<j.id AND p.state='pending' AND p.next_at<=?)))
                ORDER BY j.id LIMIT 1""", (job_kind, now, now)).fetchone()
            if not row:
                return None
            c.execute("UPDATE tm_jobs SET state='running',phase='',updated=? WHERE id=?", (now, row["id"]))
            return dict(row)
        return await self.tx(op)

    async def payload(self, job: dict, payload: dict):
        await self.tx(lambda c: c.execute("UPDATE tm_jobs SET payload=?,updated=? WHERE id=?",
                                          (jdump(payload), time.time(), job["id"])).rowcount)
        job["payload"] = jdump(payload)

    async def phase(self, job: dict, phase: str):
        await self.tx(lambda c: c.execute("UPDATE tm_jobs SET phase=?,updated=? WHERE id=?",
                                          (phase, time.time(), job["id"])).rowcount)
        job["phase"] = phase

    def finish_in_tx(self, c, job: dict, result: Any = None):
        c.execute("UPDATE tm_jobs SET state='done',phase='',error='',result=?,updated=? WHERE id=?",
                  (jdump(result or {}), time.time(), job["id"]))

    async def expand(self, job: dict, specs: list[tuple[str, str, dict]]):
        def op(c):
            if job["kind"] in {"max_in", "to_tg"}:
                row = c.execute("SELECT state FROM tm_jobs WHERE id=?", (job["id"],)).fetchone()
                if (row and row[0] == "cancelled") or self.muted_in_tx(c, job["route"]):
                    self.cancel_muted_in_tx(c, job["id"])
                    return
            for i, (job_kind, route, payload) in enumerate(specs):
                self.insert(c, f"{job['key']}/{i}", job_kind, route, payload)
            self.finish_in_tx(c, job)
        await self.tx(op)

    async def finish(self, job: dict, result: Any = None):
        await self.tx(lambda c: self.finish_in_tx(c, job, result))

    async def fail(self, job: dict, exc: Exception):
        if isinstance(exc, Uncertain):
            state, next_at, attempts = "uncertain", 0, job["attempts"]
        elif isinstance(exc, Retry):
            attempts = job["attempts"] + int(exc.count)
            state = "dead" if exc.count and attempts >= self.cfg.max_attempts else "pending"
            delay = max(exc.delay, min(300, 5 * 2**min(attempts, 6))) if exc.count else exc.delay
            next_at = time.time() + delay + random.uniform(0, 1)
        else:
            state, next_at, attempts = "dead", 0, job["attempts"]
        since = float(job.get("retry_since") or time.time()) if isinstance(exc, Retry) else float(job.get("retry_since") or 0)
        if isinstance(exc, Retry):
            next_at = min(next_at, since + self.cfg.retry_window)
            if time.time() >= since + self.cfg.retry_window:
                state = "dead"
        reason = str(exc)[:1000]
        # Error messages may contain URLs or credentials; never save those as diagnostics.
        for secret in (self.cfg.token, self.cfg.phone, self.cfg.ntfy, self.cfg.proxy):
            if secret:
                reason = reason.replace(secret, "[redacted]")
        reason = re.sub(r"https?://\S+", "[URL redacted]", reason)
        def record(c):
            row = c.execute("SELECT state FROM tm_jobs WHERE id=?", (job["id"],)).fetchone()
            if row and row[0] in {"cancelled", "done"}:
                return False
            c.execute("UPDATE tm_jobs SET state=?,phase='',attempts=?,next_at=?,error=?,updated=?,retry_since=? WHERE id=?",
                      (state, attempts, next_at, reason, time.time(), since, job["id"]))
            return True
        if await self.tx(record):
            LOG.warning("Job %s %s: %s", job["id"], state, reason)

    async def prune_history(self) -> dict:
        def op(c):
            now = time.time()
            cursor = c.execute("SELECT id,key,payload FROM tm_jobs WHERE state IN ('pending','running','dead','uncertain')")
            active, protected, paths = [], set(), set()
            for r in cursor:
                active.append({"id": r["id"], "key": r["key"]})
                data = payload_object(r["payload"])
                paths.update(iter_paths(data))
                protected.add(data.get("source_key") or (data.get("origin") or {}).get("parent") or r["key"].split("/", 1)[0])
                parts = r["key"].split("/")
                protected.update("/".join(parts[:i]) for i in range(1, len(parts)))
            c.execute("CREATE TEMP TABLE IF NOT EXISTS tm_gc_keep(key TEXT PRIMARY KEY)")
            c.execute("DELETE FROM tm_gc_keep")
            c.executemany("INSERT OR IGNORE INTO tm_gc_keep VALUES(?)", ((key,) for key in protected if key))
            eligible = "state IN ('done','cancelled') AND NOT EXISTS (SELECT 1 FROM tm_gc_keep k WHERE k.key=tm_jobs.key)"
            total = c.execute("SELECT COUNT(*) FROM tm_jobs WHERE " + eligible).fetchone()[0]
            excess = max(0, total - self.cfg.history_max_jobs)
            rows = c.execute("SELECT id,key,updated FROM tm_jobs WHERE " + eligible + " ORDER BY updated,id LIMIT 2000").fetchall()
            deleted = 0
            for row in rows:
                if row["key"] in protected:
                    continue
                if row["updated"] >= now - self.cfg.history_days * 86400 and deleted >= excess:
                    continue
                expires = row["updated"] + self.cfg.dedup_days * 86400
                if expires > now:
                    c.execute("INSERT INTO tm_seen VALUES(?,?) ON CONFLICT(key) DO UPDATE SET expires=MAX(expires,excluded.expires)",
                              (row["key"], expires))
                c.execute("DELETE FROM tm_jobs WHERE id=?", (row["id"],))
                c.execute("DELETE FROM tm_meta WHERE key=?", ("diagnostic:" + str(row["id"]),))
                deleted += 1
            c.execute("DELETE FROM tm_seen WHERE expires<=?", (now,))
            count = c.execute("SELECT COUNT(*) FROM tm_seen").fetchone()[0]
            if count > self.cfg.dedup_max_keys:
                c.execute("DELETE FROM tm_seen WHERE key IN (SELECT key FROM tm_seen ORDER BY expires,key LIMIT ?)",
                          (count - self.cfg.dedup_max_keys,))
            return {"deleted": deleted, "active": active, "paths": list(paths)}
        result = await self.tx(op)
        def reclaim():
            # Page reuse works even without auto_vacuum. Offline --compact-db enables it.
            self.conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
            if self.conn.execute("PRAGMA auto_vacuum").fetchone()[0] == 2:
                for _ in range(32):
                    if not self.conn.execute("PRAGMA freelist_count").fetchone()[0]:
                        break
                    self.conn.execute("PRAGMA incremental_vacuum(32)").fetchall()
        await self.db_call(reclaim)
        return result

    async def close(self):
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(self.pool, self.conn.close)
        self.pool.shutdown(wait=True)


@dataclasses.dataclass
class ApiResult:
    ok: bool = False
    result: Any = None
    code: int = 0
    error: str = ""
    retry_after: float = 0
    ambiguous: bool = False


def decode_api(code: int, body: bytes) -> ApiResult:
    if code:
        # DNS, connection, proxy and TLS setup errors occur before an HTTP request.
        definitely_unsent = code in {5, 6, 7, 35, 60, 67, 97}
        return ApiResult(error=f"curl exit {code}", ambiguous=not definitely_unsent)
    try:
        data = json.loads(body)
        if not isinstance(data, dict):
            raise ValueError()
        if data.get("ok") is True:
            return ApiResult(ok=True, result=data.get("result"))
        n = int(data.get("error_code") or 0)
        retry = float((data.get("parameters") or {}).get("retry_after") or 0)
        if not math.isfinite(retry):
            raise ValueError("Non-finite retry interval")
        retry = max(0, retry)
        return ApiResult(code=n, error=str(data.get("description") or "Invalid Telegram response"),
                         retry_after=retry, ambiguous=n == 0 or n >= 500)
    except (ValueError, TypeError, AttributeError, OverflowError):
        return ApiResult(error="Invalid Telegram JSON response", ambiguous=True)


def require_api(response: ApiResult, *, sending=False):
    if response.ok:
        return response.result
    if response.code == 429:
        raise Retry("Telegram rate limit", max(1, response.retry_after), count=False)
    if response.code in {401, 403, 409}:
        raise Retry(f"Telegram access/configuration error {response.code}", 120, count=False)
    if sending and response.ambiguous:
        raise Uncertain(response.error or "Telegram delivery result unknown")
    if response.code == 400 or 400 <= response.code < 500:
        raise Permanent(response.error)
    raise Retry(response.error or "Telegram unavailable")


def curl_quote(value: Any) -> str:
    return '"' + str(value).replace("\\", "\\\\").replace('"', '\\"').replace("\r", "\\r").replace("\n", "\\n") + '"'


def clean_env() -> dict[str, str]:
    return {k: v for k, v in os.environ.items() if not k.lower().endswith("_proxy")}


class Curl:
    """Arguments, including the bot token, go through stdin, never through argv."""
    async def run(self, options: list[tuple[str, Any]], timeout: float,
                  destination: Path | None = None, limit: int = 16 * 1024**2):
        text = "\n".join(f"{key} = {curl_quote(value)}" for key, value in options) + "\n"
        process = await asyncio.create_subprocess_exec(
            "curl", "-q", "--config", "-", stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, env=clean_env())
        file = None
        try:
            if destination:
                file = open(destination, "xb")

            async def read_stdout():
                total, chunks = 0, []
                while True:
                    chunk = await process.stdout.read(65536)
                    if not chunk:
                        break
                    total += len(chunk)
                    if total > limit:
                        raise Permanent("Download/API response exceeds configured size limit")
                    if file:
                        write = asyncio.create_task(asyncio.to_thread(file.write, chunk))
                        try:
                            await asyncio.shield(write)
                        except asyncio.CancelledError:
                            await write
                            raise
                    else:
                        chunks.append(chunk)
                if file:
                    def flush():
                        file.flush()
                        os.fsync(file.fileno())
                    writing = asyncio.create_task(asyncio.to_thread(flush))
                    try:
                        await asyncio.shield(writing)
                    except asyncio.CancelledError:
                        await writing
                        raise
                return b"".join(chunks), total

            async def drain_stderr():
                # Never log stderr: curl can include authenticated URLs in errors.
                while await process.stderr.read(65536):
                    pass

            async def communicate():
                process.stdin.write(text.encode("utf-8"))
                with contextlib.suppress(BrokenPipeError, ConnectionResetError):
                    await process.stdin.drain()
                process.stdin.close()
                streams = [asyncio.create_task(read_stdout()), asyncio.create_task(drain_stderr()),
                           asyncio.create_task(process.wait())]
                try:
                    outputs = await asyncio.gather(*streams)
                    return process.returncode, outputs[0][0], outputs[0][1]
                finally:
                    for stream in streams:
                        if not stream.done():
                            stream.cancel()
                    await asyncio.gather(*streams, return_exceptions=True)

            task = asyncio.create_task(communicate())
            try:
                return await asyncio.wait_for(task, timeout + 10)
            finally:
                if process.returncode is None:
                    with contextlib.suppress(ProcessLookupError):
                        process.kill()
                await process.wait()
                if not task.done():
                    task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        finally:
            if process.returncode is None:
                with contextlib.suppress(ProcessLookupError):
                    process.kill()
                await process.wait()
            if file:
                file.close()

    @staticmethod
    def base(url: str, timeout: int, proxy: str | None) -> list[tuple[str, Any]]:
        return [("url", url), ("globoff", ""), ("silent", ""), ("show-error", ""),
                ("max-time", timeout), ("connect-timeout", 15),
                ("proto", "=https"), ("proxy", proxy or ""),
                ("noproxy", "" if proxy else "*")]


class Telegram:
    def __init__(self, cfg: Config, store: Store, curl: Curl):
        self.cfg, self.store, self.curl = cfg, store, curl
        self.pause_until = 0.0
        self.send_lock = asyncio.Lock()
        self.last_send = 0.0
        self.username = ""

    async def call(self, method: str, params: dict | None = None,
                   files: dict[str, Path] | None = None, timeout: int = 60) -> ApiResult:
        if time.time() < self.pause_until:
            return ApiResult(code=429, error="Telegram cooldown", retry_after=self.pause_until - time.time())
        opts = self.curl.base(f"https://api.telegram.org/bot{self.cfg.token}/{method}", timeout, self.cfg.proxy)
        if files:
            for k, value in (params or {}).items():
                if value is not None:
                    opts.append(("form-string", f"{k}={value}"))
            for field, path in files.items():
                # Curl's form syntax also has its own quoting layer.
                p = str(path).replace("\\", "\\\\").replace('"', '\\"')
                opts.append(("form", f'{field}=@"{p}"'))
        else:
            opts.extend([("header", "Content-Type: application/json"), ("data-binary", jdump(params or {}))])
        if method not in {"getMe", "getUpdates", "getFile"}:
            await self.admit()
        if time.time() < self.pause_until:
            return ApiResult(code=429, error="Telegram cooldown", retry_after=self.pause_until - time.time())
        try:
            rc, body, _ = await self.curl.run(opts, timeout)
            result = decode_api(rc, body)
        except (OSError, asyncio.TimeoutError, Permanent):
            result = ApiResult(error="Telegram transport/response exception", ambiguous=True)
        if result.code == 429:
            self.pause_until = time.time() + max(1, result.retry_after)
            await self.store.set_meta("tg_pause_until", self.pause_until)
        return result

    async def paced(self, callback: Callable):
        # Kept as a hook for callers/tests. Admission is inside call(), AFTER callers
        # persist send intent; a stalled SQLite write must not accumulate rate slots.
        return await callback()

    async def admit(self):
        async with self.send_lock:
            await asyncio.sleep(max(0, 3.1 - (time.monotonic() - self.last_send)))
            self.last_send = time.monotonic()



BLOCKED_IPV6 = tuple(ipaddress.IPv6Network(n) for n in (
    "64:ff9b::/96", "64:ff9b:1::/48", "::ffff:0:0/96", "::ffff:0:0:0/96",
    "2002::/16", "2001::/32"))


def public_address(ip: str, extra_prefixes: tuple[str, ...] = ()) -> bool:
    try:
        address = ipaddress.ip_address(ip)
    except ValueError:
        return False
    if (not address.is_global or address.is_multicast or address.is_reserved
            or address.is_loopback or address.is_link_local or address.is_unspecified):
        return False
    if address.version == 6:
        if getattr(address, "scope_id", None) or address.ipv4_mapped is not None:
            return False
        if any(address in net for net in BLOCKED_IPV6):
            return False
        if any(address in ipaddress.IPv6Network(n) for n in extra_prefixes):
            return False
    return True


async def public_resolve(url: str, *, ipv4_only=True,
                         extra_prefixes: tuple[str, ...] = ()) -> tuple[str, str]:
    """Pin a validated address for EACH redirect hop. Direct media uses IPv4 by default.

    Network-specific NAT64 prefixes cannot be inferred from is_global. IPv6 opt-in
    requires listing such local translation ranges in BLOCKED_IPV6_PREFIXES as well.
    """
    if any(ord(c) < 33 for c in url):
        raise Permanent("Invalid media URL")
    try:
        u = urlsplit(url)
        port = u.port
    except ValueError as exc:
        raise Permanent("Invalid media URL") from exc
    if u.scheme != "https" or not u.hostname or u.username or u.password or port not in {None, 443}:
        raise Permanent("Only public HTTPS media URLs on port 443 are allowed")
    host = u.hostname.encode("idna").decode("ascii")
    try:
        records = await asyncio.wait_for(asyncio.to_thread(
            socket.getaddrinfo, host, 443, socket.AF_INET if ipv4_only else 0,
            socket.SOCK_STREAM), 10)
    except (OSError, asyncio.TimeoutError) as exc:
        raise Retry("Media hostname resolution failed") from exc
    ips = {r[4][0] for r in records}
    if not ips or any(not public_address(ip, extra_prefixes) for ip in ips):
        raise Permanent("Private/reserved/IPv4-translating media destination blocked")
    if ipv4_only:
        ips = {ip for ip in ips if ipaddress.ip_address(ip).version == 4}
    if not ips:
        raise Permanent("Direct IPv6 media disabled; no permitted IPv4 address")
    address = sorted(ips, key=lambda ip: (":" in ip, ip))[0]
    return host, f"[{address}]" if ":" in address else address


def parse_headers(raw: str) -> tuple[int, dict[str, str]]:
    blocks = re.split(r"\r?\n\r?\n", raw)
    code, headers = 0, {}
    for block in blocks:
        lines = block.splitlines()
        if not lines or not lines[0].startswith("HTTP/"):
            continue
        fields = lines[0].split()
        if len(fields) < 2 or not fields[1].isdigit():
            continue
        code, headers = int(fields[1]), {}
        for line in lines[1:]:
            if ":" in line:
                key, value = line.split(":", 1)
                headers[key.lower().strip()] = value.strip()
    return code, headers


class Downloads:
    def __init__(self, cfg: Config, curl: Curl):
        self.cfg, self.curl = cfg, curl
        self.lock = asyncio.Semaphore(cfg.download_workers)
        self.budget_lock = asyncio.Lock()
        self.reserved = 0
        self.inflight: set[Path] = set()

    @contextlib.asynccontextmanager
    async def reservation(self, path: Path, limit: int):
        async with self.budget_lock:
            if path in self.inflight:
                raise Retry("Attachment is being downloaded by another task", 5, count=False)
            await asyncio.to_thread(self.check_budget, limit)
            self.reserved += limit
            self.inflight.add(path)
        try:
            yield
        finally:
            async with self.budget_lock:
                self.reserved -= limit
                self.inflight.discard(path)

    def checked_path(self, raw: str | Path) -> Path:
        path = Path(raw)
        if not path.is_absolute():
            path = self.cfg.root / path
        if path.is_symlink():
            raise Permanent("Symlink media paths are not allowed")
        path = path.resolve()
        if path.parent != self.cfg.media.resolve():
            raise Permanent("Media path is outside media_queue")
        return path

    def check_budget(self, limit: int):
        usage = sum(p.stat().st_size for p in self.cfg.media.iterdir() if p.is_file() and not p.is_symlink())
        if usage + self.reserved + limit > self.cfg.disk_limit:
            raise Retry("Media disk budget exceeded; drain/clear the queue", 120, count=False)
        if shutil.disk_usage(self.cfg.media).free < self.cfg.min_free + self.reserved + limit:
            raise Retry("Insufficient free disk space", 120, count=False)

    async def fetch(self, url: str, path: Path, *, telegram=False,
                    expected: int | None = None, media_type="document") -> Path:
        path = self.checked_path(path)
        limit = self.cfg.max_tg_download_bytes if telegram else self.cfg.max_file_bytes
        if expected is not None and expected > limit:
            raise Permanent("File exceeds the configured/API download limit")
        async with self.lock:
            if path.exists() and path.stat().st_size > 0:
                size = path.stat().st_size
                if size > limit or (expected is not None and size != expected):
                    raise Permanent("Cached file violates size/length checks; retained for inspection")
                return path
            async with self.reservation(path, limit):
                for _ in range(6):
                    part = path.with_name(path.name + "." + uuid.uuid4().hex + ".part")
                    head = path.with_name(path.name + "." + uuid.uuid4().hex + ".headers")
                    try:
                        opts = self.curl.base(url, 300, self.cfg.proxy if telegram else None)
                        if telegram:
                            if urlsplit(url).hostname != "api.telegram.org":
                                raise Permanent("Unexpected Telegram file host")
                        else:
                            host, address = await public_resolve(url, ipv4_only=self.cfg.media_ipv4_only,
                                                                 extra_prefixes=self.cfg.blocked_ipv6_prefixes)
                            opts.append(("resolve", f"{host}:443:{address}"))
                        opts.extend([("dump-header", str(head)), ("user-agent", f"Telemax/{VERSION}")])
                        try:
                            rc, _, size = await self.curl.run(opts, 300, part, limit)
                        except (OSError, asyncio.TimeoutError) as exc:
                            raise Retry("Media transfer interrupted; partial file discarded") from exc
                        if rc:
                            raise Retry(f"Download failed (curl {rc}); partial file discarded")
                        status, headers = parse_headers(head.read_text(encoding="iso-8859-1"))
                        if 300 <= status < 400 and headers.get("location"):
                            if telegram:
                                raise Permanent("Unexpected Telegram file redirect")
                            url = urljoin(url, headers["location"])
                            continue
                        if status == 429 or status >= 500:
                            raise Retry(f"Media server HTTP {status}")
                        if status != 200 or not size:
                            raise Retry(f"Media download rejected/empty (HTTP {status})")
                        length = number(headers.get("content-length"))
                        if (length is not None and length != size) or (expected is not None and expected != size):
                            raise Retry("Downloaded file length mismatch")
                        content_type = headers.get("content-type", "").lower()
                        if media_type != "document" and any(x in content_type for x in ("text/html", "application/json")):
                            raise Retry("Server returned an error page instead of media")
                        await asyncio.to_thread(os.replace, part, path)
                        await asyncio.to_thread(fsync_dir, path.parent)
                        return path
                    finally:
                        part.unlink(missing_ok=True)
                        head.unlink(missing_ok=True)
                raise Permanent("Too many media redirects")


def fsync_dir(path: Path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def normalize_attachment(attach: Any, chat_id, message_id) -> dict:
    keys = ("type", "id", "file_id", "photo_id", "video_id", "audio_id", "sticker_id",
            "name", "file_name", "size", "file_size", "url", "base_url", "file_url",
            "download_url", "token", "duration", "title", "text", "first_name", "phone", "event")
    data = {k: get(attach, k) for k in keys if isinstance(get(attach, k), (str, int, float, bool))}
    data["type"] = kind(get(attach, "type") or get(attach, "_type"))
    data["source"], data["chat_id"], data["message_id"] = "max", chat_id, message_id
    data["raw"] = message_json(attach)
    return data


def normalize_message(message: Any, *, inherited_chat=None, depth=0) -> dict:
    chat = get(message, "chat_id")
    if chat is None:
        chat = inherited_chat
    mid = get(message, "id")
    data = {"id": mid, "chat_id": chat, "sender": get(message, "sender"),
            "text": str(get(message, "text") or get(message, "caption") or ""),
            "type": kind(get(message, "type")), "time": get(message, "time"),
            "title": get(message, "chat_title") or get(message, "title"),
            "out": any(bool(get(message, p, False)) for p in ("out", "outgoing", "is_out")),
            "action": str(get(message, "action") or get(message, "event_type") or ""), "files": []}
    attachments = get(message, "attaches")
    if attachments is None:
        attachments = get(message, "attachments") or []
    if not isinstance(attachments, (list, tuple)):
        attachments = [attachments]
    data["files"] = [normalize_attachment(a, chat, mid) for a in attachments]
    link = get(message, "link")
    if depth < 3 and kind(get(link, "type")) == "FORWARD" and get(link, "message"):
        # Forwarded FILE/VIDEO must use the source chat, NOT the enclosing chat.
        data["forward"] = normalize_message(get(link, "message"), inherited_chat=get(link, "chat_id"), depth=depth + 1)
    return data


def own_message(message: dict, own_id: int | None) -> bool:
    return bool(message.get("out") or (own_id and number(message.get("sender")) == own_id))


def candidate_urls(obj: Any) -> list[str]:
    """Explicit download fields only: thumbnails are never substituted for videos."""
    result = []
    for field in ("url", "download_url", "file_url", "base_url", "mp4_1080", "mp4_720", "mp4_480", "mp4_360", "mp4_240", "mp4_144"):
        value = get(obj, field)
        if isinstance(value, str) and value.startswith("https://"):
            result.append(value)
    for key in ("urls", "video_urls"):
        values = get(obj, key)
        if isinstance(values, dict):
            result.extend(v for _, v in sorted(values.items(), reverse=True) if isinstance(v, str) and v.startswith("https://"))
    return result


class Inbox:
    """Write-ahead MAX ingress independent of SQLite and QUEUE_LIMIT.

    Receipt is durable only after atomic rename + file/directory fsync succeeds.
    Never evicts unimported events. A full/unwritable inbox applies callback
    backpressure and raises a visible health alarm; finite storage is not magic.
    """
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.lock = asyncio.Lock()
        self.last_error = ""
        self.last_log = 0.0
        self.waiting = 0
        self.usage = 0
        self._prepared = False
        self._prepare_lock = threading.Lock()

    def _prepare(self):
        with self._prepare_lock:
            self._prepare_locked()

    def _prepare_locked(self):
        if self._prepared:
            return
        if self.cfg.inbox.is_symlink():
            raise OSError("Inbox directory must not be a symlink")
        self.cfg.inbox.mkdir(mode=0o700, exist_ok=True)
        os.chmod(self.cfg.inbox, 0o700)
        # A power loss before rename can leave a complete fsynced envelope as .part.
        # Recover complete envelopes; quarantine incomplete ones instead of discarding them.
        for part in self.cfg.inbox.glob("*.part"):
            if not part.is_file() or part.is_symlink():
                continue
            try:
                item = self.read(part)
                final = self.cfg.inbox / (hashlib.sha256(item["key"].encode()).hexdigest() + ".json")
                if final.exists():
                    if self.read(final)["key"] != item["key"]:
                        raise ValueError("Inbox key collision")
                    part.unlink()
                else:
                    os.replace(part, final)
                fsync_dir(self.cfg.inbox)
            except (ValueError, TypeError):
                self.quarantine(part)
        self.usage = sum(p.stat().st_size for p in self.cfg.inbox.rglob("*")
                         if p.is_file() and not p.is_symlink())
        self._prepared = True

    def _put(self, key: str, route: str, payload: dict):
        self._prepare()
        path = self.cfg.inbox / (hashlib.sha256(key.encode()).hexdigest() + ".json")
        if path.is_symlink():
            raise OSError("Inbox file must not be a symlink")
        if path.exists():
            old = json.loads(path.read_text())
            if old.get("key") != key:
                raise OSError("Invalid existing inbox envelope")
            with open(path, "rb") as existing:
                os.fsync(existing.fileno())
            fsync_dir(self.cfg.inbox)
            return path
        envelope = {"version": 1, "key": key, "route": route, "payload": payload,
                    "tg_chat_id": self.cfg.chat_id, "tg_bot_id": self.cfg.token.split(":", 1)[0]}
        blob = (jdump(envelope) + "\n").encode("utf-8")
        if self.usage + len(blob) > self.cfg.inbox_limit:
            raise CapacityError("Inbox disk budget exhausted; event is NOT yet durable")
        if shutil.disk_usage(self.cfg.inbox).free < self.cfg.min_free + len(blob):
            raise OSError("Insufficient disk space for durable inbox")
        temp = path.with_name(path.name + "." + uuid.uuid4().hex + ".part")
        fd = os.open(temp, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
        try:
            with os.fdopen(fd, "wb") as out:
                out.write(blob)
                out.flush()
                os.fsync(out.fileno())
            os.replace(temp, path)
            self.usage += len(blob)
            fsync_dir(self.cfg.inbox)
        finally:
            temp.unlink(missing_ok=True)
        return path

    async def put(self, key: str, route: str, payload: dict):
        async with self.lock:
            task = asyncio.create_task(asyncio.to_thread(self._put, key, route, payload))
            try:
                return await asyncio.shield(task)
            except asyncio.CancelledError:
                with contextlib.suppress(Exception):
                    await task
                raise

    def candidates(self):
        self._prepare()
        # Bounded batch, preserves original admission order except exact timestamp ties.
        return heapq.nsmallest(100, (p for p in self.cfg.inbox.glob("*.json")
                                    if p.is_file() and not p.is_symlink()),
                               key=lambda p: (p.stat().st_mtime_ns, p.name))

    def read(self, path: Path) -> dict:
        obj = json.loads(path.read_text(encoding="utf-8"))
        if (not isinstance(obj, dict) or obj.get("version") != 1
                or not isinstance(obj.get("key"), str) or not isinstance(obj.get("payload"), dict)
                or obj.get("tg_chat_id") != self.cfg.chat_id
                or obj.get("tg_bot_id") != self.cfg.token.split(":", 1)[0]):
            raise ValueError("Invalid/mismatched inbox envelope; retained for manual inspection")
        return obj

    async def remove(self, path: Path):
        async with self.lock:
            def remove():
                size = path.stat().st_size if path.exists() else 0
                path.unlink(missing_ok=True)
                self.usage = max(0, self.usage - size)
                fsync_dir(self.cfg.inbox)
            task = asyncio.create_task(asyncio.to_thread(remove))
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                with contextlib.suppress(Exception):
                    await task
                raise

    def quarantine(self, path: Path):
        directory = self.cfg.inbox / "quarantine"
        if directory.is_symlink():
            raise OSError("Inbox quarantine directory must not be a symlink")
        directory.mkdir(mode=0o700, exist_ok=True)
        os.replace(path, directory / (path.name + "." + uuid.uuid4().hex))
        fsync_dir(directory)
        fsync_dir(self.cfg.inbox)
        LOG.error("Inbox event quarantined, NOT discarded: %s", path.name)


class Bridge:
    def __init__(self, cfg: Config, store: Store, client: Any, media_classes: dict[str, Any]):
        self.cfg, self.store, self.client = cfg, store, client
        self.media_classes = media_classes
        self.curl = Curl()
        self.tg = Telegram(cfg, store, self.curl)
        self.downloads = Downloads(cfg, self.curl)
        self.inbox = Inbox(cfg)
        self.max_ready, self.max_available = asyncio.Event(), asyncio.Event()
        self.stop = asyncio.Event()
        self.fatal: BaseException | None = None
        self.own_id = cfg.my_max_id
        self.topic_lock = asyncio.Lock()
        self.diagnostic_lock = asyncio.Lock()
        self.started = time.time()

    def abort(self, exc: BaseException):
        self.fatal = exc
        self.stop.set()

    async def on_start(self, client):
        try:
            actual = number(get(get(get(client, "me"), "contact"), "id"))
            actual = actual or number(get(get(client, "me"), "id"))
            if not actual:
                raise RuntimeError("MAX self ID unavailable; refusing unsafe echo filtering")
            if self.cfg.my_max_id and actual != self.cfg.my_max_id:
                raise RuntimeError("MY_MAX_ID does not match the authenticated MAX account")
            previous = await self.store.meta("max_account_id")
            if previous and previous != str(actual):
                raise RuntimeError("This queue belongs to another MAX account")
            await self.store.set_meta("max_account_id", actual)
            self.own_id = actual
            self.max_ready.set()
            self.max_available.set()
            await self.store.set_meta("max_probe_ok", time.time())
            LOG.info("MAX session ready")
        except Exception as exc:
            self.abort(exc)

    async def on_message(self, message, client=None):
        if self.stop.is_set():
            return
        # No SQLite await in the receive callback. Database faults cannot lose this handoff.
        raw = message_json(message)
        try:
            data = normalize_message(message)
        except Exception as exc:
            data = {"id": get(message, "id"), "chat_id": get(message, "chat_id"),
                    "sender": get(message, "sender"), "text": "", "files": [],
                    "normalization_error": type(exc).__name__}
        if own_message(data, self.own_id):
            return
        data["raw_message"], data["received_at"] = raw, time.time()
        data["max_account_id"] = self.own_id
        mid, chat = data.get("id"), data.get("chat_id")
        key = f"max:{chat}:{mid}" if mid is not None and chat is not None else "max:unidentified:" + uuid.uuid4().hex
        self.inbox.waiting += 1
        try:
            while True:
                try:
                    await self.inbox.put(key, str(chat), data)
                    self.inbox.last_error = ""
                    return
                except (OSError, CapacityError, ValueError) as exc:
                    self.inbox.last_error = type(exc).__name__
                    if time.monotonic() - self.inbox.last_log >= 30:
                        LOG.critical("MAX inbox unavailable (%s); callback is holding an UNPERSISTED event, no restart. "
                                     "Restore disk/permissions; process death can lose held events", type(exc).__name__)
                        self.inbox.last_log = time.monotonic()
                    # A callback cancellation/host failure before successful fsync still has no durability guarantee.
                    await asyncio.sleep(2)
        finally:
            self.inbox.waiting -= 1

    async def import_inbox_once(self) -> int:
        imported = 0
        for path in await asyncio.to_thread(self.inbox.candidates):
            try:
                item = await asyncio.to_thread(self.inbox.read, path)
            except (ValueError, TypeError):
                await asyncio.to_thread(self.inbox.quarantine, path)
                continue
            own = item["payload"].get("max_account_id")
            if own is not None and self.own_id is not None and own != self.own_id:
                await asyncio.to_thread(self.inbox.quarantine, path)
                continue
            try:
                await self.store.add(item["key"], "max_in", item["route"], item["payload"])
            except CapacityError:
                break  # Keep the envelope on disk; workers continue draining SQLite.
            await self.inbox.remove(path)  # Only after commit (or a proven duplicate).
            imported += 1
        return imported

    async def inbox_worker(self):
        while not self.stop.is_set():
            if not self.max_ready.is_set():
                await asyncio.sleep(1)
                continue
            try:
                count = await self.import_inbox_once()
                if count:
                    await self.store.set_meta("max_received", time.time())
            except (OSError, sqlite3.Error) as exc:
                LOG.error("Inbox import delayed (%s); durable envelopes are retained", type(exc).__name__)
            await asyncio.sleep(1)

    async def storage_health(self):
        """Alert independently of the SQLite workers when persistence is degraded."""
        last = 0.0
        while not self.stop.is_set():
            if (self.inbox.last_error or self.store.db_error) and time.monotonic() - last > 300:
                last = time.monotonic()
                await self.push("Telemax storage degraded: check SQLite/inbox, free disk and permissions. "
                                "Unpersisted callbacks: " + str(self.inbox.waiting))
            await asyncio.sleep(10)

    async def name_for(self, sender) -> str:
        sid = number(sender)
        if sid is None:
            return "Система"
        if "tm_aliases" in self.store.extra_tables:
            aliases = await self.store.read("SELECT alias FROM tm_aliases WHERE max_id=?", (str(sid),))
            if aliases:
                return aliases[0]["alias"]
        try:
            user = await asyncio.wait_for(self.client.get_user(sid), 5)
            name = " ".join(str(get(user, x) or "") for x in ("first_name", "last_name")).strip()
            names = get(user, "names") or []
            candidate = name or (get(names[0], "name") if names else None) or get(user, "name")
            return str(candidate or f"ID:{sid}")
        except Exception:
            return f"ID:{sid}"

    async def prepare_max(self, job: dict, data: dict):
        if not await self.store.forward_allowed(job):
            return
        if number(data.get("chat_id")) is None or data.get("id") is None:
            raise Permanent("MAX event has no stable chat/message ID; raw normalized payload retained")
        if own_message(data, self.own_id):
            await self.store.finish(job)
            return
        target = str(data["chat_id"])
        rows = await self.store.read("SELECT * FROM tm_routes WHERE max_id=?", (target,))
        title, chat_type = data.get("title"), "group"
        if rows:
            title, chat_type = rows[0]["name"], rows[0]["type"]
        else:
            try:
                chat = await asyncio.wait_for(self.client.get_chat(int(target)), 5)
                title = title or get(chat, "title") or get(chat, "name")
                chat_type = "private" if kind(get(chat, "type")) in {"DIALOG", "PRIVATE", "USER", "BOT"} else "group"
            except Exception:
                pass
        name = await self.name_for(data.get("sender"))
        title = str(title or (name if chat_type == "private" else f"MAX {target}"))[:128]
        await self.store.tx(lambda c: c.execute("INSERT OR IGNORE INTO tm_routes(max_id,name,type) VALUES(?,?,?)",
                                                (target, title, chat_type)).rowcount)
        header = f"[{name}]:" if chat_type == "private" else f"[{title}], [{name}]:"
        text = data.get("text") or ""
        if data.get("action"):
            text = f"[Событие: {data['action']}]\n{text}".strip()
        files = list(data.get("files") or [])
        forward = data.get("forward")
        while forward:
            fname = await self.name_for(forward.get("sender"))
            text += f"\n\n[Переслано от {fname}]\n{forward.get('text') or ''}"
            files.extend(forward.get("files") or [])
            forward = forward.get("forward")
        if not text and not files:
            text = f"[Событие MAX: {data.get('type') or 'UNKNOWN'}]"
        normalized = []
        for original in files:
            if not isinstance(original, dict):
                text += "\n[Нераспознанное вложение; исходный объект сохранён]"
                continue
            f = dict(original)
            typ = kind(f.get("type") or f.get("unsupported"))
            if typ not in MEDIA_TYPES:
                text += "\n" + attachment_text(f)
                continue
            default = {"PHOTO": "photo.jpg", "VIDEO": "video.mp4", "AUDIO": "voice.ogg", "VOICE": "voice.ogg", "STICKER": "sticker.webp"}.get(typ, "attachment.bin")
            f["name"] = safe_name(f.get("name") or f.get("file_name"), default)
            f["kind"] = {"PHOTO": "photo", "VIDEO": "video", "AUDIO": "voice", "VOICE": "voice", "STICKER": "photo"}.get(typ, "document")
            f.pop("unsupported", None)
            normalized.append(f)
        if any(isinstance(f, dict) and kind(f.get("type")) not in MEDIA_TYPES | {"SHARE", "CONTROL", "CONTACT", "POLL", "CALL"} for f in files):
            async with self.diagnostic_lock:
                try:
                    detail, _ = await self.error_details(job)
                    detail["fallback"] = "Unknown attachment rendered as text; source message NOT discarded"
                    await asyncio.to_thread(self.write_error_dump, job["id"], detail)
                except (OSError, ValueError, TypeError):
                    LOG.error("Fallback JSON could not be written for job %s; source remains in DB", job["id"])
        parts = independent_tg_parts(f"{header}\n{text}".strip(), normalized)
        for part in parts:
            part["source_key"] = job["key"]
            part["context"] = {"time": data.get("time"), "time_unit": "ms",
                               "sender": data.get("sender"), "sender_name": name,
                               "message_id": data.get("id"), "recipient": title}
        await self.store.expand(job, [("to_tg", target, p) for p in parts])

    def notice_specs(self, text: str, thread=None) -> list[tuple[str, str, dict]]:
        thread = None if thread == 1 else thread
        return [("notice", f"notice:{thread}", {"text": part, "thread": thread}) for part in split_text(text)]

    async def prepare_tg(self, job: dict, data: dict):
        msg = data["message"]
        if not self.cfg.authorized(msg):
            raise Permanent("Sender is no longer authorized")
        target = number(data.get("target"))
        if target is None:
            await self.store.expand(job, self.notice_specs("Нет привязки к MAX. Администратор может выполнить /chat MAX_USER_ID для нового личного диалога или /bind MAX_CHAT_ID для существующего чата.", msg.get("message_thread_id")))
            return
        text = str(msg.get("text") or msg.get("caption") or "")
        file = None
        if msg.get("photo"):
            obj, typ, default = msg["photo"][-1], "photo", "photo.jpg"
        else:
            typ = next((k for k in ("document", "video", "voice", "audio", "animation", "sticker", "video_note") if msg.get(k)), None)
            obj = msg[typ] if typ else None
            default = {"video": "video.mp4", "voice": "voice.ogg", "audio": "audio.mp3", "animation": "animation.mp4", "sticker": "sticker.webp", "video_note": "video_note.mp4"}.get(typ, "attachment.bin")
        if obj:
            # Animated/vector stickers remain ordinary files; no fake format conversion.
            if typ == "sticker":
                default = "sticker.tgs" if obj.get("is_animated") else "sticker.webm" if obj.get("is_video") else "sticker.webp"
            file = {"source": "telegram", "file_id": obj["file_id"],
                    "name": safe_name(obj.get("file_name"), default),
                    "kind": {"photo": "photo", "video": "video", "voice": "voice", "video_note": "video_note"}.get(typ, "document"),
                    "size": obj.get("file_size"), "duration": obj.get("duration")}
        if any(msg.get(field) for field in ("contact", "location", "poll", "venue", "dice")):
            raise Permanent("Unsupported Telegram content; original update retained")
        if not text and not file:
            raise Permanent("Unsupported Telegram content; original update retained")
        origin = {"parent": job["key"], "message_id": msg["message_id"],
                  "thread": msg.get("message_thread_id"), "sender": number(msg["from"]["id"])}
        pieces = split_text(text) or [""]
        specs = []
        for index, piece in enumerate(pieces):
            specs.append(("to_max", str(target), {"text": piece, "files": [file] if file and index == 0 else [], "origin": origin,
                "source_key": job["key"], "context": {"time": msg.get("date"), "time_unit": "s",
                    "message_id": msg.get("message_id"), "sender": msg["from"].get("id"),
                    "sender_name": user_name(msg["from"], str(msg["from"].get("id")))}}))
        await self.store.expand(job, specs)

    async def ensure_topic(self, target: str) -> int:
        async with self.topic_lock:
            rows = await self.store.read("SELECT * FROM tm_routes WHERE max_id=?", (target,))
            if not rows:
                raise Permanent("No route; use /bind MAX_CHAT_ID in the intended Telegram topic")
            row = rows[0]
            if row["thread_id"]:
                return row["thread_id"]
            if row["state"] in {"creating", "uncertain"}:
                raise Permanent("Topic creation result unknown; check Telegram and use /bind MAX_CHAT_ID")
            if time.time() < self.tg.pause_until:
                raise Retry("Telegram cooldown", self.tg.pause_until - time.time(), count=False)
            await self.store.tx(lambda c: c.execute("UPDATE tm_routes SET state='creating' WHERE max_id=?", (target,)).rowcount)
            try:
                response = await self.tg.paced(lambda: self.tg.call("createForumTopic", {"chat_id": self.cfg.chat_id, "name": row["name"][:128] or f"MAX {target}"}, timeout=30))
                result = require_api(response, sending=True)
                thread = number(get(result, "message_thread_id"))
                if not thread:
                    raise Uncertain("Topic created but thread ID missing")
                await self.store.tx(lambda c: c.execute("UPDATE tm_routes SET thread_id=?,state='ready' WHERE max_id=?", (thread, target)).rowcount)
                return thread
            except Uncertain:
                await self.store.tx(lambda c: c.execute("UPDATE tm_routes SET state='uncertain' WHERE max_id=?", (target,)).rowcount)
                raise Permanent("Topic creation result unknown; inspect Telegram and /bind the existing topic")
            except (Retry, Permanent):
                await self.store.tx(lambda c: c.execute("UPDATE tm_routes SET state='new' WHERE max_id=?", (target,)).rowcount)
                raise

    async def materialize(self, job: dict, data: dict) -> list[Path]:
        paths = []
        for index, f in enumerate(data.get("files") or []):
            if f.get("unsupported"):
                raise Permanent(f"Unsupported MAX attachment type: {f['unsupported']}")
            if f.get("source") not in {"max", "telegram", "legacy"}:
                raise Permanent("Unsupported media source; descriptor retained")
            if "path" not in f:
                f["path"] = str(self.cfg.media / f"tm_{job['id']}_{index}_{safe_name(f.get('name'))}")
                await self.store.payload(job, data)  # Reference exists BEFORE the file does.
            path = self.downloads.checked_path(f["path"])
            if path.exists() and path.stat().st_size > 0:
                size = path.stat().st_size
                limit = (self.cfg.max_tg_download_bytes if f["source"] == "telegram"
                         else self.cfg.max_file_bytes)
                if size > limit:
                    raise Permanent("Local attachment exceeds configured size limit")
                expected = number(f.get("size")) if f["source"] == "telegram" else None
                if expected is not None and size != expected:
                    raise Permanent("Local attachment length mismatch; retained for inspection")
                paths.append(path)
                continue
            if f.get("source") == "legacy":
                raise Permanent("Legacy media file missing/empty; original job retained")
            if f.get("source") == "telegram":
                response = await self.tg.call("getFile", {"file_id": f["file_id"]})
                info = require_api(response)
                remote = get(info, "file_path")
                if not remote or str(remote).startswith("/") or ".." in str(remote).split("/"):
                    raise Permanent("Invalid Telegram file path")
                url = f"https://api.telegram.org/file/bot{self.cfg.token}/" + quote(str(remote), safe="/")
                expected = number(get(info, "file_size")) or number(f.get("size"))
                await self.downloads.fetch(url, path, telegram=True, expected=expected, media_type=f["kind"])
            else:
                if not self.max_available.is_set():
                    raise Retry("MAX connection unavailable", 30, count=False)
                typ = f.get("type")
                urls = []
                if typ in {"FILE", "VIDEO"}:
                    cid, mid = number(f.get("chat_id")), number(f.get("message_id"))
                    fid = number(f.get("file_id") if typ == "FILE" else f.get("video_id"))
                    if cid is not None and mid is not None and fid is not None:
                        try:
                            if typ == "FILE":
                                info = await asyncio.wait_for(self.client.get_file_by_id(chat_id=cid, message_id=mid, file_id=fid), 30)
                            else:
                                info = await asyncio.wait_for(self.client.get_video_by_id(cid, mid, fid), 30)
                            urls = candidate_urls(info)
                        except Exception as exc:
                            raise Retry(f"MAX file metadata request failed ({type(exc).__name__})") from exc
                urls = urls or candidate_urls(f)
                if not urls:
                    raise Permanent("No supported download URL/context in MAX attachment; descriptor retained")
                await self.downloads.fetch(urls[0], path, media_type=f["kind"])
            paths.append(path)
        return paths

    async def send_tg(self, job: dict, data: dict):
        if not await self.store.forward_allowed(job):
            return
        repaired, changed = remove_controls(data)
        if changed or (repaired.get("text") and repaired.get("files")):
            parts = independent_tg_parts(repaired.get("text") or "", repaired.get("files") or [])
            for part in parts:
                part["source_key"] = data.get("source_key") or job["key"].split("/", 1)[0]
                if data.get("context"):
                    part["context"] = data["context"]
            await self.store.expand(job, [("to_tg", job["route"], part) for part in parts])
            return
        thread = await self.ensure_topic(job["route"])
        paths = await self.materialize(job, data)
        params = {"chat_id": self.cfg.chat_id, "message_thread_id": thread}
        text = data.get("text") or ""
        media = data.get("files") or []
        files = {}
        if not paths:
            if not text or utf16len(text) > 4096:
                raise Permanent("Invalid/oversized Telegram text job")
            method = "sendMessage"
            params["text"] = text
        elif len(paths) == 1:
            field = media[0]["kind"]
            if field not in {"photo", "video", "voice", "document"}:
                field = "document"
            # sendPhoto has a smaller file-size limit than sendDocument.
            if field == "photo" and paths[0].stat().st_size > 10 * 1024**2:
                field = "document"
            method, files = "send" + field.capitalize(), {field: paths[0]}
            params["caption"] = text
        else:
            if not 2 <= len(paths) <= 10:
                raise Permanent("Invalid album size")
            if any(f["kind"] not in {"photo", "video"} for f in media):
                raise Permanent("Mixed unsupported album types")
            if any(f["kind"] == "photo" and p.stat().st_size > 10 * 1024**2 for f, p in zip(media, paths)):
                raise Permanent("Album photo exceeds Telegram photo limit; original files retained")
            method = "sendMediaGroup"
            items = []
            for i, (f, p) in enumerate(zip(media, paths)):
                key = f"file{i}"
                files[key] = p
                items.append({"type": f["kind"], "media": f"attach://{key}"})
            items[0]["caption"] = text
            params["media"] = jdump(items)

        async def send():
            if not await self.store.forward_allowed(job, sending=True):
                raise Suppressed()
            return await self.tg.call(method, params, files or None, timeout=300 if paths else 40)
        response = await self.tg.paced(send)
        if response.code == 400 and any(s in response.error.lower() for s in ("thread not found", "message thread not found", "topic not found")):
            await self.store.tx(lambda c: c.execute("UPDATE tm_routes SET thread_id=NULL,state='new' WHERE max_id=? AND thread_id=?", (job["route"], thread)).rowcount)
            raise Retry("Telegram topic was deleted; route retained, topic will be recreated", 5)
        result = require_api(response, sending=True)
        delivered = result if isinstance(result, list) else [result]
        if (not delivered or any(not get(x, "message_id") for x in delivered)
                or (method == "sendMediaGroup" and len(delivered) != len(paths))):
            raise Uncertain("Telegram accepted request without expected message IDs")
        await self.store.finish(job, {"ids": [get(x, "message_id") for x in delivered]})
        await self.store.set_meta("tg_delivered", time.time())

    async def send_max(self, job: dict, data: dict):
        origin = data.get("origin") or {}
        if origin.get("sender") not in self.cfg.allowed | self.cfg.admins:
            raise Permanent("Sender authorization revoked before delivery")
        if not self.max_available.is_set():
            raise Retry("MAX connection unavailable", 30, count=False)
        paths = await self.materialize(job, data)
        attachments = []
        for f, path in zip(data.get("files") or [], paths):
            typ = f.get("kind", "document")
            if typ == "voice" and path.suffix.lower() != ".ogg":
                typ = "document"  # Do not relabel or pretend to transcode MP3/OPUS.
            klass = self.media_classes[typ if typ in self.media_classes else "document"]
            kwargs = {"path": str(path)}
            if typ == "video_note":
                kwargs["duration"] = int(f.get("duration") or 1) * 1000
            attachments.append(klass(**kwargs))
        await self.store.phase(job, "send")
        try:
            response = await asyncio.wait_for(self.client.send_message(
                chat_id=int(job["route"]), text=data.get("text") or "",
                attachments=attachments or None), 300)
        except Exception as exc:
            # SDK can upload and send before raising; never blindly try other methods.
            raise Uncertain(f"MAX send result unknown ({type(exc).__name__}); check before forced retry") from exc
        mid = get(response, "id")
        if mid is None:
            raise Uncertain("MAX returned no message ID")

        def finish(c):
            self.store.finish_in_tx(c, job, {"id": str(mid)})
            parent = origin.get("parent")
            if parent:
                remaining = c.execute("SELECT COUNT(*) FROM tm_jobs WHERE kind='to_max' AND key LIKE ? AND state!='done'", (parent + "/%",)).fetchone()[0]
                if not remaining:
                    self.store.insert(c, f"ack:{parent}", "reaction", f"ack:{origin['message_id']}", {"message_id": origin["message_id"]})
        await self.store.tx(finish)
        await self.store.set_meta("max_delivered", time.time())

    async def command_chat(self, job: dict, data: dict, arg: str, thread: int | None):
        """Create/reuse a private route. No greeting or other message is sent to MAX."""
        fields = arg.split(maxsplit=1)
        if not fields or not re.fullmatch(r"(?:\+[1-9]\d{6,15}|[1-9]\d*)", fields[0]):
            raise Permanent("Использование: /chat MAX_USER_ID [Имя] или /chat +79991234567 [Имя]")
        alias = fields[1].strip() if len(fields) > 1 else ""
        if utf16len(alias) > 128:
            raise Permanent("Имя топика: не более 128 символов UTF-16")
        if not self.max_available.is_set() or not self.own_id:
            raise Retry("MAX connection unavailable", 10, count=False)
        saved = data.get("chat_request")
        if not saved:
            try:
                if fields[0].startswith("+"):
                    user = await asyncio.wait_for(self.client.search_by_phone(fields[0]), 20)
                else:
                    user = await asyncio.wait_for(self.client.get_user(int(fields[0])), 20)
            except (ConnectionError, OSError, asyncio.TimeoutError) as exc:
                raise Retry("Не удалось получить контакт MAX; повтор позже") from exc
            except Exception as exc:
                raise Permanent(f"MAX не вернул контакт ({type(exc).__name__}); проверьте ID/номер и доступ") from exc
            uid = number(get(user, "id"))
            if uid is None or uid <= 0:
                raise Permanent("Пользователь MAX не найден")
            if not fields[0].startswith("+") and uid != int(fields[0]):
                raise Permanent("MAX вернул другой ID пользователя; отправка запрещена")
            if uid == self.own_id:
                raise Permanent("/chat предназначен для другого пользователя, не для своего аккаунта")
            # Public PyMax 2.4.1 API; a user ID is NOT a dialog ID.
            target = number(self.client.get_chat_id(self.own_id, uid))
            if target is None or target <= 0:
                raise Permanent("SDK не вернул допустимый ID личного чата")
            title = alias or user_name(user, f"MAX user {uid}")
            title = split_text(title, 128)[0] if title else f"MAX user {uid}"
            saved = {"user_id": uid, "target": str(target), "name": title}
            data["chat_request"] = saved
            # Freeze the resolved identity before creating any Telegram topic.
            await self.store.payload(job, data)
        target, title = saved["target"], saved["name"]
        requested_thread = number(thread)
        if requested_thread is not None and requested_thread <= 1:
            requested_thread = None  # General is not a conversation route.
        async with self.topic_lock:
            def register(c):
                old = c.execute("SELECT * FROM tm_routes WHERE max_id=?", (target,)).fetchone()
                if old and old["type"] != "private":
                    raise Permanent("ID уже привязан не как личный чат; проверьте маршрут вручную")
                if requested_thread:
                    taken = c.execute("SELECT max_id FROM tm_routes WHERE thread_id=?", (requested_thread,)).fetchone()
                    if taken and taken[0] != target:
                        raise Permanent("Этот топик уже привязан к другому чату. Выполните /chat в General.")
                    if old and old["thread_id"] and old["thread_id"] != requested_thread:
                        raise Permanent(f"Этот диалог уже привязан к топику {old['thread_id']}. Выполните /chat в General.")
                c.execute("INSERT OR IGNORE INTO tm_routes(max_id,name,type) VALUES(?,?,'private')", (target, title))
                if requested_thread:
                    c.execute("UPDATE tm_routes SET thread_id=?,state='ready' WHERE max_id=?", (requested_thread, target))
            await self.store.tx(register)
        # ensure_topic serializes external creation and persists ambiguous results.
        dest_thread = await self.ensure_topic(target)
        muted = await self.store.muted(target)
        reply = (f"Диалог: {title}\nMAX user ID: {saved['user_id']}\nMAX chat ID: {target}"
                 f"\nTelegram topic ID: {dest_thread}\nПишите в этом топике — сообщение уйдёт в MAX."
                 "\nКоманда /chat сама ничего собеседнику не отправляет.")
        if muted:
            reply += "\nВходящая пересылка выключена: /unmute в этом топике."
        specs = self.notice_specs(reply, dest_thread)
        if requested_thread != dest_thread:
            # Bot API t.me/c deep link for private supergroup topics.
            group = str(self.cfg.chat_id)
            link = f"https://t.me/c/{group[4:]}/{dest_thread}" if group.startswith("-100") else ""
            specs += self.notice_specs(f"Диалог «{title}»: топик {dest_thread}." + (f"\n{link}" if link else ""), thread)
        await self.store.expand(job, specs)

    async def command_mute(self, job: dict, arg: str, thread: int | None, muted: bool, actor: int):
        topic = number(arg) if arg else number(thread)
        if topic is None or topic <= 1:
            raise Permanent("Отправьте /mute или /unmute внутри нужного топика; в General: /mute TOPIC_ID")
        def change(c):
            route = c.execute("SELECT max_id,name FROM tm_routes WHERE thread_id=?", (topic,)).fetchone()
            if not route:
                raise Permanent("Топик не привязан к MAX-чату")
            target, name = route[0], route[1]
            old = c.execute("SELECT value FROM tm_meta WHERE key=?", ("mute:" + target,)).fetchone()
            previous = payload_object(old[0]) if old else {}
            cutoff = time.time() if (not muted and previous.get("muted")) else float(previous.get("discard_before") or 0)
            policy = {"muted": muted, "thread_id": topic, "updated": time.time(), "actor": actor,
                      "discard_before": cutoff}
            c.execute("INSERT INTO tm_meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                      ("mute:" + target, jdump(policy)))
            count = 0
            if muted:
                # Pending forwards and preprocessing are suppressed, not deferred.
                # An external request already in phase=send cannot be recalled.
                count = c.execute("UPDATE tm_jobs SET state='cancelled',phase='',error='',result=?,updated=? "
                    "WHERE route=? AND kind IN ('max_in','to_tg') AND "
                    "(state='pending' OR (state='running' AND phase!='send'))",
                    (jdump({"suppressed": "muted"}), time.time(), target)).rowcount
            reply = (f"{'🔇' if muted else '🔔'} {name} · topic {topic} · MAX chat {target}\n" +
                (f"Пересылка MAX → Telegram отключена. Снято с очереди: {count}.\n"
                 "Новые входящие не накапливаются для пересылки. Уже начатая отправка может завершиться."
                 if muted else "Пересылка MAX → Telegram включена для новых входящих. Пропущенные сообщения не воспроизводятся.") +
                "\nОтветы Telegram → MAX по-прежнему разрешены.")
            for i, (k, r, p) in enumerate(self.notice_specs(reply, thread)):
                self.store.insert(c, f"{job['key']}/notice/{i}", k, r, p)
            self.store.finish_in_tx(c, job)
        await self.store.tx(change)

    async def error_details(self, row: dict) -> tuple[dict, str]:
        """Build diagnostics from durable source data; no external requests."""
        data = payload_object(row["payload"])
        source_key = data.get("source_key") or (data.get("origin") or {}).get("parent") or row["key"].split("/", 1)[0]
        sources = await self.store.read("SELECT * FROM tm_jobs WHERE key=?", (source_key,))
        source = sources[0] if sources else row
        body = payload_object(source["payload"])
        chat_request = data.get("chat_request") or body.get("chat_request") or {}
        target = str(chat_request.get("target") or row["route"])
        routes = await self.store.read("SELECT * FROM tm_routes WHERE max_id=?", (target,))
        route = routes[0] if routes else {}
        ctx = data.get("context") or {}
        original = None
        fidelity = "unavailable; only saved job payload remains"
        if "raw_message" in body:
            original, fidelity = body["raw_message"], "full SDK message model (not raw network frame)"
        elif "update" in body:
            original, fidelity = body["update"], "full Telegram update"
        elif "message" in body:
            original, fidelity = body["message"], "full Telegram message; update wrapper unavailable"
        elif source["kind"] == "max_in":
            original, fidelity = body, "normalized message only; 3.5.0 did not retain the full model"
        when, millis = ctx.get("time"), ctx.get("time_unit") == "ms"
        if when is None and source["kind"] == "max_in":
            when, millis = body.get("time"), True
        if when is None and body.get("message"):
            when, millis = body["message"].get("date"), False
        stamp = display_time(when, milliseconds=millis)
        stamp_label = "Время сообщения"
        if stamp == "неизвестно":
            stamp, stamp_label = display_time(source["created"]), "Принято мостом (исходное время неизвестно)"
        name = str(route.get("name") or ctx.get("recipient") or chat_request.get("name") or target)[:180]
        thread = route.get("thread_id") or (body.get("message") or {}).get("message_thread_id")
        if row["kind"] in {"max_in", "to_tg"}:
            recipient = f"{name} → Telegram topic {thread or 'не создан'} (MAX chat {row['route']})"
        elif row["kind"] in {"tg_in", "to_max"}:
            recipient = f"{name} (MAX chat {target}; Telegram topic {thread or 'General'})"
        elif chat_request:
            recipient = f"{name} (MAX user {chat_request.get('user_id')}; MAX chat {target}; Telegram topic {thread or 'не создан'})"
        else:
            recipient = f"{name}; Telegram topic {thread or 'General'}"
        sender = ctx.get("sender_name") or ctx.get("sender") or body.get("sender")
        if sender is None and body.get("message"):
            sender = user_name(body["message"].get("from"), "неизвестно")
        summary = (f"#{row['id']} {row['kind']} [{row['state']}]\n{stamp_label}: {stamp}"
                   f"\nАдресат: {recipient}\nОт: {sender if sender is not None else 'неизвестно'}"
                   f"\nОшибка: {row['error']}")
        detail = {"telemax_version": VERSION, "saved_at": display_time(time.time()),
                  "message_time": stamp, "message_time_label": stamp_label,
                  "failed_at": display_time(row["updated"]), "recipient": recipient,
                  "source_fidelity": fidelity, "original_message": original,
                  "source_job": {k: v for k, v in source.items() if k != "payload"},
                  "source_payload": body, "job": {k: v for k, v in row.items() if k != "payload"},
                  "payload": data, "route": route}
        return detail, summary

    def write_error_dump(self, job_id: int, detail: dict) -> str:
        """Private atomic JSON snapshot, no truncation; quota errors keep data in DB."""
        for directory in (self.cfg.root / "dumps", self.cfg.errors):
            if directory.is_symlink():
                raise OSError("Diagnostic directory must not be a symlink")
            directory.mkdir(mode=0o700, exist_ok=True)
            os.chmod(directory, 0o700)
        path = self.cfg.errors / f"job-{int(job_id)}.json"
        if path.is_symlink():
            raise OSError("Diagnostic path must not be a symlink")
        blob = json.dumps(detail, ensure_ascii=False, indent=2, allow_nan=False).encode("utf-8") + b"\n"
        usage = sum(p.stat().st_size for p in self.cfg.errors.glob("job-*.json")
                    if p.is_file() and not p.is_symlink())
        old = path.stat().st_size if path.exists() else 0
        if usage - old + len(blob) > self.cfg.error_dump_limit:
            raise OSError("Error dump budget exceeded")
        if shutil.disk_usage(self.cfg.errors).free < self.cfg.min_free + len(blob):
            raise OSError("Insufficient space for full diagnostic JSON")
        tmp = path.with_name(path.name + "." + uuid.uuid4().hex + ".part")
        fd = os.open(tmp, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
        try:
            with os.fdopen(fd, "wb") as out:
                out.write(blob)
                out.flush()
                os.fsync(out.fileno())
            os.replace(tmp, path)
            fsync_dir(path.parent)
        finally:
            tmp.unlink(missing_ok=True)
        return str(path.relative_to(self.cfg.root))

    async def report_error(self, job: dict):
        """Idempotent, bounded error alert; never recursively alerts about its own failure."""
        async with self.diagnostic_lock:
            found = await self.store.read("SELECT * FROM tm_jobs WHERE id=?", (job["id"],))
            if not found or found[0]["state"] not in {"dead", "uncertain"}:
                return
            row = found[0]
            signature = f"{row['state']}:{row['updated']:.6f}"
            key = f"diagnostic:{row['id']}"
            previous = payload_object(await self.store.meta(key, "{}"))
            if previous.get("signature") == signature and previous.get("file"):
                return
            detail, summary = await self.error_details(row)
            filename, dump_error = "", ""
            try:
                writing = asyncio.create_task(asyncio.to_thread(self.write_error_dump, row["id"], detail))
                try:
                    filename = await asyncio.shield(writing)
                except asyncio.CancelledError:
                    await writing
                    raise
            except (OSError, ValueError, TypeError) as exc:
                dump_error = type(exc).__name__
                LOG.error("Cannot save full diagnostic JSON for job %s (%s); original data remains in DB",
                          row["id"], dump_error)
            summary += (f"\nJSON на сервере: {filename}" if filename else
                        "\nJSON не записан: проверьте место/права/ERROR_DUMP_LIMIT_MB; данные сохранены в БД.")
            p = detail["payload"]
            def save(c):
                c.execute("INSERT INTO tm_meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                          (key, jdump({"signature": signature, "file": filename, "dump_error": dump_error})))
                if row["kind"] == "notice" and p.get("error_notice"):
                    return  # Do not create an infinite chain of failing error notices.
                for index, (_, _, payload) in enumerate(self.notice_specs(summary)):
                    payload["error_notice"] = True
                    self.store.insert(c, f"error:{row['id']}:{signature}/{index}", "notice", "notice:None", payload)
            await self.store.tx(save)

    def cleanup_error_dumps(self, active_ids: set[int]):
        if not self.cfg.errors.exists() or self.cfg.errors.is_symlink() or (self.cfg.root / "dumps").is_symlink():
            return
        cutoff = time.time() - self.cfg.history_days * 86400
        for path in self.cfg.errors.glob("job-*.json"):
            match = re.fullmatch(r"job-(\d+)\.json", path.name)
            if (match and int(match[1]) not in active_ids and not path.is_symlink()
                    and path.is_file() and path.stat().st_mtime < cutoff):
                path.unlink(missing_ok=True)

    async def execute_command(self, job: dict, data: dict):
        msg = data["message"]
        if not self.cfg.authorized(msg):
            raise Permanent("Unauthorized command")
        text = (msg.get("text") or "").strip()
        parts = text.split(maxsplit=1)
        token = parts[0].split("@", 1)
        if len(token) == 2 and not self.tg.username:
            raise Retry("Waiting for Telegram bot identity", 10, count=False)
        if len(token) == 2 and token[1].lower() != self.tg.username.lower():
            await self.store.finish(job)
            return
        command, arg = token[0].lower(), parts[1].strip() if len(parts) > 1 else ""
        thread = msg.get("message_thread_id")
        if command in {"/retry_dlq", "/clear_dlq", "/alias", "/bind", "/chat", "/mute", "/unmute"} and not self.cfg.authorized(msg, admin=True):
            await self.store.expand(job, self.notice_specs("Команда доступна только TG_ADMIN_USER_IDS.", thread))
            return
        if command == "/chat":
            await self.command_chat(job, data, arg, thread)
            return
        elif command in {"/mute", "/unmute"}:
            await self.command_mute(job, arg, thread, command == "/mute", number(msg["from"]["id"]))
            return
        elif command == "/muted":
            rows = await self.store.read("SELECT r.* FROM tm_routes r JOIN tm_meta m ON m.key='mute:'||r.max_id ORDER BY r.thread_id")
            lines = []
            for row in rows:
                if await self.store.muted(row["max_id"]):
                    lines.append(f"🔇 {row['name']} · topic {row['thread_id']} · MAX chat {row['max_id']}")
            reply = "\n".join(lines) or "Нет отключённых топиков."
        elif command == "/status":
            reply = await self.status_text()
        elif command == "/dlq":
            rows = await self.store.read("SELECT * FROM tm_jobs WHERE state IN ('dead','uncertain') ORDER BY id LIMIT 20")
            blocks = []
            for row in rows:
                _, summary = await self.error_details(row)
                diag = json.loads(await self.store.meta(f"diagnostic:{row['id']}", "{}"))
                if diag.get("file"):
                    summary += f"\nJSON на сервере: {diag['file']}"
                blocks.append(summary)
            reply = "\n\n".join(blocks) or "DLQ пуста."
        elif command in {"/retry_dlq", "/clear_dlq"}:
            self.modify_dlq_args(command, arg)  # Validate before entering the transaction.
            def change(c):
                if command == "/clear_dlq":
                    count = c.execute("UPDATE tm_jobs SET state='cancelled',phase='',updated=? WHERE state IN ('dead','uncertain')", (time.time(),)).rowcount
                    response = f"Отменено задач DLQ: {count}. Нужные другим задачам файлы сохранены."
                else:
                    values = arg.split()
                    if not values:
                        count = c.execute("UPDATE tm_jobs SET state='pending',phase='',attempts=0,next_at=0,error='',retry_since=0,updated=? WHERE state='dead'", (time.time(),)).rowcount
                    else:
                        states = "('dead','uncertain')" if len(values) == 2 else "('dead')"
                        count = c.execute(f"UPDATE tm_jobs SET state='pending',phase='',attempts=0,next_at=0,error='',retry_since=0,updated=? WHERE id=? AND state IN {states}", (time.time(), int(values[0]))).rowcount
                    response = f"Возвращено в очередь: {count}. Неопределённые отправки требуют /retry_dlq ID force; возможен дубль."
                for i, (k, r, p) in enumerate(self.notice_specs(response, thread)):
                    self.store.insert(c, f"{job['key']}/notice/{i}", k, r, p)
                self.store.finish_in_tx(c, job)
            await self.store.tx(change)
            return
        elif command == "/alias":
            if not thread or thread == 1 or not arg or utf16len(arg) > 128:
                reply = "Внутри топика: /alias Имя (до 128 символов)."
            else:
                def alias(c):
                    r = c.execute("SELECT max_id FROM tm_routes WHERE thread_id=?", (thread,)).fetchone()
                    if not r:
                        raise Permanent("Topic is not bound")
                    c.execute("UPDATE tm_routes SET name=? WHERE thread_id=?", (arg, thread))
                    self.store.insert(c, job["key"] + "/edit", "edit_topic", r[0], {"thread": thread, "name": arg})
                    for i, (k, route, p) in enumerate(self.notice_specs("Имя сохранено. Переименование в Telegram поставлено в очередь.", thread)):
                        self.store.insert(c, f"{job['key']}/notice/{i}", k, route, p)
                    self.store.finish_in_tx(c, job)
                await self.store.tx(alias)
                return
        elif command == "/bind":
            target = number(arg)
            if not thread or thread == 1 or target is None or target == 0:
                reply = "В нужном топике: /bind MAX_CHAT_ID. Нужен ID чата, не ID пользователя."
            else:
                def bind(c):
                    taken = c.execute("SELECT max_id FROM tm_routes WHERE thread_id=?", (thread,)).fetchone()
                    if taken and taken[0] != str(target):
                        raise Permanent("Topic already belongs to another MAX chat; choose a different topic")
                    c.execute("INSERT INTO tm_routes(max_id,thread_id,name,state) VALUES(?,?,?,'ready') ON CONFLICT(max_id) DO UPDATE SET thread_id=excluded.thread_id,state='ready'", (str(target), thread, f"MAX {target}"))
                    for i, (k, r, p) in enumerate(self.notice_specs(f"Топик привязан к MAX-чату {target}. Задачи DLQ можно повторить отдельно.", thread)):
                        self.store.insert(c, f"{job['key']}/notice/{i}", k, r, p)
                    self.store.finish_in_tx(c, job)
                await self.store.tx(bind)
                return
        else:
            reply = ("/status — состояние и доставка\n/dlq — ошибки\n/retry_dlq [ID] — повтор ошибок\n"
                     "/retry_dlq ID force — повтор неопределённой отправки (риск дубля)\n"
                     "/clear_dlq confirm — отменить задачи DLQ\n/alias Имя — имя топика\n/bind MAX_CHAT_ID — привязать топик\n"
                     "/chat MAX_USER_ID [Имя] — открыть личный диалог\n/chat +PHONE [Имя] — найти контакт и открыть диалог\n"
                     "/mute [TOPIC_ID] — отключить входящую пересылку\n/unmute [TOPIC_ID] — включить её\n/muted — отключённые топики")
        await self.store.expand(job, self.notice_specs(reply, thread))

    @staticmethod
    def modify_dlq_args(command, arg):
        if command == "/clear_dlq" and arg != "confirm":
            raise Permanent("Подтвердите отмену задач: /clear_dlq confirm")
        if command == "/retry_dlq" and arg and not re.fullmatch(r"[1-9]\d*(?: force)?", arg):
            raise Permanent("Использование: /retry_dlq [ID] или /retry_dlq ID force")

    async def status_text(self) -> str:
        rows = await self.store.read("SELECT kind,state,COUNT(*) n FROM tm_jobs WHERE state IN ('pending','running','dead','uncertain') GROUP BY kind,state")
        lines = [f"Telemax {VERSION}", "MAX: " + ("последняя проверка успешна" if self.max_available.is_set() else "соединение не подтверждено")]
        for key, title in (("max_probe_ok", "Проверка MAX"), ("tg_poll_ok", "Ответ Telegram"),
                           ("max_received", "Получено от MAX"), ("tg_delivered", "Доставлено в Telegram"),
                           ("max_delivered", "Доставлено в MAX")):
            stamp = await self.store.meta(key)
            lines.append(f"{title}: " + (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(stamp))) if stamp else "ещё не было"))
        lines.append(f"Inbox: {self.inbox.usage // 1024} KiB; неперсистентных callback: {self.inbox.waiting}")
        if self.inbox.last_error or self.store.db_error:
            lines.append("ВНИМАНИЕ: хранилище недоступно, проверьте диск/права; см. журнал.")
        lines.extend(f"{r['kind']} / {r['state']}: {r['n']}" for r in rows)
        oldest = await self.store.read("SELECT MIN(created) t FROM tm_jobs WHERE state IN ('pending','running')")
        if oldest[0]["t"]:
            lines.append(f"Возраст старейшей задачи: {int(time.time()-oldest[0]['t'])} с")
        if not self.cfg.allowed and not self.cfg.admins:
            lines.append("Telegram → MAX выключен: разрешённые пользователи не настроены.")
        return "\n".join(lines)

    async def auxiliary(self, job: dict, data: dict):
        if job["kind"] == "notice":
            method, params = "sendMessage", {"chat_id": self.cfg.chat_id, "text": data["text"]}
            if data.get("thread"):
                params["message_thread_id"] = data["thread"]
        elif job["kind"] == "reaction":
            method, params = "setMessageReaction", {"chat_id": self.cfg.chat_id,
                "message_id": data["message_id"], "reaction": [{"type": "emoji", "emoji": "⚡"}]}
        elif job["kind"] == "edit_status":
            method, params = "editMessageText", {"chat_id": self.cfg.chat_id,
                "message_id": data["message_id"], "text": await self.status_text()}
        else:
            method, params = "editForumTopic", {"chat_id": self.cfg.chat_id,
                "message_thread_id": data["thread"], "name": data["name"]}
        async def call():
            if job["kind"] == "notice":
                await self.store.phase(job, "send")
            return await self.tg.call(method, params, timeout=30)
        response = await self.tg.paced(call)
        if job["kind"] in {"edit_topic", "edit_status"} and response.code == 400 and "not modified" in response.error.lower():
            await self.store.finish(job)
            return
        require_api(response, sending=job["kind"] == "notice")
        await self.store.finish(job)

    async def worker(self, job_kind: str):
        while not self.stop.is_set():
            if job_kind in {"max_in", "to_max"} and not self.max_ready.is_set():
                await asyncio.sleep(1)
                continue
            try:
                job = await self.store.claim(job_kind)
            except sqlite3.Error as exc:
                LOG.error("Worker %s cannot claim (%s); leaving queue unchanged", job_kind, type(exc).__name__)
                await asyncio.sleep(5)
                continue
            if not job:
                await asyncio.sleep(0.5)
                continue
            job["phase"] = ""
            try:
                data = json.loads(job["payload"])
                if job_kind == "max_in":
                    await self.prepare_max(job, data)
                elif job_kind == "tg_in":
                    await self.prepare_tg(job, data)
                elif job_kind == "to_tg":
                    await self.send_tg(job, data)
                elif job_kind == "to_max":
                    await self.send_max(job, data)
                elif job_kind == "command":
                    await self.execute_command(job, data)
                else:
                    await self.auxiliary(job, data)
            except asyncio.CancelledError:
                # Persisted running/phase is recovered on the next startup.
                raise
            except (sqlite3.Error, CapacityError):
                raise
            except Suppressed:
                pass
            except (Retry, Permanent, Uncertain) as exc:
                await self.store.fail(job, exc)
                await self.report_error(job)
            except Exception as exc:
                LOG.exception("Unexpected failure in job %s", job["id"])
                cls = Uncertain if job.get("phase") == "send" else Permanent
                await self.store.fail(job, cls(f"{type(exc).__name__}; see local log"))
                await self.report_error(job)

    async def polling(self):
        self.tg.pause_until = float(await self.store.meta("tg_pause_until", "0"))
        while not self.stop.is_set():
            try:
                if not self.tg.username:
                    me = require_api(await self.tg.call("getMe"))
                    if str(get(me, "id")) != self.cfg.token.split(":", 1)[0]:
                        raise RuntimeError("Telegram returned an unexpected bot identity")
                    self.tg.username = str(get(me, "username") or "")
                offset = int(await self.store.meta("tg_offset", "0"))
                result = require_api(await self.tg.call("getUpdates", {
                    "offset": offset, "timeout": 20, "limit": 100, "allowed_updates": ["message"]}, timeout=35))
                if not isinstance(result, list):
                    raise Retry("Unexpected getUpdates payload")
                await self.store.accept_updates(result)
            except Retry as exc:
                LOG.warning("Telegram polling delayed: %s", exc)
                await asyncio.sleep(max(2, min(300, exc.delay)))
            except Permanent as exc:
                LOG.error("Telegram polling configuration error: %s", exc)
                await asyncio.sleep(60)
            except sqlite3.Error as exc:
                LOG.error("Telegram persistence delayed (%s); offset not acknowledged", type(exc).__name__)
                await asyncio.sleep(5)
            except CapacityError:
                # Offset was not committed: Telegram still owns these updates.
                LOG.error("Queue is full: Telegram polling paused without acknowledgment")
                await asyncio.sleep(30)

    async def health(self):
        await self.max_ready.wait()
        while not self.stop.is_set():
            try:
                # fetch_users, unlike get_user, is a network operation, not a cache hit.
                await asyncio.wait_for(self.client.fetch_users([self.own_id]), 15)
                self.max_available.set()
                await self.store.set_meta("max_probe_ok", time.time())
            except (sqlite3.Error, CapacityError) as exc:
                LOG.error("Health storage unavailable (%s)", type(exc).__name__)
            except Exception as exc:
                self.max_available.clear()
                LOG.warning("MAX probe failed: %s", type(exc).__name__)
            await asyncio.sleep(60)

    async def watchdog(self):
        interval = 5.0
        if os.environ.get("WATCHDOG_USEC"):
            interval = max(0.5, min(5, int(os.environ["WATCHDOG_USEC"]) / 3_000_000))
        announced = False
        while not self.stop.is_set():
            if self.max_ready.is_set() and not announced:
                systemd_notify("READY=1\nSTATUS=Telemax workers started; use /status for delivery health")
                announced = True
            if announced:
                systemd_notify("WATCHDOG=1")
            else:
                systemd_notify("EXTEND_TIMEOUT_USEC=30000000\nSTATUS=Waiting for MAX authentication")
            await asyncio.sleep(interval)

    async def maintenance(self):
        last_alert = 0.0
        while not self.stop.is_set():
            try:
                # Also handles failures recovered as uncertain at process startup and
                # old 3.5.0 DLQ rows. At most 20 unsnapshotted failures per maintenance tick.
                failures = await self.store.read("SELECT j.*,m.value AS diagnostic_metadata FROM tm_jobs j LEFT JOIN tm_meta m "
                    "ON m.key='diagnostic:'||j.id WHERE j.state IN ('dead','uncertain') ORDER BY j.id")
                reported = 0
                for failed in failures:
                    previous = payload_object(failed.get("diagnostic_metadata") or "{}")
                    signature = f"{failed['state']}:{failed['updated']:.6f}"
                    if previous.get("signature") == signature and previous.get("file"):
                        continue
                    await self.report_error(failed)
                    reported += 1
                    if reported >= 20:
                        break
                cleanup = await self.store.prune_history()
                rows = cleanup["active"]
                used = set()
                for path in cleanup["paths"]:
                    with contextlib.suppress(Permanent, ValueError):
                        used.add(self.downloads.checked_path(path))
                await asyncio.to_thread(self.cleanup_files, used)
                await asyncio.to_thread(self.cleanup_error_dumps, {r['id'] for r in rows})
                counts = await self.store.read("SELECT COUNT(*) n FROM tm_jobs WHERE state IN ('dead','uncertain')")
                if counts[0]["n"] and time.time() - last_alert > 1800:
                    LOG.warning("DLQ contains %s jobs; inspect /dlq", counts[0]["n"])
                    await self.push(f"Telemax: {counts[0]['n']} jobs need attention. Use /dlq.")
                    last_alert = time.time()
            except (sqlite3.Error, OSError, ValueError) as exc:
                LOG.error("Maintenance deferred (%s); no unprocessed messages discarded", type(exc).__name__)
            await asyncio.sleep(60)

    def cleanup_files(self, used: set[Path]):
        # Grace period also protects files referenced by a concurrent transaction.
        cutoff = time.time() - 86400
        for path in self.cfg.media.iterdir():
            if path.is_symlink() or not path.is_file():
                continue
            if path.resolve() not in used and path.stat().st_mtime < cutoff:
                path.unlink(missing_ok=True)

    async def push(self, text: str):
        if not self.cfg.ntfy:
            return
        try:
            opts = self.curl.base(self.cfg.ntfy, 10, None)
            opts.extend([("proto", "=https,http"), ("header", "Title: Telemax"), ("data-binary", text)])
            opts.append(("fail", ""))
            code, _, _ = await self.curl.run(opts, 10, limit=1024 * 1024)
            if code:
                LOG.warning("NTFY alert rejected or unreachable (curl %s)", code)
        except Exception:
            LOG.warning("NTFY alert could not be sent")

    async def run(self):
        self.client.on_start()(self.on_start)
        self.client.on_message()(self.on_message)
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self.stop.set)
        coros = {"max-client": self.client.start(), "tg-poll": self.polling(),
                 "health": self.health(), "watchdog": self.watchdog(), "maintenance": self.maintenance(),
                 "inbox": self.inbox_worker(), "storage-health": self.storage_health()}
        for name in ("max_in", "tg_in", "to_tg", "to_max", "command", "notice", "reaction", "edit_topic", "edit_status"):
            count = self.cfg.workers if name in {"max_in", "to_tg", "to_max"} else 1
            for index in range(count):
                coros[f"{name}-{index}"] = self.worker(name)
        tasks = [asyncio.create_task(coro, name=name) for name, coro in coros.items()]
        stop_task = asyncio.create_task(self.stop.wait(), name="stop")
        try:
            done, _ = await asyncio.wait(tasks + [stop_task], return_when=asyncio.FIRST_COMPLETED)
            if self.fatal:
                raise self.fatal
            for task in done:
                if task is not stop_task and not task.cancelled():
                    exc = task.exception()
                    if exc:
                        raise exc
                    if not self.stop.is_set():
                        raise RuntimeError(f"Critical task stopped: {task.get_name()}")
        finally:
            self.stop.set()
            systemd_notify("STOPPING=1")
            for task in tasks + [stop_task]:
                task.cancel()
            await asyncio.gather(*tasks, stop_task, return_exceptions=True)
            with contextlib.suppress(Exception):
                await asyncio.wait_for(self.client.close(), 10)
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.remove_signal_handler(sig)


def iter_paths(value):
    if isinstance(value, dict):
        for key, child in value.items():
            if key == "path" and isinstance(child, str):
                yield child
            else:
                yield from iter_paths(child)
    elif isinstance(value, list):
        for child in value:
            yield from iter_paths(child)


def systemd_notify(message: str):
    address = os.environ.get("NOTIFY_SOCKET")
    if not address:
        return
    if address.startswith("@"):
        address = "\0" + address[1:]
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as sock:
            sock.sendto(message.encode(), address)
    except OSError:
        LOG.warning("systemd notification failed")


def load_sdk():
    try:
        installed = importlib.metadata.version("maxapi-python")
    except importlib.metadata.PackageNotFoundError as exc:
        raise RuntimeError(f"Install maxapi-python=={SDK_VERSION} in the service venv") from exc
    if installed != SDK_VERSION:
        raise RuntimeError(f"Expected maxapi-python=={SDK_VERSION}; installed: {installed}")
    from pymax import Client, File, Photo, Video, VideoNote, Voice
    from pymax.config import ExtraConfig
    return Client, ExtraConfig, {"document": File, "photo": Photo, "video": Video,
                                "voice": Voice, "video_note": VideoNote}


async def serve(cfg: Config, client, classes):
    store = Store(cfg)
    bridge = Bridge(cfg, store, client, classes)
    try:
        await bridge.run()
    except Exception as exc:
        LOG.exception("Telemax stopped after a critical error")
        await bridge.push(f"Telemax stopped: {type(exc).__name__}. Inspect the service log.")
        raise
    finally:
        await store.close()


@contextlib.contextmanager
def instance_lock(root: Path):
    path = root / ".telemax.lock"
    fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("Another Telemax process is running in this directory") from exc
        yield


def prepare_directories(cfg: Config):
    for directory in (cfg.media, cfg.root / "session_cache", cfg.root / "dumps", cfg.errors, cfg.inbox):
        if directory.is_symlink():
            raise RuntimeError("Runtime directories must not be symlinks")
        directory.mkdir(mode=0o700, exist_ok=True)
        os.chmod(directory, 0o700)


def main():
    parser = argparse.ArgumentParser(description=f"Telemax {VERSION} MAX ↔ Telegram bridge")
    parser.add_argument("--version", action="version", version=f"Telemax {VERSION}")
    parser.add_argument("--config", type=Path,
                        default=Path(__file__).resolve().parent / "constants.json")
    actions = parser.add_mutually_exclusive_group()
    actions.add_argument("--init", action="store_true",
                         help="Create empty 3.5.x state; refuses existing DB; no SDK/network required")
    actions.add_argument("--check-config", action="store_true",
                         help="Validate configuration, SDK and curl; no DB access or network")
    actions.add_argument("--check", action="store_true",
                         help="Check configuration/SDK and existing database without changing queue state")
    actions.add_argument("--upgrade-db", action="store_true",
                         help="Offline additive upgrade of schema 3/350; keeps jobs/routes/session; makes a verified backup")
    actions.add_argument("--compact-db", action="store_true",
                         help="Offline backup + VACUUM; keeps all jobs, requires spare disk space")
    parser.add_argument("--confirm-legacy-bot", action="store_true",
                        help="Confirm the current bot token belongs to the SAME bot as legacy schema 3")
    args = parser.parse_args()
    if args.confirm_legacy_bot and not args.upgrade_db:
        parser.error("--confirm-legacy-bot is only used with --upgrade-db")
    cfg = Config.load(args.config)
    if args.upgrade_db or args.compact_db:
        os.umask(0o077)
        with instance_lock(cfg.root):
            if args.upgrade_db:
                snapshot = Store.upgrade(cfg, confirm_legacy_bot=args.confirm_legacy_bot)
            else:
                snapshot = Store.compact(cfg)
        print(f"OK: database format {SCHEMA_VERSION}; backup: {snapshot or 'already current, no changes'}")
        return 0
    if args.init:
        os.umask(0o077)
        with instance_lock(cfg.root):
            if os.path.lexists(cfg.root / "telegram_queue.db"):
                raise RuntimeError("Database already exists; --init never overwrites state")
            prepare_directories(cfg)
            Store.create(cfg)
        print(f"Initialized Telemax {VERSION}: {cfg.root / 'telegram_queue.db'}")
        return 0
    if not shutil.which("curl"):
        raise RuntimeError("curl executable is required")
    Client, ExtraConfig, classes = load_sdk()
    if args.check_config or args.check:
        if args.check:
            Store.check(cfg)
        print(f"OK: Telemax {VERSION}; configuration; maxapi-python=={SDK_VERSION}; curl"
              + ("; state database" if args.check else ""))
        if not cfg.allowed and not cfg.admins:
            print("WARNING: Telegram → MAX and commands disabled: no authorized user IDs")
        return 0
    os.umask(0o077)
    with instance_lock(cfg.root):
        # Validate before authentication. Upgrades are explicit, offline and backed up, never a reset.
        Store.check(cfg)
        prepare_directories(cfg)
        os.chmod(args.config, 0o600)
        # All MAX network paths must not inherit an application-level proxy.
        for key in list(os.environ):
            if key.lower().endswith("_proxy"):
                del os.environ[key]
        client = Client(phone=cfg.phone, work_dir=str(cfg.root / "session_cache"),
                        extra_config=ExtraConfig(proxy=None))
        configure_logging(cfg)
        LOG.info("Starting Telemax %s", VERSION)
        if not cfg.allowed and not cfg.admins:
            LOG.warning("Telegram → MAX and all commands disabled: no authorized user IDs")
        asyncio.run(serve(cfg, client, classes))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception as exc:
        # Avoid leaking configuration values before logging is configured.
        detail = re.sub(r"https?://\S+|\b\d{6,}:[A-Za-z0-9_-]+|\+\d{7,16}", "[redacted]", str(exc))
        print(f"Telemax startup failed: {type(exc).__name__}: {detail}", file=sys.stderr)
        sys.exit(1)
