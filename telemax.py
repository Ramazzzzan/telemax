#!/usr/bin/env python3
"""Telemax 3.5.0 — MAX <-> Telegram bridge for a clean installation.

Python 3.10+; maxapi-python==2.4.1; curl; Linux.
Configure constants.json, run --init once, then start normally.
State from other releases is rejected, never imported or overwritten.

Commands: /status, /dlq, /retry_dlq [ID], /retry_dlq ID force,
          /clear_dlq confirm, /alias Name, /bind MAX_CHAT_ID, /help.
An interrupted external send is held as uncertain until an explicit forced retry.
Exactly-once delivery and replay of MAX events missed while offline are not promised.

Checks: python telemax.py --check-config; python telemax.py --check
Tests:  python -m unittest -v telemax_test
"""
from __future__ import annotations

import argparse
import asyncio
import concurrent.futures
import contextlib
import dataclasses
import enum
import fcntl
import hashlib
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
import uuid
from pathlib import Path
from typing import Any, Callable
from urllib.parse import quote, urljoin, urlsplit

VERSION = "3.5.0"
SCHEMA_VERSION = "350"
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


class CapacityError(RuntimeError):
    pass


def jdump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


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

    KEYS = frozenset({
        "MAX_PHONE", "TG_BOT_TOKEN", "TG_CHAT_ID", "TG_ALLOWED_USER_IDS",
        "TG_ADMIN_USER_IDS", "MY_MAX_ID", "TG_PROXY", "NTFY_URL", "MAX_MEDIA_MB",
        "TG_DOWNLOAD_MB", "MEDIA_DISK_LIMIT_MB", "MIN_FREE_MB", "QUEUE_LIMIT",
        "HISTORY_DAYS", "MAX_ATTEMPTS",
    })

    @property
    def media(self) -> Path:
        return self.root / "media_queue"

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
        return cls(path.resolve().parent, phone, token, chat_id,
                   ids("TG_ALLOWED_USER_IDS"), ids("TG_ADMIN_USER_IDS"), own, proxy, ntfy,
                   positive("MAX_MEDIA_MB", 49) * 1024**2,
                   min(20, positive("TG_DOWNLOAD_MB", 20)) * 1024**2,
                   positive("MEDIA_DISK_LIMIT_MB", 2048) * 1024**2,
                   positive("MIN_FREE_MB", 256) * 1024**2,
                   positive("QUEUE_LIMIT", 50000), positive("HISTORY_DAYS", 30),
                   positive("MAX_ATTEMPTS", 10))

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
                    "attempts", "next_at", "error", "result", "created", "updated"},
    }

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.path = cfg.root / "telegram_queue.db"
        self.conn = self.open_existing(cfg)
        self.pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="telemax-db")
        try:
            self.validate(self.conn, cfg)
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA synchronous=FULL")
            self.conn.execute("PRAGMA busy_timeout=30000")
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
    def validate(cls, conn, cfg: Config):
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")}
        if tables != set(cls.TABLES):
            raise RuntimeError("Not a Telemax 3.5.0 database; use a fresh installation directory")
        for table, columns in cls.TABLES.items():
            actual = {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}
            if actual != columns:
                raise RuntimeError(f"Invalid database layout: {table}")
        meta = dict(conn.execute("SELECT key,value FROM tm_meta"))
        if meta.get("schema") != SCHEMA_VERSION:
            raise RuntimeError("Unsupported database schema; this build requires a 3.5.0 state database")
        if meta.get("tg_chat_id") != str(cfg.chat_id):
            raise RuntimeError("TG_CHAT_ID differs from this database; saved messages will not be rerouted")
        if meta.get("tg_bot_id") != cfg.token.split(":", 1)[0]:
            raise RuntimeError("Telegram bot differs from this database; use a separate installation")
        if cfg.my_max_id and meta.get("max_account_id") not in {None, str(cfg.my_max_id)}:
            raise RuntimeError("MY_MAX_ID differs from this database")

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
                "created REAL NOT NULL,updated REAL NOT NULL)",
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

    @staticmethod
    def insert(c, key, job_kind, route, payload, state="pending", error=""):
        now = time.time()
        cur = c.execute("""INSERT INTO tm_jobs(key,kind,route,payload,state,error,created,updated)
                         VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(key) DO NOTHING""",
                        (key, job_kind, str(route), jdump(payload), state, error, now, now))
        return cur.lastrowid if cur.rowcount else None

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
        return await asyncio.get_running_loop().run_in_executor(self.pool, run)

    async def read(self, sql: str, params: tuple = ()) -> list[dict]:
        def run():
            return [dict(r) for r in self.conn.execute(sql, params).fetchall()]
        return await asyncio.get_running_loop().run_in_executor(self.pool, run)

    async def meta(self, key: str, default="") -> str:
        rows = await self.read("SELECT value FROM tm_meta WHERE key=?", (key,))
        return rows[0]["value"] if rows else default

    async def set_meta(self, key: str, value: Any):
        await self.tx(lambda c: c.execute("INSERT OR REPLACE INTO tm_meta VALUES(?,?)", (key, str(value))).rowcount)

    def capacity(self, c):
        count = c.execute("SELECT COUNT(*) FROM tm_jobs WHERE state IN ('pending','running','dead','uncertain')").fetchone()[0]
        if count >= self.cfg.queue_limit:
            raise CapacityError("Durable queue limit reached; no new events acknowledged")

    async def add(self, key, job_kind, route, payload):
        def op(c):
            if c.execute("SELECT 1 FROM tm_jobs WHERE key=?", (key,)).fetchone():
                return False
            self.capacity(c)
            self.insert(c, key, job_kind, route, payload)
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
                        payload = {"message": msg, "target": route[0] if route else None}
                        self.insert(c, f"tg:{uid}", "command" if is_command else "tg_in",
                                    route[0] if route else f"thread:{thread}", payload)
                offset = max(offset, uid + 1)
            c.execute("INSERT OR REPLACE INTO tm_meta VALUES('tg_offset',?)", (str(offset),))
            c.execute("INSERT OR REPLACE INTO tm_meta VALUES('tg_poll_ok',?)", (str(time.time()),))
            return offset
        return await self.tx(op)

    async def claim(self, job_kind: str) -> dict | None:
        def op(c):
            row = c.execute("""SELECT j.* FROM tm_jobs j WHERE j.kind=? AND j.state='pending'
                AND j.next_at<=? AND NOT EXISTS(SELECT 1 FROM tm_jobs p WHERE p.kind=j.kind
                AND p.route=j.route AND p.id<j.id AND p.state IN ('pending','running'))
                ORDER BY j.id LIMIT 1""", (job_kind, time.time())).fetchone()
            if not row:
                return None
            c.execute("UPDATE tm_jobs SET state='running',phase='',updated=? WHERE id=?", (time.time(), row["id"]))
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
        reason = str(exc)[:1000]
        # Error messages may contain URLs or credentials; never save those as diagnostics.
        for secret in (self.cfg.token, self.cfg.phone, self.cfg.ntfy, self.cfg.proxy):
            if secret:
                reason = reason.replace(secret, "[redacted]")
        reason = re.sub(r"https?://\S+", "[URL redacted]", reason)
        await self.tx(lambda c: c.execute("UPDATE tm_jobs SET state=?,phase='',attempts=?,next_at=?,error=?,updated=? WHERE id=?",
                         (state, attempts, next_at, reason, time.time(), job["id"])).rowcount)
        LOG.warning("Job %s %s: %s", job["id"], state, reason)

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
        async with self.send_lock:
            # Conservative per-supergroup pacing; 429 remains the authoritative limit.
            await asyncio.sleep(max(0, 3.1 - (time.monotonic() - self.last_send)))
            self.last_send = time.monotonic()
            return await callback()


async def public_resolve(url: str) -> tuple[str, str]:
    """Pin a public address for each HTTPS hop, preventing DNS rebinding to LAN."""
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
        records = await asyncio.wait_for(asyncio.to_thread(socket.getaddrinfo, host, 443, 0, socket.SOCK_STREAM), 10)
    except (OSError, asyncio.TimeoutError) as exc:
        raise Retry("Media hostname resolution failed") from exc
    ips = {r[4][0] for r in records}
    if not ips or any(not ipaddress.ip_address(ip).is_global for ip in ips):
        raise Permanent("Private/reserved media destination blocked")
    # Prefer IPv4 on hosts without working IPv6 routing.
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
        self.lock = asyncio.Lock()

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
        if usage + limit > self.cfg.disk_limit:
            raise Retry("Media disk budget exceeded; drain/clear the queue", 120, count=False)
        if shutil.disk_usage(self.cfg.media).free < self.cfg.min_free + limit:
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
            await asyncio.to_thread(self.check_budget, limit)
            for _ in range(6):
                part = path.with_name(path.name + "." + uuid.uuid4().hex + ".part")
                head = path.with_name(path.name + "." + uuid.uuid4().hex + ".headers")
                try:
                    opts = self.curl.base(url, 300, self.cfg.proxy if telegram else None)
                    if telegram:
                        if urlsplit(url).hostname != "api.telegram.org":
                            raise Permanent("Unexpected Telegram file host")
                    else:
                        host, address = await public_resolve(url)
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
            "download_url", "token", "duration", "title", "text", "first_name", "phone")
    data = {k: get(attach, k) for k in keys if isinstance(get(attach, k), (str, int, float, bool))}
    data["type"] = kind(get(attach, "type") or get(attach, "_type"))
    data["source"], data["chat_id"], data["message_id"] = "max", chat_id, message_id
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


class Bridge:
    def __init__(self, cfg: Config, store: Store, client: Any, media_classes: dict[str, Any]):
        self.cfg, self.store, self.client = cfg, store, client
        self.media_classes = media_classes
        self.curl = Curl()
        self.tg = Telegram(cfg, store, self.curl)
        self.downloads = Downloads(cfg, self.curl)
        self.max_ready, self.max_available = asyncio.Event(), asyncio.Event()
        self.stop = asyncio.Event()
        self.fatal: BaseException | None = None
        self.own_id = cfg.my_max_id
        self.topic_lock = asyncio.Lock()
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
        try:
            data = normalize_message(message)
            if own_message(data, self.own_id):
                return
            # No text-based deduplication. Stable source IDs are persisted.
            mid, chat = data.get("id"), data.get("chat_id")
            key = f"max:{chat}:{mid}" if mid is not None and chat is not None else "max:unidentified:" + uuid.uuid4().hex
            await self.store.add(key, "max_in", str(chat), data)
            await self.store.set_meta("max_received", time.time())
        except Exception as exc:
            # SDK handlers may swallow exceptions. Signal the supervisor explicitly.
            self.abort(exc)

    async def name_for(self, sender) -> str:
        sid = number(sender)
        if sid is None:
            return "Система"
        try:
            user = await asyncio.wait_for(self.client.get_user(sid), 5)
            name = " ".join(str(get(user, x) or "") for x in ("first_name", "last_name")).strip()
            names = get(user, "names") or []
            candidate = name or (get(names[0], "name") if names else None) or get(user, "name")
            return str(candidate or f"ID:{sid}")
        except Exception:
            return f"ID:{sid}"

    async def prepare_max(self, job: dict, data: dict):
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
        for f in files:
            typ = f.get("type", "")
            default = {"PHOTO": "photo.jpg", "VIDEO": "video.mp4", "AUDIO": "voice.ogg", "VOICE": "voice.ogg", "STICKER": "sticker.webp"}.get(typ, "attachment.bin")
            f["name"] = safe_name(f.get("name") or f.get("file_name"), default)
            f["kind"] = {"PHOTO": "photo", "VIDEO": "video", "AUDIO": "voice", "VOICE": "voice", "STICKER": "photo"}.get(typ, "document")
            if typ not in MEDIA_TYPES:
                f["unsupported"] = typ or "UNKNOWN"
                text += f"\n[Вложение {typ or 'UNKNOWN'}: формат не поддерживается; сохранено в DLQ]"
            normalized.append(f)
        parts = tg_parts(f"{header}\n{text}".strip(), normalized)
        await self.store.expand(job, [("to_tg", target, p) for p in parts])

    def notice_specs(self, text: str, thread=None) -> list[tuple[str, str, dict]]:
        return [("notice", f"notice:{thread}", {"text": part, "thread": thread}) for part in split_text(text)]

    async def prepare_tg(self, job: dict, data: dict):
        msg = data["message"]
        if not self.cfg.authorized(msg):
            raise Permanent("Sender is no longer authorized")
        target = number(data.get("target"))
        if target is None:
            await self.store.expand(job, self.notice_specs("Нет привязки к MAX. Администратор может выполнить /bind MAX_CHAT_ID внутри топика.", msg.get("message_thread_id")))
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
            specs.append(("to_max", str(target), {"text": piece, "files": [file] if file and index == 0 else [], "origin": origin}))
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
            if f.get("source") not in {"max", "telegram"}:
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
            await self.store.phase(job, "send")
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
        if command in {"/retry_dlq", "/clear_dlq", "/alias", "/bind"} and not self.cfg.authorized(msg, admin=True):
            await self.store.expand(job, self.notice_specs("Команда доступна только TG_ADMIN_USER_IDS.", thread))
            return
        if command == "/status":
            reply = await self.status_text()
        elif command == "/dlq":
            rows = await self.store.read("SELECT id,kind,state,error FROM tm_jobs WHERE state IN ('dead','uncertain') ORDER BY id LIMIT 20")
            reply = "DLQ пуста." if not rows else "\n\n".join(f"#{r['id']} {r['kind']} [{r['state']}]\n{r['error']}" for r in rows)
        elif command in {"/retry_dlq", "/clear_dlq"}:
            self.modify_dlq_args(command, arg)  # Validate before entering the transaction.
            def change(c):
                if command == "/clear_dlq":
                    count = c.execute("UPDATE tm_jobs SET state='cancelled',phase='',updated=? WHERE state IN ('dead','uncertain')", (time.time(),)).rowcount
                    response = f"Отменено задач DLQ: {count}. Нужные другим задачам файлы сохранены."
                else:
                    values = arg.split()
                    if not values:
                        count = c.execute("UPDATE tm_jobs SET state='pending',phase='',attempts=0,next_at=0,error='',updated=? WHERE state='dead'", (time.time(),)).rowcount
                    else:
                        states = "('dead','uncertain')" if len(values) == 2 else "('dead')"
                        count = c.execute(f"UPDATE tm_jobs SET state='pending',phase='',attempts=0,next_at=0,error='',updated=? WHERE id=? AND state IN {states}", (time.time(), int(values[0]))).rowcount
                    response = f"Возвращено в очередь: {count}. Неопределённые отправки требуют /retry_dlq ID force; возможен дубль."
                for i, (k, r, p) in enumerate(self.notice_specs(response, thread)):
                    self.store.insert(c, f"{job['key']}/notice/{i}", k, r, p)
                self.store.finish_in_tx(c, job)
            await self.store.tx(change)
            return
        elif command == "/alias":
            if not thread or not arg or utf16len(arg) > 128:
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
            if not thread or target is None or target == 0:
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
                     "/clear_dlq confirm — отменить задачи DLQ\n/alias Имя — имя топика\n/bind MAX_CHAT_ID — привязать топик")
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
        else:
            method, params = "editForumTopic", {"chat_id": self.cfg.chat_id,
                "message_thread_id": data["thread"], "name": data["name"]}
        async def call():
            if job["kind"] == "notice":
                await self.store.phase(job, "send")
            return await self.tg.call(method, params, timeout=30)
        response = await self.tg.paced(call)
        if job["kind"] == "edit_topic" and response.code == 400 and "not modified" in response.error.lower():
            await self.store.finish(job)
            return
        require_api(response, sending=job["kind"] == "notice")
        await self.store.finish(job)

    async def worker(self, job_kind: str):
        while not self.stop.is_set():
            if job_kind in {"max_in", "to_max"} and not self.max_ready.is_set():
                await asyncio.sleep(1)
                continue
            job = await self.store.claim(job_kind)
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
            except (Retry, Permanent, Uncertain) as exc:
                await self.store.fail(job, exc)
                if job_kind in {"command", "tg_in"} and isinstance(exc, Permanent):
                    payload = json.loads(job["payload"])
                    thread = (payload.get("message") or {}).get("message_thread_id")
                    for i, (k, route, p) in enumerate(self.notice_specs(f"Задача #{job['id']}: {str(exc)[:500]}", thread)):
                        await self.store.add(f"error:{job['id']}:{i}", k, route, p)
            except Exception as exc:
                LOG.exception("Unexpected failure in job %s", job["id"])
                cls = Uncertain if job.get("phase") == "send" else Permanent
                await self.store.fail(job, cls(f"{type(exc).__name__}; see local log"))

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
            except (sqlite3.Error, CapacityError):
                raise
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
            cutoff = time.time() - self.cfg.history_days * 86400
            # Keep small deduplication tombstones, discard old completed message bodies.
            await self.store.tx(lambda c: c.execute("UPDATE tm_jobs SET payload='{}',error='' WHERE state IN ('done','cancelled') AND updated<? AND payload!='{}'", (cutoff,)).rowcount)
            rows = await self.store.read("SELECT payload FROM tm_jobs WHERE state IN ('pending','running','dead','uncertain')")
            used = set()
            for r in rows:
                for p in iter_paths(json.loads(r["payload"])):
                    with contextlib.suppress(Permanent, ValueError):
                        used.add(self.downloads.checked_path(p))
            await asyncio.to_thread(self.cleanup_files, used)
            counts = await self.store.read("SELECT COUNT(*) n FROM tm_jobs WHERE state IN ('dead','uncertain')")
            if counts[0]["n"] and time.time() - last_alert > 1800:
                LOG.warning("DLQ contains %s jobs; inspect /dlq", counts[0]["n"])
                await self.push(f"Telemax: {counts[0]['n']} jobs need attention. Use /dlq.")
                last_alert = time.time()
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
                 "health": self.health(), "watchdog": self.watchdog(), "maintenance": self.maintenance()}
        for name in ("max_in", "tg_in", "to_tg", "to_max", "command", "notice", "reaction", "edit_topic"):
            coros[name] = self.worker(name)
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
    for directory in (cfg.media, cfg.root / "session_cache"):
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
                         help="Create empty 3.5.0 state; refuses existing DB; no SDK/network required")
    actions.add_argument("--check-config", action="store_true",
                         help="Validate configuration, SDK and curl; no DB access or network")
    actions.add_argument("--check", action="store_true",
                         help="Check configuration/SDK and existing database without changing queue state")
    args = parser.parse_args()
    cfg = Config.load(args.config)
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
        # Validate before authentication or a network call, and never change another release's DB.
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
