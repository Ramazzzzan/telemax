#!/usr/bin/env python3
"""Telemax 3.5.2 regression tests. Run: python -m unittest -v telemax_test.

The core suite uses temporary SQLite databases, mocked external APIs and loopback
HTTP only. No MAX or Telegram credentials are read. Optional SDK contract tests
are skipped when maxapi-python==2.4.1 is not installed.
"""
from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import importlib.metadata
import inspect
import json
import logging
import os
import shutil
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, patch

import telemax as app
from telemax import (
    ApiResult, Bridge, CapacityError, Config, Curl, Downloads, Permanent, Retry,
    RedactedFormatter, Store, Telegram, Uncertain, clean_env,
    curl_quote, decode_api, jdump, normalize_message,
    own_message, parse_headers, public_resolve, require_api, safe_name,
    split_text, tg_parts, utf16len,
)

@contextlib.contextmanager
def sqlite_connection(path):
    """Commit/rollback fixture writes and close the handle (also on Python 3.13+)."""
    connection = sqlite3.connect(path)
    try:
        with connection:
            yield connection
    finally:
        connection.close()


class Attachment:
    def __init__(self, **kwargs):
        self.kwargs = kwargs

class UnitTests(unittest.TestCase):
    def test_utf16_split_preserves_all_text(self):
        text = ("А<&>🙂\n" * 1500) + "fin"
        pieces = split_text(text)
        self.assertEqual("".join(pieces), text)
        self.assertTrue(all(utf16len(p) <= 4000 for p in pieces))

    def test_long_caption_becomes_separate_text(self):
        parts = tg_parts("я" * 5000, [{"kind": "photo"}])
        self.assertEqual([len(x["text"]) for x in parts], [4000, 1000, 0])
        self.assertEqual(len(parts[-1]["files"]), 1)

    def test_album_partition(self):
        parts = tg_parts("caption", [{"kind": "photo"}] * 21)
        self.assertEqual([len(p["files"]) for p in parts], [10, 10, 1])
        self.assertEqual([p["text"] for p in parts], ["caption", "", ""])


    def test_no_text_based_echo_filter(self):
        self.assertFalse(own_message({"sender": 2, "chat_id": 77, "text": "OK"}, 1))
        self.assertFalse(own_message({"sender": 3, "chat_id": 88, "text": "OK"}, 1))
        self.assertTrue(own_message({"sender": "1", "text": "OK"}, 1))

    def test_authorization(self):
        cfg = Config(Path("."), "+70000000000", "123:x", -100, frozenset({7}), frozenset({8}))
        msg = {"chat": {"id": -100}, "from": {"id": 7}}
        self.assertTrue(cfg.authorized(msg))
        self.assertFalse(cfg.authorized(msg, admin=True))
        for change in ({"from": {"id": 9}}, {"from": {"id": 7, "is_bot": True}},
                       {"sender_chat": {"id": -99}}, {"chat": {"id": -200}},
                       {"is_automatic_forward": True}):
            self.assertFalse(cfg.authorized({**msg, **change}))
        self.assertTrue(cfg.authorized({**msg, "from": {"id": 8}}, admin=True))

    def test_empty_allowlist_denies(self):
        cfg = Config(Path("."), "+70000000000", "123:x", -100)
        self.assertFalse(cfg.authorized({"chat": {"id": -100}, "from": {"id": 7}}))

    def test_missing_chat_id_is_not_string_none(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "constants.json"
            for chat in (None, "None", "", True, 0, 1):
                p.write_text(jdump({"MAX_PHONE": "+70000000000", "TG_BOT_TOKEN": "123:x", "TG_CHAT_ID": chat}))
                with self.assertRaises(ValueError):
                    Config.load(p)

    def test_invalid_allowlist_and_valid_config(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "constants.json"
            obj = {"MAX_PHONE": "+70000000000", "TG_BOT_TOKEN": "123:x", "TG_CHAT_ID": "-100"}
            p.write_text(jdump({**obj, "TG_ALLOWED_USER_IDS": [True]}))
            with self.assertRaises(ValueError):
                Config.load(p)
            p.write_text(jdump({**obj, "TG_ALLOWED_USER_IDS": [7], "TG_ADMIN_USER_IDS": [7]}))
            self.assertEqual(Config.load(p).allowed, frozenset({7}))

    def test_api_429_is_retryable(self):
        response = decode_api(0, b'{"ok":false,"error_code":429,"parameters":{"retry_after":31}}')
        with self.assertRaises(Retry) as cm:
            require_api(response, sending=True)
        self.assertEqual(cm.exception.delay, 31)
        self.assertFalse(cm.exception.count)

    def test_transport_timeout_is_uncertain_only_for_sends(self):
        result = decode_api(28, b"")
        with self.assertRaises(Uncertain):
            require_api(result, sending=True)
        with self.assertRaises(Retry):
            require_api(result, sending=False)

    def test_connect_failure_is_retryable(self):
        with self.assertRaises(Retry):
            require_api(decode_api(7, b""), sending=True)

    def test_bad_request_never_becomes_success(self):
        result = decode_api(0, b'{"ok":false,"error_code":400,"description":"bad input"}')
        self.assertFalse(result.ok)
        with self.assertRaises(Permanent):
            require_api(result)

    def test_invalid_json_after_send_is_uncertain(self):
        with self.assertRaises(Uncertain):
            require_api(decode_api(0, b"not json"), sending=True)

    def test_config_curl_does_not_expand_newline_options(self):
        self.assertEqual(curl_quote('x"\ny'), '"x\\"\\ny"')
        self.assertNotIn("\n", curl_quote('x"\ny'))

    def test_header_parser_uses_final_response(self):
        status, headers = parse_headers("HTTP/1.1 100 Continue\r\n\r\nHTTP/2 200\r\nContent-Length: 3\r\n\r\n")
        self.assertEqual(status, 200)
        self.assertEqual(headers["content-length"], "3")

    def test_safe_name(self):
        self.assertEqual(safe_name("../../secret.txt"), "secret.txt")
        self.assertNotIn(";", safe_name('evil.jpg;type=text/html'))
        self.assertLessEqual(len(safe_name("я" * 1000 + ".jpg").encode()), 150)

    def test_forwarded_media_keeps_source_context(self):
        data = normalize_message(NS(id=1, chat_id=100, text="", sender=2, attaches=[],
            link=NS(type="FORWARD", chat_id=200, message=NS(id=3, chat_id=None, text="fwd", attaches=[NS(type="FILE", file_id=9)]))))
        self.assertEqual(data["forward"]["files"][0]["chat_id"], 200)
        self.assertEqual(data["forward"]["files"][0]["message_id"], 3)

    def test_log_redaction(self):
        formatter = RedactedFormatter(["123:secret"])
        record = logging.LogRecord("test", 40, "", 1, "123:secret https://host/token?q=private", (), None)
        text = formatter.format(record)
        self.assertNotIn("secret", text)
        self.assertNotIn("private", text)

class BridgeFixture(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.cfg = Config(root, "+70000000000", "123:test", -100,
                          frozenset({7}), frozenset({7}), my_max_id=1,
                          max_file_bytes=10000, max_tg_download_bytes=10000,
                          disk_limit=1000000, min_free=1)
        self.cfg.media.mkdir()
        Store.create(self.cfg)
        self.store = Store(self.cfg)
        self.client = NS(get_user=AsyncMock(return_value=NS(first_name="Name", last_name="")),
                         get_chat=AsyncMock(return_value=NS(title="Group", type="CHAT")),
                         send_message=AsyncMock(return_value=NS(id=900)),
                         get_file_by_id=AsyncMock(return_value=NS(url="https://cdn.example.test/file")),
                         fetch_users=AsyncMock(return_value=[]))
        self.bridge = Bridge(self.cfg, self.store, self.client,
                             {k: Attachment for k in ("photo", "video", "document", "voice", "video_note")})
        self.bridge.own_id = 1
        self.bridge.max_ready.set()
        self.bridge.max_available.set()
        self.bridge.tg.username = "test_bot"
        self.bridge.tg.call = AsyncMock(return_value=ApiResult(ok=True, result={"message_id": 55}))
        async def immediate(callback):
            return await callback()
        self.bridge.tg.paced = immediate
        await self.store.tx(lambda c: c.execute("INSERT INTO tm_routes VALUES('100',42,'Group','group','ready')"))

    async def asyncTearDown(self):
        await self.store.close()
        self.tmp.cleanup()

    async def job(self, kind_, payload, route="100", key=None):
        await self.store.add(key or uuid.uuid4().hex, kind_, route, payload)
        row = await self.store.claim(kind_)
        self.assertIsNotNone(row)
        row["phase"] = ""
        return row

    def tgmsg(self, text="hello", **fields):
        return {"message_id": 10, "message_thread_id": 42, "chat": {"id": -100},
                "from": {"id": 7}, "text": text, **fields}

    async def state(self, job):
        return (await self.store.read("SELECT * FROM tm_jobs WHERE id=?", (job["id"],)))[0]


class AsyncTests(BridgeFixture):





    async def test_durable_deduplication(self):
        self.assertTrue(await self.store.add("max:100:1", "max_in", "100", {"text": "OK"}))
        self.assertFalse(await self.store.add("max:100:1", "max_in", "100", {"text": "OK"}))
        self.assertTrue(await self.store.add("max:200:1", "max_in", "200", {"text": "OK"}))
        self.assertEqual(len(await self.store.read("SELECT * FROM tm_jobs")), 2)

    async def test_claim_is_atomic(self):
        await self.store.add("one", "to_tg", "100", {"text": "x"})
        first, second = await asyncio.gather(self.store.claim("to_tg"), self.store.claim("to_tg"))
        self.assertEqual(sum(x is not None for x in (first, second)), 1)

    async def test_backoff_does_not_block_another_chat(self):
        job = await self.job("to_tg", {"text": "first"})
        await self.store.fail(job, Retry("outage", 300))
        await self.store.add("same", "to_tg", "100", {"text": "later"})
        await self.store.add("different", "to_tg", "200", {"text": "other"})
        row = await self.store.claim("to_tg")
        self.assertEqual(row["route"], "100")  # Retry backoff allows later due messages through.
        other = await self.store.claim("to_tg")
        self.assertEqual(other["route"], "200")

    async def test_offset_and_target_are_saved_together(self):
        await self.store.accept_updates([{"update_id": 21, "message": self.tgmsg()}])
        await self.store.tx(lambda c: c.execute("UPDATE tm_routes SET max_id='200' WHERE thread_id=42"))
        job = (await self.store.read("SELECT * FROM tm_jobs"))[0]
        self.assertEqual(json.loads(job["payload"])["target"], "100")
        self.assertEqual(await self.store.meta("tg_offset"), "22")

    async def test_updates_are_sorted_before_acknowledging(self):
        await self.store.accept_updates([{"update_id": 5, "message": self.tgmsg()}, {"update_id": 4, "message": self.tgmsg("earlier")}])
        self.assertEqual(len(await self.store.read("SELECT * FROM tm_jobs")), 2)
        self.assertEqual(await self.store.meta("tg_offset"), "6")

    async def test_unauthorized_updates_do_not_create_jobs(self):
        msg = self.tgmsg("/clear_dlq confirm", **{"from": {"id": 999}})
        await self.store.accept_updates([{"update_id": 9, "message": msg}])
        self.assertFalse(await self.store.read("SELECT * FROM tm_jobs"))
        self.assertEqual(await self.store.meta("tg_offset"), "10")

    async def test_failed_transaction_does_not_advance_offset(self):
        with patch.object(Store, "insert", side_effect=RuntimeError("injected")):
            with self.assertRaises(RuntimeError):
                await self.store.accept_updates([{"update_id": 8, "message": self.tgmsg()}])
        self.assertEqual(await self.store.meta("tg_offset"), "")
        self.assertFalse(await self.store.read("SELECT * FROM tm_jobs"))

    async def test_missing_file_is_not_success(self):
        data = {"text": "", "files": [{"source": "max", "kind": "document", "path": str(self.cfg.media / "missing.bin")}]}
        job = await self.job("to_tg", data)
        with self.assertRaises(Permanent):
            await self.bridge.send_tg(job, data)
        self.bridge.tg.call.assert_not_awaited()
        self.assertNotEqual((await self.state(job))["state"], "done")

    async def test_shared_file_is_not_deleted_after_first_send(self):
        path = self.cfg.media / "shared.jpg"
        path.write_bytes(b"image")
        data = {"text": "", "files": [{"source": "max", "kind": "photo", "path": str(path)}]}
        first = await self.job("to_tg", data, key="first")
        await self.store.add("second", "to_tg", "100", data)
        await self.bridge.send_tg(first, data)
        self.assertTrue(path.exists())
        second = await self.store.claim("to_tg")
        await self.bridge.send_tg(second, json.loads(second["payload"]))
        self.assertEqual(self.bridge.tg.call.await_count, 2)
        self.assertEqual((await self.state(second))["state"], "done")

    async def test_partial_album_is_not_sent(self):
        path = self.cfg.media / "present.jpg"
        path.write_bytes(b"image")
        data = {"text": "", "files": [
            {"source": "max", "kind": "photo", "path": str(path)},
            {"source": "max", "kind": "photo", "path": str(self.cfg.media / "missing.jpg")}]}
        job = await self.job("to_tg", data)
        with self.assertRaises(Permanent):
            await self.bridge.send_tg(job, data)
        self.bridge.tg.call.assert_not_awaited()

    async def test_html_is_sent_as_literal_text(self):
        data = {"text": "<b>A & B</b>", "files": []}
        job = await self.job("to_tg", data)
        await self.bridge.send_tg(job, data)
        params = self.bridge.tg.call.call_args.args[1]
        self.assertEqual(params["text"], data["text"])
        self.assertNotIn("parse_mode", params)

    async def test_429_stays_pending_without_attempt_increment(self):
        data = {"text": "test", "files": []}
        job = await self.job("to_tg", data)
        self.bridge.tg.call.return_value = ApiResult(code=429, retry_after=50)
        with self.assertRaises(Retry) as cm:
            await self.bridge.send_tg(job, data)
        await self.store.fail(job, cm.exception)
        row = await self.state(job)
        self.assertEqual(row["state"], "pending")
        self.assertEqual(row["attempts"], 0)
        self.assertGreaterEqual(row["next_at"], time.time() + 49)

    async def test_topic_deletion_never_falls_back_to_general_chat(self):
        data = {"text": "test", "files": []}
        job = await self.job("to_tg", data)
        self.bridge.tg.call.return_value = ApiResult(code=400, error="Bad Request: message thread not found")
        with self.assertRaises(Retry):
            await self.bridge.send_tg(job, data)
        row = (await self.store.read("SELECT * FROM tm_routes"))[0]
        self.assertIsNone(row["thread_id"])
        self.assertEqual(row["max_id"], "100")
        self.assertEqual(self.bridge.tg.call.call_args.args[1]["message_thread_id"], 42)

    async def test_ambiguous_topic_creation_is_not_repeated(self):
        await self.store.tx(lambda c: c.execute("UPDATE tm_routes SET thread_id=NULL,state='new'"))
        self.bridge.tg.call.return_value = ApiResult(ambiguous=True, error="timeout")
        for _ in range(2):
            with self.assertRaises(Permanent):
                await self.bridge.ensure_topic("100")
        self.assertEqual(self.bridge.tg.call.await_count, 1)
        row = (await self.store.read("SELECT * FROM tm_routes"))[0]
        self.assertEqual(row["state"], "uncertain")

    async def test_max_receives_text_and_attachment_in_one_call(self):
        path = self.cfg.media / "file.pdf"
        path.write_bytes(b"pdf")
        data = {"text": "caption", "files": [{"source": "telegram", "kind": "document", "path": str(path)}],
                "origin": {"sender": 7, "parent": "tg:101", "message_id": 101}}
        job = await self.job("to_max", data, key="tg:101/0")
        await self.bridge.send_max(job, data)
        self.client.send_message.assert_awaited_once()
        call = self.client.send_message.call_args.kwargs
        self.assertEqual(call["chat_id"], 100)
        self.assertEqual(call["text"], "caption")
        self.assertEqual(call["attachments"][0].kwargs["path"], str(path))
        self.assertEqual((await self.state(job))["state"], "done")
        self.assertEqual(len(await self.store.read("SELECT * FROM tm_jobs WHERE kind='reaction'")), 1)

    async def test_uncertain_max_send_is_not_automatically_retried(self):
        data = {"text": "hello", "files": [], "origin": {"sender": 7}}
        job = await self.job("to_max", data)
        self.client.send_message.side_effect = ConnectionError("injected")
        with self.assertRaises(Uncertain) as cm:
            await self.bridge.send_max(job, data)
        await self.store.fail(job, cm.exception)
        self.assertEqual((await self.state(job))["state"], "uncertain")
        self.assertIsNone(await self.store.claim("to_max"))
        self.client.send_message.assert_awaited_once()

    async def test_revoked_sender_cannot_send_saved_job(self):
        self.bridge.cfg = dataclasses.replace(self.cfg, allowed=frozenset(), admins=frozenset())
        data = {"text": "hello", "files": [], "origin": {"sender": 7}}
        job = await self.job("to_max", data)
        with self.assertRaises(Permanent):
            await self.bridge.send_max(job, data)
        self.client.send_message.assert_not_awaited()

    async def test_reaction_waits_for_all_text_parts(self):
        payload = {"message": self.tgmsg("X" * 5000), "target": "100"}
        parent = await self.job("tg_in", payload, key="tg:500")
        await self.bridge.prepare_tg(parent, payload)
        a = await self.store.claim("to_max")
        await self.bridge.send_max(a, json.loads(a["payload"]))
        self.assertFalse(await self.store.read("SELECT * FROM tm_jobs WHERE kind='reaction'"))
        b = await self.store.claim("to_max")
        await self.bridge.send_max(b, json.loads(b["payload"]))
        self.assertEqual(len(await self.store.read("SELECT * FROM tm_jobs WHERE kind='reaction'")), 1)

    async def test_restart_classifies_interrupted_sends(self):
        sending = await self.job("to_tg", {"text": "x"}, key="sending")
        await self.store.phase(sending, "send")
        preparing = await self.job("max_in", {"id": 1}, key="preparing")
        await self.store.close()
        self.store = Store(self.cfg)
        self.assertEqual((await self.state(sending))["state"], "uncertain")
        self.assertEqual((await self.state(preparing))["state"], "pending")

    async def test_dlq_retry_excludes_uncertain_without_force(self):
        dead = await self.job("to_tg", {"text": "x"}, key="dead")
        await self.store.fail(dead, Permanent("bad"))
        unknown = await self.job("to_tg", {"text": "y"}, key="unknown")
        await self.store.fail(unknown, Uncertain("timeout"))
        cmddata = {"message": self.tgmsg("/retry_dlq")}
        cmd = await self.job("command", cmddata)
        await self.bridge.execute_command(cmd, cmddata)
        self.assertEqual((await self.state(dead))["state"], "pending")
        self.assertEqual((await self.state(unknown))["state"], "uncertain")
        forcedata = {"message": self.tgmsg(f"/retry_dlq {unknown['id']} force")}
        force = await self.job("command", forcedata)
        await self.bridge.execute_command(force, forcedata)
        self.assertEqual((await self.state(unknown))["state"], "pending")

    async def test_dlq_clear_does_not_touch_later_failure(self):
        before = await self.job("to_tg", {"text": "x"}, key="before")
        await self.store.fail(before, Permanent("old failure"))
        data = {"message": self.tgmsg("/clear_dlq confirm")}
        command = await self.job("command", data)
        await self.bridge.execute_command(command, data)
        later = await self.job("to_tg", {"text": "y"}, key="later")
        await self.store.fail(later, Permanent("new failure"))
        self.assertEqual((await self.state(before))["state"], "cancelled")
        self.assertEqual((await self.state(later))["state"], "dead")

    async def test_tg_429_cooldown_is_persisted(self):
        curl = NS(base=Curl.base, run=AsyncMock(return_value=(0, b'{"ok":false,"error_code":429,"parameters":{"retry_after":45}}', 1)))
        tg = Telegram(self.cfg, self.store, curl)
        await tg.call("getMe")
        await tg.call("getMe")
        self.assertEqual(curl.run.await_count, 1)
        self.assertGreater(float(await self.store.meta("tg_pause_until")), time.time() + 40)
        self.assertEqual(dict(curl.run.call_args.args[0])["proxy"], self.cfg.proxy)
        self.assertEqual(dict(curl.run.call_args.args[0])["noproxy"], "")

    async def test_media_paths_cannot_escape_queue(self):
        with self.assertRaises(Permanent):
            self.bridge.downloads.checked_path(self.cfg.root / "constants.json")
        (self.cfg.media / "link").symlink_to(self.cfg.root / "secret")
        with self.assertRaises(Permanent):
            self.bridge.downloads.checked_path(self.cfg.media / "link")

    async def test_private_download_host_is_blocked(self):
        fake = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", 443))]
        with patch.object(socket, "getaddrinfo", return_value=fake):
            with self.assertRaises(Permanent):
                await public_resolve("https://internal.example.test/file")

    async def test_download_timeout_discards_partial_file(self):
        async def fake(options, timeout, destination=None, limit=None):
            destination.write_bytes(b"par")
            Path(dict(options)["dump-header"]).write_text("HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\n")
            return 28, b"", 3
        downloads = Downloads(self.cfg, NS(run=fake, base=Curl.base))
        path = self.cfg.media / "download.bin"
        with patch("telemax.public_resolve", new=AsyncMock(return_value=("cdn.example.test", "8.8.8.8"))):
            with self.assertRaises(Retry):
                await downloads.fetch("https://cdn.example.test/file", path)
        self.assertFalse(path.exists())
        self.assertEqual(list(self.cfg.media.iterdir()), [])

    async def test_redirect_is_revalidated(self):
        calls = []
        async def fake(options, timeout, destination=None, limit=None):
            calls.append(options)
            destination.write_bytes(b"redirect")
            Path(dict(options)["dump-header"]).write_text("HTTP/1.1 302 Found\r\nLocation: https://127.0.0.1/private\r\n\r\n")
            return 0, b"", 8
        downloads = Downloads(self.cfg, NS(run=fake, base=Curl.base))
        resolver = AsyncMock(side_effect=[("cdn.example.test", "8.8.8.8"), Permanent("blocked")])
        with patch("telemax.public_resolve", new=resolver):
            with self.assertRaises(Permanent):
                await downloads.fetch("https://cdn.example.test/file", self.cfg.media / "out.bin")
        self.assertEqual(len(calls), 1)
        self.assertEqual(resolver.call_args.args[0], "https://127.0.0.1/private")
        self.assertEqual(list(self.cfg.media.iterdir()), [])

    async def test_health_does_not_hide_connection_error(self):
        self.client.fetch_users.side_effect = ConnectionError("offline")
        task = asyncio.create_task(self.bridge.health())
        try:
            for _ in range(100):
                if not self.bridge.max_available.is_set():
                    break
                await asyncio.sleep(0.001)
            self.assertFalse(self.bridge.max_available.is_set())
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_existing_route_cannot_be_stolen_by_bind(self):
        data = {"message": self.tgmsg("/bind 200")}
        job = await self.job("command", data)
        with self.assertRaises(Permanent):
            await self.bridge.execute_command(job, data)
        route = (await self.store.read("SELECT max_id FROM tm_routes WHERE thread_id=42"))[0]
        self.assertEqual(route["max_id"], "100")

    async def test_gc_keeps_files_referenced_by_active_jobs(self):
        path = self.cfg.media / "keep.bin"
        path.write_bytes(b"x")
        os.utime(path, (1, 1))
        self.bridge.cleanup_files({path.resolve()})
        self.assertTrue(path.exists())
        self.bridge.cleanup_files(set())
        self.assertFalse(path.exists())


    async def test_database_is_bound_to_telegram_group(self):
        with self.assertRaises(RuntimeError):
            Store(dataclasses.replace(self.cfg, chat_id=-200))


class FreshStateTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.cfg = Config(self.root, "+70000000000", "123:test", -100)

    def tearDown(self):
        self.tmp.cleanup()

    def test_fresh_init_version_and_identity(self):
        Store.create(self.cfg)
        with sqlite_connection(self.root / "telegram_queue.db") as conn:
            meta = dict(conn.execute("SELECT key,value FROM tm_meta"))
            self.assertEqual(meta["schema"], app.SCHEMA_VERSION)
            self.assertEqual(meta["created_by"], "3.5.2")
            self.assertEqual(meta["tg_bot_id"], "123")
            self.assertEqual(meta["tg_chat_id"], "-100")
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM tm_jobs").fetchone()[0], 0)
            self.assertEqual(conn.execute("PRAGMA quick_check").fetchone()[0], "ok")
        Store.check(self.cfg)

    def test_missing_state_is_not_implicitly_created(self):
        with self.assertRaisesRegex(RuntimeError, "--init"):
            Store(self.cfg)
        self.assertFalse((self.root / "telegram_queue.db").exists())

    def test_init_never_overwrites_existing_state(self):
        Store.create(self.cfg)
        path = self.root / "telegram_queue.db"
        before = path.read_bytes()
        with self.assertRaisesRegex(RuntimeError, "already exists"):
            Store.create(self.cfg)
        self.assertEqual(path.read_bytes(), before)

    def test_init_refuses_even_an_empty_existing_file(self):
        path = self.root / "telegram_queue.db"
        path.touch()
        with self.assertRaises(RuntimeError):
            Store.create(self.cfg)
        self.assertEqual(path.read_bytes(), b"")

    def test_old_state_is_not_modified(self):
        path = self.root / "telegram_queue.db"
        with sqlite_connection(path) as c:
            c.execute("CREATE TABLE queue_v2(id INTEGER PRIMARY KEY,text_data TEXT)")
            c.execute("INSERT INTO queue_v2 VALUES(1,'not for this release')")
        before = path.read_bytes()
        with self.assertRaisesRegex(RuntimeError, "Unsupported database"):
            Store(self.cfg)
        self.assertEqual(path.read_bytes(), before)

    def test_foreign_schema_marker_is_rejected(self):
        Store.create(self.cfg)
        path = self.root / "telegram_queue.db"
        with sqlite_connection(path) as c:
            c.execute("UPDATE tm_meta SET value='3' WHERE key='schema'")
        before = path.read_bytes()
        with self.assertRaisesRegex(RuntimeError, "format"):
            Store(self.cfg)
        self.assertEqual(path.read_bytes(), before)

    def test_missing_column_is_rejected(self):
        Store.create(self.cfg)
        with sqlite_connection(self.root / "telegram_queue.db") as c:
            c.execute("ALTER TABLE tm_jobs RENAME COLUMN result TO old_result")
        with self.assertRaisesRegex(RuntimeError, "layout"):
            Store.check(self.cfg)

    def test_init_rolls_back_on_failure(self):
        with patch("telemax.fsync_dir", side_effect=OSError("injected")):
            with self.assertRaises(OSError):
                Store.create(self.cfg)
        self.assertFalse((self.root / "telegram_queue.db").exists())

    def test_db_is_private(self):
        Store.create(self.cfg)
        self.assertEqual((self.root / "telegram_queue.db").stat().st_mode & 0o777, 0o600)

    def test_init_refuses_stale_sqlite_sidecars(self):
        for suffix in ("-wal", "-shm", "-journal"):
            with self.subTest(suffix=suffix):
                p = self.root / ("telegram_queue.db" + suffix)
                p.write_bytes(b"preserve")
                with self.assertRaisesRegex(RuntimeError, "sidecar"):
                    Store.create(self.cfg)
                self.assertEqual(p.read_bytes(), b"preserve")
                p.unlink()

    def test_db_symlink_is_refused(self):
        target = self.root / "target"
        target.write_bytes(b"preserve")
        (self.root / "telegram_queue.db").symlink_to(target)
        with self.assertRaises(RuntimeError):
            Store.create(self.cfg)
        with self.assertRaises(RuntimeError):
            Store.check(self.cfg)
        self.assertEqual(target.read_bytes(), b"preserve")

    def test_bot_identity_is_bound_but_token_rotation_allowed(self):
        Store.create(self.cfg)
        Store.check(dataclasses.replace(self.cfg, token="123:rotated"))
        with self.assertRaisesRegex(RuntimeError, "bot differs"):
            Store.check(dataclasses.replace(self.cfg, token="456:another_bot"))

    def test_configured_max_identity_is_checked(self):
        Store.create(self.cfg)
        with sqlite_connection(self.root / "telegram_queue.db") as c:
            c.execute("INSERT INTO tm_meta VALUES('max_account_id','77')")
        Store.check(dataclasses.replace(self.cfg, my_max_id=77))
        with self.assertRaisesRegex(RuntimeError, "MY_MAX_ID"):
            Store.check(dataclasses.replace(self.cfg, my_max_id=78))

    def test_lock_excludes_second_process(self):
        with app.instance_lock(self.root):
            with self.assertRaisesRegex(RuntimeError, "Another Telemax"):
                with app.instance_lock(self.root):
                    self.fail("A second lock was acquired")
        with app.instance_lock(self.root):
            pass

    def test_runtime_directory_symlink_is_rejected(self):
        other = self.root / "other"
        other.mkdir()
        self.cfg.media.symlink_to(other, target_is_directory=True)
        with self.assertRaisesRegex(RuntimeError, "symlinks"):
            app.prepare_directories(self.cfg)

    def test_import_has_no_runtime_side_effects(self):
        result = subprocess.run([sys.executable, "-c", "import telemax; print(telemax.VERSION)"],
                                cwd=self.root, env={**os.environ, "PYTHONPATH": str(Path(app.__file__).parent)},
                                capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "3.5.2")
        self.assertEqual(list(self.root.iterdir()), [])

    def test_cli_init_is_offline_and_repeated_init_fails(self):
        cfg_path = self.root / "constants.json"
        cfg_path.write_text(jdump({"MAX_PHONE": "+70000000000", "TG_BOT_TOKEN": "123:test", "TG_CHAT_ID": -100}))
        cmd = [sys.executable, app.__file__, "--config", str(cfg_path), "--init"]
        first = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        self.assertEqual(first.returncode, 0, first.stderr)
        second = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        self.assertNotEqual(second.returncode, 0)
        self.assertIn("already exists", second.stderr)
        Store.check(self.cfg)
        self.assertEqual(set(p.name for p in self.root.iterdir()), {
            "constants.json", ".telemax.lock", "telegram_queue.db", "media_queue", "session_cache", "dumps", "inbox"})

    def test_cli_version_needs_no_config(self):
        result = subprocess.run([sys.executable, app.__file__, "--version"], cwd=self.root,
                                capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "Telemax 3.5.2")


class Config350Tests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "constants.json"
        self.base = {"MAX_PHONE": "+70000000000", "TG_BOT_TOKEN": "123:test", "TG_CHAT_ID": -100}

    def tearDown(self):
        self.tmp.cleanup()

    def load(self, **extra):
        self.path.write_text(jdump({**self.base, **extra}), encoding="utf-8")
        return Config.load(self.path)

    def test_complete_documented_config_is_accepted(self):
        import re
        doc = Path(app.__file__).with_name("deployment.md").read_text()
        examples = re.findall(r"```json\n(.*?)\n```", doc, re.DOTALL)
        self.assertEqual(len(examples), 1)
        data = json.loads(examples[0])
        self.assertEqual(set(data), Config.KEYS)
        self.path.write_text(examples[0])
        cfg = Config.load(self.path)
        expected = Config(self.path.parent, cfg.phone, cfg.token, cfg.chat_id, cfg.allowed, cfg.admins)
        self.assertEqual(cfg, expected)

    def test_unknown_configuration_key_is_not_silently_ignored(self):
        with self.assertRaisesRegex(ValueError, "MEDIA_LIMIT_MB"):
            self.load(MEDIA_LIMIT_MB=50)

    def test_duplicate_configuration_key_is_rejected(self):
        self.path.write_text('{"TG_CHAT_ID":-100,"TG_CHAT_ID":-200}')
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            Config.load(self.path)

    def test_admins_do_not_implicitly_inherit_allowed_list(self):
        cfg = self.load(TG_ALLOWED_USER_IDS=[7])
        self.assertEqual(cfg.admins, frozenset())
        self.assertEqual(cfg.allowed, frozenset({7}))

    def test_defaults_match_running_script_names(self):
        cfg = self.load()
        self.assertEqual(cfg.max_file_bytes, 49 * 1024**2)
        self.assertEqual(cfg.disk_limit, 2048 * 1024**2)
        self.assertEqual(cfg.history_days, 30)
        self.assertEqual(cfg.max_attempts, 10)
        self.assertEqual(cfg.proxy, "socks5h://127.0.0.1:10808")

    def test_telegram_download_limit_cannot_be_raised_past_20(self):
        self.assertEqual(self.load(TG_DOWNLOAD_MB=100).max_tg_download_bytes, 20 * 1024**2)
        self.assertEqual(self.load(TG_DOWNLOAD_MB=5).max_tg_download_bytes, 5 * 1024**2)

    def test_size_and_count_settings_reject_bad_values(self):
        for field in ("MAX_MEDIA_MB", "TG_DOWNLOAD_MB", "MEDIA_DISK_LIMIT_MB", "MIN_FREE_MB",
                      "QUEUE_LIMIT", "HISTORY_DAYS", "MAX_ATTEMPTS"):
            for value in (0, -1, True, 0.5, None, "NaN"):
                with self.subTest(field=field, value=value):
                    with self.assertRaises(ValueError):
                        self.load(**{field: value})

    def test_proxy_is_explicit_socks5h_without_path(self):
        for value in ("", "http://127.0.0.1:10808", "socks5://127.0.0.1:10808",
                      "socks5h://127.0.0.1:10808/path", "socks5h://host:99999",
                      "socks5h://host:10808\n", "socks5h://host:10808?x=1"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.load(TG_PROXY=value)

    def test_ntfy_must_be_a_real_http_url(self):
        for value in ("https:", "ftp://example.test", "https://example.test/x\n", 12):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.load(NTFY_URL=value)

    def test_media_url_rejects_insecure_scheme_or_credentials(self):
        for url in ("http://example.test/x", "file:///etc/passwd", "https://u:p@host/x",
                    "https://host:bad/x", "https://host:444/x"):
            with self.subTest(url=url), self.assertRaises(Permanent):
                asyncio.run(public_resolve(url))

    def test_nonfinite_retry_interval_is_an_ambiguous_response(self):
        for raw in (b'{"ok":false,"error_code":429,"parameters":{"retry_after":Infinity}}',
                    b'{"ok":false,"error_code":Infinity}',
                    b'{"ok":false,"error_code":429,"parameters":{"retry_after":NaN}}'):
            self.assertTrue(decode_api(0, raw).ambiguous)

    def test_proxy_environment_is_removed_for_direct_requests(self):
        with patch.dict(os.environ, {"HTTP_PROXY": "http://secret", "https_proxy": "http://secret",
                                    "ALL_PROXY": "socks5h://secret", "NO_PROXY": "*"}):
            env = clean_env()
            self.assertFalse(any(k.lower().endswith("_proxy") for k in env))
            self.assertIn("HTTP_PROXY", os.environ)

    def test_durable_failure_does_not_expose_api_token(self):
        formatter = RedactedFormatter(["123:test"])
        record = logging.LogRecord("t", logging.WARNING, "", 1,
                                   "failed 123:test https://example.test/download?secret", (), None)
        self.assertNotIn("test", formatter.format(record))


class AdditionalAsyncTests(BridgeFixture):
    async def test_read_only_check_does_not_recover_running_jobs(self):
        job = await self.job("to_tg", {"text": "not sent"})
        await self.store.phase(job, "send")
        Store.check(self.cfg)
        current = await self.state(job)
        self.assertEqual((current["state"], current["phase"]), ("running", "send"))

    async def test_accepting_updates_respects_capacity_without_advancing_offset(self):
        self.store.cfg = dataclasses.replace(self.cfg, queue_limit=1)
        await self.store.add("max:1", "max_in", "100", {})
        with self.assertRaises(CapacityError):
            await self.store.accept_updates([{"update_id": 3, "message": self.tgmsg()}])
        self.assertEqual(await self.store.meta("tg_offset"), "")
        self.assertEqual(len(await self.store.read("SELECT * FROM tm_jobs")), 1)

    async def test_active_duplicate_is_accepted_even_at_admission_limit(self):
        self.store.cfg = dataclasses.replace(self.cfg, queue_limit=1)
        await self.store.add("max:1", "max_in", "100", {})
        self.assertFalse(await self.store.add("max:1", "max_in", "100", {}))

    async def test_max_normalization_and_outbox_are_idempotent(self):
        message = NS(id=77, chat_id=100, sender=2, text="hello", type="USER", attaches=[])
        await self.bridge.on_message(message)
        await self.bridge.import_inbox_once()
        await self.bridge.on_message(message)
        await self.bridge.import_inbox_once()
        incoming = await self.store.claim("max_in")
        await self.bridge.prepare_max(incoming, json.loads(incoming["payload"]))
        outgoing = await self.store.claim("to_tg")
        await self.bridge.send_tg(outgoing, json.loads(outgoing["payload"]))
        self.bridge.tg.call.assert_awaited_once()
        self.assertEqual((await self.state(incoming))["state"], "done")
        self.assertEqual((await self.state(outgoing))["state"], "done")
        self.assertIsNone(await self.store.claim("max_in"))
        self.assertIsNone(await self.store.claim("to_tg"))

    async def test_max_own_messages_are_filtered_before_queueing(self):
        await self.bridge.on_message(NS(id=88, chat_id=100, sender=1, text="OK", attaches=[]))
        await self.bridge.import_inbox_once()
        self.assertFalse(await self.store.read("SELECT * FROM tm_jobs"))

    async def test_cancelled_send_waits_for_manual_review_after_restart(self):
        job = await self.job("to_max", {"text": "hi", "origin": {"sender": 7}})
        await self.store.phase(job, "send")
        await self.store.close()
        self.store = Store(self.cfg)
        self.assertEqual((await self.state(job))["state"], "uncertain")
        self.assertIsNone(await self.store.claim("to_max"))

    async def test_missing_album_confirmation_is_uncertain(self):
        files = []
        for index in range(2):
            path = self.cfg.media / f"pic{index}.jpg"
            path.write_bytes(b"fake image")
            files.append({"source": "max", "kind": "photo", "path": str(path)})
        data = {"text": "", "files": files}
        job = await self.job("to_tg", data)
        self.bridge.tg.call.return_value = ApiResult(ok=True, result=[{"message_id": 1}])
        with self.assertRaises(Uncertain):
            await self.bridge.send_tg(job, data)
        self.assertNotEqual((await self.state(job))["state"], "done")

    async def test_cached_file_size_mismatch_is_not_sent(self):
        path = self.cfg.media / "partial.bin"
        path.write_bytes(b"short")
        data = {"text": "caption", "files": [{"source": "telegram", "kind": "document",
                                                 "path": str(path), "size": 100}]}
        job = await self.job("to_max", {**data, "origin": {"sender": 7}})
        with self.assertRaisesRegex(Permanent, "length mismatch"):
            await self.bridge.send_max(job, {**data, "origin": {"sender": 7}})
        self.client.send_message.assert_not_awaited()

    async def test_cached_telegram_file_obeys_download_cap(self):
        path = self.cfg.media / "oversized.bin"
        path.write_bytes(b"x" * 20)
        cfg = dataclasses.replace(self.cfg, max_tg_download_bytes=10)
        downloader = Downloads(cfg, self.bridge.curl)
        with self.assertRaisesRegex(Permanent, "Cached file"):
            await downloader.fetch("https://api.telegram.org/file/test", path, telegram=True)

    async def test_exceeded_download_budget_is_retryable_without_attempt(self):
        path = self.cfg.media / "large.bin"
        path.write_bytes(b"x" * 20)
        cfg = dataclasses.replace(self.cfg, disk_limit=25)
        downloader = Downloads(cfg, self.bridge.curl)
        with self.assertRaises(Retry) as cm:
            downloader.check_budget(10)
        self.assertFalse(cm.exception.count)

    async def test_alias_preserves_literal_text_and_queues_edit(self):
        data = {"message": self.tgmsg("/alias <Family> & Friends")}
        job = await self.job("command", data)
        await self.bridge.execute_command(job, data)
        route = (await self.store.read("SELECT * FROM tm_routes"))[0]
        self.assertEqual(route["name"], "<Family> & Friends")
        edit = await self.store.claim("edit_topic")
        self.bridge.tg.call.return_value = ApiResult(code=400, error="topic not modified")
        await self.bridge.auxiliary(edit, json.loads(edit["payload"]))
        self.assertEqual((await self.state(edit))["state"], "done")

    async def test_admin_command_is_denied_to_non_admin(self):
        self.bridge.cfg = dataclasses.replace(self.cfg, admins=frozenset())
        data = {"message": self.tgmsg("/clear_dlq confirm")}
        failed = await self.job("to_tg", {"text": "x"})
        await self.store.fail(failed, Permanent("test"))
        command = await self.job("command", data)
        await self.bridge.execute_command(command, data)
        self.assertEqual((await self.state(failed))["state"], "dead")

    async def test_clear_dlq_requires_exact_confirmation(self):
        for value in ("", "all", "force", "confirm now"):
            with self.subTest(value=value), self.assertRaises(Permanent):
                Bridge.modify_dlq_args("/clear_dlq", value)

    async def test_telegram_bot_suffix_for_another_bot_is_ignored(self):
        data = {"message": self.tgmsg("/clear_dlq@another_bot confirm")}
        job = await self.job("command", data)
        await self.bridge.execute_command(job, data)
        self.assertEqual((await self.state(job))["state"], "done")
        self.assertEqual(len(await self.store.read("SELECT * FROM tm_jobs")), 1)

    async def test_unknown_telegram_route_creates_notice_not_max_delivery(self):
        data = {"message": self.tgmsg(), "target": None}
        job = await self.job("tg_in", data)
        await self.bridge.prepare_tg(job, data)
        self.assertIsNone(await self.store.claim("to_max"))
        self.assertIsNotNone(await self.store.claim("notice"))

    async def test_unsupported_telegram_object_with_text_is_not_acknowledged_as_text_only(self):
        data = {"message": self.tgmsg("poll", poll={"question": "Q?"}), "target": "100"}
        job = await self.job("tg_in", data)
        with self.assertRaises(Permanent):
            await self.bridge.prepare_tg(job, data)
        self.assertIsNone(await self.store.claim("to_max"))

    async def test_retry_budget_eventually_stops_definite_failures(self):
        job = await self.job("to_tg", {"text": "hi"})
        job["attempts"] = self.cfg.max_attempts - 1
        await self.store.fail(job, Retry("network unavailable"))
        self.assertEqual((await self.state(job))["state"], "dead")

    async def test_download_zero_body_is_not_success(self):
        async def fake(options, timeout, destination=None, limit=None):
            destination.write_bytes(b"")
            Path(dict(options)["dump-header"]).write_text("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
            return 0, b"", 0
        downloads = Downloads(self.cfg, NS(run=fake, base=Curl.base))
        with patch("telemax.public_resolve", new=AsyncMock(return_value=("cdn.example.test", "8.8.8.8"))):
            with self.assertRaises(Retry):
                await downloads.fetch("https://cdn.example.test/test", self.cfg.media / "empty.bin")
        self.assertFalse(list(self.cfg.media.iterdir()))



@unittest.skipUnless(shutil.which("curl"), "curl is not installed")
class LocalCurlTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.response = b'{"ok":true,"result":{"message_id":42}}'
        self.requests = []
        self.accepted = asyncio.Event()
        self.hang = False
        self.handlers = set()
        self.server = await asyncio.start_server(self.handle, "127.0.0.1", 0)
        self.url = f"http://127.0.0.1:{self.server.sockets[0].getsockname()[1]}/test"

    async def handle(self, reader, writer):
        task = asyncio.current_task()
        self.handlers.add(task)
        try:
            head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 5)
            headers = {}
            for line in head.split(b"\r\n")[1:]:
                if b":" in line:
                    k, v = line.split(b":", 1)
                    headers[k.strip().lower()] = v.strip()
            if headers.get(b"expect", b"").lower() == b"100-continue":
                writer.write(b"HTTP/1.1 100 Continue\r\n\r\n")
                await writer.drain()
            body = await asyncio.wait_for(reader.readexactly(int(headers.get(b"content-length", b"0"))), 5)
            self.requests.append((head, headers, body))
            self.accepted.set()
            if self.hang:
                await reader.read()
                return
            payload = (b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: "
                       + str(len(self.response)).encode() + b"\r\nConnection: close\r\n\r\n" + self.response)
            writer.write(payload)
            await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionError, asyncio.TimeoutError):
            pass
        finally:
            writer.close()
            with contextlib.suppress(ConnectionError):
                await writer.wait_closed()
            self.handlers.discard(task)

    async def asyncTearDown(self):
        self.server.close()
        await self.server.wait_closed()
        tasks = list(self.handlers)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.tmp.cleanup()

    def options(self):
        # HTTP is allowed ONLY in this loopback transport test. The production base
        # options restrict traffic to HTTPS and explicitly select the proxy.
        return Curl.base(self.url, 5, None) + [("proto", "=http")]

    async def test_real_json_post_preserves_unicode_and_markup(self):
        text = 'Unicode 🙂 <b>&</b> "quotes"\nnew line'
        opts = self.options() + [("header", "Content-Type: application/json"),
                                 ("data-binary", jdump({"text": text}))]
        rc, body, _ = await Curl().run(opts, 5)
        self.assertEqual(rc, 0)
        self.assertTrue(decode_api(rc, body).ok)
        self.assertEqual(json.loads(self.requests[0][2])["text"], text)

    async def test_real_multipart_preserves_literal_form_value_and_file_bytes(self):
        path = self.root / 'upload space,semi;.bin'
        path.write_bytes(b"file\x00bytes")
        escaped = str(path).replace("\\", "\\\\").replace('"', '\\"')
        opts = self.options() + [("form-string", "caption=@literal;type=text/html"),
                                 ("form", f'document=@"{escaped}"')]
        rc, _, _ = await Curl().run(opts, 5)
        self.assertEqual(rc, 0)
        body = self.requests[0][2]
        self.assertIn(b"@literal;type=text/html", body)
        self.assertIn(b"file\x00bytes", body)

    async def test_real_streamed_download_flushes_exact_bytes(self):
        self.response = b"data" * 8192
        path = self.root / "result.bin"
        rc, body, size = await Curl().run(self.options(), 5, path, limit=len(self.response))
        self.assertEqual((rc, body, size), (0, b"", len(self.response)))
        self.assertEqual(path.read_bytes(), self.response)

    async def test_real_response_size_limit(self):
        self.response = b"a" * 10000
        with self.assertRaisesRegex(Permanent, "size limit"):
            await Curl().run(self.options(), 5, limit=100)

    async def test_real_cancel_kills_curl_child(self):
        self.hang = True
        children = []
        original = asyncio.create_subprocess_exec
        async def spawn(*args, **kwargs):
            child = await original(*args, **kwargs)
            children.append(child)
            return child
        with patch("telemax.asyncio.create_subprocess_exec", new=spawn):
            task = asyncio.create_task(Curl().run(self.options(), 5))
            try:
                await asyncio.wait_for(self.accepted.wait(), 3)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        self.assertTrue(children)
        self.assertIsNotNone(children[0].returncode)

    async def test_secrets_are_sent_on_stdin_not_process_arguments(self):
        calls = []
        original = asyncio.create_subprocess_exec
        async def spawn(*args, **kwargs):
            calls.append(args)
            return await original(*args, **kwargs)
        with patch("telemax.asyncio.create_subprocess_exec", new=spawn):
            opts = self.options() + [("data-binary", "123:fake_test_secret")]
            rc, _, _ = await Curl().run(opts, 5)
        self.assertEqual(rc, 0)
        self.assertNotIn("fake_test_secret", str(calls))
        self.assertEqual(self.requests[0][2], b"123:fake_test_secret")


try:
    importlib.metadata.version("maxapi-python")
    SDK_PRESENT = True
except importlib.metadata.PackageNotFoundError:
    SDK_PRESENT = False


@unittest.skipUnless(SDK_PRESENT, "maxapi-python is not installed; SDK checks require the service venv")
class InstalledSDKTests(unittest.TestCase):
    def test_exact_version_and_media_exports(self):
        client, config, media = app.load_sdk()
        self.assertEqual(importlib.metadata.version("maxapi-python"), "2.4.1")
        self.assertEqual(set(media), {"photo", "video", "document", "voice", "video_note"})
        self.assertIsNone(config(proxy=None).proxy)
        self.assertTrue(callable(client))

    def test_message_service_signature(self):
        app.load_sdk()
        from pymax.api.messages.service import MessageService
        params = inspect.signature(MessageService.send_message).parameters
        self.assertTrue({"chat_id", "text", "attachments"} <= set(params))
        params = inspect.signature(MessageService.get_file_by_id).parameters
        self.assertTrue({"chat_id", "message_id", "file_id"} <= set(params))

    def test_native_file_constructor(self):
        _, _, media = app.load_sdk()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "file.txt"
            path.write_text("SDK contract test")
            attachment = media["document"](path=str(path))
            self.assertIsNotNone(attachment)

    def test_native_photo_normalization(self):
        app.load_sdk()
        from pymax.types.domain.attachments.photo import PhotoAttachment
        photo = PhotoAttachment(base_url="https://cdn.example.test/photo", height=1,
                                width=1, photo_id=42, photo_token="token", _type="PHOTO")
        data = app.normalize_attachment(photo, 100, "77")
        self.assertEqual(data["type"], "PHOTO")
        self.assertEqual(data["photo_id"], 42)
        self.assertEqual(data["base_url"], "https://cdn.example.test/photo")



class Release351UnitTests(unittest.TestCase):
    def test_control_metadata_is_preserved(self):
        f = app.normalize_attachment({"_type": "CONTROL", "event": "USER_ADDED", "title": "А & Б"}, 100, 2)
        self.assertEqual(f["event"], "USER_ADDED")
        self.assertEqual(f["type"], "CONTROL")
        self.assertEqual(app.control_text(f), "[Событие MAX: USER_ADDED] А & Б")

    def test_unknown_control_code_is_not_mislabeled(self):
        self.assertEqual(app.control_text({"event": "FUTURE_EVENT"}), "[Событие MAX: FUTURE_EVENT]")

    def test_snapshot_preserves_unknown_nested_fields_and_repeated_values(self):
        source = {"x": 1, "same": 1, "nested": {"extra": ["same", "same", None]}, "preview": b"\x00\xff"}
        result = app.message_json(source)
        self.assertEqual(result["nested"], source["nested"])
        self.assertEqual(result["x"], result["same"])
        self.assertEqual(result["preview"], {"__encoding__": "base64", "data": "AP8="})
        json.dumps(result, allow_nan=False)

    def test_snapshot_uses_public_model_not_client_bindings(self):
        class Model:
            _client_secret = "DO_NOT_COPY"
            def model_dump(self, **kwargs):
                self.kwargs = kwargs
                return {"id": 1, "attaches": [{"event": "rename", "_type": "CONTROL"}], "extra": 3}
        obj = Model()
        result = app.message_json(obj)
        self.assertEqual(result["extra"], 3)
        self.assertTrue(obj.kwargs["by_alias"])
        self.assertNotIn("DO_NOT_COPY", json.dumps(result))

    def test_snapshot_cycles_do_not_crash(self):
        obj = {"items": []}
        obj["items"].append(obj)
        self.assertEqual(app.message_json(obj)["items"][0], {"__circular_reference__": "dict"})

    def test_timestamps_seconds_and_milliseconds_agree(self):
        self.assertEqual(app.display_time(1700000000), app.display_time(1700000000000, milliseconds=True))
        self.assertRegex(app.display_time(1700000000), r"[+-]\d\d:\d\d$")
        for value in (None, True, "bad", float("inf"), -1):
            self.assertEqual(app.display_time(value), "неизвестно")

    def test_control_repair_leaves_ordinary_files_intact(self):
        doc = {"type": "FILE", "path": "file.bin"}
        result, changed = app.remove_controls({"text": "text", "files": [{"unsupported": "CONTROL"}, doc]})
        self.assertTrue(changed)
        self.assertEqual(result["files"], [doc])
        self.assertIn("text", result["text"])

    def test_full_diagnostic_budget_is_optional(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "constants.json"
            data = {"MAX_PHONE": "+70000000000", "TG_BOT_TOKEN": "123:x", "TG_CHAT_ID": -100}
            p.write_text(jdump(data))
            self.assertEqual(Config.load(p).error_dump_limit, 256 * 1024**2)
            p.write_text(jdump({**data, "ERROR_DUMP_LIMIT_MB": 8}))
            self.assertEqual(Config.load(p).error_dump_limit, 8 * 1024**2)


class Release351Tests(BridgeFixture):
    async def command(self, text, thread=42, sender=7):
        data = {"message": self.tgmsg(text, message_thread_id=thread, **{"from": {"id": sender}})}
        job = await self.job("command", data, key="tg:" + uuid.uuid4().hex)
        try:
            await self.bridge.execute_command(job, data)
        except (Permanent, Retry, Uncertain) as exc:
            await self.store.fail(job, exc)
            raise
        return job

    async def receive(self, payload):
        await self.bridge.on_message(payload)
        await self.bridge.import_inbox_once()
        job = await self.store.claim("max_in")
        if job:
            await self.bridge.prepare_max(job, json.loads(job["payload"]))
        return job

    async def notices(self):
        return await self.store.read("SELECT * FROM tm_jobs WHERE kind='notice'")

    async def test_control_only_event_becomes_text_and_sends(self):
        await self.receive({"id": 900, "chatId": 100, "sender": 2, "time": 1700000000000,
                            "attaches": [{"_type": "CONTROL", "event": "USER_ADDED", "title": "А & Б"}]})
        job = await self.store.claim("to_tg")
        data = json.loads(job["payload"])
        self.assertFalse(data["files"])
        self.assertIn("USER_ADDED", data["text"])
        await self.bridge.send_tg(job, data)
        self.assertEqual((await self.state(job))["state"], "done")
        self.assertEqual(self.bridge.tg.call.call_args.args[0], "sendMessage")

    async def test_text_control_photo_preserves_text_and_photo(self):
        await self.receive({"id": 901, "chat_id": 100, "sender": 2, "text": "caption",
            "attaches": [{"type": "CONTROL", "event": "PIN"}, {"type": "PHOTO", "photo_id": 23, "base_url": "https://cdn.test/p"}]})
        rows = await self.store.read("SELECT * FROM tm_jobs WHERE kind='to_tg' ORDER BY id")
        self.assertEqual(len(rows), 2)
        text, media = (json.loads(row["payload"]) for row in rows)
        self.assertFalse(text["files"])
        self.assertIn("caption", text["text"])
        self.assertIn("PIN", text["text"])
        self.assertEqual(media["files"][0]["type"], "PHOTO")
        self.assertEqual(media["text"], "")

    async def test_forwarded_control_keeps_source_context(self):
        await self.receive({"id": 902, "chat_id": 100, "sender": 2, "link": {"type": "FORWARD", "chatId": 200,
            "message": {"id": 44, "sender": 3, "text": "forward", "attaches": [{"type": "CONTROL", "event": "TITLE", "title": "new"}]}}})
        child = (await self.store.read("SELECT * FROM tm_jobs WHERE kind='to_tg'"))[0]
        data = json.loads(child["payload"])
        self.assertIn("forward", data["text"])
        self.assertIn("TITLE", data["text"])
        self.assertFalse(data["files"])

    async def test_saved_350_control_dlq_can_be_retried(self):
        data = {"text": "original\n[Вложение CONTROL: формат не поддерживается; сохранено в DLQ]",
                "files": [{"type": "CONTROL", "unsupported": "CONTROL", "kind": "document"}]}
        old = await self.job("to_tg", data, key="max:100:99/0")
        await self.store.fail(old, Permanent("Unsupported MAX attachment type: CONTROL"))
        await self.command(f"/retry_dlq {old['id']}")
        retried = await self.store.claim("to_tg")
        await self.bridge.send_tg(retried, json.loads(retried["payload"]))
        child = await self.store.claim("to_tg")
        repaired = json.loads(child["payload"])
        self.assertFalse(repaired["files"])
        self.assertNotIn("сохранено в DLQ", repaired["text"])
        await self.bridge.send_tg(child, repaired)
        self.assertEqual((await self.state(child))["state"], "done")

    async def test_long_control_is_split_not_truncated(self):
        title = "Z" * 9000
        await self.receive({"id": 903, "chat_id": 100, "attaches": [{"type": "CONTROL", "event": "LONG", "title": title}]})
        rows = await self.store.read("SELECT payload FROM tm_jobs WHERE kind='to_tg' ORDER BY id")
        self.assertTrue(all(utf16len(json.loads(r["payload"])["text"]) <= 4000 for r in rows))
        self.assertIn(title, "".join(json.loads(r["payload"])["text"] for r in rows))

    async def test_other_unsupported_type_is_not_silently_discarded(self):
        parent = await self.receive({"id": 904, "chat_id": 100, "text": "keep body", "attaches": [{"type": "FUTURE_FILE", "extra": 99}]})
        row = await self.store.claim("to_tg")
        payload = json.loads(row["payload"])
        self.assertFalse(payload["files"])
        self.assertIn("FUTURE_FILE", payload["text"])
        self.assertIn("keep body", payload["text"])
        await self.bridge.send_tg(row, payload)
        self.assertEqual((await self.state(row))["state"], "done")
        diagnostic = json.loads((self.cfg.errors / f"job-{parent['id']}.json").read_text())
        self.assertEqual(diagnostic["original_message"]["attaches"][0]["extra"], 99)

    async def test_error_dump_full_original_and_general_notice(self):
        raw = {"id": 905, "chatId": 100, "sender": 2, "time": 1700000000000,
               "extra_not_normalized": {"a": [1, 2]}, "attaches": [{"type": "FUTURE", "opaque": "secret-signed-url"}]}
        await self.receive(raw)
        child = await self.store.claim("to_tg")
        await self.store.fail(child, Permanent("bad attachment"))
        await self.bridge.report_error(child)
        path = self.cfg.errors / f"job-{child['id']}.json"
        dump = json.loads(path.read_text())
        self.assertEqual(dump["original_message"], raw)
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        self.assertEqual(self.cfg.errors.stat().st_mode & 0o777, 0o700)
        notices = await self.notices()
        self.assertEqual(len(notices), 1)
        notice = json.loads(notices[0]["payload"])
        self.assertIsNone(notice["thread"])
        self.assertIn("Время сообщения: " + app.display_time(raw["time"], milliseconds=True), notice["text"])
        self.assertIn("Group", notice["text"])
        self.assertIn("42", notice["text"])
        self.assertNotIn("secret-signed-url", notice["text"])

    async def test_error_report_is_idempotent(self):
        row = await self.job("to_tg", {"text": "test"})
        await self.store.fail(row, Permanent("fail"))
        await asyncio.gather(self.bridge.report_error(row), self.bridge.report_error(row))
        self.assertEqual(len(await self.notices()), 1)
        self.assertEqual(len(list(self.cfg.errors.glob("*.json"))), 1)

    async def test_failed_error_notice_does_not_recurse(self):
        row = await self.job("to_tg", {"text": "test"})
        await self.store.fail(row, Permanent("fail"))
        await self.bridge.report_error(row)
        notice = await self.store.claim("notice")
        await self.store.fail(notice, Permanent("no permission"))
        await self.bridge.report_error(notice)
        self.assertEqual(len(await self.notices()), 1)

    async def test_transient_error_does_not_dump_or_alert(self):
        row = await self.job("to_tg", {"text": "test"})
        await self.store.fail(row, Retry("slow", 10))
        await self.bridge.report_error(row)
        self.assertFalse(await self.notices())
        self.assertFalse(self.cfg.errors.exists())

    async def test_dump_failure_does_not_claim_file_exists(self):
        row = await self.job("to_tg", {"text": "private-body"})
        await self.store.fail(row, Permanent("fail"))
        with patch.object(self.bridge, "write_error_dump", side_effect=OSError("disk full")):
            await self.bridge.report_error(row)
        notice = json.loads((await self.notices())[0]["payload"])
        self.assertIn("JSON не записан", notice["text"])
        self.assertIn("private-body", (await self.state(row))["payload"])
        self.assertEqual(json.loads(await self.store.meta(f"diagnostic:{row['id']}"))["file"], "")

    async def test_symlink_diagnostic_directory_is_rejected(self):
        elsewhere = self.cfg.root / "elsewhere"
        elsewhere.mkdir()
        (self.cfg.root / "dumps").symlink_to(elsewhere, target_is_directory=True)
        with self.assertRaises(OSError):
            self.bridge.write_error_dump(1, {"test": "private"})
        self.assertFalse(list(elsewhere.iterdir()))

    async def test_diagnostic_quota_never_truncates_message(self):
        self.bridge.cfg = dataclasses.replace(self.cfg, error_dump_limit=100)
        with self.assertRaises(OSError):
            self.bridge.write_error_dump(1, {"test": "private" * 100})
        self.assertFalse(list(self.cfg.errors.glob("*.json")))

    async def test_telegram_full_update_and_date_are_kept(self):
        msg = self.tgmsg("private", date=1700000000, extra={"opaque": [1, 2]})
        update = {"update_id": 51, "message": msg, "future_extra": True}
        await self.store.accept_updates([update])
        src = await self.store.claim("tg_in")
        await self.bridge.prepare_tg(src, json.loads(src["payload"]))
        child = await self.store.claim("to_max")
        await self.store.fail(child, Uncertain("disconnected"))
        await self.bridge.report_error(child)
        dump = json.loads((self.cfg.errors / f"job-{child['id']}.json").read_text())
        self.assertEqual(dump["original_message"], update)
        self.assertEqual(dump["message_time"], app.display_time(1700000000))
        self.assertIn("MAX chat 100", dump["recipient"])

    async def test_old_350_normalized_data_is_not_claimed_as_full(self):
        data = {"id": 50, "chat_id": 100, "time": 1700000000000, "text": "only normalized"}
        parent = await self.job("max_in", data, key="max:100:50")
        await self.store.expand(parent, [("to_tg", "100", {"text": "only normalized"})])
        child = await self.store.claim("to_tg")
        await self.store.fail(child, Permanent("fail"))
        await self.bridge.report_error(child)
        dump = json.loads((self.cfg.errors / f"job-{child['id']}.json").read_text())
        self.assertIn("normalized message only", dump["source_fidelity"])
        self.assertEqual(dump["message_time"], app.display_time(data["time"], milliseconds=True))

    async def test_dump_cleanup_keeps_active_failures(self):
        name = self.bridge.write_error_dump(31, {"message": "full"})
        path = self.cfg.root / name
        os.utime(path, (1, 1))
        self.bridge.cleanup_error_dumps({31})
        self.assertTrue(path.exists())
        self.bridge.cleanup_error_dumps(set())
        self.assertFalse(path.exists())

    async def test_mute_suppresses_existing_and_new_forwards(self):
        await self.store.add("waiting", "to_tg", "100", {"text": "queued"})
        await self.command("/mute")
        self.assertTrue(await self.store.muted("100"))
        self.assertEqual((await self.store.read("SELECT state FROM tm_jobs WHERE key='waiting'"))[0]["state"], "cancelled")
        await self.bridge.on_message({"id": 2, "chat_id": 100, "sender": 2, "text": "while muted"})
        await self.bridge.import_inbox_once()
        self.assertIsNone(await self.store.claim("max_in"))
        row = (await self.store.read("SELECT * FROM tm_jobs WHERE key='max:100:2'"))[0]
        self.assertEqual(row["state"], "cancelled")
        self.assertEqual(json.loads(row["result"])["suppressed"], "muted")

    async def test_unmute_does_not_replay_muted_messages(self):
        await self.command("/mute")
        await self.bridge.on_message({"id": 3, "chat_id": 100, "sender": 2, "text": "muted"})
        await self.bridge.import_inbox_once()
        await self.command("/unmute")
        self.assertFalse(await self.store.muted("100"))
        self.assertIsNone(await self.store.claim("max_in"))
        await self.bridge.on_message({"id": 4, "chat_id": 100, "sender": 2, "text": "new"})
        await self.bridge.import_inbox_once()
        row = await self.store.claim("max_in")
        self.assertEqual(json.loads(row["payload"])["id"], 4)

    async def test_mute_survives_restart_without_schema_changes(self):
        tables_before = await self.store.read("SELECT name,sql FROM sqlite_master ORDER BY name")
        await self.command("/mute")
        await self.store.close()
        self.store = Store(self.cfg)
        self.assertTrue(await self.store.muted("100"))
        self.assertEqual(await self.store.meta("schema"), app.SCHEMA_VERSION)
        self.assertEqual(tables_before, await self.store.read("SELECT name,sql FROM sqlite_master ORDER BY name"))

    async def test_mute_does_not_affect_other_chat(self):
        await self.command("/mute")
        await self.store.add("other", "to_tg", "200", {"text": "other"})
        row = await self.store.claim("to_tg")
        self.assertEqual(row["route"], "200")

    async def test_mute_does_not_disable_outbound(self):
        await self.command("/mute")
        data = {"text": "outgoing", "files": [], "origin": {"sender": 7}}
        row = await self.job("to_max", data)
        await self.bridge.send_max(row, data)
        self.client.send_message.assert_awaited_once()
        self.assertEqual((await self.state(row))["state"], "done")

    async def test_mute_during_preparation_cannot_be_undone_by_expand(self):
        data = {"id": 6, "chat_id": 100, "sender": 2, "text": "hello"}
        row = await self.job("max_in", data)
        await self.command("/mute")
        await self.store.expand(row, [("to_tg", "100", {"text": "hello"})])
        self.assertEqual((await self.state(row))["state"], "cancelled")
        self.assertFalse(await self.store.read("SELECT * FROM tm_jobs WHERE kind='to_tg'"))

    async def test_mute_while_waiting_to_send_prevents_network_call(self):
        data = {"text": "test", "files": []}
        row = await self.job("to_tg", data)
        async def mute_then_call(callback):
            await self.command("/mute")
            return await callback()
        self.bridge.tg.paced = mute_then_call
        with self.assertRaises(app.Suppressed):
            await self.bridge.send_tg(row, data)
        self.bridge.tg.call.assert_not_awaited()
        self.assertEqual((await self.state(row))["state"], "cancelled")

    async def test_inflight_delivery_is_not_marked_cancelled(self):
        row = await self.job("to_tg", {"text": "already started"})
        self.assertTrue(await self.store.forward_allowed(row, sending=True))
        await self.command("/mute")
        self.assertEqual((await self.state(row))["state"], "running")
        await self.store.finish(row, {"ids": [8]})
        self.assertEqual((await self.state(row))["state"], "done")

    async def test_error_after_suppression_does_not_make_dlq(self):
        row = await self.job("to_tg", {"text": "test"})
        await self.command("/mute")
        await self.store.fail(row, Permanent("download failed"))
        self.assertEqual((await self.state(row))["state"], "cancelled")

    async def test_mute_admin_only(self):
        self.bridge.cfg = dataclasses.replace(self.cfg, allowed=frozenset({7, 8}))
        await self.command("/mute", sender=8)
        self.assertFalse(await self.store.muted("100"))
        self.assertIn("TG_ADMIN_USER_IDS", json.loads((await self.notices())[0]["payload"])["text"])

    async def test_mute_from_general_requires_topic_id(self):
        with self.assertRaises(Permanent):
            await self.command("/mute", thread=None)
        await self.command("/mute 42", thread=None)
        self.assertTrue(await self.store.muted("100"))

    async def test_muted_list_and_unmute_from_general(self):
        await self.command("/mute 42", thread=None)
        await self.command("/muted", thread=None)
        text = "\n".join(json.loads(row["payload"])["text"] for row in await self.notices())
        self.assertIn("Group · topic 42", text)
        await self.command("/unmute 42", thread=None)
        self.assertFalse(await self.store.muted("100"))

    async def test_mute_does_not_cancel_dead_or_uncertain(self):
        row = await self.job("to_tg", {"text": "failed"})
        await self.store.fail(row, Uncertain("result unknown"))
        await self.command("/mute")
        self.assertEqual((await self.state(row))["state"], "uncertain")

    async def configure_new_chat(self, uid=44):
        self.client.get_user = AsyncMock(return_value=NS(id=uid, first_name="Иван", last_name="Петров"))
        self.client.search_by_phone = AsyncMock(return_value=NS(id=uid, first_name="Иван", last_name="Петров"))
        self.client.get_chat_id = lambda first, second: first ^ second
        self.bridge.tg.call.return_value = ApiResult(ok=True, result={"message_thread_id": 81, "message_id": 82})

    async def test_new_chat_from_general_uses_dialog_id_not_user_id(self):
        await self.configure_new_chat()
        await self.command("/chat 44", thread=None)
        rows = await self.store.read("SELECT * FROM tm_routes WHERE max_id='45'")
        self.assertEqual(rows[0]["thread_id"], 81)
        self.assertEqual(rows[0]["type"], "private")
        self.assertEqual(rows[0]["name"], "Иван Петров")
        self.client.send_message.assert_not_awaited()
        self.assertEqual(self.bridge.tg.call.call_args.args[0], "createForumTopic")

    async def test_new_chat_by_phone_with_alias(self):
        await self.configure_new_chat()
        await self.command("/chat +79991234567 Иван работа", thread=None)
        self.client.search_by_phone.assert_awaited_once_with("+79991234567")
        row = (await self.store.read("SELECT * FROM tm_routes WHERE max_id='45'"))[0]
        self.assertEqual(row["name"], "Иван работа")
        self.client.send_message.assert_not_awaited()

    async def test_new_chat_binds_empty_topic(self):
        await self.configure_new_chat()
        await self.command("/chat 44", thread=80)
        row = (await self.store.read("SELECT * FROM tm_routes WHERE max_id='45'"))[0]
        self.assertEqual(row["thread_id"], 80)
        self.bridge.tg.call.assert_not_awaited()

    async def test_new_chat_cannot_steal_bound_topic(self):
        await self.configure_new_chat()
        with self.assertRaises(Permanent):
            await self.command("/chat 44", thread=42)
        self.assertFalse(await self.store.read("SELECT * FROM tm_routes WHERE max_id='45'"))
        self.bridge.tg.call.assert_not_awaited()

    async def test_repeated_chat_reuses_topic_and_never_sends_greeting(self):
        await self.configure_new_chat()
        await self.command("/chat 44", thread=None)
        await self.command("/chat 44", thread=None)
        self.assertEqual(self.bridge.tg.call.await_count, 1)
        self.client.send_message.assert_not_awaited()

    async def test_unknown_or_self_user_is_rejected(self):
        await self.configure_new_chat(uid=1)
        with self.assertRaises(Permanent):
            await self.command("/chat 1", thread=None)
        self.client.get_user.return_value = None
        with self.assertRaises(Permanent):
            await self.command("/chat 999", thread=None)
        self.bridge.tg.call.assert_not_awaited()

    async def test_new_chat_admin_only(self):
        await self.configure_new_chat()
        self.bridge.cfg = dataclasses.replace(self.cfg, allowed=frozenset({7, 8}))
        await self.command("/chat 44", thread=None, sender=8)
        self.client.get_user.assert_not_awaited()
        self.bridge.tg.call.assert_not_awaited()

    async def test_new_chat_unknown_creation_does_not_repeat(self):
        await self.configure_new_chat()
        self.bridge.tg.call.return_value = ApiResult(ambiguous=True, error="timeout")
        for _ in range(2):
            with self.assertRaises(Permanent):
                await self.command("/chat 44", thread=None)
        self.assertEqual(self.bridge.tg.call.await_count, 1)
        row = (await self.store.read("SELECT * FROM tm_routes WHERE max_id='45'"))[0]
        self.assertEqual(row["state"], "uncertain")

    async def test_failed_chat_notice_identifies_resolved_recipient(self):
        await self.configure_new_chat()
        self.bridge.tg.call.return_value = ApiResult(ambiguous=True, error="timeout")
        data = {"message": self.tgmsg("/chat 44", message_thread_id=None)}
        job = await self.job("command", data, route="thread:None")
        with self.assertRaises(Permanent) as caught:
            await self.bridge.execute_command(job, data)
        await self.store.fail(job, caught.exception)
        detail, summary = await self.bridge.error_details(await self.state(job))
        self.assertIn("MAX user 44; MAX chat 45", summary)
        self.assertIn("Иван Петров", detail["recipient"])
        self.assertEqual(detail["route"]["max_id"], "45")

    async def test_first_message_in_new_topic_goes_to_resolved_dialog(self):
        await self.configure_new_chat()
        await self.command("/chat 44", thread=None)
        await self.store.accept_updates([{"update_id": 100, "message": self.tgmsg("Hello", message_thread_id=81)}])
        parent = await self.store.claim("tg_in")
        await self.bridge.prepare_tg(parent, json.loads(parent["payload"]))
        child = await self.store.claim("to_max")
        await self.bridge.send_max(child, json.loads(child["payload"]))
        self.assertEqual(self.client.send_message.call_args.kwargs["chat_id"], 45)
        self.assertEqual(self.client.send_message.call_args.kwargs["text"], "Hello")

    async def test_general_is_never_bound_as_conversation_topic(self):
        with self.assertRaises(Permanent):
            await self.command("/mute", thread=1)
        await self.configure_new_chat()
        await self.command("/chat 44", thread=1)
        row = (await self.store.read("SELECT * FROM tm_routes WHERE max_id='45'"))[0]
        self.assertEqual(row["thread_id"], 81)
        self.assertTrue(all(json.loads(n["payload"])["thread"] != 1 for n in await self.notices()))

    async def test_350_meta_accepted_and_not_reimported(self):
        await self.store.set_meta("created_by", "3.5.0")
        await self.store.add("existing350", "to_tg", "100", {"text": "preserve"})
        before = await self.store.read("SELECT * FROM tm_jobs")
        await self.store.close()
        self.store = Store(self.cfg)
        Store.check(self.cfg)
        self.assertEqual(await self.store.meta("created_by"), "3.5.0")
        self.assertEqual(await self.store.read("SELECT * FROM tm_jobs"), before)
        self.assertEqual(await self.store.meta("schema"), app.SCHEMA_VERSION)


@unittest.skipUnless(SDK_PRESENT, "maxapi-python is not installed; run in service venv")
class Release351SDKTests(unittest.TestCase):
    def test_public_private_chat_methods_match_used_signatures(self):
        Client, _, _ = app.load_sdk()
        self.assertEqual(set(inspect.signature(Client.get_chat_id).parameters), {"self", "first_user_id", "second_user_id"})
        self.assertIn("phone", inspect.signature(Client.search_by_phone).parameters)
        self.assertIn("user_id", inspect.signature(Client.get_user).parameters)
        from pymax.api.users.service import UserService
        self.assertEqual(UserService(NS()).get_chat_id(1, 44), 45)

    def test_real_control_model_full_snapshot(self):
        app.load_sdk()
        from pymax.types.domain.attachments.control import ControlAttachment
        model = ControlAttachment(_type="CONTROL", event="USER_ADDED", title="name")
        self.assertEqual(app.normalize_attachment(model, 100, 9)["event"], "USER_ADDED")
        self.assertEqual(app.message_json(model)["event"], "USER_ADDED")



class Release351LifecycleTests(BridgeFixture):
    async def maintenance_once(self):
        with patch("telemax.asyncio.sleep", new=AsyncMock(side_effect=asyncio.CancelledError)):
            with self.assertRaises(asyncio.CancelledError):
                await self.bridge.maintenance()

    async def test_maintenance_snapshots_existing_350_failure(self):
        row = await self.job("to_tg", {"text": "older"})
        await self.store.fail(row, Permanent("older failure"))
        await self.maintenance_once()
        self.assertTrue((self.cfg.errors / f"job-{row['id']}.json").exists())
        count = len(await self.store.read("SELECT * FROM tm_jobs WHERE kind='notice'"))
        await self.maintenance_once()
        self.assertEqual(len(await self.store.read("SELECT * FROM tm_jobs WHERE kind='notice'")), count)

    async def test_failed_dump_is_retried_without_duplicate_notice(self):
        row = await self.job("to_tg", {"text": "older"})
        await self.store.fail(row, Permanent("older failure"))
        with patch.object(self.bridge, "write_error_dump", side_effect=OSError()):
            await self.maintenance_once()
        await self.maintenance_once()
        self.assertTrue((self.cfg.errors / f"job-{row['id']}.json").exists())
        self.assertEqual(len(await self.store.read("SELECT * FROM tm_jobs WHERE kind='notice'")), 1)

    async def test_new_failure_after_previous_report_is_reported(self):
        row = await self.job("to_tg", {"text": "test"})
        await self.store.fail(row, Permanent("first"))
        await self.bridge.report_error(row)
        await self.store.tx(lambda c: c.execute("UPDATE tm_jobs SET state='running',phase='send' WHERE id=?", (row["id"],)))
        await self.store.close()
        self.store = Store(self.cfg)
        self.bridge.store = self.store
        self.bridge.tg.store = self.store
        await self.maintenance_once()
        dump = json.loads((self.cfg.errors / f"job-{row['id']}.json").read_text())
        self.assertEqual(dump["job"]["state"], "uncertain")
        self.assertEqual(len(await self.store.read("SELECT * FROM tm_jobs WHERE kind='notice'")), 2)

    async def test_maintenance_preserves_source_of_active_child_beyond_retention(self):
        raw = {"id": 71, "chat_id": 100, "sender": 2, "text": "original", "extra": "must retain"}
        await self.bridge.on_message(raw)
        await self.bridge.import_inbox_once()
        parent = await self.store.claim("max_in")
        await self.bridge.prepare_max(parent, json.loads(parent["payload"]))
        await self.store.tx(lambda c: c.execute("UPDATE tm_jobs SET updated=1 WHERE id=?", (parent["id"],)))
        await self.maintenance_once()
        source = (await self.state(parent))["payload"]
        self.assertEqual(json.loads(source)["raw_message"], raw)
        child = await self.store.claim("to_tg")
        await self.store.finish(child)
        await self.maintenance_once()
        self.assertFalse(await self.store.read("SELECT 1 FROM tm_jobs WHERE id=?", (parent["id"],)))

    async def test_worker_failure_creates_general_notice_and_full_dump(self):
        await self.store.add("worker-failure", "to_tg", "100", {"text": "", "files": [{"type": "FILE", "source": "max", "kind": "document", "path": str(self.cfg.media / "missing.bin") }]})
        task = asyncio.create_task(self.bridge.worker("to_tg"))
        try:
            async def wait_notice():
                while not await self.store.read("SELECT * FROM tm_jobs WHERE kind='notice'"):
                    await asyncio.sleep(0.005)
            await asyncio.wait_for(wait_notice(), 2)
            self.assertTrue(list(self.cfg.errors.glob("*.json")))
            notice = (await self.store.read("SELECT * FROM tm_jobs WHERE kind='notice'"))[0]
            self.assertIsNone(json.loads(notice["payload"])["thread"])
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_invalid_payload_is_still_saved_as_diagnostic(self):
        job = await self.job("to_tg", {"text": "body"})
        await self.store.tx(lambda c: c.execute("UPDATE tm_jobs SET payload='[broken' WHERE id=?", (job["id"],)))
        await self.store.fail(job, Permanent("invalid json"))
        await self.bridge.report_error(job)
        dump = json.loads((self.cfg.errors / f"job-{job['id']}.json").read_text())
        self.assertEqual(dump["payload"]["unparsed_payload"], "[broken")


class Release352Tests(BridgeFixture):
    command = Release351Tests.command

    async def receive(self, payload):
        await self.bridge.on_message(payload)
        await self.bridge.import_inbox_once()
        row = await self.store.claim('max_in')
        if row:
            await self.bridge.prepare_max(row, json.loads(row['payload']))
        return row

    async def test_all_five_nonfile_types_are_text_not_downloads(self):
        raw = {'id': 500, 'chat_id': 100, 'sender': 2, 'text': 'Original link text',
               'attaches': [
                   {'_type': 'SHARE', 'url': 'https://example.test/a', 'title': 'Link title', 'description': 'Description'},
                   {'_type': 'CONTROL', 'event': 'TITLE', 'title': 'new title'},
                   {'_type': 'CONTACT', 'name': 'Alice', 'contactId': 44},
                   {'_type': 'POLL', 'title': 'Lunch?', 'answers': [{'text': 'A'}, {'text': 'B'}], 'state': {'total': 3}},
                   {'_type': 'CALL', 'callType': 'VIDEO', 'duration': 60, 'contactIds': [44]}]}
        await self.receive(raw)
        row = await self.store.claim('to_tg')
        data = json.loads(row['payload'])
        self.assertFalse(data['files'])
        for expected in ('Original link text', 'https://example.test/a', 'Description', 'Alice', '44', 'Lunch?', '1. A', '2. B', 'VIDEO', '60'):
            self.assertIn(expected, data['text'])
        self.bridge.materialize = AsyncMock(return_value=[])
        await self.bridge.send_tg(row, data)
        self.assertEqual(self.bridge.tg.call.call_args.args[0], 'sendMessage')
        self.assertEqual((await self.state(row))['state'], 'done')
        self.assertFalse(await self.store.read("SELECT 1 FROM tm_jobs WHERE state='dead'"))

    async def test_binary_failure_does_not_lose_source_text(self):
        await self.receive({'id': 501, 'chat_id': 100, 'sender': 2, 'text': 'Important text',
                            'attaches': [{'type': 'VIDEO', 'video_id': 1}]})
        text = await self.store.claim('to_tg')
        await self.bridge.send_tg(text, json.loads(text['payload']))
        self.assertIn('Important text', self.bridge.tg.call.call_args.args[1]['text'])
        video = await self.store.claim('to_tg')
        self.assertNotEqual(video['id'], text['id'])
        self.bridge.materialize = AsyncMock(side_effect=Permanent('Video cannot be downloaded'))
        with self.assertRaises(Permanent) as cm:
            await self.bridge.send_tg(video, json.loads(video['payload']))
        await self.store.fail(video, cm.exception)
        self.assertEqual((await self.state(text))['state'], 'done')
        self.assertEqual((await self.state(video))['state'], 'dead')
        self.bridge.tg.call.assert_awaited_once()

    async def test_saved_dead_nonfile_jobs_can_be_retried_without_losing_text(self):
        for i, typ in enumerate(('SHARE', 'CONTROL', 'CONTACT', 'POLL', 'CALL', 'NEW_UNKNOWN')):
            data = {'text': f'body {i}', 'files': [{'type': typ, 'unsupported': typ, 'source': 'max'}]}
            old = await self.job('to_tg', data, key=f'old:{i}')
            await self.store.fail(old, Permanent('Unsupported type'))
            await self.command(f"/retry_dlq {old['id']}")
            retry = await self.store.claim('to_tg')
            await self.bridge.send_tg(retry, json.loads(retry['payload']))
            child = await self.store.claim('to_tg')
            payload = json.loads(child['payload'])
            self.assertIn(f'body {i}', payload['text'])
            self.assertFalse(payload['files'])
            await self.bridge.send_tg(child, payload)
            self.assertEqual((await self.state(child))['state'], 'done')

    async def test_full_queue_spools_then_drains_without_restart(self):
        self.store.cfg = dataclasses.replace(self.cfg, queue_limit=1)
        blocker = await self.job('to_tg', {'text': 'pending'})
        raw = {'id': 600, 'chat_id': 100, 'sender': 2, 'text': 'received during overload'}
        await self.bridge.on_message(raw)
        self.assertEqual(len(list(self.cfg.inbox.glob('*.json'))), 1)
        self.assertEqual(await self.bridge.import_inbox_once(), 0)
        self.assertIsNone(self.bridge.fatal)
        self.assertFalse(self.bridge.stop.is_set())
        await self.store.finish(blocker)
        self.assertEqual(await self.bridge.import_inbox_once(), 1)
        row = await self.store.claim('max_in')
        self.assertEqual(json.loads(row['payload'])['raw_message'], raw)
        self.assertFalse(list(self.cfg.inbox.glob('*.json')))

    async def test_ingress_does_not_access_sqlite(self):
        with patch.object(self.store, 'add', AsyncMock(side_effect=sqlite3.OperationalError('broken DB'))) as add:
            await self.bridge.on_message({'id': 601, 'chat_id': 100, 'sender': 2, 'text': 'safe'})
            add.assert_not_awaited()
        self.assertEqual(len(list(self.cfg.inbox.glob('*.json'))), 1)
        self.assertIsNone(self.bridge.fatal)

    async def test_import_sqlite_error_keeps_envelope(self):
        await self.bridge.on_message({'id': 602, 'chat_id': 100, 'sender': 2, 'text': 'safe'})
        with patch.object(self.store, 'add', AsyncMock(side_effect=sqlite3.OperationalError('injected'))):
            with self.assertRaises(sqlite3.Error):
                await self.bridge.import_inbox_once()
        self.assertEqual(len(list(self.cfg.inbox.glob('*.json'))), 1)
        self.assertEqual(await self.bridge.import_inbox_once(), 1)

    async def test_replay_after_commit_before_unlink_is_idempotent(self):
        await self.bridge.on_message({'id': 603, 'chat_id': 100, 'sender': 2, 'text': 'safe'})
        path = next(self.cfg.inbox.glob('*.json'))
        obj = self.bridge.inbox.read(path)
        await self.store.add(obj['key'], 'max_in', obj['route'], obj['payload'])
        self.bridge.inbox = app.Inbox(self.cfg)
        self.assertEqual(await self.bridge.import_inbox_once(), 1)
        self.assertEqual(len(await self.store.read('SELECT * FROM tm_jobs')), 1)
        self.assertFalse(path.exists())

    async def test_temporary_inbox_write_failure_retries_callback(self):
        put = self.bridge.inbox.put
        calls = 0
        async def unstable(*args):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise OSError('disk write interrupted')
            return await put(*args)
        sleep = asyncio.sleep
        async def fast(_):
            await sleep(0)
        with patch.object(self.bridge.inbox, 'put', unstable), patch.object(app.asyncio, 'sleep', fast):
            await self.bridge.on_message({'id': 604, 'chat_id': 100, 'sender': 2, 'text': 'safe'})
        self.assertEqual(calls, 2)
        self.assertFalse(self.bridge.stop.is_set())
        self.assertEqual(self.bridge.inbox.waiting, 0)
        self.assertEqual(len(list(self.cfg.inbox.glob('*.json'))), 1)

    async def test_spool_quota_never_evicts_unimported_events(self):
        await self.bridge.on_message({'id': 605, 'chat_id': 100, 'sender': 2, 'text': 'keep'})
        path = next(self.cfg.inbox.glob('*.json'))
        before = path.read_bytes()
        self.bridge.inbox.cfg = dataclasses.replace(self.cfg, inbox_limit=len(before))
        with self.assertRaises(CapacityError):
            await self.bridge.inbox.put('another', '100', {'text': 'cannot fit'})
        self.assertEqual(path.read_bytes(), before)
        self.assertEqual(len(list(self.cfg.inbox.glob('*.json'))), 1)

    async def test_spool_restart_recovers_complete_part_and_quarantines_bad_part(self):
        await self.bridge.on_message({'id': 606, 'chat_id': 100, 'sender': 2, 'text': 'recover'})
        path = next(self.cfg.inbox.glob('*.json'))
        path.rename(path.with_suffix('.part'))
        (self.cfg.inbox / 'bad.part').write_text('{incomplete')
        self.bridge.inbox = app.Inbox(self.cfg)
        self.assertEqual(await self.bridge.import_inbox_once(), 1)
        self.assertEqual(len(list((self.cfg.inbox / 'quarantine').iterdir())), 1)
        self.assertEqual(len(await self.store.read('SELECT * FROM tm_jobs')), 1)

    async def test_spool_receipt_during_mute_is_not_replayed_after_unmute(self):
        await self.command('/mute')
        await self.bridge.on_message({'id': 607, 'chat_id': 100, 'sender': 2, 'text': 'muted'})
        await self.command('/unmute')
        await self.bridge.import_inbox_once()
        row = (await self.store.read("SELECT * FROM tm_jobs WHERE key='max:100:607'"))[0]
        self.assertEqual(row['state'], 'cancelled')

    async def test_recoverable_sqlite_write_error_retries_not_external_send(self):
        original = self.store.finish_in_tx
        writes = 0
        def unstable(c, job, result=None):
            nonlocal writes
            writes += 1
            if writes == 1:
                raise sqlite3.OperationalError('disk I/O error')
            return original(c, job, result)
        job = await self.job('to_max', {'text': 'one send only', 'origin': {'sender': 7}})
        sleep = asyncio.sleep
        async def fast(_):
            await sleep(0)
        with patch.object(self.store, 'finish_in_tx', unstable), patch.object(app.asyncio, 'sleep', fast):
            await self.bridge.send_max(job, json.loads(job['payload']))
        self.client.send_message.assert_awaited_once()
        self.assertEqual(writes, 2)
        self.assertEqual((await self.state(job))['state'], 'done')

    async def test_done_job_is_not_reopened_by_post_send_metadata_failure(self):
        job = await self.job('to_tg', {'text': 'delivered'})
        await self.store.finish(job)
        await self.store.fail(job, Uncertain('failure updating statistics'))
        self.assertEqual((await self.state(job))['state'], 'done')

    async def test_count_false_retries_have_a_finite_wall_clock_window(self):
        self.store.cfg = dataclasses.replace(self.cfg, retry_window=5)
        job = await self.job('to_tg', {'text': 'blocked'})
        await self.store.fail(job, Retry('no space', 100000, count=False))
        row = await self.state(job)
        self.assertEqual(row['attempts'], 0)
        self.assertLessEqual(row['next_at'], row['retry_since'] + 5)
        await self.store.tx(lambda c: c.execute('UPDATE tm_jobs SET retry_since=? WHERE id=?', (time.time()-10, job['id'])))
        self.assertIsNone(await self.store.claim('to_tg'))
        self.assertEqual((await self.state(job))['state'], 'dead')
        await self.command(f"/retry_dlq {job['id']}")
        self.assertEqual((await self.state(job))['retry_since'], 0)
        self.assertIsNotNone(await self.store.claim('to_tg'))

    async def test_blocked_retry_does_not_pin_its_own_chat_forever(self):
        old = await self.job('to_tg', {'text': 'old'})
        await self.store.fail(old, Retry('wait', 600, count=False))
        await self.store.add('newer', 'to_tg', '100', {'text': 'newer'})
        ready = await self.store.claim('to_tg')
        self.assertEqual(ready['key'], 'newer')
        self.assertEqual((await self.state(old))['state'], 'pending')

    async def test_running_job_excludes_only_its_own_route(self):
        await self.store.add('a', 'to_tg', '100', {'text': 'a'})
        await self.store.add('b', 'to_tg', '100', {'text': 'b'})
        await self.store.add('c', 'to_tg', '200', {'text': 'c'})
        one, two = await asyncio.gather(self.store.claim('to_tg'), self.store.claim('to_tg'))
        self.assertEqual({one['route'], two['route']}, {'100', '200'})
        self.assertIsNone(await self.store.claim('to_tg'))

    async def test_pacing_lock_is_released_before_slow_network_call(self):
        tg = Telegram(self.cfg, self.store, self.bridge.curl)
        entered, release = asyncio.Event(), asyncio.Event()
        async def slow():
            entered.set()
            await release.wait()
            return 'slow'
        task = asyncio.create_task(tg.paced(slow))
        try:
            await entered.wait()
            # Simulate the legal next start slot without waiting 3.1 seconds in a unit test.
            tg.last_send = 0
            self.assertEqual(await asyncio.wait_for(tg.paced(AsyncMock(return_value='fast')), 1), 'fast')
        finally:
            release.set()
            await task

    async def test_downloads_in_different_jobs_overlap_and_release_reservations(self):
        entered, release = asyncio.Event(), asyncio.Event()
        async def curl_run(opts, timeout, destination=None, limit=None):
            d = dict(opts)
            if 'slow' in d['url']:
                entered.set()
                await release.wait()
            destination.write_bytes(b'abc')
            Path(d['dump-header']).write_text('HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\n')
            return 0, b'', 3
        downloads = Downloads(self.cfg, NS(base=Curl.base, run=curl_run))
        with patch.object(app, 'public_resolve', AsyncMock(return_value=('cdn.test', '8.8.8.8'))):
            slow = asyncio.create_task(downloads.fetch('https://cdn.test/slow', self.cfg.media / 'slow.bin'))
            try:
                await entered.wait()
                fast = await asyncio.wait_for(downloads.fetch('https://cdn.test/fast', self.cfg.media / 'fast.bin'), 1)
                self.assertEqual(fast.read_bytes(), b'abc')
                self.assertFalse(slow.done())
            finally:
                release.set()
                await slow
        self.assertEqual(downloads.reserved, 0)
        self.assertFalse(downloads.inflight)

    async def test_history_rows_deleted_but_recent_dedup_is_retained(self):
        self.store.cfg = dataclasses.replace(self.cfg, history_max_jobs=1)
        for key in ('old1', 'old2', 'newest'):
            job = await self.job('to_tg', {'text': key}, key=key)
            await self.store.finish(job)
        await self.store.prune_history()
        self.assertEqual(len(await self.store.read("SELECT * FROM tm_jobs WHERE state='done'")), 1)
        self.assertEqual(len(await self.store.read('SELECT * FROM tm_seen')), 2)
        self.assertFalse(await self.store.add('old1', 'to_tg', '100', {'text': 'duplicate'}))

    async def test_dedup_tombstones_expire_and_have_a_count_cap(self):
        self.store.cfg = dataclasses.replace(self.cfg, dedup_max_keys=2)
        now = time.time()
        await self.store.tx(lambda c: c.executemany('INSERT INTO tm_seen VALUES(?,?)',
            [('expired', now-1), ('a', now+1), ('b', now+2), ('c', now+3)]))
        await self.store.prune_history()
        seen = {r['key'] for r in await self.store.read('SELECT key FROM tm_seen')}
        self.assertEqual(seen, {'b', 'c'})
        self.assertTrue(await self.store.add('expired', 'max_in', '100', {'text': 'after horizon'}))

    async def test_cleanup_never_discards_dead_uncertain_or_source_data(self):
        await self.store.add('source', 'max_in', '100', {'raw_message': {'secret': 'debug'}})
        parent = await self.store.claim('max_in')
        await self.store.expand(parent, [('to_tg', '100', {'source_key': 'source', 'text': 'x'})])
        child = await self.store.claim('to_tg')
        await self.store.fail(child, Permanent('review needed'))
        uncertain = await self.job('to_max', {'text': 'unknown'}, route='200')
        await self.store.fail(uncertain, Uncertain('check recipient'))
        await self.store.tx(lambda c: c.execute('UPDATE tm_jobs SET updated=1'))
        await self.store.prune_history()
        self.assertEqual((await self.state(parent))['payload'], jdump({'raw_message': {'secret': 'debug'}}))
        self.assertEqual((await self.state(child))['state'], 'dead')
        self.assertEqual((await self.state(uncertain))['state'], 'uncertain')

    async def test_nat64_mapped_and_transition_addresses_are_rejected(self):
        addresses = ('64:ff9b::127.0.0.1', '64:ff9b::10.0.0.1', '64:ff9b::8.8.8.8',
                     '64:ff9b:1::a00:1', '::ffff:8.8.8.8', '2002:0808:0808::1', '2001::1')
        for ip in addresses:
            with self.subTest(ip=ip):
                self.assertFalse(app.public_address(ip))
                record = [(socket.AF_INET6, socket.SOCK_STREAM, 6, '', (ip, 443, 0, 0))]
                with patch.object(socket, 'getaddrinfo', return_value=record):
                    with self.assertRaises(Permanent):
                        await app.public_resolve('https://example.test/file', ipv4_only=False)

    async def test_network_specific_prefix_is_explicitly_denied(self):
        self.assertFalse(app.public_address('2001:4860:abcd::a00:1', ('2001:4860:abcd::/96',)))
        self.assertTrue(app.public_address('2001:4860:4860::8888'))
        self.assertTrue(app.public_address('8.8.8.8'))

    async def test_default_direct_media_resolution_is_ipv4_only(self):
        records = [(socket.AF_INET, socket.SOCK_STREAM, 6, '', ('8.8.8.8', 443))]
        with patch.object(socket, 'getaddrinfo', return_value=records) as resolve:
            await app.public_resolve('https://example.test/file')
        self.assertEqual(resolve.call_args.args[2], socket.AF_INET)


def create_pre352_database(root, schema):
    path = root / 'telegram_queue.db'
    with sqlite_connection(path) as c:
        c.executescript('''
            CREATE TABLE tm_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
            CREATE TABLE tm_routes(max_id TEXT PRIMARY KEY,thread_id INTEGER UNIQUE,name TEXT NOT NULL,
                type TEXT NOT NULL DEFAULT 'group',state TEXT NOT NULL DEFAULT 'new');
            CREATE TABLE tm_jobs(id INTEGER PRIMARY KEY AUTOINCREMENT,key TEXT NOT NULL UNIQUE,
                kind TEXT NOT NULL,route TEXT NOT NULL,payload TEXT NOT NULL,state TEXT NOT NULL DEFAULT 'pending',
                phase TEXT NOT NULL DEFAULT '',attempts INTEGER NOT NULL DEFAULT 0,next_at REAL NOT NULL DEFAULT 0,
                error TEXT NOT NULL DEFAULT '',result TEXT NOT NULL DEFAULT '{}',created REAL NOT NULL,updated REAL NOT NULL);
            CREATE INDEX tm_job_due ON tm_jobs(kind,state,next_at,id);
            CREATE INDEX tm_job_route ON tm_jobs(kind,route,state,id);
        ''')
        meta = [('schema', schema), ('tg_chat_id', '-100'), ('max_account_id', '1'), ('tg_offset', '4242'),
                ('mute:100', jdump({'muted': True, 'thread_id': 42}))]
        if schema == '350':
            meta.append(('tg_bot_id', '123'))
        c.executemany('INSERT INTO tm_meta VALUES(?,?)', meta)
        c.execute("INSERT INTO tm_routes VALUES('100',42,'Existing topic','private','ready')")
        c.execute("INSERT INTO tm_jobs(key,kind,route,payload,state,phase,created,updated) VALUES(?,?,?,?,?,?,?,?)",
                  ('saved', 'to_tg', '100', jdump({'text': 'important', 'files': []}), 'running', 'send', 123, 456))
        if schema == '3':
            c.executescript("CREATE TABLE tm_aliases(max_id TEXT PRIMARY KEY,alias TEXT);"
                            "INSERT INTO tm_aliases VALUES('2','Sender alias');"
                            "CREATE TABLE queue_v2(id INTEGER PRIMARY KEY,text_data TEXT);"
                            "INSERT INTO queue_v2 VALUES(1,'ARCHIVE MUST NOT REPLAY');")
    (root / 'session_cache').mkdir()
    (root / 'session_cache' / 'session.db').write_bytes(b'unchanged session fixture')
    (root / 'media_queue').mkdir()
    (root / 'media_queue' / 'keep.bin').write_bytes(b'unchanged media')
    return path


class Upgrade352Tests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.cfg = Config(self.root, '+70000000000', '123:test', -100, my_max_id=1, min_free=1)
        (self.root / 'constants.json').write_text(jdump({'MAX_PHONE': self.cfg.phone,
            'TG_BOT_TOKEN': self.cfg.token, 'TG_CHAT_ID': self.cfg.chat_id}))
        (self.root / 'telemax.py').write_text('# old deployed script fixture\n')

    def tearDown(self):
        self.temp.cleanup()

    def test_upgrade_350_preserves_jobs_routes_offset_policy_and_session(self):
        path = create_pre352_database(self.root, '350')
        with sqlite_connection(path) as c:
            old_jobs = c.execute('SELECT * FROM tm_jobs').fetchall()
            old_routes = c.execute('SELECT * FROM tm_routes').fetchall()
        snap = Store.upgrade(self.cfg)
        self.assertIsNotNone(snap)
        Store.check(self.cfg)
        with sqlite_connection(path) as c:
            rows = c.execute('SELECT * FROM tm_jobs').fetchall()
            self.assertEqual([r[:-1] for r in rows], old_jobs)
            self.assertEqual(c.execute('SELECT * FROM tm_routes').fetchall(), old_routes)
            meta = dict(c.execute('SELECT * FROM tm_meta'))
            self.assertEqual(meta['tg_offset'], '4242')
            self.assertEqual(meta['schema'], app.SCHEMA_VERSION)
            self.assertEqual(json.loads(meta['mute:100'])['muted'], True)
        self.assertEqual((self.root/'session_cache/session.db').read_bytes(), b'unchanged session fixture')
        self.assertEqual((self.root/'media_queue/keep.bin').read_bytes(), b'unchanged media')
        with sqlite_connection(snap/'telegram_queue.db') as c:
            self.assertEqual(c.execute("SELECT value FROM tm_meta WHERE key='schema'").fetchone()[0], '350')
            self.assertEqual(c.execute('PRAGMA quick_check').fetchone()[0], 'ok')
        self.assertEqual((snap/'constants.json').stat().st_mode & 0o777, 0o600)
        self.assertIsNone(Store.upgrade(self.cfg))
        self.assertEqual(len(list((self.root/'backups').iterdir())), 1)

    def test_legacy_3_requires_explicit_bot_confirmation_and_keeps_archives(self):
        path = create_pre352_database(self.root, '3')
        before = path.read_bytes()
        with self.assertRaisesRegex(RuntimeError, 'confirm-legacy-bot'):
            Store.upgrade(self.cfg)
        self.assertEqual(path.read_bytes(), before)
        Store.upgrade(self.cfg, confirm_legacy_bot=True)
        Store.check(self.cfg)
        with sqlite_connection(path) as c:
            self.assertEqual(c.execute('SELECT COUNT(*) FROM tm_jobs').fetchone()[0], 1)
            self.assertEqual(c.execute('SELECT text_data FROM queue_v2').fetchone()[0], 'ARCHIVE MUST NOT REPLAY')
            self.assertEqual(c.execute('SELECT alias FROM tm_aliases').fetchone()[0], 'Sender alias')
        async def check_runtime():
            store = Store(self.cfg)
            try:
                self.assertEqual((await store.read('SELECT state FROM tm_jobs'))[0]['state'], 'uncertain')
                client = NS(get_user=AsyncMock())
                bridge = Bridge(self.cfg, store, client, {})
                self.assertEqual(await bridge.name_for(2), 'Sender alias')
                client.get_user.assert_not_awaited()
            finally:
                await store.close()
        asyncio.run(check_runtime())

    def test_upgrade_failure_rolls_back_schema_and_retains_backup(self):
        path = create_pre352_database(self.root, '350')
        class Failing(Store):
            @classmethod
            def validate(cls, c, cfg, **kwargs):
                if not kwargs.get('for_upgrade'):
                    raise RuntimeError('injected before COMMIT')
                return Store.validate(c, cfg, **kwargs)
        with self.assertRaisesRegex(RuntimeError, 'injected'):
            Failing.upgrade(self.cfg)
        with sqlite_connection(path) as c:
            self.assertEqual(c.execute("SELECT value FROM tm_meta WHERE key='schema'").fetchone()[0], '350')
            self.assertNotIn('retry_since', {r[1] for r in c.execute('PRAGMA table_info(tm_jobs)')})
            self.assertFalse(c.execute("SELECT 1 FROM sqlite_master WHERE name='tm_seen'").fetchone())
            self.assertEqual(c.execute('SELECT COUNT(*) FROM tm_jobs').fetchone()[0], 1)
        self.assertTrue(list((self.root/'backups').glob('*/telegram_queue.db')))

    def test_different_bot_refused_before_any_upgrade(self):
        path = create_pre352_database(self.root, '350')
        before = path.read_bytes()
        with self.assertRaisesRegex(RuntimeError, 'bot differs'):
            Store.upgrade(dataclasses.replace(self.cfg, token='456:other'))
        self.assertEqual(path.read_bytes(), before)
        self.assertFalse((self.root/'backups').exists())

    def test_compaction_preserves_state_and_enables_incremental_vacuum(self):
        create_pre352_database(self.root, '350')
        Store.upgrade(self.cfg)
        snap = Store.compact(self.cfg)
        Store.check(self.cfg)
        self.assertTrue((snap/'telegram_queue.db').is_file())
        with sqlite_connection(self.root/'telegram_queue.db') as c:
            self.assertEqual(c.execute('PRAGMA auto_vacuum').fetchone()[0], 2)
            self.assertEqual(c.execute('SELECT COUNT(*) FROM tm_jobs').fetchone()[0], 1)
            self.assertEqual(c.execute("SELECT value FROM tm_meta WHERE key='tg_offset'").fetchone()[0], '4242')

    def test_compaction_checks_free_space_before_writes(self):
        create_pre352_database(self.root, '350')
        Store.upgrade(self.cfg)
        with patch.object(app.shutil, 'disk_usage', return_value=NS(free=0)):
            with self.assertRaisesRegex(RuntimeError, 'free space'):
                Store.compact(self.cfg)

    def test_cli_upgrade_is_offline_and_idempotent(self):
        create_pre352_database(self.root, '350')
        cmd = [sys.executable, app.__file__, '--config', str(self.root/'constants.json'), '--upgrade-db']
        first = subprocess.run(cmd, capture_output=True, text=True)
        self.assertEqual(first.returncode, 0, first.stderr)
        second = subprocess.run(cmd, capture_output=True, text=True)
        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertIn('already current', second.stdout)
        Store.check(self.cfg)

    def test_upgrade_does_not_accept_different_v3_bridge_jobs_implementation(self):
        with sqlite_connection(self.root/'telegram_queue.db') as c:
            c.execute('CREATE TABLE bridge_jobs(id INTEGER PRIMARY KEY)')
        with self.assertRaises(RuntimeError):
            Store.upgrade(self.cfg, confirm_legacy_bot=True)
        self.assertFalse((self.root/'backups').exists())


class Additional352Tests(BridgeFixture):
    async def test_transport_gate_does_not_hold_lock_for_upload_duration(self):
        entered, release = asyncio.Event(), asyncio.Event()
        calls = 0
        async def request(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                entered.set()
                await release.wait()
            return 0, b'{"ok":true,"result":{"message_id":1}}', 1
        tg = Telegram(self.cfg, self.store, NS(base=Curl.base, run=request))
        first = asyncio.create_task(tg.call('sendDocument', {'chat_id': -100}))
        try:
            await entered.wait()
            tg.last_send = 0  # Simulate elapsed pacing interval.
            fast = await asyncio.wait_for(tg.call('sendMessage', {'chat_id': -100, 'text': 'fast'}), 1)
            self.assertTrue(fast.ok)
            self.assertFalse(first.done())
        finally:
            release.set()
            await first
        self.assertEqual(calls, 2)

    async def test_two_worker_pool_delivers_other_chat_while_video_waits(self):
        await self.store.tx(lambda c: c.execute("INSERT INTO tm_routes VALUES('200',43,'Other','group','ready')"))
        entered, release = asyncio.Event(), asyncio.Event()
        path = self.cfg.media / 'video.mp4'
        path.write_bytes(b'video')
        async def materialize(job, data):
            if data.get('files'):
                entered.set()
                await release.wait()
                return [path]
            return []
        self.bridge.materialize = materialize
        await self.store.add('slow-video', 'to_tg', '100', {'text':'','files':[{'source':'max','type':'VIDEO','kind':'video'}]})
        await self.store.add('other-text', 'to_tg', '200', {'text':'fast','files':[]})
        workers = [asyncio.create_task(self.bridge.worker('to_tg')) for _ in range(2)]
        try:
            await entered.wait()
            async def wait_fast():
                while (await self.store.read("SELECT state FROM tm_jobs WHERE key='other-text'"))[0]['state'] != 'done':
                    await asyncio.sleep(0.001)
            await asyncio.wait_for(wait_fast(), 1)
            self.assertEqual((await self.store.read("SELECT state FROM tm_jobs WHERE key='slow-video'"))[0]['state'], 'running')
        finally:
            release.set()
            for task in workers:task.cancel()
            await asyncio.gather(*workers, return_exceptions=True)

    async def test_protected_old_parents_do_not_starve_history_cleanup(self):
        def seed(c):
            for i in range(2001):
                Store.insert(c, f'root{i}', 'max_in', '100', {'raw_message':{'n':i}}, 'done')
                Store.insert(c, f'root{i}/0', 'to_tg', '100', {'source_key':f'root{i}'}, 'dead')
            Store.insert(c, 'disposable', 'notice', 'notice:None', {'text':'old'}, 'done')
            c.execute('UPDATE tm_jobs SET updated=1')
        await self.store.tx(seed)
        result = await self.store.prune_history()
        self.assertEqual(result['deleted'], 1)
        self.assertFalse(await self.store.read("SELECT 1 FROM tm_jobs WHERE key='disposable'"))
        self.assertEqual((await self.store.read("SELECT COUNT(*) n FROM tm_jobs WHERE kind='max_in'"))[0]['n'], 2001)

    async def test_error_dump_includes_full_received_share_even_with_fallback_text(self):
        message = {'id':700,'chat_id':100,'sender':2,'text':'link',
                   'attaches':[{'_type':'SHARE','url':'https://example.test','image':{'opaque': 'kept'}}]}
        await self.bridge.on_message(message)
        await self.bridge.import_inbox_once()
        parent = await self.store.claim('max_in')
        await self.bridge.prepare_max(parent, json.loads(parent['payload']))
        child = await self.store.claim('to_tg')
        await self.store.fail(child, Permanent('recipient rejected message'))
        await self.bridge.report_error(child)
        diagnostic = json.loads((self.cfg.errors/f"job-{child['id']}.json").read_text())
        self.assertEqual(diagnostic['original_message'], message)


class Config352Tests(unittest.TestCase):
    def test_new_limits_and_ipv6_options_validate(self):
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)/'constants.json'
            base={'MAX_PHONE':'+70000000000','TG_BOT_TOKEN':'123:x','TG_CHAT_ID':-100}
            for extra in ({'WORKERS':0},{'WORKERS':17},{'DOWNLOAD_WORKERS':17},
                          {'INBOX_LIMIT_MB':False},{'RETRY_WINDOW_SECONDS':0},
                          {'MEDIA_IPV4_ONLY':'false'},{'BLOCKED_IPV6_PREFIXES':['127.0.0.0/8']}):
                with self.subTest(extra=extra):
                    p.write_text(jdump({**base,**extra}))
                    with self.assertRaises(ValueError):
                        Config.load(p)
            p.write_text(jdump({**base,'MEDIA_IPV4_ONLY':False,'BLOCKED_IPV6_PREFIXES':['2001:4860:abcd::/96']}))
            cfg=Config.load(p)
            self.assertFalse(cfg.media_ipv4_only)
            self.assertEqual(cfg.blocked_ipv6_prefixes, ('2001:4860:abcd::/96',))


if __name__ == "__main__":
    unittest.main(verbosity=2)
