#!/usr/bin/env python3
"""Telemax 3.5.0 regression tests. Run: python -m unittest -v telemax_test.

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
        self.assertEqual(row["route"], "200")

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
        data = {"text": "caption", "files": [{"source": "max", "kind": "document", "path": str(self.cfg.media / "missing.bin")}]}
        job = await self.job("to_tg", data)
        with self.assertRaises(Permanent):
            await self.bridge.send_tg(job, data)
        self.bridge.tg.call.assert_not_awaited()
        self.assertNotEqual((await self.state(job))["state"], "done")

    async def test_shared_file_is_not_deleted_after_first_send(self):
        path = self.cfg.media / "shared.jpg"
        path.write_bytes(b"image")
        data = {"text": "caption", "files": [{"source": "max", "kind": "photo", "path": str(path)}]}
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
        data = {"text": "caption", "files": [
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
            self.assertEqual(meta["schema"], "350")
            self.assertEqual(meta["created_by"], "3.5.0")
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
        with self.assertRaisesRegex(RuntimeError, "fresh installation"):
            Store(self.cfg)
        self.assertEqual(path.read_bytes(), before)

    def test_foreign_schema_marker_is_rejected(self):
        Store.create(self.cfg)
        path = self.root / "telegram_queue.db"
        with sqlite_connection(path) as c:
            c.execute("UPDATE tm_meta SET value='3' WHERE key='schema'")
        before = path.read_bytes()
        with self.assertRaisesRegex(RuntimeError, "schema"):
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
        self.assertEqual(result.stdout.strip(), "3.5.0")
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
            "constants.json", ".telemax.lock", "telegram_queue.db", "media_queue", "session_cache"})

    def test_cli_version_needs_no_config(self):
        result = subprocess.run([sys.executable, app.__file__, "--version"], cwd=self.root,
                                capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "Telemax 3.5.0")


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
        await self.bridge.on_message(message)
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
        data = {"text": "caption", "files": files}
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


if __name__ == "__main__":
    unittest.main(verbosity=2)
