# Telemax 3.0 deployment

This guide accompanies **`telemax.py` 3.0.0 + `migrate_telemax.py`**, database schema **3**, and **`maxapi-python==2.4.1`**. Use both Python files from the same release.

For an existing installation, follow **Upgrade**. Migration is a separate, offline step: the application does **not** migrate or create a database at startup. The earlier single-file options `--self-test` and `--check-config` do not apply to this release.

## Requirements

Linux with systemd, Python **3.10+**, `curl`, and a working MAX account. The commands below also use `sqlite3`, `tar`, and an existing Python virtual environment. Run application, test, and migration commands as **`htpc`**, the owner of the existing database—not as root. Use `sudo` only for system administration.

Keep the current installation directory:

```text
/home/htpc/telemax/
├── telemax.py
├── migrate_telemax.py
├── constants.json
├── telegram_queue.db
├── session_cache/          # Existing MAX authentication
├── media_queue/            # Existing files; new files go in v3/j<ID>/
├── dumps/                  # Optional new diagnostic dumps go in v3/
├── backups/
├── telemax.log
└── venv/
```

Telegram uses the explicit SOCKS5 proxy in `TG_PROXY`, defaulting to `socks5h://127.0.0.1:10808`. MAX connections/downloads and optional ntfy notifications bypass application-level proxies. Host-level VPN and routing rules still apply.

Use a Telegram **forum supergroup**, with the bot added as an administrator and allowed to manage topics and send messages/media. Bot administrators receive ordinary group messages; Telemax separately authorizes their senders. All group members can read bridged conversations; the allowlists restrict actions, not visibility. Only one process may poll this bot token. An existing webhook must be removed before polling; preserve pending updates when doing so. [Telegram setup][tg-topics] · [Message visibility][tg-privacy] · [Webhooks][tg-webhook]

## Upgrade an existing installation

**Do not run a fresh-install initializer, delete the database, replace the MAX session, or change `TG_CHAT_ID`.** The migrated database is bound to that Telegram group.

Unpack the new release into `/home/htpc/telemax/update_v3/`, leaving the running files untouched. This staging directory must contain `telemax.py`, `migrate_telemax.py`, and `test_telemax.py`.

Run the following in Bash as `htpc`. If any command fails, stop and resolve the error; do not start the new service anyway.

```bash
set -euo pipefail
cd /home/htpc/telemax

# Offline checks against the existing configuration and installed SDK.
test -f update_v3/telemax.py
test -f update_v3/migrate_telemax.py
test -f update_v3/test_telemax.py
command -v sqlite3 >/dev/null
./venv/bin/python update_v3/telemax.py --work-dir "$PWD" --check-sdk
(cd update_v3 && ../venv/bin/python -m unittest -v test_telemax)

read -r -p "Your Telegram USER ID (positive number): " TG_USER_ID
[[ "$TG_USER_ID" =~ ^[1-9][0-9]*$ ]] || exit 1

sudo systemctl stop telemax.service
# Also stop any manually launched instance before continuing.

umask 077
SNAP="$PWD/backups/pre-deploy-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -m 700 -p "$SNAP"
cp -p telemax.py constants.json "$SNAP/"
# Fail rather than omit the currently deployed session or queued media.
tar -czf "$SNAP/runtime-files.tar.gz" -- media_queue session_cache
sqlite3 telegram_queue.db ".backup '$SNAP/telegram_queue.db'"
test "$(sqlite3 "$SNAP/telegram_queue.db" 'PRAGMA quick_check;')" = "ok"
./venv/bin/python -m pip freeze > "$SNAP/requirements.freeze.txt"
sudo systemctl cat telemax.service > "$SNAP/telemax.service.txt"
printf 'Pre-deployment snapshot: %s\n' "$SNAP"

cp update_v3/telemax.py update_v3/migrate_telemax.py .
./venv/bin/python migrate_telemax.py --work-dir "$PWD" --owner-id "$TG_USER_ID"
./venv/bin/python telemax.py --work-dir "$PWD" --check

sudo systemctl start telemax.service
sudo systemctl status telemax.service --no-pager
sudo journalctl -u telemax.service -n 50 --no-pager
```

`--check-sdk` validates configuration, the installed SDK and availability of `curl`; it does not open the database or call external APIs. If it reports an SDK mismatch, prepare and verify a compatible environment separately before attempting the deployment. Do not blindly upgrade the working virtual environment.

### What migration does

The migration creates another verified SQLite backup and a configuration copy under `backups/pre-v3-<timestamp>-<suffix>/`, and prints the path. This automatic backup does **not** include code, media, or the MAX session; the pre-deployment snapshot above includes those separately.

It imports `queue_v2` and `queue_dead_letter` into `bridge_jobs` in one database transaction, sets the schema version and Telegram-group binding, and preserves existing topics, aliases, settings, source queue rows, media, and session files. Legacy queue IDs are retained in the imported payload; use the **new IDs shown by `/dlq`** for commands. A task found in both old queues is held as `uncertain` for review rather than sent twice.

`--owner-id` adds your **Telegram user ID**, not a group ID or MAX ID, to both authorization lists without removing existing users. Repeat the option for additional owners. Re-running migration against the same migrated database does not import the old queues again; it still creates a new backup. For a differently named service, pass `--service your-name.service`.

**Never run the old application against a migrated database after v3 has begun processing.** Old queue rows are retained as archives and can be replayed by the old code.

## Configuration

Keep your existing credentials and group ID. This is a complete example; replace the example credentials and user ID before use:

```json
{
  "MAX_PHONE": "+79001234567",
  "TG_BOT_TOKEN": "1234567890:REPLACE_WITH_REAL_BOT_TOKEN",
  "TG_CHAT_ID": "-1001234567890",
  "MY_MAX_ID": null,
  "TG_ALLOWED_USER_IDS": [123456789],
  "TG_ADMIN_USER_IDS": [123456789],
  "TG_PROXY": "socks5h://127.0.0.1:10808",
  "NTFY_URL": "",
  "MEDIA_LIMIT_MB": 50,
  "MEDIA_BUDGET_MB": 1024,
  "DISK_RESERVE_MB": 256,
  "RETENTION_DAYS": 7,
  "TG_SEND_INTERVAL": 3.1,
  "DEBUG_DUMPS": false
}
```

| Setting | Meaning |
| --- | --- |
| `TG_ALLOWED_USER_IDS` | Users allowed to send from Telegram through your MAX account and use `/status` and `/dlq`. |
| `TG_ADMIN_USER_IDS` | Users allowed to manage aliases, routes and failed tasks. Admins can also send messages. **If omitted, defaults to the allowed-user list**, so set it explicitly. |
| `MY_MAX_ID` | Optional own MAX ID. Login normally resolves it automatically; configure it if that lookup fails. |
| `TG_PROXY` | Must use `socks5h://host:port`; Telegram DNS resolution goes through the proxy. |
| `NTFY_URL` | Optional notification destination; empty string disables it. |
| `MEDIA_LIMIT_MB` | Per-file limit, default 50 MiB. The Telegram download path also enforces a 20 MiB ceiling. |
| `MEDIA_BUDGET_MB` | Download-budget check for new `media_queue/v3` files; not a total disk/DB quota. |
| `DISK_RESERVE_MB` | Free-space reserve checked during media preparation/downloads. |
| `RETENTION_DAYS` | Age before completed/discarded job payloads and v3 debug dumps are cleared. |
| `TG_SEND_INTERVAL` | Minimum interval between rate-limited Telegram mutation requests, in seconds. |
| `DEBUG_DUMPS` | Additional message dumps, disabled by default. They contain private message data. |

All size/retention settings must be positive integers. Size settings use MiB (`1024²` bytes), despite the `_MB` names. `TG_SEND_INTERVAL` must be at least `0.1`; keep the default unless there is a measured reason to change it. Raising `MEDIA_LIMIT_MB` does not bypass Telegram's cloud `getFile` limit. [Telegram file downloads][tg-files]

If both authorization lists are empty, MAX → Telegram continues, but Telegram → MAX and commands are disabled. Unauthorized Telegram messages are ignored, not saved for replay after access is granted. Messages from bots, anonymous `sender_chat` identities, and automatic forwards are not authorized.

After editing configuration, validate and restart:

```bash
cd /home/htpc/telemax
chmod 600 constants.json
./venv/bin/python telemax.py --check && sudo systemctl restart telemax.service
```

Do not commit `constants.json`, databases, sessions, dumps, logs, media, or backups to Git.

## Fresh installation only

Skip this section entirely when upgrading. The migration script expects an existing legacy database; it is **not** a new-install initializer.

Create `/home/htpc/telemax` owned by `htpc`, place the three release Python files there, and create `constants.json` as above. Choose a Python interpreter reporting version 3.10 or later.

```bash
set -euo pipefail
cd /home/htpc/telemax
umask 077
python3 -c 'import sys; assert sys.version_info >= (3, 10), "Python 3.10+ required"'
python3 -m venv venv
./venv/bin/python -m pip install 'maxapi-python[video]==2.4.1'
./venv/bin/python telemax.py --check-sdk
./venv/bin/python -m unittest -v test_telemax
chmod 600 constants.json
```

The install command assumes access to PyPI. For a SOCKS-only package-install route, install `PySocks` from an available wheel first, then pass `--proxy socks5h://127.0.0.1:10808` to pip. Do not install the unrelated `pymax` package. The `video` extra installs the optional video-metadata dependency; `requests` is not required by this bridge. The SDK's own dependencies are version ranges, so the SDK pin alone is not a full dependency lock. [Pinned SDK metadata][sdk]

Initialize an **empty** schema once using the functions shipped in `telemax.py`. This block refuses to overwrite an existing database and does not contact MAX or Telegram:

```bash
./venv/bin/python - <<'PY'
import os
from pathlib import Path
import sqlite3
import time
from telemax import Config, SCHEMA_VERSION, create_schema

root = Path.cwd().resolve()
cfg = Config.load(root)
os.umask(0o077)
# Exclusive creation: never replace an existing installation.
fd = os.open(cfg.db_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
os.close(fd)
conn = sqlite3.connect(cfg.db_path, isolation_level=None)
try:
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("BEGIN IMMEDIATE")
    create_schema(conn)
    conn.executemany("INSERT INTO bridge_meta(key,value) VALUES (?,?)", [
        ("schema_version", str(SCHEMA_VERSION)),
        ("telegram_chat_id", cfg.chat),
        ("legacy_imported", "fresh-install:" + str(time.time())),
    ])
    if conn.execute("PRAGMA quick_check").fetchone()[0] != "ok":
        raise RuntimeError("SQLite integrity check failed")
    conn.execute("COMMIT")
except BaseException:
    if conn.in_transaction:
        conn.execute("ROLLBACK")
    raise
finally:
    conn.close()
print("Empty schema initialized. Run telemax.py --check next.")
PY
```

On a fresh install, run `./venv/bin/python telemax.py --check`, then start `./venv/bin/python telemax.py` interactively as `htpc` to complete the SDK's authentication prompts. Wait for `MAX authenticated; Telemax 3.0.0 ready`, then stop with Ctrl+C before starting the systemd service. Authentication state remains in `session_cache/`. Do not run interactive authentication alongside an active service.

## systemd service

An existing compatible service can remain in place during the upgrade. For a fresh installation, or to adopt explicit preflight and timeout settings, create `/etc/systemd/system/telemax.service`:

```ini
[Unit]
Description=Telemax MAX-Telegram Bridge
Wants=network-online.target
After=network-online.target

[Service]
Type=notify
NotifyAccess=main
User=htpc
Group=htpc
WorkingDirectory=/home/htpc/telemax
Environment=PYTHONUNBUFFERED=1
UMask=0077
ExecStartPre=/home/htpc/telemax/venv/bin/python /home/htpc/telemax/telemax.py --check
ExecStart=/home/htpc/telemax/venv/bin/python /home/htpc/telemax/telemax.py --work-dir /home/htpc/telemax
Restart=on-failure
RestartSec=10
TimeoutStartSec=180
TimeoutStopSec=60
WatchdogSec=30
KillMode=control-group
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=full

[Install]
WantedBy=multi-user.target
```

Ensure the SOCKS proxy is running; `network-online.target` does not start it. Add ordering/dependency on its actual service separately if appropriate. Do not enable `ProtectHome=true` for an installation under `/home/htpc`.

```bash
sudo systemd-analyze verify /etc/systemd/system/telemax.service
sudo systemctl daemon-reload
sudo systemctl enable telemax.service
sudo systemctl restart telemax.service
sudo systemctl status telemax.service --no-pager
```

V3 sends readiness after MAX authentication and emits watchdog heartbeats separately. A healthy watchdog means the process is responsive, not that every message has been delivered. Inspect `/status` and `/dlq` as well.

## Operation and commands

Send commands from an authorized account inside the configured Telegram group. Ordinary non-command messages in a bound topic are sent through your personal MAX account. Admin operations require an ID in `TG_ADMIN_USER_IDS`.

| Command | Access and effect |
| --- | --- |
| `/status` | Allowed user/admin: connectivity, delivery timestamps, waiting and failed task counts. |
| `/dlq` | Allowed user/admin: first 20 `dead`/`uncertain` jobs, IDs, errors and checkpoint counts. |
| `/retry_dlq` | Admin: retry all `dead` jobs only; never bulk-retry `uncertain` jobs. |
| `/retry_dlq ID` | Admin: retry one `dead` job. |
| `/retry_dlq ID confirm` | Admin: also allow one `uncertain` job to retry. **Inspect the destination first; a duplicate is possible.** |
| `/discard_dlq ID confirm` | Admin: abandon one failed/uncertain job without sending it. |
| `/clear_dlq confirm` | Admin: abandon all failed/uncertain jobs; pending jobs are untouched. This is not a retry. |
| `/alias Name` | Admin, inside a mapped topic: rename it; 1–128 characters. |
| `/bind_topic MAX_CHAT_ID` | Admin, inside the intended topic: bind/rebind a numeric MAX chat ID, then retry the affected job. Conflicting mappings are refused. |

Interrupted or ambiguous external sends become `uncertain`, not automatic retries. Such a job blocks later jobs for the same route **within its processing lane** until retried or discarded. Other routes can continue. Confirmed operations retain checkpoints, but the bridge does not guarantee exactly-once delivery or complete recovery of events never received by the application.

For a deleted Telegram topic, create/select a replacement, bind the correct MAX chat ID there and retry the affected job. There is no silent fallback into the group's general topic. If topic creation itself has an uncertain result, inspect Telegram for an already-created topic before retrying.

Telegram → MAX supports photos, documents, video, voice and video notes; audio, stickers and animations are sent as files. Telegram text/media limits still apply. Sending does not provide full edit/deletion/reaction synchronization. MAX → Telegram output uses plain text rather than the old HTML rendering.

### Logs, checks and retention

```bash
sudo journalctl -u telemax.service -f -n 50
tail -f /home/htpc/telemax/telemax.log
```

The application log rotates at a 5 MiB threshold with three backups; systemd journal retention is separate. Maintenance runs hourly: it removes terminal jobs' v3 media, clears old completed/discarded payloads, and removes expired `dumps/v3` files. Job rows, receipts and deduplication keys remain. Pending/failed/uncertain payloads are not age-purged.

Old queue tables, old media/dumps, and backups are **not** automatically purged. Treat them as private data and manage their storage separately. Never delete files still needed by queued jobs. Media budgets do not cap total database or backup growth.

| Symptom | Action |
| --- | --- |
| SDK mismatch or missing method | Use the pinned compatible environment; do not bypass `--check-sdk`. |
| Missing schema/`bridge_meta` | Existing install: stop and migrate. Fresh install: initialize once. Never initialize over an existing DB. |
| `TG_CHAT_ID` binding mismatch | Restore the original group ID; do not edit schema metadata to bypass the check. |
| Commands/replies ignored | Check user IDs, bot permissions, group ID, non-anonymous sender, and topic binding. |
| Telegram polling conflict | Stop the other poller or remove its webhook without dropping pending updates. |
| `MEDIA_BUDGET_REACHED` / `DISK_RESERVE_REACHED` | Review disk usage and DLQ; adjust limits only with sufficient space. |
| `uncertain` in `/dlq` | Inspect the destination, then explicitly retry or discard; do not bulk replay. |
| No pinned status board on a fresh install | Use `/status`; v3 updates an existing saved status message but does not create/pin a new board. |

After deployment, test text and an attachment in **both directions**, `/status`, `/dlq`, and a controlled restart. Offline tests and preflight checks do not authenticate accounts or prove live delivery. Keep the original messengers available while validating.

## Backups and rollback

For ongoing backups, stop the service and repeat the snapshot steps in **Upgrade**, also saving the current `migrate_telemax.py`. Preserve code, configuration, SQLite state, media and session together, with restricted permissions. Record the installed environment. Keep an additional protected copy outside this host.

Use SQLite's `.backup` or Python's backup API for the database; do not copy only `telegram_queue.db` while a live WAL may contain changes.

### Failure before the new service was started

The following restores code, configuration and database from the **pre-deployment** snapshot above. It is only appropriate when v3 has **not** started processing. At that point migration has not altered the original media or MAX session. The virtual environment must also still match the old deployment.

```bash
set -euo pipefail
cd /home/htpc/telemax
sudo systemctl stop telemax.service

read -r -p "Absolute pre-deployment snapshot directory: " SNAP
[[ "$SNAP" = /* ]] || exit 1
for f in telemax.py constants.json telegram_queue.db; do test -f "$SNAP/$f"; done
test "$(sqlite3 "$SNAP/telegram_queue.db" 'PRAGMA quick_check;')" = "ok"

umask 077
HOLD="$PWD/backups/pre-rollback-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -m 700 -p "$HOLD"
# Preserve the failed deployment and ALL SQLite sidecars, rather than deleting them.
for f in telemax.py migrate_telemax.py constants.json telegram_queue.db \
         telegram_queue.db-wal telegram_queue.db-shm; do
    if [ -e "$f" ]; then mv -- "$f" "$HOLD/"; fi
done
cp -p "$SNAP/telemax.py" "$SNAP/constants.json" "$SNAP/telegram_queue.db" .
printf 'Old files restored. Failed deployment retained in %s\n' "$HOLD"
```

If you changed the systemd unit, restore its old configuration and reload systemd **before** starting the old code: the old script does not support the new `--check`/`--work-dir` flags. Then start and inspect the service.

### Failure after v3 has processed traffic

Stop the service and save a new complete snapshot first. **Do not execute the simple rollback above or merely replace `telemax.py`.** The old queues and the pre-upgrade snapshot can replay delivered messages and omit newly received ones. Reconcile `bridge_jobs`, checkpoints and destination messages before restoring anything; fixing forward is preferable to a blind rollback. Retain the post-upgrade database and media until reconciliation is complete.

## References

Implementation: [`telemax.py`](telemax.py), [`migrate_telemax.py`](migrate_telemax.py), [`test_telemax.py`](test_telemax.py). The guide describes the accompanying files, not the superseded single-file draft.

[sdk]: https://github.com/MaxApiTeam/PyMax/blob/v2.4.1/pyproject.toml
[tg-topics]: https://core.telegram.org/bots/api#createforumtopic
[tg-privacy]: https://core.telegram.org/bots/features#privacy-mode
[tg-webhook]: https://core.telegram.org/bots/api#deletewebhook
[tg-files]: https://core.telegram.org/bots/api#getfile
