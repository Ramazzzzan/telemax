# Telemax 3.5.0 — clean installation

This release contains exactly three files: `telemax.py`, `telemax_test.py`, and this guide.
Use them together. The application creates **new state only** with `--init`; normal startup
resumes an existing **3.5.0** database. It never imports, resets, or overwrites a database
from another release.

The examples use Linux/systemd, user **`htpc`**, and a new directory
**`/home/htpc/telemax-3.5.0`**. Keep an existing installation elsewhere; do not copy its
database, queued files, or session into this directory. Run Python and pip as `htpc`,
not root. If using another account or path, change the commands and service unit together.

## 1. Requirements and Telegram setup

Use Python **3.10 or newer**, `curl`, CA certificates, and **`maxapi-python==2.4.1`**.
The runtime rejects a different SDK version; it does not install or upgrade dependencies.
The tests use the Python standard library and loopback HTTP. SDK-specific checks also
run when the package is installed.

On Debian/Ubuntu, install the operating-system tools and verify Python:

```bash
sudo apt-get update
sudo apt-get install -y python3 python3-venv curl ca-certificates sqlite3
python3 -c 'import sys; assert sys.version_info >= (3, 10), "Python 3.10+ is required"'
```

Prepare a Telegram bot and a **forum supergroup** with topics enabled. Give the bot
administrator permissions to manage topics and send messages/media. Obtain the negative
supergroup ID and your positive **Telegram user ID**. They are different from your MAX
ID and from each other. Administrators receive ordinary group messages; Telemax applies
its own sender allowlists. Anyone who can read the Telegram group can read the forwarded
conversations—the allowlists restrict actions, not visibility. [Telegram FAQ][tg-faq]

A new bot and group avoid old pending updates and topic bindings. Reusing a bot requires
stopping every other poller and ensuring it has no active webhook. Telemax neither deletes
a webhook nor discards pending updates automatically. [Telegram updates][tg-updates]

Telegram traffic requires a working SOCKS5 proxy, default
`socks5h://127.0.0.1:10808`. MAX connections, MAX downloads, and optional ntfy notifications
bypass application-level proxies. Host VPN/routing rules still apply. This release does
not install or configure the proxy itself.

## 2. Place the release and install the SDK

As `htpc`:

```bash
umask 077
mkdir -p /home/htpc/telemax-3.5.0
cd /home/htpc/telemax-3.5.0
```

Place `telemax.py`, `telemax_test.py`, and `deployment.md` from this release directly in
that directory. Do not substitute files from another archive or the earlier running
installation.

```bash
set -e
cd /home/htpc/telemax-3.5.0
python3 -m venv venv
./venv/bin/python -m pip install 'maxapi-python==2.4.1'
./venv/bin/python -m pip check
./venv/bin/python -m pip freeze > requirements.installed.txt
./venv/bin/python telemax.py --version
```

The version command must print **`Telemax 3.5.0`**. The SDK is pinned; its transitive
packages are resolved during installation, not locked by these three files. The generated
`requirements.installed.txt` records that environment. Do not blindly update it afterward.
If this host cannot reach PyPI, provision compatible wheels through your own package
mirror or offline wheelhouse before proceeding; the Telegram proxy setting does not
configure pip.

## 3. Create constants.json

Create `constants.json` in the same directory. Replace the example phone, bot token,
group ID, and the two user-ID entries. Keep `MY_MAX_ID` as `null` to resolve the account
ID during login.

```json
{
  "MAX_PHONE": "+79001234567",
  "TG_BOT_TOKEN": "123456789:REPLACE_WITH_REAL_BOT_TOKEN",
  "TG_CHAT_ID": "-1001234567890",
  "MY_MAX_ID": null,
  "TG_ALLOWED_USER_IDS": [123456789],
  "TG_ADMIN_USER_IDS": [123456789],
  "TG_PROXY": "socks5h://127.0.0.1:10808",
  "NTFY_URL": "",
  "MAX_MEDIA_MB": 49,
  "TG_DOWNLOAD_MB": 20,
  "MEDIA_DISK_LIMIT_MB": 2048,
  "MIN_FREE_MB": 256,
  "QUEUE_LIMIT": 50000,
  "HISTORY_DAYS": 30,
  "MAX_ATTEMPTS": 10
}
```

These are **all** the supported configuration keys. Unknown or duplicate keys are
rejected instead of silently ignored. Only the phone, token, and group ID are mandatory
for configuration validation; the other keys use the following defaults when omitted.
For two-way operation, configure the authorization lists explicitly.

| Key | Default / meaning |
| --- | --- |
| `MAX_PHONE` | Required; phone number with country code. |
| `TG_BOT_TOKEN` | Required; token for this bot. |
| `TG_CHAT_ID` | Required; negative forum supergroup ID. The database is bound to this group and bot ID. |
| `MY_MAX_ID` | `null`; resolved from the authenticated MAX profile. A supplied ID must match that profile. |
| `TG_ALLOWED_USER_IDS` | `[]`; users allowed to reply through MAX and use read-only commands. |
| `TG_ADMIN_USER_IDS` | `[]`; users allowed to manage routes, aliases and DLQ. Admins can also reply. This list does **not** inherit the allowed-user list. |
| `TG_PROXY` | `socks5h://127.0.0.1:10808`; explicit SOCKS5 proxy with remote DNS. No direct Telegram fallback. |
| `NTFY_URL` | Empty string; notifications disabled. Set an HTTP(S) endpoint to receive DLQ/crash alerts. |
| `MAX_MEDIA_MB` | `49`; maximum downloaded MAX file size and maximum reused local MAX attachment size. |
| `TG_DOWNLOAD_MB` | `20`; Telegram download size budget. Values above 20 are capped at 20. Server-side limits still apply. |
| `MEDIA_DISK_LIMIT_MB` | `2048`; budget for files directly inside `media_queue`, including retained temporary files. |
| `MIN_FREE_MB` | `256`; free-space reserve for media downloads. |
| `QUEUE_LIMIT` | `50000`; admission limit for new message events. Internal expansion and administrative jobs may temporarily exceed it; it is not a total database quota. |
| `HISTORY_DAYS` | `30`; after this age, completed/cancelled job bodies are cleared. Small deduplication records remain. Active and failed jobs are retained. |
| `MAX_ATTEMPTS` | `10`; counted failures before holding a job as `dead`. Rate limits and configured no-count waits do not exhaust this budget. |

Size settings use **MiB** (`1024²` bytes); size/count settings must be positive integers.
Neither configuration nor a new SDK can override Telegram's cloud API limits.
[Telegram file downloads][tg-files]

When both user lists are empty, MAX → Telegram still works; Telegram → MAX and commands
are disabled. Bots, anonymous `sender_chat` messages, and automatic forwards are denied.
Unauthorized updates are ignored, not retained for later replay.

```bash
cd /home/htpc/telemax-3.5.0
chmod 600 constants.json
./venv/bin/python telemax.py --check-config
./venv/bin/python -m unittest -v telemax_test
```

`--check-config` checks configuration, the SDK version/imports, and availability of
`curl`. It does not open the database or call the services. Tests do not use your account
or real credentials. Run the tests in this venv so the SDK checks are not skipped; a
passing suite is not proof of live delivery.

## 4. Initialize new state and authenticate MAX

Before the first network run, stop any older Telemax instance using this bot/account.
If the existing service is named `telemax.service`:

```bash
if systemctl cat telemax.service >/dev/null 2>&1; then
  sudo systemctl stop telemax.service
fi
```

Also stop manually launched copies. A directory lock prevents two copies in the same
state directory, but cannot stop a different installation from using the same bot.

Run as `htpc`:

```bash
set -e
cd /home/htpc/telemax-3.5.0
./venv/bin/python telemax.py --init
./venv/bin/python telemax.py --check
./venv/bin/python telemax.py
```

`--init` is an offline, one-time initializer. It fails if the database or SQLite sidecar
files already exist. There is no force/reset option. After initialization, use normal
startup—not another `--init`. `--check` validates existing state read-only and does not
recover or resend interrupted jobs.

During the first interactive run, complete the MAX SMS/2FA prompts. After the log reports
**`MAX session ready`**, stop with **Ctrl+C** before starting the systemd service. The
session is saved in `session_cache`. Reauthentication, when required, is also interactive;
do not expect systemd to answer SMS/2FA prompts.

The resulting layout is:

```text
/home/htpc/telemax-3.5.0/
├── telemax.py
├── telemax_test.py
├── deployment.md
├── constants.json
├── requirements.installed.txt
├── telegram_queue.db       # State marker 350; tm_meta, tm_routes, tm_jobs
├── telegram_queue.db-wal   # SQLite may create/remove these sidecars
├── telegram_queue.db-shm
├── .telemax.lock
├── session_cache/
├── media_queue/
├── telemax.log
└── venv/
```

## 5. Install the systemd service

Create `/etc/systemd/system/telemax.service`:

```ini
[Unit]
Description=Telemax 3.5.0 MAX-Telegram bridge
Wants=network-online.target
After=network-online.target

[Service]
Type=notify
NotifyAccess=main
User=htpc
Group=htpc
WorkingDirectory=/home/htpc/telemax-3.5.0
ExecStartPre=/home/htpc/telemax-3.5.0/venv/bin/python /home/htpc/telemax-3.5.0/telemax.py --check
ExecStart=/home/htpc/telemax-3.5.0/venv/bin/python /home/htpc/telemax-3.5.0/telemax.py
Restart=on-failure
RestartSec=10
TimeoutStartSec=120
TimeoutStopSec=45
WatchdogSec=30
UMask=0077
NoNewPrivileges=yes
PrivateTmp=yes
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now telemax.service
sudo systemctl status telemax.service --no-pager
sudo journalctl -u telemax.service -n 50 --no-pager
```

Systemd readiness means the initial MAX login completed. The watchdog monitors the
running process; neither signal proves that both message directions are delivering.
Inspect `/status` and perform the checks below. [Systemd service semantics][systemd]

## 6. Smoke test and commands

Send a new MAX message to the account from another user. Confirm that a Telegram topic
appears and contains it. Reply in that topic from an allowed Telegram user; confirm the
MAX recipient sees it and that Telegram receives the lightning reaction. Test a photo,
a document, a voice message, and a long text in both directions. An unauthorized group
member must not be able to send through the MAX account or modify the DLQ.

For commands, use the configured Telegram group:

| Command | Access and effect |
| --- | --- |
| `/help` | Allowed users/admins; command reference. |
| `/status` | Allowed users/admins; timestamps, queue states, and last observed MAX health. |
| `/dlq` | Allowed users/admins; first 20 `dead`/`uncertain` jobs and their errors. |
| `/retry_dlq` | Admins; retry all `dead` jobs, but not `uncertain` sends. |
| `/retry_dlq ID` | Admins; retry one `dead` job. |
| `/retry_dlq ID force` | Admins; also retry an `uncertain` job. **May duplicate a delivery.** |
| `/clear_dlq confirm` | Admins; cancel all `dead`/`uncertain` jobs. No undo or immediate deletion of every file. |
| `/alias Name` | Admins, inside a bound topic; save its display name and queue a Telegram topic rename. Does not rename MAX contacts. |
| `/bind MAX_CHAT_ID` | Admins, inside a topic; explicitly bind a MAX **chat ID**, not a user ID. |

An outgoing message with an unknown result after a timeout or interrupted send is held
as `uncertain`. Check the destination before forcing a retry. A failed topic creation
with unknown result likewise needs manual inspection and `/bind`; it is not repeated
blindly. A confirmed deleted topic is recreated for the same MAX route, never silently
replaced by posting into the general topic. A clean database knows none of your earlier
topic bindings; new topics are created as messages arrive, or can be bound explicitly.

Telegram rate limits are delayed using `retry_after`. Mutation requests are paced at a
fixed minimum interval of 3.1 seconds; there is no configuration key to change this.
[Telegram API][tg-api]

## 7. Operation and limits

```bash
# Read logs.
sudo journalctl -u telemax.service -f -n 50
tail -f /home/htpc/telemax-3.5.0/telemax.log

# Validate a configuration edit and apply it.
cd /home/htpc/telemax-3.5.0
./venv/bin/python telemax.py --check && sudo systemctl restart telemax.service
```

Configuration is loaded at startup; changing it requires a restart. The group ID and
bot ID must continue matching this database. Rotating the token for the same Telegram
bot does not change its ID.

Messages received by Telemax are saved before processing, and Telegram offsets are
saved with accepted work. MAX events missed while the process is offline still depend
on SDK/server replay. This is not a full history archiver or an exactly-once bridge.

Text, photos, supported files, voice and video paths are handled. Telegram albums arrive
as separate updates and are forwarded to MAX as separate messages. Edits, deletions,
interactive polls, contacts and locations are not synchronized. Unsupported content is
held for inspection; the bridge does not transcode incompatible audio/video. Telegram
output uses plain text; PyMax's send path may interpret Markdown in text sent to MAX.
[PyMax media contracts][pymax-files] · [PyMax formatting][pymax-format]

There are no raw diagnostic dumps. Logs rotate at 5 MiB with three retained backups.
Queue payloads and session files still contain private data. Completed media becomes
eligible for cleanup after 24 hours when no active/failed job references it. Cleanup
runs approximately every minute. Failed/uncertain jobs and their files are not removed
by age; manage them through the DLQ. Deduplication rows accumulate, so monitor database
and disk usage separately from the media budget.

Do not put `constants.json`, databases, `session_cache`, media, logs, or backups in Git.
Only the three release files belong in the repository. Before any later maintenance,
stop the service and take a complete private snapshot of this release's code,
configuration, database **and its sidecars**, session, and media together. Do not restore
an old queue over a newer one without reviewing which messages have already been sent.

[pymax-files]: https://docs.pymax.org/files.html
[pymax-format]: https://docs.pymax.org/formatting.html
[tg-api]: https://core.telegram.org/bots/api
[tg-files]: https://core.telegram.org/bots/api#getfile
[tg-faq]: https://core.telegram.org/bots/faq#what-messages-will-my-bot-get
[tg-updates]: https://core.telegram.org/bots/api#getting-updates
[systemd]: https://www.freedesktop.org/software/systemd/man/systemd.service.html
