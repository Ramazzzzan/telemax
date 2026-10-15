# Telemax

Telemax — self-hosted MAX ↔ Telegram bridge for a personal MAX account, based on [PyMax](https://github.com/MaxApiTeam/PyMax).

Current release: **3.5.5**  
Database schema: **351**  
Pinned SDK: **maxapi-python==2.4.1**

Telemax mirrors MAX chats into Telegram forum topics, allows replies from Telegram back to MAX, keeps a durable SQLite queue, persists incoming MAX events before queue admission, maintains DLQ diagnostics, and supports additional MAX/Telegram mirror destinations.

## Main features

- MAX → Telegram forwarding into one topic per MAX chat.
- Telegram → MAX replies from linked topics.
- Existing MAX chat ↔ Telegram topic mappings are persisted in SQLite.
- `/chat`, `/bind`, `/alias`, `/mute`, `/unmute`, `/status`, DLQ management.
- Durable filesystem inbox before SQLite admission for received MAX events.
- Parallel workers and bounded retry logic.
- `dead` and `uncertain` queue states with explicit recovery.
- Persistent diagnostic JSON for failed jobs.
- Automatic pinned status message in Telegram General.
- Best-effort PyMax version monitoring; SDK is never upgraded automatically.
- Silent Telegram delivery for MAX system events.
- One-time warning when a Telegram topic is not linked to MAX.
- Native Telegram-photo → MAX-photo compatibility workaround for PyMax 2.4.1.
- Optional forwarding of messages sent by the same MAX account from other MAX clients.
- Additional per-chat fan-out through `MIRROR_RULES`:
  - MAX → another MAX chat using native forward;
  - MAX → another Telegram chat/group;
  - MAX → a forum topic in another Telegram group.

Telemax does **not** guarantee exactly-once delivery. If an external API may have accepted a message but the response was lost, the job is kept as `uncertain` rather than blindly retried.

---

## Requirements

- Linux
- Python 3.10+
- systemd
- `curl`
- SQLite
- `maxapi-python==2.4.1`
- SOCKS5 proxy for Telegram API

The production installation used by this project is normally:

```text
/home/htpc/telemax
```

with the service running from:

```text
/home/htpc/telemax/venv/bin/python
```

---

# Configuration

Configuration is stored in `constants.json`.

## Important: you do not need every supported setting in `constants.json`

Only these three settings are strictly required by the parser:

```json
{
  "MAX_PHONE": "+79001234567",
  "TG_BOT_TOKEN": "123456789:BOT_TOKEN",
  "TG_CHAT_ID": "-1001234567890"
}
```

All other settings have defaults.

For an existing working installation it is normally better to keep `constants.json` **sparse** rather than copy every default into it. This makes future upgrades easier because a new default can take effect automatically unless you deliberately override it.

Unknown keys are rejected, so do not add settings copied from older/alternative Telemax builds unless they are listed below.

## Recommended practical configuration

For a normal production installation with Telegram → MAX enabled:

```json
{
  "MAX_PHONE": "+79001234567",
  "TG_BOT_TOKEN": "123456789:BOT_TOKEN",
  "TG_CHAT_ID": "-1001234567890",

  "TG_ALLOWED_USER_IDS": [123456789],
  "TG_ADMIN_USER_IDS": [123456789],

  "TG_PROXY": "socks5h://127.0.0.1:10808",

  "MIRROR_RULES": []
}
```

If ntfy is used:

```json
"NTFY_URL": "https://ntfy.example/topic"
```

If messages sent by your MAX account from other MAX clients should also appear in Telegram:

```json
"FORWARD_OWN_MAX_MESSAGES": true
```

Otherwise leave it absent; the default is `false`.

---

## Full configuration reference

The following is the complete set of currently supported top-level settings. Values shown are defaults where applicable.

```json
{
  "MAX_PHONE": "+79001234567",
  "TG_BOT_TOKEN": "123456789:BOT_TOKEN",
  "TG_CHAT_ID": "-1001234567890",

  "TG_ALLOWED_USER_IDS": [],
  "TG_ADMIN_USER_IDS": [],
  "MY_MAX_ID": null,

  "TG_PROXY": "socks5h://127.0.0.1:10808",
  "NTFY_URL": "",

  "MAX_MEDIA_MB": 49,
  "TG_DOWNLOAD_MB": 20,
  "MEDIA_DISK_LIMIT_MB": 2048,
  "MIN_FREE_MB": 256,

  "QUEUE_LIMIT": 50000,
  "HISTORY_DAYS": 30,
  "MAX_ATTEMPTS": 10,
  "ERROR_DUMP_LIMIT_MB": 256,
  "INBOX_LIMIT_MB": 512,

  "WORKERS": 4,
  "DOWNLOAD_WORKERS": 3,
  "RETRY_WINDOW_SECONDS": 86400,

  "HISTORY_MAX_JOBS": 10000,
  "DEDUP_DAYS": 90,
  "DEDUP_MAX_KEYS": 200000,

  "MEDIA_IPV4_ONLY": true,
  "BLOCKED_IPV6_PREFIXES": [],

  "PYMAX_UPDATE_CHECK": true,
  "PYMAX_UPDATE_CHECK_HOURS": 24,

  "FORWARD_OWN_MAX_MESSAGES": false,

  "MIRROR_RULES": []
}
```

### Access and identity

| Setting | Default | Meaning |
| --- | --- | --- |
| `MAX_PHONE` | required | MAX account phone number with country code. |
| `TG_BOT_TOKEN` | required | Telegram bot token. |
| `TG_CHAT_ID` | required | Base Telegram forum supergroup used by Telemax. Must be a negative ID. |
| `TG_ALLOWED_USER_IDS` | `[]` | Telegram users allowed to send messages Telegram → MAX. |
| `TG_ADMIN_USER_IDS` | `[]` | Users allowed to run administrative commands. |
| `MY_MAX_ID` | `null` | Optional expected MAX account user ID. Normally can be omitted. |

If both Telegram user lists are empty, MAX → Telegram still works, but Telegram → MAX and administrative operations are effectively disabled.

### Network and notifications

| Setting | Default | Meaning |
| --- | --- | --- |
| `TG_PROXY` | `socks5h://127.0.0.1:10808` | SOCKS5 proxy used for Telegram/API checks. |
| `NTFY_URL` | `""` | Optional ntfy endpoint. Empty disables ntfy notifications. |
| `MEDIA_IPV4_ONLY` | `true` | Restrict remote MAX media download resolution to IPv4 for SSRF/NAT64 safety. |
| `BLOCKED_IPV6_PREFIXES` | `[]` | Additional IPv6 networks to block if IPv6 media downloads are enabled. |

MAX itself is not forced through `TG_PROXY`.

### Storage, retry and concurrency

| Setting | Default | Meaning |
| --- | ---: | --- |
| `MAX_MEDIA_MB` | `49` | Maximum accepted MAX media size. |
| `TG_DOWNLOAD_MB` | `20` | Telegram download limit used by Telemax; capped at 20 MiB. |
| `MEDIA_DISK_LIMIT_MB` | `2048` | Media working-area disk budget. |
| `MIN_FREE_MB` | `256` | Minimum free disk reserve. |
| `QUEUE_LIMIT` | `50000` | Maximum queue size. |
| `HISTORY_DAYS` | `30` | Retention for completed history. |
| `MAX_ATTEMPTS` | `10` | Maximum counted delivery attempts. |
| `ERROR_DUMP_LIMIT_MB` | `256` | Budget for diagnostic dumps. |
| `INBOX_LIMIT_MB` | `512` | Budget for durable, not-yet-imported MAX inbox envelopes. |
| `WORKERS` | `4` | Worker count; valid range 1–16. |
| `DOWNLOAD_WORKERS` | `3` | Concurrent media downloads; valid range 1–16. |
| `RETRY_WINDOW_SECONDS` | `86400` | Maximum retry-age window. |
| `HISTORY_MAX_JOBS` | `10000` | Bound on retained completed job history. |
| `DEDUP_DAYS` | `90` | Deduplication retention period. |
| `DEDUP_MAX_KEYS` | `200000` | Maximum retained deduplication keys. |

The defaults are suitable for the current installation unless there is a concrete storage/performance reason to change them.

### PyMax update monitoring

```json
"PYMAX_UPDATE_CHECK": true,
"PYMAX_UPDATE_CHECK_HOURS": 24
```

Telemax periodically checks for a newer PyMax version.

It does **not** install or upgrade PyMax automatically.

Normal minor/patch updates are surfaced through status information. More prominent notification is reserved for important changes such as security, authentication/session, protocol/connectivity, delivery/data-integrity, upload, compatibility or major-version changes.

---

# Forwarding own MAX messages

Default:

```json
"FORWARD_OWN_MAX_MESSAGES": false
```

When enabled, Telemax may forward to Telegram messages sent from the same MAX account through another MAX client.

This still depends on MAX actually delivering those events to the active PyMax session.

Messages sent by Telemax itself are correlated using exact returned MAX message IDs so they are not echoed back as if they came from another client.

A mirror rule can independently enable own-message handling for one source chat using:

```json
"INCLUDE_OWN_MESSAGES": true
```

even if the global setting remains `false`.

---

# Additional mirroring with MIRROR_RULES

`MIRROR_RULES` adds independent forwarding destinations for selected MAX chats.

Example:

```json
"MIRROR_RULES": [
  {
    "SOURCE_MAX_CHAT_ID": -75811741490645,
    "TARGET_MAX_CHAT_ID": -22222222222222,

    "TARGET_TG_CHAT_ID": -1003333333333,
    "TARGET_TG_THREAD_ID": 456,

    "NORMAL_FORWARD": true,
    "INCLUDE_OWN_MESSAGES": false,
    "FORWARD_SYSTEM_EVENTS": false,
    "SILENT_TG": true
  }
]
```

For one source MAX chat this can produce three independent deliveries:

```text
source MAX chat
 ├── normal Telemax topic in TG_CHAT_ID
 ├── native forward to TARGET_MAX_CHAT_ID
 └── copy to TARGET_TG_CHAT_ID / TARGET_TG_THREAD_ID
```

Each extra destination is a separate durable job. Failure of one destination does not block the others.

## Mirror rule fields

| Field | Default | Meaning |
| --- | --- | --- |
| `SOURCE_MAX_CHAT_ID` | required | MAX chat being mirrored. |
| `TARGET_MAX_CHAT_ID` | `null` | Another MAX chat. At least one MAX/TG target is required. |
| `TARGET_TG_CHAT_ID` | `null` | Additional Telegram chat/group, which may differ from the base `TG_CHAT_ID`. |
| `TARGET_TG_THREAD_ID` | `null` | Existing forum-topic ID inside `TARGET_TG_CHAT_ID`. |
| `NORMAL_FORWARD` | `true` | Keep normal forwarding into the base Telemax forum. |
| `INCLUDE_OWN_MESSAGES` | `false` | Include messages sent by your own MAX account through another MAX client. |
| `FORWARD_SYSTEM_EVENTS` | `false` | Mirror MAX join/add/remove/control events to extra destinations. |
| `SILENT_TG` | `true` | Send the additional Telegram copy without push notification. |

`TARGET_TG_THREAD_ID` requires `TARGET_TG_CHAT_ID`.

For a normal Telegram group/chat without forum topics:

```json
"TARGET_TG_THREAD_ID": null
```

For a topic in another Telegram forum group:

```json
"TARGET_TG_CHAT_ID": -1003333333333,
"TARGET_TG_THREAD_ID": 456
```

The same Telegram bot must be present in that external group and have permission to post in the selected topic. Telemax does not create external mirror topics.

MAX → MAX mirror uses native PyMax `forward_message()`, so original MAX attachments do not need to be downloaded and re-uploaded.

Loop protection uses the exact returned MAX message ID.

`/mute` affects the normal MAX → base-Telegram-topic route only. Mirror destinations remain active.

---

# How to find IDs

## MAX chat IDs

Known Telemax routes:

```bash
sqlite3 /home/htpc/telemax/telegram_queue.db "
SELECT max_id,thread_id,name,type
FROM tm_routes
ORDER BY name;
"
```

Example:

```text
-75811741490645|1706|3-Г - Родители - Школа № 57 - 2026/2027|group
```

Here:

```text
MAX chat ID       = -75811741490645
Telegram thread ID = 1706
```

`SOURCE_MAX_CHAT_ID` and `TARGET_MAX_CHAT_ID` require a **MAX chat ID**, not a MAX user ID.

## External Telegram group/topic IDs

Add the Telemax bot to the destination Telegram group and send `/mirrorprobe` in the required topic.

Stop Telemax first so another `getUpdates` consumer does not consume the update:

```bash
sudo systemctl stop telemax.service
```

Query the bot API and inspect:

```text
chat.id
message_thread_id
```

Use:

```text
chat.id             → TARGET_TG_CHAT_ID
message_thread_id   → TARGET_TG_THREAD_ID
```

Then start Telemax again.

A full probe command is documented in `deployment.md`.

---

# Telegram commands

| Command | Purpose |
| --- | --- |
| `/status` | Service, MAX, Telegram and queue status. |
| `/dlq` | Show failed/uncertain jobs. |
| `/retry_dlq` | Retry all retryable `dead` jobs. |
| `/retry_dlq ID` | Retry one `dead` job. |
| `/retry_dlq ID force` | Explicitly retry an `uncertain` job; may duplicate delivery. |
| `/clear_dlq confirm` | Cancel DLQ jobs. |
| `/alias NAME` | Rename linked route/topic. |
| `/bind MAX_CHAT_ID` | Bind current Telegram topic to an existing MAX chat. |
| `/chat MAX_USER_ID [NAME]` | Create/link a MAX personal chat. |
| `/chat +PHONE [NAME]` | Create/link a MAX personal chat by phone. |
| `/mute [THREAD_ID]` | Disable normal MAX → Telegram route. |
| `/unmute [THREAD_ID]` | Re-enable it. |
| `/muted` | Show muted routes. |
| `/help` | Command help. |

Command validation errors are returned to the user and are not placed into DLQ.

---

# MAX system events

System/service events such as join/add/remove/control events are forwarded to Telegram without push notification.

Known `add` events are rendered as:

```text
Добавление пользователя в группу
```

Unknown event codes are kept visible instead of guessing their meaning.

---

# Telegram photo → MAX photo

Telemax uses native `pymax.Photo`.

PyMax 2.4.1 expects `photoIds` in the MAX photo-upload URL, but MAX may return a valid upload URL without that query parameter. Telemax installs a compatibility wrapper which uses the actual upload response token, preserving the attachment as a native MAX photo rather than degrading it to a generic file.

The compatibility workaround is tied to the pinned SDK version and should be reassessed when upgrading PyMax.

---

# Queue and reliability

Incoming MAX events are first written atomically into the filesystem inbox and only then admitted into SQLite.

This protects against temporary SQLite failures and queue pressure.

Relevant job states include:

```text
pending
running
done
dead
uncertain
cancelled
```

An `uncertain` job means Telemax cannot prove whether an external send completed. It is not automatically retried because doing so may create a duplicate.

Diagnostic dumps are written under:

```text
dumps/errors/
```

Do not commit databases, session data, inbox contents, media files, dumps or backups to Git.

---

# Recommended Git layout

This installation uses:

```text
/home/htpc/telemax_new    Git checkout / staging
/home/htpc/telemax        production runtime
```

Runtime state stays only in production.

Recommended `.gitignore`:

```gitignore
constants.json
telegram_queue.db
telegram_queue.db-*
session_cache/
media_queue/
inbox/
dumps/
backups/
*.bak
telemax.log*
.telemax.lock
venv/
.venv/
__pycache__/
```

---

# Updating production from Git

Update staging:

```bash
cd ~/telemax_new
git pull --ff-only origin main
```

Stop service and back up the production database:

```bash
sudo systemctl stop telemax.service

sqlite3 ~/telemax/telegram_queue.db \
  ".backup '$HOME/telemax/telegram_queue.db.pre-update.bak'"
```

Copy release files only:

```bash
cp ~/telemax_new/telemax.py \
   ~/telemax_new/telemax_test.py \
   ~/telemax_new/deployment.md \
   ~/telemax_new/README.md \
   ~/telemax/
```

Do **not** copy staging `constants.json`, database, session, media, inbox or virtualenv into production.

Check production:

```bash
cd ~/telemax

./venv/bin/python telemax.py --version
./venv/bin/python telemax.py --check
./venv/bin/python -m unittest -v telemax_test
```

If all checks pass:

```bash
sudo systemctl start telemax.service
sudo systemctl status telemax.service --no-pager
sudo journalctl -u telemax.service -n 100 --no-pager
```

For database upgrades from older schema versions, clean installation details and rollback guidance, see `deployment.md`.

---

# Security and operational notes

- `constants.json` contains secrets and must not be committed.
- `session_cache/` contains MAX session credentials/state and must not be committed.
- SQLite/inbox/error dumps may contain private conversations.
- Telegram API traffic uses the configured SOCKS5 proxy.
- MAX media downloads use SSRF protections and conservative network filtering.
- The PyMax dependency is deliberately pinned.
- Do not run two Telemax instances or two Telegram `getUpdates` consumers with the same bot token.
- Before forcing an `uncertain` DLQ job, verify whether the recipient already received the message.

For installation, migration, ID probes, service configuration and recovery procedures, see [`deployment.md`](deployment.md).
