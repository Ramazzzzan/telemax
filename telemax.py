import os
import sys
import socket
import asyncio
import html
import sqlite3
import json
import logging
import time
import re
import collections
from datetime import datetime
from pathlib import Path

# --- PYMAX 2.4.1 ---
from pymax import Client, Message

# --- CONFIGURATION & PATHS ---
WORK_DIR = Path(__file__).parent.resolve()
CONFIG_PATH = WORK_DIR / "constants.json"
TEMP_DOWNLOAD_DIR = WORK_DIR / "media_queue"
DUMPS_DIR = WORK_DIR / "dumps"
LOG_FILE = WORK_DIR / "telemax.log"
DB_PATH = WORK_DIR / "telegram_queue.db"

TEMP_DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
DUMPS_DIR.mkdir(parents=True, exist_ok=True)

# --- LOAD CONSTANTS ---
try:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        config = json.load(f)
        MAX_PHONE = config.get("MAX_PHONE")
        TG_BOT_TOKEN = config.get("TG_BOT_TOKEN")
        TG_CHAT_ID = str(config.get("TG_CHAT_ID"))
        NTFY_URL = config.get("NTFY_URL")
        MY_MAX_ID = config.get("MY_MAX_ID")
        
        if not all([MAX_PHONE, TG_BOT_TOKEN, TG_CHAT_ID]):
            raise ValueError("Missing required keys in constants.json")
except Exception as e:
    print(f"Critical configuration initialization error: {e}")
    sys.exit(1)

SERVER_NAME = "Telemax"
RECENT_SENT_TEXTS = collections.deque(maxlen=50)

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# --- NON-BLOCKING ASYNC DATABASE MANAGER ---
class DatabaseManager:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = asyncio.Lock()
        self._init_db()

    def _init_db(self):
        with self._conn:
            self._conn.execute("PRAGMA journal_mode=WAL;")
            self._conn.execute("PRAGMA synchronous=NORMAL;")
            self._conn.execute('''CREATE TABLE IF NOT EXISTS queue_v2 
                                 (id INTEGER PRIMARY KEY AUTOINCREMENT, type TEXT, max_chat_id TEXT, thread_id INTEGER, text_data TEXT, file_data TEXT)''')
            self._conn.execute('''CREATE TABLE IF NOT EXISTS topics 
                                 (max_chat_id TEXT PRIMARY KEY, thread_id INTEGER, name TEXT, type TEXT)''')
            self._conn.execute('''CREATE TABLE IF NOT EXISTS contacts 
                                 (max_id TEXT PRIMARY KEY, alias TEXT)''')
            self._conn.execute('''CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)''')
            self._conn.execute('''CREATE TABLE IF NOT EXISTS queue_dead_letter 
                                 (id INTEGER PRIMARY KEY, type TEXT, max_chat_id TEXT, thread_id INTEGER, text_data TEXT, file_data TEXT, reason TEXT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)''')

    async def execute(self, sql: str, params: tuple = ()):
        async with self._lock:
            def _run():
                with self._conn:
                    cursor = self._conn.execute(sql, params)
                    return cursor.fetchall(), cursor.lastrowid
            return await asyncio.to_thread(_run)

    async def fetchone(self, sql: str, params: tuple = ()):
        async with self._lock:
            def _run():
                cursor = self._conn.execute(sql, params)
                return cursor.fetchone()
            return await asyncio.to_thread(_run)

    async def fetchall(self, sql: str, params: tuple = ()):
        async with self._lock:
            def _run():
                cursor = self._conn.execute(sql, params)
                return cursor.fetchall()
            return await asyncio.to_thread(_run)

db = DatabaseManager(DB_PATH)
queue_event = asyncio.Event()

async def enqueue_v2(item_type, max_chat_id, thread_id, text_data, file_data=None):
    try:
        await db.execute(
            "INSERT INTO queue_v2 (type, max_chat_id, thread_id, text_data, file_data) VALUES (?, ?, ?, ?, ?)",
            (item_type, str(max_chat_id), thread_id, text_data, file_data)
        )
        queue_event.set()
    except Exception as e:
        logger.error(f"DB Enqueue Error: {e}")

# --- UTILITIES ---
def dump_to_dict(obj, visited=None):
    if visited is None: visited = set()
    if id(obj) in visited: return "<circular_reference>"
    visited.add(id(obj))
    if isinstance(obj, (int, float, str, bool, type(None))): return obj
    elif isinstance(obj, (list, tuple, set)): return [dump_to_dict(item, visited) for item in obj]
    elif isinstance(obj, dict): return {str(k): dump_to_dict(v, visited) for k, v in obj.items()}
    elif isinstance(obj, bytes): return f"<bytes: {len(obj)}>"
    result = {"__class__": obj.__class__.__name__}
    try:
        if hasattr(obj, "__dict__"):
            for k, v in obj.__dict__.items():
                if not k.startswith("_"): result[k] = dump_to_dict(v, visited)
        elif hasattr(obj, "__slots__"):
            for slot in obj.__slots__:
                if not slot.startswith("_") and hasattr(obj, slot): result[slot] = dump_to_dict(getattr(obj, slot), visited)
    except Exception as e: result["__dump_error__"] = str(e)
    return result

def dump_message_to_json(message, reason="debug"):
    try:
        now = datetime.now()
        time_str = now.strftime("%Y%m%d-%H%M%S")
        msg_type_raw = getattr(message, "type", "UNKNOWN").upper()
        msg_id = getattr(message, "id", "no_id")
        filename = f"{time_str}-{msg_type_raw}-{msg_id}.json"
        with open(DUMPS_DIR / filename, "w", encoding="utf-8") as f:
            json.dump({"timestamp": int(now.timestamp()), "reason": reason, "message_dump": dump_to_dict(message)}, f, ensure_ascii=False, indent=2)
    except Exception: pass

def systemd_notify(message):
    notify_socket = os.environ.get('NOTIFY_SOCKET')
    if not notify_socket: return
    try:
        if notify_socket.startswith('@'): notify_socket = '\0' + notify_socket[1:]
        with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as sock:
            sock.sendto(message.encode('utf-8'), notify_socket)
    except Exception: pass

async def send_push(msg, tags="warning", priority=3):
    if not NTFY_URL: return
    def _post():
        try:
            import requests
            requests.post(NTFY_URL, data=msg.encode('utf-8'), headers={"Title": SERVER_NAME, "Tags": tags, "Priority": str(priority)}, timeout=10)
        except Exception: pass
    await asyncio.to_thread(_post)

# --- ASYNC TELEGRAM API ENGINE ---
async def tg_api_call(method: str, params: dict = None, files: dict = None, timeout: int = 60):
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/{method}"
    cmd = ["curl", "-sS", "-x", "socks5h://127.0.0.1:10808", "--max-time", str(timeout)]
    if params:
        for k, v in params.items():
            if v is not None: cmd.extend(["--form-string", f"{k}={v}"])
    if files:
        for field, path in files.items(): cmd.extend(["-F", f"{field}=@{path}"])
    cmd.append(url)

    try:
        proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0: return False, f"CURL_ERROR_{proc.returncode}"
        
        data = json.loads(stdout.decode('utf-8', errors='ignore'))
        if not data.get("ok", False):
            desc = data.get("description", str(data))
            code = data.get("error_code", 0)
            return ("FATAL" if 400 <= code < 500 else False), desc
        return True, data
    except Exception as e:
        return False, str(e)

async def create_telegram_topic(chat_id, name):
    ok, data = await tg_api_call("createForumTopic", params={"chat_id": chat_id, "name": name[:128]}, timeout=20)
    if ok is True and isinstance(data, dict): return data.get("result", {}).get("message_thread_id")
    return None

async def set_telegram_reaction(chat_id, message_id, emoji="👍"):
    return await tg_api_call("setMessageReaction", params={"chat_id": chat_id, "message_id": message_id, "reaction": json.dumps([{"type": "emoji", "emoji": emoji}])}, timeout=10)

async def send_telegram_media(chat_id, thread_id, text, file_info):
    file_path, ext = file_info["path"], file_info["ext"]
    if not os.path.exists(file_path): return True, None
    params = {"chat_id": chat_id, "caption": text or "", "parse_mode": "HTML"}
    if thread_id: params["message_thread_id"] = thread_id
    field, timeout_sec = "document", 300
    if ext in [".jpg", ".jpeg", ".png", ".webp"]: field, timeout_sec = "photo", 60
    elif ext == ".ogg": field, timeout_sec = "voice", 60
    elif ext == ".mp4": field, timeout_sec = "video", 300
    return await tg_api_call(f"send{field.capitalize()}", params=params, files={field: file_path}, timeout=timeout_sec)

async def send_telegram_album(chat_id, thread_id, text, files_info):
    valid_files = [f for f in files_info if os.path.exists(f["path"])]
    if not valid_files: return True, None
    media_group, files_dict = [], {}
    for i, f_info in enumerate(valid_files):
        files_dict[f"file{i}"] = f_info["path"]
        item = {"type": "video" if f_info["ext"] == ".mp4" else "photo", "media": f"attach://file{i}"}
        if i == 0 and text: item.update({"caption": text, "parse_mode": "HTML"})
        media_group.append(item)
    params = {"chat_id": chat_id, "media": json.dumps(media_group, ensure_ascii=False)}
    if thread_id: params["message_thread_id"] = thread_id
    return await tg_api_call("sendMediaGroup", params=params, files=files_dict, timeout=300)

async def send_telegram_message(chat_id, thread_id, text):
    params = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
    if thread_id: params["message_thread_id"] = thread_id
    return await tg_api_call("sendMessage", params=params, timeout=30)

async def update_status_message(text):
    try:
        row = await db.fetchone("SELECT value FROM settings WHERE key='status_msg_id'")
        msg_id, needs_new = row["value"] if row else None, False
        if msg_id:
            ok, data = await tg_api_call("editMessageText", params={"chat_id": TG_CHAT_ID, "message_id": msg_id, "text": text, "parse_mode": "HTML"}, timeout=10)
            if ok == "FATAL" and isinstance(data, str) and "not found" in data.lower(): needs_new = True
        if not msg_id or needs_new:
            ok, data = await tg_api_call("sendMessage", params={"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"}, timeout=10)
            if ok is True:
                new_id = data.get("result", {}).get("message_id")
                await db.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", ("status_msg_id", str(new_id)))
                await tg_api_call("pinChatMessage", params={"chat_id": TG_CHAT_ID, "message_id": new_id, "disable_notification": "true"})
    except Exception: pass

# --- FILE TRANSFER PIPELINE ---
async def download_tg_file(file_id, ext=".jpg"):
    ok, data = await tg_api_call("getFile", {"file_id": file_id})
    if ok is True and isinstance(data, dict):
        file_path = data.get("result", {}).get("file_path")
        if file_path:
            dl_path = TEMP_DOWNLOAD_DIR / f"tg_{file_id}{ext}"
            cmd = ["curl", "-sS", "-x", "socks5h://127.0.0.1:10808", "--max-time", "60", "-o", str(dl_path), f"https://api.telegram.org/file/bot{TG_BOT_TOKEN}/{file_path}"]
            proc = await asyncio.create_subprocess_exec(*cmd)
            await proc.communicate()
            if dl_path.exists(): return str(dl_path)
    return None

async def brutal_download(client_instance, attach, download_path):
    url_to_download = None
    try:
        if hasattr(attach, "get_url"):
            url_to_download = await attach.get_url() if asyncio.iscoroutinefunction(attach.get_url) else attach.get_url()
        elif hasattr(client_instance, "get_file_url"):
            url_to_download = await client_instance.get_file_url(attach)
    except: pass

    if not url_to_download:
        actual_id, token = None, attach.get('token') if isinstance(attach, dict) else getattr(attach, 'token', None)
        for attr_name in ['file_id', 'video_id', 'image_id', 'audio_id', 'id']:
            val = attach.get(attr_name) if isinstance(attach, dict) else getattr(attach, attr_name, None)
            if val: actual_id = val; break
        if actual_id:
            file_id_str = f"{actual_id}?token={token}" if token else f"{actual_id}"
            try:
                api_obj = getattr(client_instance, "api", getattr(client_instance, "_api", None))
                if api_obj and hasattr(api_obj, "get_file"):
                    file_content = await api_obj.get_file(file_id_str)
                    if file_content:
                        with open(download_path, 'wb') as f: f.write(file_content)
                        return True
            except: pass

    if not url_to_download:
        for attr in ['url', 'file_url', 'download_url', 'source', 'link', 'href', 'base_url']:
            val = attach.get(attr) if isinstance(attach, dict) else getattr(attach, attr, None)
            if isinstance(val, str) and val.startswith("http"): url_to_download = val; break

    if url_to_download:
        cmd = ["curl", "-sS", "-L", "-A", "Mozilla/5.0", "--max-time", "300", "-o", str(download_path), url_to_download]
        proc = await asyncio.create_subprocess_exec(*cmd)
        await proc.communicate()
        if os.path.exists(download_path) and os.path.getsize(download_path) > 0: return True

    for attr in ['bytes', 'file_bytes', 'data', 'content']:
        val = attach.get(attr) if isinstance(attach, dict) else getattr(attach, attr, None)
        if isinstance(val, bytes):
            with open(download_path, 'wb') as f: f.write(val)
            return True
    return False

# --- HELPER: ADVANCED CHAT TITLE RESOLUTION ---
async def resolve_chat_title(client_instance, message: Message, chat_id) -> str:
    # 1. Direct message attribute checks
    for attr in ["chat_title", "title", "chat_name"]:
        val = getattr(message, attr, None)
        if val and isinstance(val, str) and val.strip():
            return val.strip()

    # 2. Direct message.chat object checks
    chat_obj = getattr(message, "chat", None)
    if chat_obj:
        for attr in ["title", "name", "chat_name"]:
            val = getattr(chat_obj, attr, None)
            if val and isinstance(val, str) and val.strip():
                return val.strip()

    # 3. Active client lookup fallback
    if chat_id:
        try:
            target_id = int(chat_id) if str(chat_id).lstrip("-").isdigit() else str(chat_id)
            ci = await asyncio.wait_for(client_instance.get_chat(target_id), timeout=5.0)
            if ci:
                for attr in ["title", "name", "chat_name"]:
                    val = getattr(ci, attr, None)
                    if val and isinstance(val, str) and val.strip():
                        return val.strip()
        except Exception: pass

    return None

# --- HELPER: SYSTEM & CHAT EVENT PARSER ---
def parse_system_event(message: Message, msg_type_raw: str) -> str:
    action_type = str(getattr(message, "action", "") or getattr(message, "event_type", "") or getattr(message, "event", "") or "").upper()
    
    # Event translations dictionary
    action_map = {
        "USER_ADDED": "пользователь добавлен в чат",
        "USER_JOINED": "пользователь присоединился к чату",
        "USER_LEFT": "пользователь покинул чат",
        "USER_REMOVED": "пользователь удален из чата",
        "CHAT_TITLE_CHANGED": "название чата изменено",
        "CHAT_ICON_CHANGED": "иконка чата изменена",
        "MESSAGE_PINNED": "сообщение закреплено",
        "MESSAGE_UNPINNED": "сообщение откреплено",
        "JOIN_BY_LINK": "пользователь вошел по ссылке"
    }
    
    raw_text = str(getattr(message, "text", "") or getattr(message, "caption", "") or "").strip()
    if raw_text == "None": raw_text = ""

    desc = action_map.get(action_type, raw_text if raw_text else f"Событие чата ({action_type or msg_type_raw})")
    return f"ℹ️ <i>[Системное уведомление]: {desc}</i>"

# --- CLIENT INIT & HANDLERS ---
client = Client(phone=MAX_PHONE, work_dir=str(WORK_DIR / "session_cache"))
message_queue = asyncio.Queue()

@client.on_message()
async def handle_message(message: Message, client=None) -> None:
    await message_queue.put(message)

async def tg_forward_worker():
    while True:
        message = await message_queue.get()
        try: await process_and_enqueue(message)
        except Exception as e: logger.error(f"Processing Error: {e}")
        finally: message_queue.task_done()

async def process_and_enqueue(message: Message) -> None:
    last_msg_time = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    await db.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", ("last_msg_time", last_msg_time))
    dump_message_to_json(message, reason="incoming")

    msg_type_raw = getattr(message, "type", "").upper()
    chat_id = getattr(message, "chat_id", None)
    sender_id = getattr(message, "sender", None)
    t = str(getattr(message, "text", "") or getattr(message, "caption", "")).strip()

    if t and t in RECENT_SENT_TEXTS: return
    if getattr(message, "out", False) or getattr(message, "outgoing", False) or getattr(message, "is_out", False): return
    if MY_MAX_ID and sender_id == MY_MAX_ID: return

    # --- SENDER NAME RESOLUTION ---
    sender_name = "Неизвестный"
    if sender_id is not None:
        alias_row = await db.fetchone("SELECT alias FROM contacts WHERE max_id = ?", (str(sender_id),))
        if alias_row:
            sender_name = alias_row["alias"]
        else:
            try:
                ui = await asyncio.wait_for(client.get_user(sender_id), timeout=5.0)
                extracted = f"{getattr(ui, 'first_name', '')} {getattr(ui, 'last_name', '')}".strip()
                if not extracted and hasattr(ui, "names") and ui.names: extracted = ui.names[0].name
                elif not extracted and hasattr(ui, "name") and ui.name: extracted = ui.name
                sender_name = extracted if extracted else f"ID:{sender_id}"
            except: sender_name = f"ID:{sender_id}"
    elif msg_type_raw == "CHANNEL": sender_name = "Канал"

    # --- ADVANCED CHAT TITLE RESOLUTION ---
    chat_title = await resolve_chat_title(client, message, chat_id)
    if msg_type_raw == "CHANNEL" and sender_name == "Канал" and chat_title: sender_name = chat_title

    # --- PRIVATE VS GROUP CLASSIFICATION ---
    is_private = False
    if msg_type_raw in ["PRIVATE", "BOT"]: is_private = True
    elif msg_type_raw == "USER": is_private = not (chat_id and str(chat_id).startswith("-"))

    # --- TOPIC RESOLUTION ENGINE ---
    raw_chat_id = str(chat_id) if chat_id else "UNKNOWN_GROUP"
    if is_private:
        target = chat_id if chat_id else sender_id
        topic_target_id = f"PRIVATE_{target}" if target else "PRIVATE_UNKNOWN"
        m_type = "private"
        topic_alias_row = await db.fetchone("SELECT alias FROM contacts WHERE max_id = ?", (str(target),))
        if topic_alias_row: topic_name = topic_alias_row["alias"]
        elif chat_title: topic_name = chat_title
        else: topic_name = sender_name if sender_name and not sender_name.startswith("ID:") else f"Chat {target}"
    else:
        topic_target_id = raw_chat_id
        m_type = "group"
        clean_id = topic_target_id.lstrip("-")
        topic_name = chat_title if chat_title else f"Группа {clean_id}"

    # Query existing topic by normalized target ID
    row = await db.fetchone("SELECT thread_id, name FROM topics WHERE max_chat_id = ? OR max_chat_id = ?", (topic_target_id, topic_target_id.lstrip("-")))
    
    if row:
        thread_id, old_topic_name = row["thread_id"], row["name"]
        # Automatically upgrade generic fallback titles when a real title becomes available
        if chat_title and old_topic_name.startswith("Группа ") and not chat_title.startswith("Группа "):
            ok, _ = await tg_api_call("editForumTopic", {"chat_id": TG_CHAT_ID, "message_thread_id": thread_id, "name": chat_title[:128]})
            if ok:
                await db.execute("UPDATE topics SET name = ?, max_chat_id = ? WHERE thread_id = ?", (chat_title, topic_target_id, thread_id))
    else:
        thread_id = await create_telegram_topic(TG_CHAT_ID, topic_name)
        if not thread_id:
            safe_name = "".join(c for c in topic_name if c.isalnum() or c in " _-")[:128] or f"Topic {topic_target_id.lstrip('-')}"
            thread_id = await create_telegram_topic(TG_CHAT_ID, safe_name)
        if thread_id:
            await db.execute("INSERT OR REPLACE INTO topics (max_chat_id, thread_id, name, type) VALUES (?, ?, ?, ?)", (topic_target_id, thread_id, topic_name, m_type))

    header = f"[{sender_name}]:" if (is_private or not chat_title or chat_title == sender_name) else f"[{chat_title}], [{sender_name}]:"
    text_parts, all_attachments, forward_prefix = [], [], ""

    # --- SYSTEM / CHAT EVENT HANDLING ---
    is_service_event = msg_type_raw in ["SERVICE", "SYSTEM", "EVENT", "ACTION"] or bool(getattr(message, "action", None))
    if is_service_event:
        event_notice = parse_system_event(message, msg_type_raw)
        text_parts.append(event_notice)
    elif t and t != "None":
        text_parts.append(t)

    # --- ATTACHMENTS PROCESSING ---
    for attr in ["attaches", "attachments", "document", "video", "photo", "sticker", "voice"]:
        val = getattr(message, attr, None)
        if val: all_attachments.extend(val) if isinstance(val, list) else all_attachments.append(val)

    # --- FORWARDED MESSAGES PROCESSING ---
    link_obj = getattr(message, "link", None)
    if link_obj and getattr(link_obj, "type", None) == "FORWARD":
        nested_msg = getattr(link_obj, "message", None)
        if nested_msg:
            orig_sender_name, orig_sender_id = "Неизвестный", getattr(nested_msg, "sender", None)
            if orig_sender_id and isinstance(orig_sender_id, (int, str)):
                try:
                    oui = await asyncio.wait_for(client.get_user(orig_sender_id), timeout=2.0)
                    extracted = f"{getattr(oui, 'first_name', '')} {getattr(oui, 'last_name', '')}".strip()
                    if extracted: orig_sender_name = extracted
                    elif hasattr(oui, "names") and oui.names: orig_sender_name = oui.names[0].name
                    elif hasattr(oui, "name") and oui.name: orig_sender_name = oui.name
                except: pass
            forward_prefix = f"<i>[FW от {orig_sender_name}]</i>\n"
            ft = str(getattr(nested_msg, "text", "") or getattr(nested_msg, "caption", "")).strip()
            if ft and ft != "None": text_parts.append(ft)
            for attr in ["attaches", "attachments", "document", "video", "photo", "sticker", "voice"]:
                val = getattr(nested_msg, attr, None)
                if val: all_attachments.extend(val) if isinstance(val, list) else all_attachments.append(val)

    body_text = "\n\n".join(text_parts)
    downloaded_files = []
    
    if all_attachments:
        for attach in all_attachments:
            try:
                f_id = attach.get("id", str(id(attach))) if isinstance(attach, dict) else getattr(attach, "id", str(id(attach)))
                f_name = attach.get("name", "") if isinstance(attach, dict) else getattr(attach, "name", "")
                a_type = str(attach.get("type", "") if isinstance(attach, dict) else getattr(attach, "type", "")).upper()
                c_name = str(attach.get("__class__", "")) if isinstance(attach, dict) else getattr(attach.__class__, "__name__", "")
                ext = "." + f_name.split(".")[-1] if f_name and "." in f_name else ".mp4" if "VIDEO" in a_type or "Video" in c_name else ".mp3" if "AUDIO" in a_type or "Audio" in c_name else ".ogg" if "VOICE" in a_type or "Voice" in c_name else ".webp" if "STICKER" in a_type or "Sticker" in c_name else ".jpg" if "PHOTO" in a_type or "IMAGE" in a_type or "Photo" in c_name else ".file"
                dl_path = str(TEMP_DOWNLOAD_DIR / f"{f_id}{ext}")
                is_dl = os.path.exists(dl_path)
                if not is_dl and not isinstance(attach, dict):
                    try:
                        if hasattr(client, "download_media"): await client.download_media(attach, out_dir=str(TEMP_DOWNLOAD_DIR), file_name=f"{f_id}{ext}")
                        elif hasattr(attach, "download"): await attach.download(out_dir=str(TEMP_DOWNLOAD_DIR), file_name=f"{f_id}{ext}")
                    except: pass
                is_dl = os.path.exists(dl_path)
                if not is_dl: is_dl = await brutal_download(client, attach, dl_path)
                if is_dl: downloaded_files.append({"path": dl_path, "ext": ext})
                else: body_text += f"\n\n<i>[Ошибка: Вложение {ext} не скачалось]</i>"
            except Exception as e: logger.error(f"Attachment error: {e}")

    if not body_text and not downloaded_files: return

    full_caption = f"<b>{html.escape(header)}</b>\n{forward_prefix}{html.escape(body_text)}".strip()
    album_files = [df for df in downloaded_files if df["ext"] in [".jpg", ".jpeg", ".png", ".mp4"]]
    single_files = [df for df in downloaded_files if df not in album_files]
    caption_assigned = False

    if len(full_caption) > 1000:
        await enqueue_v2("text", chat_id, thread_id, full_caption, None)
        caption_assigned = True

    for i in range(0, len(album_files), 10):
        chunk = album_files[i:i+10]
        c = full_caption if not caption_assigned else ""
        caption_assigned = True
        await enqueue_v2("media" if len(chunk) == 1 else "album", chat_id, thread_id, c, json.dumps([chunk[0]] if len(chunk) == 1 else chunk))

    for single in single_files:
        c = full_caption if not caption_assigned else ""
        caption_assigned = True
        await enqueue_v2("media", chat_id, thread_id, c, json.dumps([single]))

    if not downloaded_files and not caption_assigned:
        await enqueue_v2("text", chat_id, thread_id, full_caption, None)

# --- QUEUE WORKER ---
async def queue_processor():
    retry_counts = {}
    while True:
        try:
            row = await db.fetchone("SELECT id, type, thread_id, text_data, file_data FROM queue_v2 ORDER BY id ASC LIMIT 1")
            if row:
                qid, msg_type, thread_id, text_data, file_data = row["id"], row["type"], row["thread_id"], row["text_data"], row["file_data"]
                ok, error_data = False, "Unknown Error"
                if msg_type == "text": ok, error_data = await send_telegram_message(TG_CHAT_ID, thread_id, text_data)
                elif msg_type == "media":
                    files_info = json.loads(file_data) if file_data else []
                    if files_info: ok, error_data = await send_telegram_media(TG_CHAT_ID, thread_id, text_data, files_info[0])
                    else: ok = True
                elif msg_type == "album":
                    files_info = json.loads(file_data) if file_data else []
                    if files_info: ok, error_data = await send_telegram_album(TG_CHAT_ID, thread_id, text_data, files_info)
                    else: ok = True
                    
                if ok is True:
                    retry_counts.pop(qid, None)
                    await db.execute("DELETE FROM queue_v2 WHERE id = ?", (qid,))
                    if file_data:
                        for f in json.loads(file_data):
                            if os.path.exists(f.get("path", "")): os.remove(f["path"])
                    await asyncio.sleep(0.1)
                else:
                    if isinstance(error_data, str) and ("thread not found" in error_data.lower() or "topic not found" in error_data.lower()):
                        await db.execute("DELETE FROM topics WHERE thread_id = ?", (thread_id,))
                        await db.execute("UPDATE queue_v2 SET thread_id = NULL WHERE id = ?", (qid,))
                        continue
                    retry_counts[qid] = retry_counts.get(qid, 0) + 1
                    if retry_counts[qid] >= 10 or ok == "FATAL":
                        await db.execute("INSERT INTO queue_dead_letter (id, type, max_chat_id, thread_id, text_data, file_data, reason) VALUES (?, ?, ?, ?, ?, ?, ?)", 
                                         (qid, msg_type, "N/A", thread_id, text_data, file_data, str(error_data)))
                        await db.execute("DELETE FROM queue_v2 WHERE id = ?", (qid,))
                        retry_counts.pop(qid, None)
                    else: await asyncio.sleep(min(300, 5 * (2 ** (retry_counts[qid] - 1))))
            else:
                queue_event.clear()
                try: await asyncio.wait_for(queue_event.wait(), timeout=5.0)
                except asyncio.TimeoutError: pass
        except Exception: await asyncio.sleep(5.0)

# --- TELEGRAM LONG POLLING & COMMANDS ---
async def tg_command_polling():
    offset = 0
    logger.info("Starting Telegram Long Polling module...")
    while True:
        try:
            ok, response = await tg_api_call("getUpdates", {"offset": offset, "timeout": 20, "allowed_updates": '["message"]'}, timeout=30)
            if ok is True and isinstance(response, dict):
                for update in response.get("result", []):
                    offset = update["update_id"] + 1
                    msg = update.get("message")
                    if msg and str(msg.get("chat", {}).get("id", "")) == TG_CHAT_ID:
                        text = msg.get("text", "") or msg.get("caption", "")
                        text = text.strip() if isinstance(text, str) else ""
                        if text.startswith("/"): await handle_tg_command(msg)
                        elif msg.get("message_thread_id") and not msg.get("is_automatic_forward"):
                            await handle_tg_reply_to_max(msg)
            else: await asyncio.sleep(2)
        except Exception: await asyncio.sleep(5)

async def send_to_max_wrapper(target_id, text, dl_path=None):
    success = False
    if dl_path:
        for method_name in ["send_media", "send_file", "send_document", "send_photo"]:
            if hasattr(client, method_name):
                method = getattr(client, method_name)
                try: 
                    await method(target_id, dl_path)
                    success = True; break
                except Exception:
                    try: 
                        await method(dl_path, target_id)
                        success = True; break
                    except Exception: pass

    if text and (success or not dl_path):
        try:
            if hasattr(client, "send_message"): await client.send_message(text, target_id)
            elif hasattr(client, "send_text"): await client.send_text(text, target_id)
            success = True
        except Exception as e:
            if not dl_path: raise e 
            else: logger.error(f"File sent, text failed: {e}")
            
    if not success and dl_path: raise Exception("File send methods failed in PyMax")
    return success

async def handle_tg_reply_to_max(msg):
    photo, document = msg.get("photo"), msg.get("document")
    text = msg.get("text") or msg.get("caption") or ""
    text = text.strip() if isinstance(text, str) else ""
    thread_id, message_id = msg.get("message_thread_id"), msg.get("message_id")

    if not thread_id or (not text and not photo and not document): return

    try:
        row = await db.fetchone("SELECT max_chat_id, type FROM topics WHERE thread_id = ?", (thread_id,))
        if row:
            raw_target, m_type = row["max_chat_id"], row["type"]
            target_id = int(raw_target.replace("PRIVATE_", "")) if m_type == "private" and raw_target.startswith("PRIVATE_") else (int(raw_target) if raw_target.lstrip("-").isdigit() else raw_target)

            if target_id:
                dl_path = None
                if photo or document:
                    file_id = photo[-1]["file_id"] if photo else document["file_id"]
                    f_name = document.get("file_name", "") if document else ""
                    ext = "." + f_name.split(".")[-1] if "." in f_name else ".file" if document else ".jpg"
                    dl_path = await download_tg_file(file_id, ext)
                    if not dl_path:
                        await send_telegram_message(TG_CHAT_ID, thread_id, "❌ <b>Ошибка: Не удалось скачать файл из Telegram.</b>")
                        return

                if text: RECENT_SENT_TEXTS.append(text)
                try:
                    success = await send_to_max_wrapper(target_id, text, dl_path)
                    if success and message_id:
                        await set_telegram_reaction(TG_CHAT_ID, message_id, "👍")
                except Exception as e:
                    logger.error(f"Error sending reply to MAX: {e}")
                    await send_telegram_message(TG_CHAT_ID, thread_id, f"❌ <b>Ошибка отправки:</b> <code>{e}</code>")
                finally:
                    if dl_path and os.path.exists(dl_path): os.remove(dl_path)
    except Exception as e: logger.error(f"Reply processing failure: {e}")

async def handle_tg_command(msg):
    text, thread_id = msg.get("text", "").strip(), msg.get("message_thread_id")
    command = text.split("@")[0].lower()

    if command.startswith("/alias"):
        parts = text.split(maxsplit=1)
        if not thread_id:
            await send_telegram_message(TG_CHAT_ID, None, "❌ Эту команду нужно отправлять **строго внутри топика**.")
            return
        if len(parts) < 2:
            await send_telegram_message(TG_CHAT_ID, thread_id, "❌ **Использование:** `/alias <Имя>`")
            return
            
        new_alias = parts[1].strip()
        topic_row = await db.fetchone("SELECT max_chat_id, type FROM topics WHERE thread_id = ?", (thread_id,))
        if not topic_row:
            await send_telegram_message(TG_CHAT_ID, thread_id, "❌ Ошибка: Топик не найден в БД.")
            return

        raw_target, m_type = topic_row["max_chat_id"], topic_row["type"]
        target_id = raw_target.replace("PRIVATE_", "") if m_type == "private" and raw_target.startswith("PRIVATE_") else raw_target

        try:
            await db.execute("INSERT OR REPLACE INTO contacts (max_id, alias) VALUES (?, ?)", (target_id, new_alias))
            ok, _ = await tg_api_call("editForumTopic", {"chat_id": TG_CHAT_ID, "message_thread_id": thread_id, "name": new_alias[:128]})
            await db.execute("UPDATE topics SET name = ? WHERE thread_id = ?", (new_alias, thread_id))
            reply = f"✅ Топик переименован в: <b>{new_alias}</b>" if ok else f"⚠️ Алиас сохранен (<b>{new_alias}</b>), но не удалось изменить имя топика в ТГ."
        except Exception as e: reply = f"❌ Ошибка сохранения: {e}"
        await send_telegram_message(TG_CHAT_ID, thread_id, reply)

    elif command == "/status":
        max_status = "🔴 Офлайн"
        try:
            await asyncio.wait_for(client.get_user(543835), timeout=5.0)
            max_status = "🟢 Онлайн"
        except Exception: pass
        q_row = await db.fetchone("SELECT COUNT(*) as cnt FROM queue_v2")
        dlq_row = await db.fetchone("SELECT COUNT(*) as cnt FROM queue_dead_letter")
        last_row = await db.fetchone("SELECT value FROM settings WHERE key='last_msg_time'")
        q_count = q_row["cnt"] if q_row else "?"
        dlq_count = dlq_row["cnt"] if dlq_row else "?"
        last_time = last_row["value"] if last_row else "Ещё не было"
        reply = f"📊 <b>Статус Telemax</b>\n\n🔌 MAX API: {max_status}\n🚀 Telegram: 🟢 Онлайн\n📨 В очереди: <b>{q_count}</b> шт.\n⚠️ Ошибки (DLQ): <b>{dlq_count}</b> шт.\n⏱ Последнее от MAX: <code>{last_time}</code>"
        await send_telegram_message(TG_CHAT_ID, thread_id, reply)

    elif command == "/dlq":
        rows = await db.fetchall("SELECT id, timestamp, type, text_data, file_data FROM queue_dead_letter ORDER BY id ASC LIMIT 20")
        if not rows: reply = "✅ Очередь DLQ пуста."
        else:
            cnt_row = await db.fetchone("SELECT COUNT(*) as cnt FROM queue_dead_letter")
            total_count = cnt_row["cnt"] if cnt_row else len(rows)
            lines = [f"⚠️ <b>Зависшие ({total_count} шт.):</b>\n"]
            for r in rows:
                qid, ts, mtype, text_data, file_data = r["id"], r["timestamp"], r["type"], r["text_data"], r["file_data"]
                sm = re.search(r'<b>\[(.*?)\]:</b>', text_data) if text_data else None
                sn = sm.group(1) if sm else "Неизвестный"
                ct = re.sub(r'<[^>]+>', '', text_data or "").replace(f"[{sn}]:", "").strip()
                snip = ct[:60] + "..." if len(ct) > 60 else ct or "<Нет текста>"
                att = "Нет"
                if file_data and file_data != "null":
                    try:
                        exts = [f.get("ext", "") for f in json.loads(file_data) if "ext" in f]
                        att = ", ".join(exts).replace(".", "").upper() if exts else "Медиа"
                    except: att = "Ошибка"
                lines.append(f"🆔 <b>ID:</b> {qid}\n🕒 <b>Время:</b> {ts}\n👤 <b>От:</b> {sn}\n📎 <b>Вложение:</b> {att}\n📝 <b>Текст:</b> <i>{html.escape(snip)}</i>\n〰️〰️〰️")
            reply = "\n".join(lines)
        await send_telegram_message(TG_CHAT_ID, thread_id, reply)

    elif command == "/retry_dlq":
        rows = await db.fetchall("SELECT id, type, max_chat_id, thread_id, text_data, file_data FROM queue_dead_letter")
        if not rows:
            await send_telegram_message(TG_CHAT_ID, thread_id, "✅ DLQ пуста, нечего восстанавливать.")
            return
        for r in rows:
            await db.execute("INSERT INTO queue_v2 (type, max_chat_id, thread_id, text_data, file_data) VALUES (?, ?, ?, ?, ?)",
                             (r["type"], r["max_chat_id"], r["thread_id"], r["text_data"], r["file_data"]))
        await db.execute("DELETE FROM queue_dead_letter")
        queue_event.set()
        await send_telegram_message(TG_CHAT_ID, thread_id, f"🔄 <b>Восстановлено {len(rows)} сообщений из DLQ в рабочую очередь!</b>")

    elif command == "/clear_dlq":
        rows = await db.fetchall("SELECT file_data FROM queue_dead_letter")
        for r in rows:
            fdata = r["file_data"]
            if fdata and fdata != "null":
                try:
                    for f in json.loads(fdata):
                        if os.path.exists(f.get("path", "")): os.remove(f["path"])
                except: pass
        await db.execute("DELETE FROM queue_dead_letter")
        await send_telegram_message(TG_CHAT_ID, thread_id, "🗑 <b>DLQ очищена!</b> Файлы удалены с диска.")

# --- WATCHDOG ---
async def watchdog_worker():
    systemd_notify("READY=1")
    fails, last_stat, last_ping = 0, 0, 0
    while True:
        try:
            now = time.time()
            if now - last_ping >= 60:
                try: await asyncio.wait_for(client.get_user(543835), timeout=10.0)
                except asyncio.TimeoutError: raise Exception("Ping timeout")
                except Exception: pass
                last_ping, fails = now, 0
            if now - last_stat >= 1800:
                last_row = await db.fetchone("SELECT value FROM settings WHERE key='last_msg_time'")
                last_time = last_row["value"] if last_row else "Ещё не было"
                await update_status_message(f"<b>Статус: MAX-TG онлайн</b>\nПроверка: <code>{datetime.now().strftime('%d.%m.%Y %H:%M:%S')}</code>\nСМС: <code>{last_time}</code>")
                last_stat = now
            systemd_notify("WATCHDOG=1")
        except Exception:
            fails += 1
            if fails >= 3: os._exit(1)
        await asyncio.sleep(15)

# --- MAIN ENTRYPOINT ---
async def main() -> None:
    logger.info("Starting Telemax Bridge...")
    tasks = [
        asyncio.create_task(tg_forward_worker()),
        asyncio.create_task(queue_processor()),
        asyncio.create_task(watchdog_worker()),
        asyncio.create_task(tg_command_polling())
    ]
    try:
        await client.start()
        await asyncio.Event().wait()
    except Exception as e:
        logger.critical(f"Bridge crash: {e}")
        await send_push(f"Telemax Error: {e}", "skull", 5)
        raise e
    finally:
        for t in tasks: t.cancel()

if __name__ == "__main__":
    asyncio.run(main())
