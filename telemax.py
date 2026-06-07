import os
import sys
import socket
import asyncio
import html
import sqlite3
import json
import subprocess
import logging
import time
import requests
import re
import collections
from datetime import datetime

# --- ЗАГРУЗКА КОНСТАНТ ИЗ JSON ---
CONFIG_PATH = "/home/htpc/telemax/constants.json"
try:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        config = json.load(f)
        MAX_PHONE = config.get("MAX_PHONE")
        TG_BOT_TOKEN = config.get("TG_BOT_TOKEN")
        TG_CHAT_ID = str(config.get("TG_CHAT_ID"))
        NTFY_URL = config.get("NTFY_URL")
        MY_MAX_ID = config.get("MY_MAX_ID") # Ваш личный ID для фильтрации эха
        
        if not all([MAX_PHONE, TG_BOT_TOKEN, TG_CHAT_ID]):
            raise ValueError("В constants.json отсутствуют обязательные ключи (MAX_PHONE, TG_BOT_TOKEN, TG_CHAT_ID)")
except Exception as e:
    print(f"Критическая ошибка инициализации конфигурации: {e}")
    sys.exit(1)

# Кэш для предотвращения "эха" при пересылке сообщений из ТГ в MAX
RECENT_SENT_TEXTS = collections.deque(maxlen=50)

# --- РЕКВИЗИТЫ ---
CONTACTS = {
    0: "null",
}

SERVER_NAME = "Telemax"

from pymax import SocketMaxClient, Message
from pymax.payloads import UserAgentPayload

# --- ПОДГОТОВКА ПАПОК И БАЗЫ ДАННЫХ ---
WORK_DIR = os.path.dirname(os.path.abspath(__file__))
TEMP_DOWNLOAD_DIR = os.path.join(WORK_DIR, "media_queue")
DUMPS_DIR = os.path.join(WORK_DIR, "dumps")
os.makedirs(TEMP_DOWNLOAD_DIR, exist_ok=True)
os.makedirs(DUMPS_DIR, exist_ok=True)

# --- НАСТРОЙКА ЛОГИРОВАНИЯ ---
LOG_FILE = os.path.join(WORK_DIR, "telemax.log")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8"), logging.StreamHandler()])
logger = logging.getLogger(__name__)

# --- БАЗА ДАННЫХ ---
db_conn = sqlite3.connect(os.path.join(WORK_DIR, "telegram_queue.db"), check_same_thread=False)
db_cursor = db_conn.cursor()

db_cursor.execute('''CREATE TABLE IF NOT EXISTS queue_v2 
                     (id INTEGER PRIMARY KEY AUTOINCREMENT, type TEXT, max_chat_id TEXT, thread_id INTEGER, text_data TEXT, file_data TEXT)''')

# ОБНОВЛЕННАЯ ТАБЛИЦА TOPICS: Добавлено поле type
db_cursor.execute('''CREATE TABLE IF NOT EXISTS topics 
                     (max_chat_id TEXT PRIMARY KEY, thread_id INTEGER, name TEXT, type TEXT)''')

db_cursor.execute('''CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)''')
db_cursor.execute('''CREATE TABLE IF NOT EXISTS queue_dead_letter 
                     (id INTEGER PRIMARY KEY, type TEXT, max_chat_id TEXT, thread_id INTEGER, text_data TEXT, file_data TEXT, reason TEXT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)''')
db_conn.commit()

db_cursor.execute("SELECT value FROM settings WHERE key='last_msg_time'")
row = db_cursor.fetchone()
global_last_msg_time = row[0] if row else "Ещё не было"

def enqueue_v2(item_type, max_chat_id, thread_id, text_data, file_data=None):
    try:
        db_cursor.execute("INSERT INTO queue_v2 (type, max_chat_id, thread_id, text_data, file_data) VALUES (?, ?, ?, ?, ?)",
                          (item_type, str(max_chat_id), thread_id, text_data, file_data))
        db_conn.commit()
    except Exception as e:
        logger.error(f"DB Insert Error: {e}")

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
        with open(os.path.join(DUMPS_DIR, filename), "w", encoding="utf-8") as f:
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

def send_push(msg, tags="warning", priority=3):
    if not NTFY_URL: return
    try:
        requests.post(NTFY_URL, data=msg.encode('utf-8'), headers={"Title": SERVER_NAME, "Tags": tags, "Priority": str(priority)}, timeout=10)
    except Exception: pass

# --- ФУНКЦИИ ОТПРАВКИ В TELEGRAM ---
def tg_api_call(method, params=None, files=None, timeout=60):
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/{method}"
    cmd = ["curl", "-sS", "-x", "socks5h://127.0.0.1:10808", "--max-time", str(timeout)]
    if params:
        for k, v in params.items():
            if v is not None: cmd.extend(["--form-string", f"{k}={v}"])
    if files:
        for field, path in files.items(): cmd.extend(["-F", f"{field}=@{path}"])
    cmd.append(url)
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0: return False, "CURL_ERROR_OR_TIMEOUT"
        try: data = json.loads(result.stdout)
        except json.JSONDecodeError: return False, "JSON_DECODE_ERROR"
        if not data.get("ok", False):
            desc = data.get("description", str(data))
            if 400 <= data.get("error_code", 0) < 500: return "FATAL", desc
            return False, desc
        return True, data
    except Exception as e: return False, str(e)

def create_telegram_topic(chat_id, name):
    ok, data = tg_api_call("createForumTopic", params={"chat_id": chat_id, "name": name[:128]}, timeout=20)
    if ok is True and isinstance(data, dict): return data.get("result", {}).get("message_thread_id")
    return None

def set_telegram_reaction(chat_id, message_id, emoji="👍"):
    params = {
        "chat_id": chat_id,
        "message_id": message_id,
        "reaction": json.dumps([{"type": "emoji", "emoji": emoji}])
    }
    return tg_api_call("setMessageReaction", params=params, timeout=10)

def send_telegram_media(chat_id, thread_id, text, file_info):
    file_path = file_info["path"]
    ext = file_info["ext"]
    if not os.path.exists(file_path): return True, None
        
    params = {"chat_id": chat_id, "caption": text or "", "parse_mode": "HTML"}
    if thread_id: params["message_thread_id"] = thread_id

    # По умолчанию считаем всё неизвестное документами и даем большой таймаут
    field = "document"
    timeout_sec = 300 
    
    if ext in [".jpg", ".jpeg", ".png", ".webp"]: 
        field = "photo"
        timeout_sec = 60
    elif ext == ".ogg": 
        field = "voice"
        timeout_sec = 60
    elif ext == ".mp4": 
        field = "video"
        timeout_sec = 300 

    return tg_api_call(f"send{field.capitalize()}", params=params, files={field: file_path}, timeout=timeout_sec)

def send_telegram_album(chat_id, thread_id, text, files_info):
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
    return tg_api_call("sendMediaGroup", params=params, files=files_dict, timeout=300)

def send_telegram_message(chat_id, thread_id, text):
    params = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
    if thread_id: params["message_thread_id"] = thread_id
    return tg_api_call("sendMessage", params=params, timeout=30)

def update_status_message(text):
    try:
        db_cursor.execute("SELECT value FROM settings WHERE key='status_msg_id'")
        row = db_cursor.fetchone()
        msg_id = row[0] if row else None
        needs_new = False
        if msg_id:
            ok, data = tg_api_call("editMessageText", params={"chat_id": TG_CHAT_ID, "message_id": msg_id, "text": text, "parse_mode": "HTML"}, timeout=10)
            if ok == "FATAL" and isinstance(data, str) and "not found" in data.lower(): needs_new = True
        if not msg_id or needs_new:
            ok, data = tg_api_call("sendMessage", params={"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"}, timeout=10)
            if ok is True:
                new_id = data.get("result", {}).get("message_id")
                db_cursor.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", ("status_msg_id", str(new_id)))
                db_conn.commit()
                tg_api_call("pinChatMessage", params={"chat_id": TG_CHAT_ID, "message_id": new_id, "disable_notification": "true"})
    except Exception: pass

# --- ПОТОКОВОЕ СКАЧИВАНИЕ ФАЙЛОВ ИЗ MAX ---
async def brutal_download(client_instance, attach, download_path):
    url_to_download = None
    try:
        if hasattr(client_instance, "get_file_url"):
            url_to_download = await client_instance.get_file_url(attach)
    except: pass

    if not url_to_download:
        # ИСПРАВЛЕНИЕ: Теперь ищем любые возможные ID, включая file_id для документов
        actual_id = None
        for attr_name in ['file_id', 'video_id', 'image_id', 'audio_id', 'id']:
            val = attach.get(attr_name) if isinstance(attach, dict) else getattr(attach, attr_name, None)
            if val:
                actual_id = val
                break
                
        token = attach.get('token') if isinstance(attach, dict) else getattr(attach, 'token', None)
        
        if actual_id:
            file_id_str = f"{actual_id}"
            if token: file_id_str += f"?token={token}"
            try:
                if hasattr(client_instance, "_api") and hasattr(client_instance._api, "get_file"):
                    file_content = await client_instance._api.get_file(file_id_str)
                    if file_content:
                        with open(download_path, 'wb') as f: f.write(file_content)
                        return True
            except: pass

    if not url_to_download:
        for attr in ['url', 'file_url', 'download_url', 'source', 'link', 'href', 'base_url']:
            val = attach.get(attr) if isinstance(attach, dict) else getattr(attach, attr, None)
            if isinstance(val, str) and val.startswith("http"):
                url_to_download = val
                break
            
    if url_to_download:
        try:
            def do_download():
                headers = {"User-Agent": "Mozilla/5.0"}
                with requests.get(url_to_download, headers=headers, timeout=(15, 300), stream=True) as r:
                    if r.status_code == 200:
                        with open(download_path, 'wb') as f:
                            for chunk in r.iter_content(chunk_size=8192):
                                if chunk: f.write(chunk)
                        return True
                    return False
            loop = asyncio.get_running_loop()
            if await loop.run_in_executor(None, do_download): return True
        except: pass

    for attr in ['bytes', 'file_bytes', 'data', 'content']:
        val = attach.get(attr) if isinstance(attach, dict) else getattr(attach, attr, None)
        if isinstance(val, bytes):
            with open(download_path, 'wb') as f: f.write(val)
            return True
            
    return False

ua = UserAgentPayload(device_type="DESKTOP")
client = SocketMaxClient(phone=MAX_PHONE, work_dir="session_cache", headers=ua)
async def fake_send_navigation_event(*args, **kwargs): pass
client._send_navigation_event = fake_send_navigation_event
client.send_navigation_event = fake_send_navigation_event

message_queue = asyncio.Queue()

@client.on_message()
async def handle_message(message: Message) -> None:
    await message_queue.put(message)

async def tg_forward_worker():
    while True:
        message = await message_queue.get()
        try: await process_and_enqueue(message)
        except Exception as e: logger.error(f"Ошибка обработки: {e}")
        finally: message_queue.task_done()

async def process_and_enqueue(message: Message) -> None:
    global global_last_msg_time
    global_last_msg_time = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    db_cursor.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", ("last_msg_time", global_last_msg_time))
    db_conn.commit()
    
    dump_message_to_json(message, reason="incoming")

    msg_type_raw = getattr(message, "type", "").upper()
    chat_id = getattr(message, "chat_id", None)
    sender_id = getattr(message, "sender", None)
    t = str(getattr(message, "text", "") or getattr(message, "caption", "")).strip()

    # --- ФИЛЬТР 1: Защита от дублей (наш собственный ответ из ТГ) ---
    if t and t in RECENT_SENT_TEXTS:
        logger.info("Пропущено: Эхо-сообщение (мы его только что отправили из Telegram).")
        return

    # --- ФИЛЬТР 2: Базовые исходящие ---
    is_outgoing = getattr(message, "out", False) or getattr(message, "outgoing", False) or getattr(message, "is_out", False)
    if is_outgoing:
        logger.info("Пропущено: Сообщение помечено как исходящее (out=True).")
        return

    # --- ФИЛЬТР 3: Защита от своих же сообщений, отправленных с телефона ---
    if MY_MAX_ID and sender_id == MY_MAX_ID:
        logger.info(f"Пропущено: Сообщение от самого себя (ID: {MY_MAX_ID}).")
        return

    # --- ФИЛЬТР 4: Системные события ---
    if msg_type_raw in ["SERVICE", "SYSTEM", "EVENT", "ACTION"] or getattr(message, "action", None):
        return

    # Получение имени отправителя
    sender_name = "Неизвестный"
    if sender_id is not None:
        if sender_id in CONTACTS: sender_name = CONTACTS[sender_id]
        else:
            try:
                ui = await asyncio.wait_for(client.get_user(sender_id), timeout=5.0)
                extracted = f"{getattr(ui, 'first_name', '')} {getattr(ui, 'last_name', '')}".strip()
                if not extracted and hasattr(ui, "names") and ui.names: extracted = ui.names[0].name
                elif not extracted and hasattr(ui, "name") and ui.name: extracted = ui.name
                sender_name = extracted if extracted else f"ID:{sender_id}"
            except: sender_name = f"ID:{sender_id}"
    elif msg_type_raw == "CHANNEL": sender_name = "Канал"

    # Получение названия чата
    chat_title = getattr(message, "chat_title", None) or getattr(message, "title", None)
    if not chat_title and getattr(message, "chat", None):
        chat_title = getattr(message.chat, "title", None) or getattr(message.chat, "name", None)
    if not chat_title and chat_id:
        try:
            ci = await asyncio.wait_for(client.get_chat(str(chat_id)), timeout=5.0)
            if ci: chat_title = getattr(ci, "title", None) or getattr(ci, "name", None)
        except: pass

    if msg_type_raw == "CHANNEL" and sender_name == "Канал" and chat_title: sender_name = chat_title

    # --- УМНАЯ КЛАССИФИКАЦИЯ ID И ТИПОВ ЧАТОВ ---
    is_private = False
    if msg_type_raw in ["PRIVATE", "BOT"]: is_private = True
    elif msg_type_raw == "USER":
        is_private = not (chat_id and str(chat_id).startswith("-"))

    if is_private:
        # ИСПРАВЛЕНИЕ: Биндим топик к ID диалога (бота), а не к служебному отправителю
        target = chat_id if chat_id else sender_id
        topic_target_id = f"PRIVATE_{target}" if target else "PRIVATE_UNKNOWN"
        m_type = "private"
        
        # Имя топика в первую очередь берем от названия бота/чата
        if chat_title:
            topic_name = chat_title
        else:
            topic_name = sender_name if sender_name and not sender_name.startswith("ID:") else f"Chat {target}"
    else:
        topic_target_id = str(chat_id) if chat_id else "UNKNOWN_GROUP"
        m_type = "group"
        topic_name = chat_title if chat_title else f"Группа {topic_target_id}"

    # Создание/поиск топика в БД
    db_cursor.execute("SELECT thread_id FROM topics WHERE max_chat_id = ?", (topic_target_id,))
    row = db_cursor.fetchone()
    if row: thread_id = row[0]
    else:
        loop = asyncio.get_running_loop()
        thread_id = await loop.run_in_executor(None, create_telegram_topic, TG_CHAT_ID, topic_name)
        if not thread_id:
            safe_name = "".join(c for c in topic_name if c.isalnum() or c in " _-")[:128] or f"Topic {topic_target_id}"
            thread_id = await loop.run_in_executor(None, create_telegram_topic, TG_CHAT_ID, safe_name)
        if thread_id:
            # ЗАПИСЫВАЕМ С ПОЛЕМ TYPE
            db_cursor.execute("INSERT INTO topics (max_chat_id, thread_id, name, type) VALUES (?, ?, ?, ?)", 
                              (topic_target_id, thread_id, topic_name, m_type))
            db_conn.commit()

    header = f"[{sender_name}]:" if (is_private or not chat_title or chat_title == sender_name) else f"[{chat_title}], [{sender_name}]:"
    text_parts, all_attachments, forward_prefix = [], [], ""
    
    if t and t != "None": text_parts.append(t)
    for attr in ["attaches", "attachments", "document", "video", "photo", "sticker", "voice"]:
        val = getattr(message, attr, None)
        if val: all_attachments.extend(val) if isinstance(val, list) else all_attachments.append(val)

    # Обработка Forward
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
                dl_path = os.path.join(TEMP_DOWNLOAD_DIR, f"{f_id}{ext}")
                is_dl = os.path.exists(dl_path)
                if not is_dl and not isinstance(attach, dict):
                    try:
                        if hasattr(client, "download_media"): await client.download_media(attach, out_dir=TEMP_DOWNLOAD_DIR, file_name=f"{f_id}{ext}")
                        elif hasattr(attach, "download"): await attach.download(out_dir=TEMP_DOWNLOAD_DIR, file_name=f"{f_id}{ext}")
                    except: pass
                is_dl = os.path.exists(dl_path)
                if not is_dl: is_dl = await brutal_download(client, attach, dl_path)
                if is_dl: downloaded_files.append({"path": dl_path, "ext": ext})
                else: body_text += f"\n\n<i>[Ошибка: Вложение {ext} не скачалось]</i>"
            except Exception as e: logger.error(f"Ошибка вложения: {e}")

    if not body_text and not downloaded_files: return

    full_caption = f"<b>{html.escape(header)}</b>\n{forward_prefix}{html.escape(body_text)}".strip()
    album_files = [df for df in downloaded_files if df["ext"] in [".jpg", ".jpeg", ".png", ".mp4"]]
    single_files = [df for df in downloaded_files if df not in album_files]
    caption_assigned = False

    if len(full_caption) > 1000:
        enqueue_v2("text", chat_id, thread_id, full_caption, None)
        caption_assigned = True

    for i in range(0, len(album_files), 10):
        chunk = album_files[i:i+10]
        c = full_caption if not caption_assigned else ""
        caption_assigned = True
        enqueue_v2("media" if len(chunk) == 1 else "album", chat_id, thread_id, c, json.dumps([chunk[0]] if len(chunk) == 1 else chunk))

    for single in single_files:
        c = full_caption if not caption_assigned else ""
        caption_assigned = True
        enqueue_v2("media", chat_id, thread_id, c, json.dumps([single]))

    if not downloaded_files and not caption_assigned:
        enqueue_v2("text", chat_id, thread_id, full_caption, None)

async def queue_processor():
    loop = asyncio.get_running_loop()
    retry_counts = {}
    while True:
        try:
            db_cursor.execute("SELECT id, type, thread_id, text_data, file_data FROM queue_v2 ORDER BY id ASC LIMIT 1")
            row = db_cursor.fetchone()
            if row:
                qid, msg_type, thread_id, text_data, file_data = row
                ok, error_data = False, "Unknown Error"
                if msg_type == "text": ok, error_data = await loop.run_in_executor(None, send_telegram_message, TG_CHAT_ID, thread_id, text_data)
                elif msg_type == "media":
                    files_info = json.loads(file_data) if file_data else []
                    if files_info: ok, error_data = await loop.run_in_executor(None, send_telegram_media, TG_CHAT_ID, thread_id, text_data, files_info[0])
                    else: ok = True
                elif msg_type == "album":
                    files_info = json.loads(file_data) if file_data else []
                    if files_info: ok, error_data = await loop.run_in_executor(None, send_telegram_album, TG_CHAT_ID, thread_id, text_data, files_info)
                    else: ok = True
                    
                if ok is True:
                    retry_counts.pop(qid, None)
                    db_cursor.execute("DELETE FROM queue_v2 WHERE id = ?", (qid,))
                    db_conn.commit()
                    if file_data:
                        for f in json.loads(file_data):
                            if os.path.exists(f.get("path", "")): os.remove(f["path"])
                    await asyncio.sleep(1.0)
                else:
                    if isinstance(error_data, str) and ("thread not found" in error_data.lower() or "topic not found" in error_data.lower()):
                        db_cursor.execute("DELETE FROM topics WHERE thread_id = ?", (thread_id,))
                        db_cursor.execute("UPDATE queue_v2 SET thread_id = NULL WHERE id = ?", (qid,))
                        db_conn.commit()
                        continue
                    retry_counts[qid] = retry_counts.get(qid, 0) + 1
                    if retry_counts[qid] >= 10 or ok == "FATAL":
                        db_cursor.execute("INSERT INTO queue_dead_letter (id, type, max_chat_id, thread_id, text_data, file_data, reason) VALUES (?, ?, ?, ?, ?, ?, ?)", 
                                          (qid, msg_type, "N/A", thread_id, text_data, file_data, str(error_data)))
                        db_cursor.execute("DELETE FROM queue_v2 WHERE id = ?", (qid,))
                        db_conn.commit()
                        retry_counts.pop(qid, None)
                    else: await asyncio.sleep(min(300, 5 * (2 ** (retry_counts[qid] - 1))))
            else: await asyncio.sleep(2.0)
        except Exception: await asyncio.sleep(5.0)

# --- TELEGRAM LONG POLLING ---
async def tg_command_polling():
    loop = asyncio.get_running_loop()
    offset = 0
    logger.info("Запуск модуля Telegram (Long Polling)...")
    while True:
        try:
            ok, response = await loop.run_in_executor(None, tg_api_call, "getUpdates", {"offset": offset, "timeout": 20, "allowed_updates": '["message"]'}, None, 30)
            if ok is True and isinstance(response, dict):
                for update in response.get("result", []):
                    offset = update["update_id"] + 1
                    msg = update.get("message")
                    if msg and str(msg.get("chat", {}).get("id", "")) == TG_CHAT_ID:
                        text = msg.get("text", "").strip()
                        if text.startswith("/"): await handle_tg_command(msg)
                        elif msg.get("message_thread_id") and not msg.get("is_automatic_forward"):
                            await handle_tg_reply_to_max(msg)
            else: await asyncio.sleep(2)
        except Exception: await asyncio.sleep(5)


async def handle_tg_reply_to_max(msg):
    text = msg.get("text", "")
    if isinstance(text, str): text = text.strip()
    thread_id = msg.get("message_thread_id")
    message_id = msg.get("message_id") # <-- Извлекаем ID сообщения

    if not text or not thread_id: return

    try:
        db_cursor.execute("SELECT max_chat_id, type FROM topics WHERE thread_id = ?", (thread_id,))
        row = db_cursor.fetchone()
        if row:
            raw_target, m_type = row
            
            # РАСШИФРОВКА ID НА ОСНОВЕ ТИПА
            if m_type == "private" and raw_target.startswith("PRIVATE_"):
                try: target_id = int(raw_target.replace("PRIVATE_", ""))
                except ValueError: return
            else:
                try: target_id = int(raw_target)
                except ValueError: target_id = raw_target 

            if target_id:
                logger.info(f"Отправка ответа в MAX (target_id: {target_id}, текст: {text[:20]}...)")
                
                # Добавляем текст в кэш ДО отправки, чтобы фильтр сразу поймал эхо
                RECENT_SENT_TEXTS.append(text)
                
                try:
                    success = False
                    if hasattr(client, "send_message"): 
                        await client.send_message(text, target_id)
                        success = True
                    elif hasattr(client, "send_text"): 
                        await client.send_text(text, target_id)
                        success = True
                    else: 
                        logger.error("В PyMax нет методов отправки.")

                    # --- ЕСЛИ ОТПРАВЛЕНО УСПЕШНО, СТАВИМ ЛАЙК В ТГ ---
                    if success and message_id:
                        loop = asyncio.get_running_loop()
                        await loop.run_in_executor(None, set_telegram_reaction, TG_CHAT_ID, message_id, "👍")

                except Exception as e:
                    logger.error(f"Ошибка отправки ответа в MAX: {e}")
                    loop = asyncio.get_running_loop()
                    await loop.run_in_executor(None, send_telegram_message, TG_CHAT_ID, thread_id, f"❌ <b>Ошибка отправки в MAX:</b> <code>{e}</code>")
    except Exception as e: logger.error(f"Сбой логики обработки ответа: {e}")


async def handle_tg_command(msg):
    text, thread_id = msg.get("text", "").strip(), msg.get("message_thread_id")
    command = text.split("@")[0].lower()
    
    if command == "/status":
        max_status = "🔴 Офлайн"
        try:
            await asyncio.wait_for(client.get_user(543835), timeout=5.0)
            max_status = "🟢 Онлайн"
        except Exception: pass
        try:
            db_cursor.execute("SELECT COUNT(*) FROM queue_v2")
            q_count = db_cursor.fetchone()[0]
            db_cursor.execute("SELECT COUNT(*) FROM queue_dead_letter")
            dlq_count = db_cursor.fetchone()[0]
        except Exception: q_count, dlq_count = "?", "?"
        reply = f"📊 <b>Статус Telemax</b>\n\n🔌 MAX API: {max_status}\n🚀 Telegram: 🟢 Онлайн\n📨 В очереди: <b>{q_count}</b> шт.\n⚠️ Ошибки (DLQ): <b>{dlq_count}</b> шт.\n⏱ Последнее от MAX: <code>{global_last_msg_time}</code>"
        await asyncio.get_running_loop().run_in_executor(None, send_telegram_message, TG_CHAT_ID, thread_id, reply)

    elif command == "/dlq":
        try:
            db_cursor.execute("SELECT id, timestamp, type, text_data, file_data FROM queue_dead_letter ORDER BY id ASC LIMIT 20")
            rows = db_cursor.fetchall()
            if not rows: reply = "✅ Очередь DLQ пуста."
            else:
                db_cursor.execute("SELECT COUNT(*) FROM queue_dead_letter")
                total_count = db_cursor.fetchone()[0]
                lines = [f"⚠️ <b>Зависшие ({total_count} шт.):</b>\n"]
                for r in rows:
                    qid, ts, mtype, text_data, file_data = r
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
                    elif mtype in ["media", "album"]: att = "Да"
                    lines.append(f"🆔 <b>ID:</b> {qid}\n🕒 <b>Время:</b> {ts}\n👤 <b>От:</b> {sn}\n📎 <b>Вложение:</b> {att}\n📝 <b>Текст:</b> <i>{html.escape(snip)}</i>\n〰️〰️〰️")
                if total_count > 20: lines.append(f"\n<i>...и еще {total_count - 20} (показаны 20).</i>")
                reply = "\n".join(lines)
        except Exception as e: reply = f"❌ Ошибка БД: {e}"
        await asyncio.get_running_loop().run_in_executor(None, send_telegram_message, TG_CHAT_ID, thread_id, reply)

    elif command == "/clear_dlq":
        try:
            db_cursor.execute("SELECT file_data FROM queue_dead_letter")
            for (fdata,) in db_cursor.fetchall():
                if fdata and fdata != "null":
                    try:
                        for f in json.loads(fdata):
                            if os.path.exists(f.get("path", "")) : os.remove(f["path"])
                    except: pass
            db_cursor.execute("DELETE FROM queue_dead_letter")
            db_conn.commit()
            reply = "🗑 <b>DLQ очищена!</b> \nМедиа удалены с диска."
        except Exception as e: reply = f"❌ Ошибка: {e}"
        await asyncio.get_running_loop().run_in_executor(None, send_telegram_message, TG_CHAT_ID, thread_id, reply)

async def watchdog_worker():
    systemd_notify("READY=1")
    fails, last_stat, last_ping = 0, 0, 0
    loop = asyncio.get_running_loop()
    while True:
        try:
            now = time.time()
            if now - last_ping >= 60:
                try: await asyncio.wait_for(client.get_user(543835), timeout=10.0)
                except asyncio.TimeoutError: raise Exception("Таймаут")
                except Exception: pass
                last_ping, fails = now, 0
            if now - last_stat >= 1800:
                await loop.run_in_executor(None, update_status_message, f"<b>Статус: MAX-TG онлайн</b>\nПроверка: <code>{datetime.now().strftime('%d.%m.%Y %H:%M:%S')}</code>\nСМС: <code>{global_last_msg_time}</code>")
                last_stat = now
            systemd_notify("WATCHDOG=1")
        except Exception:
            fails += 1
            if fails >= 3: os._exit(1)
        await asyncio.sleep(15)

async def main() -> None:
    logger.info("Запуск Bridge...")
    wt, qt, wdt, pt = asyncio.create_task(tg_forward_worker()), asyncio.create_task(queue_processor()), asyncio.create_task(watchdog_worker()), asyncio.create_task(tg_command_polling())
    try:
        await client.start()
        await asyncio.Event().wait()
    except Exception as e:
        logger.critical(f"Падение: {e}")
        send_push(f"Ошибка MAX: {e}", "skull", 5)
        raise e
    finally:
        for t in [wt, qt, wdt, pt]: t.cancel()

if __name__ == "__main__":
    asyncio.run(main())
