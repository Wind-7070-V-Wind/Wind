import asyncio
import json
import os
import hashlib
import secrets
import time
import re
import base64
from datetime import datetime, timezone, timedelta
from urllib.parse import quote
from collections import deque, defaultdict

from fastapi import FastAPI, Request, HTTPException, WebSocket, WebSocketDisconnect, Depends
from fastapi.responses import Response, HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import httpx
import logging
import psutil
import ipaddress
import socket

try:
    import telebot
    from telebot.async_telebot import AsyncTeleBot
    from telebot import types
    TELEBOT_AVAILABLE = True
except ImportError:
    TELEBOT_AVAILABLE = False
    print("WARNING: Please install pyTelegramBotAPI to enable the Telegram Bot: pip install pyTelegramBotAPI")

log_queue = deque(maxlen=150)

class QueueHandler(logging.Handler):
    def emit(self, record):
        try:
            msg = self.format(record)
            log_queue.append(msg)
        except Exception:
            pass

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("Wind-Gateway")

q_handler = QueueHandler()
q_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(q_handler)
logging.getLogger("uvicorn.error").addHandler(q_handler)
logging.getLogger("uvicorn.access").addHandler(q_handler)

app = FastAPI(title="Wind Panel", docs_url=None, redoc_url=None)

CONFIG = {
    "port": int(os.environ.get("PORT", 8000)),
    "secret": os.environ.get("SECRET_KEY", secrets.token_urlsafe(32)),
    "telegram_token": "",
    "telegram_admin_id": "",
    "bot_lang": "fa",
    "status_display_mode": "separate",
    "custom_status_text": "🌟 اشتراک شما فعال است",
}

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

connections: dict = {}
connections_lock = asyncio.Lock()
connection_sockets: dict = {}
link_ip_map: dict = defaultdict(set)
stats = {"total_bytes": 0, "total_requests": 0, "total_errors": 0, "start_time": time.time()}
error_logs: deque = deque(maxlen=50)
hourly_traffic: dict = defaultdict(int)
daily_traffic: dict = defaultdict(int)
http_client: httpx.AsyncClient | None = None

LINKS: dict = {}
LINKS_LOCK = asyncio.Lock()

# ===== CLEAN IPS =====
CLEAN_IPS: dict = {}
CLEAN_IPS_LOCK = asyncio.Lock()

SUBSCRIPTION_GROUPS: dict = {}
GROUPS_LOCK = asyncio.Lock()

notified_uids = set()

SESSION_COOKIE = "ren_session"
SESSION_TTL = 60 * 60 * 24 * 7
UNLIMITED_QUOTA_BYTES = 53687091200000
DEFAULT_PORT = 443

DB_FILE = "panel_db.json"
bot = None
bot_polling_task: asyncio.Task | None = None

# Telegram Bot i18n
BOT_I18N = {
    "fa": {
        "btn_stats": "📊 آمار",
        "btn_users": "👥 کاربران",
        "btn_top": "🔝 پرمصرف‌ترین‌ها",
        "btn_create": "➕ ساخت کاربر",
        "btn_addip": "🌐 افزودن آی‌پی تمیز",
        "welcome": "👑 <b>به ربات تلگرامی پنل Wind خوش اومدی!</b>\nاینباندهای VLESS رو مستقیم از تلگرام مدیریت کن.",
        "lang_switched": "🌐 زبان به <b>فارسی</b> تغییر یافت.",
        "stats": (
            "<b>📊 وضعیت سرور</b>\n\n"
            "🌐 <b>دامنه:</b> <code>{domain}</code>\n"
            "🔋 <b>پردازنده:</b> <code>{cpu:.1f}%</code>\n"
            "💾 <b>رم:</b> <code>{mem:.1f}%</code>\n"
            "⏱ <b>آپ‌تایم:</b> <code>{uptime}</code>\n"
            "👥 <b>اتصالات فعال:</b> <code>{active}</code>\n"
            "📈 <b>ترافیک کل:</b> <code>{traffic} MB</code>\n"
            "🔑 <b>تعداد کاربران:</b> <code>{links}</code>"
        ),
        "users_title": "<b>👥 لیست کاربران و میزان مصرف:</b>\n",
        "users_line": "• <b>{label}</b>: {used} / {limit} (⌛ {exp}) | {status}",
        "no_inbounds": "هیچ کاربری یافت نشد.",
        "status_on": "🟢 فعال",
        "status_off": "🔴 غیرفعال",
        "top_title": "<b>🔝 ۵ کاربر پرمصرف:</b>\n",
        "top_line": "{i}. <b>{label}</b>: مصرف {used} از {limit}",
        "create_format": (
            "❌ <b>فرمت اشتباه است.</b>\n"
            "فرمت: <code>/create [نام] [حجم_GB] [روز]</code>\n"
            "مثال: <code>/create Ali 15 30</code>"
        ),
        "create_bad_name": "❌ <b>نام فقط باید شامل حروف انگلیسی و عدد باشد.</b>",
        "create_bad_limit": "❌ <b>حجم ترافیک باید عدد باشد.</b>",
        "create_bad_days": "❌ <b>تعداد روز باید عدد صحیح باشد.</b>",
        "create_exists": "❌ <b>کاربری با نام «{label}» از قبل وجود دارد.</b>",
        "create_success": (
            "✅ <b>کاربر با موفقیت ساخته شد!</b>\n\n"
            "👤 <b>نام:</b> <code>{label}</code>\n"
            "📊 <b>حجم:</b> <code>{quota}</code>\n"
            "⌛ <b>انقضا:</b> <code>{expiry}</code>\n\n"
            "🔗 <b>لینک VLESS:</b>\n<code>{vless}</code>\n\n"
            "🌐 <b>آدرس اشتراک:</b>\n<code>{sub}</code>"
        ),
        "unlimited": "نامحدود",
        "days_fmt": "{days} روز",
        "addaddr_format": "❌ فرمت: <code>/addaddr [آی‌پی_یا_دامنه]</code>",
        "addaddr_invalid": "❌ فرمت آدرس نامعتبر است.",
        "addaddr_exists": "⚠️ آدرس «{addr}» قبلاً در لیست موجود است.",
        "addaddr_success": "✅ آی‌پی/دامنه‌ی <code>{addr}</code> با موفقیت اضافه شد.",
        "toggle_format": "❌ فرمت: <code>/{action} [نام‌کاربری]</code>",
        "toggle_not_found": "❌ کاربر «{name}» پیدا نشد.",
        "toggle_success": "✅ کاربر <code>{name}</code> با موفقیت <b>{state}</b> شد.",
        "state_enabled": "فعال",
        "state_disabled": "غیرفعال",
        "reset_format": "❌ فرمت: <code>/reset [نام‌کاربری]</code>",
        "reset_success": "🔄 مصرف کاربر <code>{name}</code> به صفر بازنشانی شد.",
        "create_guide": (
            "➕ <b>راهنمای ساخت کاربر:</b>\n\n"
            "از دستور <code>/create</code> استفاده کن. فرمت:\n"
            "<code>/create [نام] [حجم_GB] [روز]</code>\n\n"
            "<b>مثال‌ها:</b>\n"
            "• <code>/create Ali 15 30</code> (۱۵ گیگ، ۳۰ روز اعتبار)\n"
            "• <code>/create Reza 0 0</code> (نامحدود، بدون انقضا)"
        ),
        "addip_guide": (
            "🌐 <b>راهنمای افزودن آی‌پی تمیز:</b>\n\n"
            "از دستور <code>/addaddr</code> استفاده کن. فرمت:\n"
            "<code>/addaddr [آی‌پی_یا_دامنه]</code>\n\n"
            "<b>مثال:</b>\n"
            "• <code>/addaddr cf.example.com</code>\n"
            "• <code>/addaddr 1.1.1.1</code>"
        ),
        "quota_alert": (
            "⚠️ <b>هشدار اتمام حجم!</b>\n"
            "کاربر: <code>{label}</code> به سقف مصرف رسید.\n"
            "مصرف: <code>{used} / {limit}</code>"
        ),
        "expiry_alert": (
            "⏰ <b>هشدار انقضا!</b>\n"
            "کاربر: <code>{label}</code> منقضی شد.\n"
            "تاریخ انقضا: <code>{exp}</code>"
        ),
    },
}

def bot_lang() -> str:
    return "fa"

def L(key: str, **kwargs) -> str:
    template = BOT_I18N.get("fa", {}).get(key) or key
    try:
        return template.format(**kwargs)
    except Exception:
        return template

def build_main_keyboard():
    if not TELEBOT_AVAILABLE:
        return None
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton(L("btn_stats"), callback_data="tg_stats"),
        types.InlineKeyboardButton(L("btn_users"), callback_data="tg_users"),
        types.InlineKeyboardButton(L("btn_top"), callback_data="tg_top"),
        types.InlineKeyboardButton(L("btn_create"), callback_data="tg_create_guide"),
        types.InlineKeyboardButton(L("btn_addip"), callback_data="tg_add_ip_guide"),
    )
    return kb

# Database
def save_db():
    data = {
        "auth_hash": AUTH["password_hash"],
        "links": LINKS,
        "telegram_token": CONFIG["telegram_token"],
        "telegram_admin_id": CONFIG["telegram_admin_id"],
        "subscription_groups": SUBSCRIPTION_GROUPS,
        "status_display_mode": CONFIG["status_display_mode"],
        "custom_status_text": CONFIG["custom_status_text"],
        "clean_ips": CLEAN_IPS,
    }
    try:
        with open(DB_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
    except Exception as e:
        logger.error(f"Error saving DB: {e}")

def load_db():
    global LINKS, SUBSCRIPTION_GROUPS, CLEAN_IPS
    if not os.path.exists(DB_FILE):
        return
    try:
        with open(DB_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        AUTH["password_hash"] = data.get("auth_hash", AUTH["password_hash"])
        LINKS.clear()
        LINKS.update(data.get("links", {}))
        CONFIG["telegram_token"] = data.get("telegram_token", "")
        CONFIG["telegram_admin_id"] = data.get("telegram_admin_id", "")
        SUBSCRIPTION_GROUPS.clear()
        SUBSCRIPTION_GROUPS.update(data.get("subscription_groups", {}))
        CONFIG["status_display_mode"] = data.get("status_display_mode", "separate")
        CONFIG["custom_status_text"] = data.get("custom_status_text", "🌟 اشتراک شما فعال است")
        CLEAN_IPS.clear()
        CLEAN_IPS.update(data.get("clean_ips", {}))
    except Exception as e:
        logger.error(f"Error loading DB: {e}")

# Auth
def hash_password(pw: str) -> str:
    return hashlib.sha256(f"{pw}{CONFIG['secret']}".encode()).hexdigest()

AUTH = {"password_hash": hash_password("admin")}
SESSIONS: dict = {}
SESSIONS_LOCK = asyncio.Lock()

async def create_session() -> str:
    token = secrets.token_urlsafe(32)
    async with SESSIONS_LOCK:
        SESSIONS[token] = time.time() + SESSION_TTL
    return token

async def is_valid_session(token: str | None) -> bool:
    if not token:
        return False
    async with SESSIONS_LOCK:
        exp = SESSIONS.get(token)
        if exp is None or exp < time.time():
            SESSIONS.pop(token, None)
            return False
        return True

async def destroy_session(token: str | None):
    if token:
        async with SESSIONS_LOCK:
            SESSIONS.pop(token, None)

async def require_auth(request: Request):
    auth_header = request.headers.get("Authorization")
    token = None
    if auth_header and auth_header.startswith("Bearer "):
        token = auth_header[7:]
    if not token:
        token = request.cookies.get(SESSION_COOKIE)
    if not await is_valid_session(token):
        raise HTTPException(status_code=401, detail="unauthorized")
    return token

# Keep-alive
async def keep_alive():
    while True:
        await asyncio.sleep(600)
        try:
            domain = get_domain()
            if domain and domain != "localhost":
                async with httpx.AsyncClient(timeout=10.0) as client:
                    await client.get(f"https://{domain}/health")
        except Exception:
            pass

@app.on_event("startup")
async def startup():
    global http_client
    load_db()
    limits = httpx.Limits(max_connections=500, max_keepalive_connections=100)
    timeout = httpx.Timeout(30.0, connect=10.0)
    http_client = httpx.AsyncClient(limits=limits, timeout=timeout, follow_redirects=True)
    asyncio.create_task(keep_alive())
    await restart_telegram_bot()
    asyncio.create_task(telegram_notifier_cron())
    await ensure_default_link()

@app.on_event("shutdown")
async def shutdown():
    await _stop_telegram_bot()
    if http_client:
        await http_client.aclose()

# Helpers
def get_domain() -> str:
    return (
        os.environ.get("RENDER_EXTERNAL_URL", os.environ.get("RAILWAY_PUBLIC_DOMAIN", "localhost"))
        .replace("https://", "").replace("http://", "")
    )

def generate_vless_link(uuid: str, remark: str = "", address: str = None, port: int = None) -> str:
    domain = get_domain()
    addr = address if address else domain
    use_port = port if port else DEFAULT_PORT
    path = f"/ws/{uuid}"
    params = {
        "encryption": "none", "security": "tls", "type": "ws",
        "host": domain, "path": path, "sni": domain, "fp": "chrome", "alpn": "http/1.1"
    }
    query = "&".join(f"{k}={quote(str(v))}" for k, v in params.items())
    return f"vless://{uuid}@{addr}:{use_port}?{query}#{quote(remark)}"

def uptime() -> str:
    secs = int(time.time() - stats["start_time"])
    h, m, s = secs // 3600, (secs % 3600) // 60, secs % 60
    return f"{h:02d}:{m:02d}:{s:02d}"

def parse_size_to_bytes(value: float, unit: str) -> int:
    unit = unit.upper()
    if unit == "GB": return int(value * 1024 * 1024 * 1024)
    if unit == "MB": return int(value * 1024 * 1024)
    if unit == "KB": return int(value * 1024)
    return int(value)

def parse_expires_at(raw: str | None) -> datetime | None:
    if not raw:
        return None
    try:
        normalised = raw.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalised)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None

def seconds_until_expiry(expires_at_str: str | None) -> int | None:
    exp = parse_expires_at(expires_at_str)
    if exp is None:
        return None
    remaining = (exp - datetime.now(timezone.utc)).total_seconds()
    return max(0, int(remaining))

async def ensure_default_link():
    async with LINKS_LOCK:
        if not LINKS:
            LINKS["Default"] = {
                "label": "پیش‌فرض",
                "limit_bytes": 0,
                "used_bytes": 0,
                "max_connections": 0,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "active": True,
                "expires_at": None,
                "ports": [DEFAULT_PORT],
            }

def get_client_ip(websocket: WebSocket) -> str:
    forwarded = websocket.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    if websocket.client:
        return websocket.client.host
    return "unknown"

async def count_connections_for_link(uid: str) -> int:
    async with connections_lock:
        return sum(1 for info in connections.values() if info.get("uuid") == uid)

async def close_connections_for_link(uid: str):
    async with connections_lock:
        to_close = [cid for cid, info in connections.items() if info.get("uuid") == uid]
    for cid in to_close:
        ws = connection_sockets.get(cid)
        if ws:
            try:
                await ws.close(code=1000, reason="link deleted")
            except Exception:
                pass
        async with connections_lock:
            connections.pop(cid, None)
        connection_sockets.pop(cid, None)
    async with connections_lock:
        link_ip_map.pop(uid, None)

# ===== CLEAN IPS FUNCTIONS =====

def add_clean_ip(ip: str) -> bool:
    # This function is called from within async context, but we use it only inside async with lock
    # We'll handle locking outside
    if ip in CLEAN_IPS:
        return False
    CLEAN_IPS[ip] = {
        "active": True,
        "assigned_to": None,
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    return True

def remove_clean_ip(ip: str) -> bool:
    if ip not in CLEAN_IPS:
        return False
    assigned_to = CLEAN_IPS[ip].get("assigned_to")
    if assigned_to:
        for uid, data in LINKS.items():
            if data.get("label") == f"{assigned_to} - Clean" and data.get("clean_ip") == ip:
                LINKS[uid]["active"] = False
                break
    del CLEAN_IPS[ip]
    return True

def assign_clean_ip_to_node(ip: str, node_label: str) -> str | None:
    if ip not in CLEAN_IPS or not CLEAN_IPS[ip]["active"]:
        return None
    if CLEAN_IPS[ip]["assigned_to"] is not None:
        return None
    
    original_uid = None
    original_data = None
    for uid, data in LINKS.items():
        if data.get("label") == node_label:
            original_uid = uid
            original_data = data
            break
    
    if not original_uid or not original_data:
        return None
    
    new_uid = f"{original_uid}-clean"
    clean_link = {
        "label": f"{node_label} - Clean",
        "limit_bytes": original_data["limit_bytes"],
        "used_bytes": 0,
        "max_connections": original_data.get("max_connections", 0),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "active": True,
        "expires_at": original_data.get("expires_at"),
        "clean_ip": ip,
        "parent": original_uid,
    }
    
    LINKS[new_uid] = clean_link
    CLEAN_IPS[ip]["assigned_to"] = node_label
    return new_uid

# ===== TELEGRAM BOT ENGINE =====

def _is_admin_chat(chat_id, admin_id) -> bool:
    if str(chat_id) != str(admin_id):
        logger.warning(
            f"Telegram Bot: ignored message from chat_id={chat_id} "
            f"(configured admin_id={admin_id!r} does not match)"
        )
        return False
    return True

async def _stop_telegram_bot():
    global bot, bot_polling_task
    if bot is not None:
        try:
            bot.stop_polling()
        except Exception:
            pass
    if bot_polling_task is not None and not bot_polling_task.done():
        bot_polling_task.cancel()
        try:
            await bot_polling_task
        except (asyncio.CancelledError, Exception):
            pass
    bot = None
    bot_polling_task = None

async def restart_telegram_bot():
    global bot, bot_polling_task
    if not TELEBOT_AVAILABLE:
        logger.warning("Telegram Bot is disabled because pyTelegramBotAPI library is not installed.")
        return

    await _stop_telegram_bot()

    token = CONFIG.get("telegram_token")
    admin_id = CONFIG.get("telegram_admin_id")
    if not token or not admin_id:
        logger.info("Telegram Bot configuration is incomplete. Disabled.")
        return

    logger.info("Restarting Telegram Bot with official library...")
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            await client.get(f"https://api.telegram.org/bot{token}/deleteWebhook?drop_pending_updates=true")
            me_resp = await client.get(f"https://api.telegram.org/bot{token}/getMe")
            me_data = me_resp.json()
            if not me_data.get("ok"):
                logger.error(f"Telegram Bot: token rejected by Telegram ({me_data.get('description')}). Bot NOT started.")
                return
            logger.info(f"Telegram Bot: token verified, connected as @{me_data['result'].get('username')}")
    except Exception as e:
        logger.error(f"Telegram Bot: could not reach Telegram API, bot NOT started: {e}")
        return

    bot = AsyncTeleBot(token)

    @bot.message_handler(commands=['start'])
    async def cmd_start(message):
        if not _is_admin_chat(message.chat.id, admin_id):
            return
        await bot.send_message(
            message.chat.id,
            L("welcome"),
            parse_mode="HTML",
            reply_markup=build_main_keyboard()
        )

    @bot.message_handler(commands=['stats'])
    async def cmd_stats(message):
        if not _is_admin_chat(message.chat.id, admin_id):
            return
        s_data = await get_internal_stats()
        await bot.send_message(message.chat.id, make_stats_text(s_data), parse_mode="HTML")

    @bot.message_handler(commands=['users'])
    async def cmd_users(message):
        if not _is_admin_chat(message.chat.id, admin_id):
            return
        utext = await make_users_text()
        await bot.send_message(message.chat.id, utext, parse_mode="HTML")

    @bot.message_handler(commands=['top'])
    async def cmd_top(message):
        if not _is_admin_chat(message.chat.id, admin_id):
            return
        utext = await make_top_users_text()
        await bot.send_message(message.chat.id, utext, parse_mode="HTML")

    @bot.message_handler(commands=['create'])
    async def cmd_create(message):
        if not _is_admin_chat(message.chat.id, admin_id):
            return
        resp = await handle_create_command(message.text)
        await bot.send_message(message.chat.id, resp, parse_mode="HTML")

    @bot.message_handler(commands=['addaddr'])
    async def cmd_addaddr(message):
        if not _is_admin_chat(message.chat.id, admin_id):
            return
        resp = await handle_addaddr_command(message.text)
        await bot.send_message(message.chat.id, resp, parse_mode="HTML")

    @bot.message_handler(commands=['disable'])
    async def cmd_disable(message):
        if not _is_admin_chat(message.chat.id, admin_id):
            return
        resp = await handle_toggle_command(message.text, False)
        await bot.send_message(message.chat.id, resp, parse_mode="HTML")

    @bot.message_handler(commands=['enable'])
    async def cmd_enable(message):
        if not _is_admin_chat(message.chat.id, admin_id):
            return
        resp = await handle_toggle_command(message.text, True)
        await bot.send_message(message.chat.id, resp, parse_mode="HTML")

    @bot.message_handler(commands=['reset'])
    async def cmd_reset(message):
        if not _is_admin_chat(message.chat.id, admin_id):
            return
        resp = await handle_reset_command(message.text)
        await bot.send_message(message.chat.id, resp, parse_mode="HTML")

    @bot.callback_query_handler(func=lambda call: True)
    async def handle_callback(call):
        if not _is_admin_chat(call.message.chat.id, admin_id):
            return
        await bot.answer_callback_query(call.id)

        if call.data == "tg_stats":
            s_data = await get_internal_stats()
            await bot.send_message(call.message.chat.id, make_stats_text(s_data), parse_mode="HTML", reply_markup=build_main_keyboard())
        elif call.data == "tg_users":
            utext = await make_users_text()
            await bot.send_message(call.message.chat.id, utext, parse_mode="HTML", reply_markup=build_main_keyboard())
        elif call.data == "tg_top":
            utext = await make_top_users_text()
            await bot.send_message(call.message.chat.id, utext, parse_mode="HTML", reply_markup=build_main_keyboard())
        elif call.data == "tg_create_guide":
            await bot.send_message(call.message.chat.id, L("create_guide"), parse_mode="HTML", reply_markup=build_main_keyboard())
        elif call.data == "tg_add_ip_guide":
            await bot.send_message(call.message.chat.id, L("addip_guide"), parse_mode="HTML", reply_markup=build_main_keyboard())

    async def _run_polling(bot_instance):
        try:
            await bot_instance.infinity_polling()
        except Exception as e:
            logger.error(f"Telegram Bot: polling loop stopped unexpectedly: {e}")

    bot_polling_task = asyncio.create_task(_run_polling(bot))
    logger.info("Telegram Bot is now polling for updates.")

async def send_tg_message(text: str):
    global bot
    admin_id = CONFIG.get("telegram_admin_id")
    if bot and admin_id:
        try:
            await bot.send_message(admin_id, text, parse_mode="HTML")
        except Exception as e:
            logger.error(f"Error sending TG notification: {e}")

async def telegram_notifier_cron():
    while True:
        try:
            token = CONFIG.get("telegram_token")
            admin_id = CONFIG.get("telegram_admin_id")
            if not token or not admin_id:
                await asyncio.sleep(60)
                continue

            async with LINKS_LOCK:
                items = list(LINKS.items())
            
            for uid, data in items:
                if not data["active"]:
                    continue
                
                used = data["used_bytes"]
                limit = data["limit_bytes"]
                label = data["label"]
                
                if limit > 0 and used >= limit:
                    notif_key = f"quota_{uid}"
                    if notif_key not in notified_uids:
                        msg = L("quota_alert", label=label, used=_fmt_bytes(used), limit=_fmt_bytes(limit))
                        await send_tg_message(msg)
                        notified_uids.add(notif_key)
                
                expires_at_str = data.get("expires_at")
                if expires_at_str:
                    exp = parse_expires_at(expires_at_str)
                    if exp and exp < datetime.now(timezone.utc):
                        notif_key = f"expiry_{uid}"
                        if notif_key not in notified_uids:
                            msg = L("expiry_alert", label=label, exp=expires_at_str)
                            await send_tg_message(msg)
                            notified_uids.add(notif_key)
                            
        except Exception as e:
            logger.error(f"Error in notification cron: {e}")
            
        await asyncio.sleep(60)

# ===== API ROUTES =====

@app.get("/")
async def root():
    return Response(content="OK", media_type="text/plain")

@app.get("/health")
async def health():
    async with connections_lock:
        conn_count = len(connections)
    return {"status": "ok", "connections": conn_count, "uptime": uptime()}

@app.post("/api/login")
async def api_login(request: Request):
    body = await request.json()
    password = str(body.get("password") or "")
    if hash_password(password) != AUTH["password_hash"]:
        raise HTTPException(status_code=401, detail="Invalid password")
    token = await create_session()
    resp = JSONResponse({"ok": True, "token": token})
    resp.set_cookie(
        key=SESSION_COOKIE,
        value=token,
        max_age=SESSION_TTL,
        path="/",
        httponly=False,
        samesite="lax",
        secure=False
    )
    logger.info(f"Login successful, token={token[:8]}...")
    return resp

@app.get("/api/me")
async def api_me(request: Request):
    auth_header = request.headers.get("Authorization")
    token = None
    if auth_header and auth_header.startswith("Bearer "):
        token = auth_header[7:]
    if not token:
        token = request.cookies.get(SESSION_COOKIE)
    valid = await is_valid_session(token)
    logger.info(f"api_me: token={token[:8] if token else 'None'}, valid={valid}")
    return {"authenticated": valid}

@app.post("/api/logout")
async def api_logout(request: Request):
    token = request.cookies.get(SESSION_COOKIE)
    await destroy_session(token)
    resp = JSONResponse({"ok": True})
    resp.delete_cookie(SESSION_COOKIE, path="/")
    return resp

@app.post("/api/change-password")
async def api_change_password(request: Request, _=Depends(require_auth)):
    body = await request.json()
    current = str(body.get("current_password") or "")
    new = str(body.get("new_password") or "")
    if hash_password(current) != AUTH["password_hash"]:
        raise HTTPException(status_code=400, detail="Current password is incorrect")
    if len(new) < 4:
        raise HTTPException(status_code=400, detail="Password must be at least 4 characters")
    AUTH["password_hash"] = hash_password(new)
    save_db()
    current_token = request.cookies.get(SESSION_COOKIE)
    async with SESSIONS_LOCK:
        SESSIONS.clear()
        if current_token:
            SESSIONS[current_token] = time.time() + SESSION_TTL
    return {"ok": True}

@app.get("/api/settings")
async def get_settings(_=Depends(require_auth)):
    return {
        "telegram_token": CONFIG["telegram_token"],
        "telegram_admin_id": CONFIG["telegram_admin_id"],
        "status_display_mode": CONFIG["status_display_mode"],
        "custom_status_text": CONFIG["custom_status_text"],
    }

@app.post("/api/settings")
async def update_settings(request: Request, _=Depends(require_auth)):
    body = await request.json()
    CONFIG["telegram_token"] = body.get("telegram_token", "").strip()
    CONFIG["telegram_admin_id"] = body.get("telegram_admin_id", "").strip()
    if "status_display_mode" in body:
        mode = body["status_display_mode"]
        if mode in ["separate", "combined", "custom", "separate+custom", "combined+custom"]:
            CONFIG["status_display_mode"] = mode
    if "custom_status_text" in body:
        CONFIG["custom_status_text"] = body["custom_status_text"].strip()[:200]
    save_db()
    await restart_telegram_bot()
    return {"ok": True}

@app.get("/stats")
async def get_stats(_=Depends(require_auth)):
    async with connections_lock:
        conn_count = len(connections)
    return {
        "active_connections": conn_count,
        "total_traffic_mb": round(stats["total_bytes"] / (1024 * 1024), 2),
        "total_requests": stats["total_requests"],
        "total_errors": stats["total_errors"],
        "uptime": uptime(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "recent_errors": list(error_logs)[-10:],
        "links_count": len(LINKS),
        "domain": get_domain(),
        "cpu_percent": psutil.cpu_percent(interval=0.1),
        "memory_percent": psutil.virtual_memory().percent,
        "hourly_traffic": dict(hourly_traffic),
    }

# ===== LINKS API =====

@app.post("/api/links")
async def create_link(request: Request, _=Depends(require_auth)):
    body = await request.json()
    label = (body.get("label") or "New Link").strip()[:60]
    if not re.match(r'^[a-zA-Z0-9\-_. ]+$', label):
        raise HTTPException(status_code=400, detail="Inbound name must contain only English letters, numbers, and characters: - _ . space")
    if not label:
        raise HTTPException(status_code=400, detail="Inbound name is required")
    async with LINKS_LOCK:
        if label in LINKS:
            raise HTTPException(status_code=400, detail="An inbound with this name already exists")
    limit_value = float(body.get("limit_value") or 0)
    limit_unit = body.get("limit_unit") or "GB"
    limit_bytes = 0 if limit_value <= 0 else parse_size_to_bytes(limit_value, limit_unit)
    max_conn = int(body.get("max_connections") or 0)
    if max_conn < 0:
        max_conn = 0
    days_valid = body.get("days_valid")
    expires_at: str | None = None
    if days_valid is not None:
        try:
            days_valid = int(days_valid)
            if days_valid > 0:
                expires_at = (datetime.now(timezone.utc) + timedelta(days=days_valid)).isoformat()
        except (ValueError, TypeError):
            pass
    uid = label
    async with LINKS_LOCK:
        LINKS[uid] = {
            "label": label,
            "limit_bytes": limit_bytes,
            "used_bytes": 0,
            "max_connections": max_conn,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "active": True,
            "expires_at": expires_at,
        }
    save_db()
    return {
        "uuid": uid, "label": label, "limit_bytes": limit_bytes, "used_bytes": 0,
        "max_connections": max_conn, "active": True, "created_at": LINKS[uid]["created_at"],
        "expires_at": expires_at,
        "vless_link": generate_vless_link(uid, remark=label, port=DEFAULT_PORT),
    }

@app.get("/api/links")
async def list_links(_=Depends(require_auth)):
    result = []
    async with LINKS_LOCK:
        items = list(LINKS.items())
    for uid, data in items:
        result.append({
            "uuid": uid,
            "label": data["label"],
            "limit_bytes": data["limit_bytes"],
            "used_bytes": data["used_bytes"],
            "max_connections": data.get("max_connections", 0),
            "active": data["active"],
            "created_at": data["created_at"],
            "expires_at": data.get("expires_at"),
            "current_connections": await count_connections_for_link(uid),
            "vless_link": generate_vless_link(uid, remark=data["label"], port=DEFAULT_PORT),
            "clean_ip": data.get("clean_ip"),
            "parent": data.get("parent"),
        })
    result.sort(key=lambda x: x["created_at"], reverse=True)
    return {"links": result}

@app.patch("/api/links/{uid}")
async def toggle_link(uid: str, request: Request, _=Depends(require_auth)):
    body = await request.json()
    async with LINKS_LOCK:
        if uid not in LINKS:
            raise HTTPException(status_code=404, detail="link not found")
        if "active" in body:
            LINKS[uid]["active"] = bool(body["active"])
        if "limit_value" in body:
            limit_value = float(body.get("limit_value") or 0)
            limit_unit = body.get("limit_unit") or "GB"
            LINKS[uid]["limit_bytes"] = 0 if limit_value <= 0 else parse_size_to_bytes(limit_value, limit_unit)
        if "reset_usage" in body and body["reset_usage"]:
            LINKS[uid]["used_bytes"] = 0
        if "label" in body:
            LINKS[uid]["label"] = str(body["label"])[:60]
        if "max_connections" in body:
            mc = int(body["max_connections"] or 0)
            LINKS[uid]["max_connections"] = mc if mc >= 0 else 0
        if "days_valid" in body:
            try:
                dv = int(body["days_valid"])
                if dv > 0:
                    LINKS[uid]["expires_at"] = (datetime.now(timezone.utc) + timedelta(days=dv)).isoformat()
                else:
                    LINKS[uid]["expires_at"] = None
            except (ValueError, TypeError):
                pass
    save_db()
    return {"ok": True}

@app.delete("/api/links/{uid}")
async def delete_link(uid: str, _=Depends(require_auth)):
    async with LINKS_LOCK:
        LINKS.pop(uid, None)
    save_db()
    await close_connections_for_link(uid)
    return {"ok": True}

# ===== CLEAN IPS API (با async with اصلاح شده) =====

@app.get("/api/clean-ips")
async def list_clean_ips(_=Depends(require_auth)):
    async with CLEAN_IPS_LOCK:
        return {"clean_ips": CLEAN_IPS}

@app.post("/api/clean-ips")
async def add_clean_ip_api(request: Request, _=Depends(require_auth)):
    body = await request.json()
    ip = body.get("ip", "").strip()
    if not ip:
        raise HTTPException(status_code=400, detail="آی‌پی الزامی است")
    if not re.match(r'^[0-9.]+$', ip):
        raise HTTPException(status_code=400, detail="فرمت آی‌پی نامعتبر است")
    
    async with CLEAN_IPS_LOCK:
        if ip in CLEAN_IPS:
            logger.warning(f"Clean IP already exists: {ip}")
            raise HTTPException(status_code=400, detail="آی‌پی قبلاً وجود دارد")
        CLEAN_IPS[ip] = {
            "active": True,
            "assigned_to": None,
            "created_at": datetime.now(timezone.utc).isoformat()
        }
    save_db()
    logger.info(f"Clean IP added successfully: {ip}")
    return {"success": True, "message": "آی‌پی تمیز اضافه شد"}

@app.delete("/api/clean-ips/{ip}")
async def delete_clean_ip(ip: str, _=Depends(require_auth)):
    async with CLEAN_IPS_LOCK:
        if ip not in CLEAN_IPS:
            raise HTTPException(status_code=404, detail="آی‌پی یافت نشد")
        assigned_to = CLEAN_IPS[ip].get("assigned_to")
        if assigned_to:
            for uid, data in LINKS.items():
                if data.get("label") == f"{assigned_to} - Clean" and data.get("clean_ip") == ip:
                    LINKS[uid]["active"] = False
                    break
        del CLEAN_IPS[ip]
    save_db()
    return {"success": True}

@app.patch("/api/clean-ips/{ip}/toggle")
async def toggle_clean_ip(ip: str, _=Depends(require_auth)):
    async with CLEAN_IPS_LOCK:
        if ip not in CLEAN_IPS:
            raise HTTPException(status_code=404, detail="آی‌پی یافت نشد")
        CLEAN_IPS[ip]["active"] = not CLEAN_IPS[ip]["active"]
        if not CLEAN_IPS[ip]["active"]:
            assigned_to = CLEAN_IPS[ip].get("assigned_to")
            if assigned_to:
                for uid, data in LINKS.items():
                    if data.get("label") == f"{assigned_to} - Clean" and data.get("clean_ip") == ip:
                        LINKS[uid]["active"] = False
                        break
    save_db()
    return {"success": True, "active": CLEAN_IPS[ip]["active"]}

@app.post("/api/clean-ips/assign")
async def assign_clean_ip(request: Request, _=Depends(require_auth)):
    body = await request.json()
    ip = body.get("ip")
    node_label = body.get("node_label")
    if not ip or not node_label:
        raise HTTPException(status_code=400, detail="آی‌پی و نام نود الزامی است")
    
    async with CLEAN_IPS_LOCK:
        if ip not in CLEAN_IPS or not CLEAN_IPS[ip]["active"]:
            raise HTTPException(status_code=400, detail="آی‌پی نامعتبر است")
        if CLEAN_IPS[ip]["assigned_to"] is not None:
            raise HTTPException(status_code=400, detail="آی‌پی قبلاً به نود دیگری اختصاص داده شده است")
        
        original_uid = None
        original_data = None
        for uid, data in LINKS.items():
            if data.get("label") == node_label:
                original_uid = uid
                original_data = data
                break
        
        if not original_uid or not original_data:
            raise HTTPException(status_code=400, detail="نود مورد نظر یافت نشد")
        
        new_uid = f"{original_uid}-clean"
        clean_link = {
            "label": f"{node_label} - Clean",
            "limit_bytes": original_data["limit_bytes"],
            "used_bytes": 0,
            "max_connections": original_data.get("max_connections", 0),
            "created_at": datetime.now(timezone.utc).isoformat(),
            "active": True,
            "expires_at": original_data.get("expires_at"),
            "clean_ip": ip,
            "parent": original_uid,
        }
        LINKS[new_uid] = clean_link
        CLEAN_IPS[ip]["assigned_to"] = node_label
    
    save_db()
    return {"success": True, "new_node_uid": new_uid}

# ===== SUBSCRIPTION GROUPS API =====

@app.get("/api/subscription-groups")
async def list_groups(_=Depends(require_auth)):
    async with GROUPS_LOCK:
        groups = list(SUBSCRIPTION_GROUPS.values())
    for g in groups:
        g["inbound_count"] = len(g.get("inbounds", []))
        total_used = 0
        async with LINKS_LOCK:
            for uid in g.get("inbounds", []):
                if uid in LINKS:
                    total_used += LINKS[uid].get("used_bytes", 0)
        g["total_used_bytes"] = total_used
    return {"groups": groups}

@app.post("/api/subscription-groups")
async def create_group(request: Request, _=Depends(require_auth)):
    body = await request.json()
    name = body.get("name", "").strip()
    if not name or not re.match(r'^[a-zA-Z0-9\-_. ]+$', name):
        raise HTTPException(status_code=400, detail="Invalid group name")
    inbounds = body.get("inbounds", [])
    if not isinstance(inbounds, list) or not inbounds:
        raise HTTPException(status_code=400, detail="At least one inbound must be selected")
    async with LINKS_LOCK:
        for uid in inbounds:
            if uid not in LINKS:
                raise HTTPException(status_code=400, detail=f"Inbound '{uid}' not found")
    limit_gb = float(body.get("limit_gb", 0))
    if limit_gb < 0:
        raise HTTPException(status_code=400, detail="limit_gb must be >= 0")
    days = int(body.get("days", 0))
    if days < 0:
        raise HTTPException(status_code=400, detail="days must be >= 0")
    
    group_id = secrets.token_urlsafe(8)
    expires_at = None
    if days > 0:
        expires_at = (datetime.now(timezone.utc) + timedelta(days=days)).isoformat()
    
    async with GROUPS_LOCK:
        SUBSCRIPTION_GROUPS[group_id] = {
            "id": group_id,
            "name": name,
            "inbounds": inbounds,
            "limit_bytes": 0 if limit_gb <= 0 else parse_size_to_bytes(limit_gb, "GB"),
            "expires_at": expires_at,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
    save_db()
    return {"ok": True, "group_id": group_id}

@app.patch("/api/subscription-groups/{group_id}")
async def update_group(group_id: str, request: Request, _=Depends(require_auth)):
    async with GROUPS_LOCK:
        if group_id not in SUBSCRIPTION_GROUPS:
            raise HTTPException(status_code=404, detail="Group not found")
        body = await request.json()
        group = SUBSCRIPTION_GROUPS[group_id]
        if "name" in body:
            name = body["name"].strip()
            if name and re.match(r'^[a-zA-Z0-9\-_. ]+$', name):
                group["name"] = name
        if "inbounds" in body:
            inbounds = body["inbounds"]
            if isinstance(inbounds, list) and inbounds:
                async with LINKS_LOCK:
                    for uid in inbounds:
                        if uid not in LINKS:
                            raise HTTPException(status_code=400, detail=f"Inbound '{uid}' not found")
                group["inbounds"] = inbounds
        if "limit_gb" in body:
            lg = float(body["limit_gb"])
            if lg >= 0:
                group["limit_bytes"] = 0 if lg <= 0 else parse_size_to_bytes(lg, "GB")
        if "days" in body:
            d = int(body["days"])
            if d >= 0:
                group["expires_at"] = (datetime.now(timezone.utc) + timedelta(days=d)).isoformat() if d > 0 else None
    save_db()
    return {"ok": True}

@app.delete("/api/subscription-groups/{group_id}")
async def delete_group(group_id: str, _=Depends(require_auth)):
    async with GROUPS_LOCK:
        if group_id not in SUBSCRIPTION_GROUPS:
            raise HTTPException(status_code=404, detail="Group not found")
        del SUBSCRIPTION_GROUPS[group_id]
    save_db()
    return {"ok": True}

# ===== SUBSCRIPTION ENDPOINTS =====

def _fmt_bytes(b: int) -> str:
    if b >= 1_073_741_824: return f"{b / 1_073_741_824:.1f}GB"
    if b >= 1_048_576: return f"{b / 1_048_576:.1f}MB"
    return f"{b / 1024:.1f}KB"

def generate_status_lines(link: dict, group_name: str = None) -> list:
    mode = CONFIG.get("status_display_mode", "separate")
    custom_text = CONFIG.get("custom_status_text", "🌟 اشتراک شما فعال است")
    
    used = link["used_bytes"]
    limit = link["limit_bytes"]
    expires_at_str = link.get("expires_at")
    
    usage_str = f"{_fmt_bytes(used)} / ∞" if limit == 0 else f"{_fmt_bytes(used)} / {_fmt_bytes(limit)}"
    secs_left = seconds_until_expiry(expires_at_str)
    if secs_left is None:
        expiry_str = "∞"
    elif secs_left == 0:
        expiry_str = "Expired"
    else:
        expiry_str = f"{secs_left // 86400} Days Left"
    
    label = group_name if group_name else link.get("label", "User")
    lines = []
    
    if mode == "separate+custom":
        quota_line = generate_vless_link("status", remark=f"📊 {label} | {usage_str}", address="0.0.0.0", port=DEFAULT_PORT)
        expiry_line = generate_vless_link("status", remark=f"⏳ {label} | {expiry_str}", address="0.0.0.0", port=DEFAULT_PORT)
        custom_line = generate_vless_link("status", remark=custom_text, address="0.0.0.0", port=DEFAULT_PORT)
        lines.extend([quota_line, expiry_line, custom_line])
    
    elif mode == "combined+custom":
        combined = generate_vless_link("status", remark=f"📊 {label} | {usage_str} | ⏳ {expiry_str}", address="0.0.0.0", port=DEFAULT_PORT)
        custom_line = generate_vless_link("status", remark=custom_text, address="0.0.0.0", port=DEFAULT_PORT)
        lines.extend([combined, custom_line])
    
    elif mode == "custom":
        lines.append(generate_vless_link("status", remark=custom_text, address="0.0.0.0", port=DEFAULT_PORT))
    
    elif mode == "separate":
        quota_line = generate_vless_link("status", remark=f"📊 {label} | {usage_str}", address="0.0.0.0", port=DEFAULT_PORT)
        expiry_line = generate_vless_link("status", remark=f"⏳ {label} | {expiry_str}", address="0.0.0.0", port=DEFAULT_PORT)
        lines.extend([quota_line, expiry_line])
    
    else:  # combined (پیش‌فرض)
        combined = generate_vless_link("status", remark=f"📊 {label} | {usage_str} | ⏳ {expiry_str}", address="0.0.0.0", port=DEFAULT_PORT)
        lines.append(combined)
    
    return lines

def generate_landing_page(link: dict, uid: str) -> str:
    used = link["used_bytes"]
    limit = link["limit_bytes"]
    expires_at_str = link.get("expires_at")
    
    usage_str = f"{_fmt_bytes(used)} / Unlimited" if limit == 0 else f"{_fmt_bytes(used)} / {_fmt_bytes(limit)}"
    pct = round((used / limit) * 100, 1) if limit > 0 else 0
    rem = limit - used if limit > 0 else -1
    rem_str = _fmt_bytes(rem) if rem >= 0 else "Unlimited"

    secs_left = seconds_until_expiry(expires_at_str)
    if secs_left is None:
        expiry_str = "Unlimited"
    elif secs_left == 0:
        expiry_str = "Expired"
    else:
        days = secs_left // 86400
        hours = (secs_left % 86400) // 3600
        expiry_str = f"{days} Days, {hours} Hours Left"

    configs = []
    status_lines = generate_status_lines(link)
    for line in status_lines:
        if "vless://status@" in line:
            line = line.replace("vless://status@", f"vless://{uid}@")
        configs.append(line)
    configs.append(generate_vless_link(uid, remark=link['label'], port=DEFAULT_PORT))

    configs_json = json.dumps(configs)
    
    html = f"""<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Wind - وضعیت اتصال</title>
    <link href="https://fonts.googleapis.com/css2?family=Vazirmatn:wght@400;600;700;800&display=swap" rel="stylesheet">
    <style>
        * {{ margin:0; padding:0; box-sizing:border-box; }}
        body {{
            background: #F5F7FA;
            font-family: 'Vazirmatn', sans-serif;
            color: #202124;
            padding: 20px;
            display: flex;
            align-items: center;
            justify-content: center;
            min-height: 100vh;
        }}
        .container {{
            width: 100%;
            max-width: 500px;
            background: #FFFFFF;
            border: 1px solid #E8EAF0;
            border-radius: 16px;
            padding: 24px;
            box-shadow: 0 2px 8px rgba(30,35,50,0.03);
        }}
        .brand {{
            text-align: center;
            margin-bottom: 20px;
        }}
        .brand h1 {{
            font-size: 20px;
            font-weight: 800;
            color: #6465F1;
            letter-spacing: 0.5px;
        }}
        .card {{
            background: #FAFBFD;
            border: 1px solid #E8EAF0;
            border-radius: 14px;
            padding: 16px;
            margin-bottom: 14px;
        }}
        .user-header {{
            display: flex;
            align-items: center;
            justify-content: space-between;
            margin-bottom: 12px;
        }}
        .username {{
            font-size: 16px;
            font-weight: 700;
            color: #202124;
        }}
        .status-badge {{
            padding: 3px 10px;
            border-radius: 6px;
            font-size: 11px;
            font-weight: 700;
            background: #E8F5E9;
            color: #2E7D32;
        }}
        .status-badge.inactive {{
            background: #FFEBEE;
            color: #C62828;
        }}
        .label {{
            font-size: 11px;
            color: #7A7F8C;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        .val {{
            font-size: 16px;
            font-weight: 700;
            color: #202124;
            margin-top: 2px;
        }}
        .progress-bar {{
            height: 6px;
            background: #E8EAF0;
            border-radius: 3px;
            overflow: hidden;
            margin: 12px 0;
        }}
        .progress-fill {{
            height: 100%;
            background: #6465F1;
            transition: width 0.5s;
        }}
        .config-list {{
            margin-top: 15px;
        }}
        .config-item {{
            display: flex;
            align-items: center;
            justify-content: space-between;
            background: #FFFFFF;
            border: 1px solid #E8EAF0;
            padding: 10px 12px;
            border-radius: 8px;
            margin-bottom: 8px;
            font-size: 13px;
        }}
        .config-name {{
            font-weight: 600;
            color: #202124;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
            max-width: 55%;
        }}
        .btn {{
            font-family: inherit;
            font-size: 12px;
            font-weight: 700;
            border-radius: 8px;
            padding: 5px 12px;
            cursor: pointer;
            border: none;
            transition: all 0.15s;
        }}
        .btn-primary {{
            background: #6465F1;
            color: #fff;
        }}
        .btn-primary:hover {{
            background: #5354E0;
        }}
        .btn-ghost {{
            background: #F4F5F8;
            color: #555A68;
        }}
        .btn-ghost:hover {{
            background: #E8EAF0;
        }}
        .mo {{
            position: fixed; inset: 0; background: rgba(0,0,0,0.3); z-index: 200; display: none; align-items: center; justify-content: center; backdrop-filter: blur(2px);
        }}
        .mo.show {{ display: flex; }}
        .mo-box {{
            background: #FFFFFF;
            border: 1px solid #E8EAF0;
            border-radius: 16px;
            padding: 24px;
            width: 90%;
            max-width: 320px;
            text-align: center;
            position: relative;
            box-shadow: 0 4px 12px rgba(0,0,0,0.06);
        }}
        .mo-box img {{ max-width: 100%; border-radius: 8px; margin-top: 15px; }}
        .mo-close {{
            position: absolute; top: 12px; left: 12px;
            font-size: 18px; cursor: pointer; color: #7A7F8C; background: none; border: none;
        }}
        .toast {{
            position: fixed; bottom: 20px; left: 50%; transform: translateX(-50%) translateY(16px);
            background: #202124; color: #fff; border-radius: 10px; padding: 10px 20px;
            font-size: 14px; font-weight: 600; opacity: 0; transition: all 0.3s; z-index: 999;
        }}
        .toast.show {{ opacity: 1; transform: translateX(-50%) translateY(0); }}
    </style>
</head>
<body>
    <div class="toast" id="toast">کپی شد!</div>
    <div class="container">
        <div class="brand">
            <h1>Wind</h1>
        </div>
        <div class="card">
            <div class="user-header">
                <span class="username">{link['label']}</span>
                <span class="status-badge {'status-badge' if link['active'] else 'status-badge inactive'}">
                    {'فعال' if link['active'] else 'غیرفعال'}
                </span>
            </div>
            <div class="progress-bar">
                <div class="progress-fill" style="width: {pct}%"></div>
            </div>
            <div style="display:flex; justify-content: space-between; margin-top: 10px;">
                <div>
                    <div class="label">مصرف</div>
                    <div class="val">{usage_str}</div>
                </div>
                <div>
                    <div class="label">باقیمانده</div>
                    <div class="val">{rem_str}</div>
                </div>
            </div>
        </div>
        <div class="card">
            <div class="label">اعتبار / انقضا</div>
            <div class="val" style="color: #6465F1; font-size: 18px; font-weight: 700; margin-top: 4px;">{expiry_str}</div>
        </div>
        <h3 style="margin: 20px 0 10px 5px; font-size: 14px; color: #7A7F8C;">نودهای موجود</h3>
        <div class="config-list" id="config-list"></div>
    </div>

    <div class="mo" id="qr-modal" onclick="if(event.target===this)this.classList.remove('show')">
        <div class="mo-box">
            <button class="mo-close" onclick="document.getElementById('qr-modal').classList.remove('show')">✕</button>
            <h3 style="color:#6465F1; font-size:16px;">کد QR</h3>
            <img id="qr-img" src="" alt="QR">
        </div>
    </div>

    <script>
        const configs = {configs_json};
        const listEl = document.getElementById('config-list');
        
        function showToast(txt) {{
            const t = document.getElementById('toast');
            t.textContent = txt;
            t.className = 'toast show';
            clearTimeout(t.timer);
            t.timer = setTimeout(() => t.className = 'toast', 2500);
        }}

        function copyTxt(text) {{
            navigator.clipboard.writeText(text)
                .then(() => showToast('کپی شد!'))
                .catch(() => showToast('خطا در کپی'));
        }}

        function showQR(text) {{
            document.getElementById('qr-img').src = 'https://api.qrserver.com/v1/create-qr-code/?size=250x250&data=' + encodeURIComponent(text);
            document.getElementById('qr-modal').classList.add('show');
        }}

        listEl.innerHTML = configs.map((cfg, i) => {{
            const parts = cfg.split('#');
            const remark = parts[1] ? decodeURIComponent(parts[1]) : 'نود ' + (i+1);
            return `
                <div class="config-item">
                    <span class="config-name">${{remark}}</span>
                    <div style="display:flex; gap: 5px;">
                        <button class="btn btn-ghost" onclick="copyTxt('${{cfg}}')">کپی</button>
                        <button class="btn btn-primary" onclick="showQR('${{cfg}}')">QR</button>
                    </div>
                </div>
            `;
        }}).join('');
    </script>
</body>
</html>"""
    return html

def generate_subscription_content(link: dict, uid: str) -> str:
    lines = []
    status_lines = generate_status_lines(link)
    for line in status_lines:
        if "vless://status@" in line:
            line = line.replace("vless://status@", f"vless://{uid}@")
        lines.append(line)
    lines.append(generate_vless_link(uid, remark=link['label'], port=DEFAULT_PORT))
    return "\n".join(lines)

@app.get("/sub/{uid}")
async def subscription_endpoint(uid: str, request: Request):
    async with LINKS_LOCK:
        link = LINKS.get(uid)
        if link is None:
            raise HTTPException(status_code=404, detail="link not found")
        link = dict(link)
        
    if not link["active"]:
        raise HTTPException(status_code=403, detail="link disabled")
        
    expires_at = parse_expires_at(link.get("expires_at"))
    if expires_at is not None and expires_at < datetime.now(timezone.utc):
        raise HTTPException(status_code=403, detail="link expired")

    ua = request.headers.get("user-agent", "").lower()
    accept = request.headers.get("accept", "").lower()
    is_browser = any(x in ua for x in ["mozilla", "chrome", "safari", "opera", "edge"]) and "text/html" in accept

    if is_browser:
        return HTMLResponse(content=generate_landing_page(link, uid))

    sub_content = generate_subscription_content(link, uid)
    encoded = base64.b64encode(sub_content.encode()).decode()
    total_bytes = link["limit_bytes"] if link["limit_bytes"] > 0 else UNLIMITED_QUOTA_BYTES
    expire_ts = 0
    if expires_at is not None:
        expire_ts = int(expires_at.timestamp())
        
    headers = {
        "Content-Type": "text/plain; charset=utf-8",
        "Content-Disposition": 'attachment; filename="sub.txt"',
        "profile-update-interval": "6",
        "subscription-userinfo": f"upload={link['used_bytes']}; download=0; total={total_bytes}; expire={expire_ts}",
    }
    return Response(content=encoded, headers=headers)

@app.get("/sub/group/{group_id}")
async def group_subscription_endpoint(group_id: str, request: Request):
    async with GROUPS_LOCK:
        group = SUBSCRIPTION_GROUPS.get(group_id)
        if not group:
            raise HTTPException(status_code=404, detail="Group not found")
        group = dict(group)
    
    group_expiry = parse_expires_at(group.get("expires_at"))
    if group_expiry is not None and group_expiry < datetime.now(timezone.utc):
        raise HTTPException(status_code=403, detail="Group subscription expired")
    
    inbound_uids = group.get("inbounds", [])
    if not inbound_uids:
        raise HTTPException(status_code=400, detail="Group has no inbounds")
    
    async with LINKS_LOCK:
        links_data = []
        total_used = 0
        total_limit = group.get("limit_bytes", 0)
        for uid in inbound_uids:
            if uid in LINKS:
                link = LINKS[uid]
                if link.get("active", False):
                    exp = parse_expires_at(link.get("expires_at"))
                    if exp is not None and exp < datetime.now(timezone.utc):
                        continue
                    links_data.append((uid, dict(link)))
                    total_used += link.get("used_bytes", 0)
    
    if not links_data:
        raise HTTPException(status_code=403, detail="No active inbounds in this group")
    
    group_name = group.get("name", "Group")
    first_link = links_data[0][1] if links_data else {}
    lines = []
    
    if first_link:
        status_lines = generate_status_lines(first_link, group_name)
        for line in status_lines:
            if "vless://status@" in line:
                line = line.replace("vless://status@", f"vless://{links_data[0][0]}@")
            lines.append(line)
    
    for uid, link in links_data:
        lines.append(generate_vless_link(uid, remark=f"{group_name} - {link['label']}", port=DEFAULT_PORT))
    
    sub_content = "\n".join(lines)
    encoded = base64.b64encode(sub_content.encode()).decode()
    
    total_bytes = total_limit if total_limit > 0 else UNLIMITED_QUOTA_BYTES
    expire_ts = 0
    if group.get("expires_at"):
        exp = parse_expires_at(group["expires_at"])
        if exp:
            expire_ts = int(exp.timestamp())
    
    headers = {
        "Content-Type": "text/plain; charset=utf-8",
        "Content-Disposition": 'attachment; filename="group_sub.txt"',
        "profile-update-interval": "6",
        "subscription-userinfo": f"upload={total_used}; download=0; total={total_bytes}; expire={expire_ts}",
    }
    return Response(content=encoded, headers=headers)

# ===== Live Logs WebSocket =====
@app.websocket("/ws/live-logs")
async def ws_live_logs(websocket: WebSocket, token: str | None = None):
    await websocket.accept()
    if not token or not await is_valid_session(token):
        await websocket.close(code=1008, reason="Unauthorized")
        return
    for item in list(log_queue):
        await websocket.send_text(item)
    last_idx = len(log_queue)
    try:
        while True:
            await asyncio.sleep(0.5)
            curr = list(log_queue)
            if len(curr) > last_idx:
                for idx in range(last_idx, len(curr)):
                    await websocket.send_text(curr[idx])
                last_idx = len(curr)
            elif len(curr) < last_idx:
                last_idx = len(curr)
    except WebSocketDisconnect:
        pass
    except Exception:
        pass

# ===== WebSocket tunnel =====
RELAY_BUF = 64 * 1024

async def parse_vless_header(first_chunk: bytes):
    if len(first_chunk) < 24:
        raise ValueError("chunk too small")
    pos = 1 + 16
    addon_len = first_chunk[pos]
    pos += 1 + addon_len
    command = first_chunk[pos]
    pos += 1
    port = int.from_bytes(first_chunk[pos:pos + 2], "big")
    pos += 2
    addr_type = first_chunk[pos]
    pos += 1
    if addr_type == 1:
        addr_bytes = first_chunk[pos:pos + 4]
        pos += 4
        address = ".".join(str(b) for b in addr_bytes)
    elif addr_type == 2:
        domain_len = first_chunk[pos]
        pos += 1
        address = first_chunk[pos:pos + domain_len].decode("utf-8", errors="ignore")
        pos += domain_len
    elif addr_type == 3:
        addr_bytes = first_chunk[pos:pos + 16]
        pos += 16
        address = ":".join(f"{addr_bytes[i]:02x}{addr_bytes[i+1]:02x}" for i in range(0, 16, 2))
    else:
        raise ValueError(f"unknown address type: {addr_type}")
    return command, address, port, first_chunk[pos:]

async def check_quota(uid: str, extra_bytes: int) -> bool:
    async with LINKS_LOCK:
        link = LINKS.get(uid)
        if link is None or not link["active"]:
            return False
        expires_at = parse_expires_at(link.get("expires_at"))
        if expires_at is not None and expires_at < datetime.now(timezone.utc):
            return False
        if link["limit_bytes"] == 0:
            return True
        return (link["used_bytes"] + extra_bytes) <= link["limit_bytes"]

async def add_usage(uid: str, n: int):
    async with LINKS_LOCK:
        if uid in LINKS:
            LINKS[uid]["used_bytes"] += n

async def ws_to_tcp(websocket, writer, conn_id, link_uid):
    try:
        while True:
            msg = await websocket.receive()
            if msg["type"] == "websocket.disconnect":
                break
            data = msg.get("bytes") or (msg.get("text") or "").encode()
            if not data:
                continue
            size = len(data)
            if not await check_quota(link_uid, size):
                await websocket.close(code=1008, reason="quota exceeded")
                break
            stats["total_bytes"] += size
            stats["total_requests"] += 1
            async with connections_lock:
                if conn_id in connections:
                    connections[conn_id]["bytes"] += size
            hourly_traffic[datetime.now(timezone.utc).strftime("%H:00")] += size
            daily_traffic[datetime.now(timezone.utc).strftime("%Y-%m-%d")] += size
            await add_usage(link_uid, size)
            try:
                writer.write(data)
                await writer.drain()
            except Exception:
                break
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        try:
            if not writer.is_closing():
                writer.write_eof()
        except Exception:
            pass

async def tcp_to_ws(websocket, reader, conn_id, link_uid):
    first = True
    try:
        while True:
            data = await reader.read(RELAY_BUF)
            if not data:
                break
            size = len(data)
            if not await check_quota(link_uid, size):
                await websocket.close(code=1008, reason="quota exceeded")
                break
            stats["total_bytes"] += size
            async with connections_lock:
                if conn_id in connections:
                    connections[conn_id]["bytes"] += size
            hourly_traffic[datetime.now(timezone.utc).strftime("%H:00")] += size
            daily_traffic[datetime.now(timezone.utc).strftime("%Y-%m-%d")] += size
            await add_usage(link_uid, size)
            try:
                await websocket.send_bytes((b"\x00\x00" + data) if first else data)
                first = False
            except Exception:
                break
    except Exception:
        pass

@app.websocket("/ws/{uuid}")
async def websocket_tunnel(websocket: WebSocket, uuid: str):
    await ensure_default_link()

    async with LINKS_LOCK:
        link_data = LINKS.get(uuid)
        if link_data is None or not link_data["active"]:
            await websocket.close(code=1008)
            return
        max_conn = link_data.get("max_connections", 0)
        link_data_copy = dict(link_data)

    expires_at = parse_expires_at(link_data_copy.get("expires_at"))
    if expires_at is not None and expires_at < datetime.now(timezone.utc):
        await websocket.close(code=1008)
        return

    if max_conn > 0:
        current_conns = await count_connections_for_link(uuid)
        if current_conns >= max_conn:
            await websocket.close(code=1008)
            return

    await websocket.accept()
    writer = None
    conn_id = None
    client_ip = get_client_ip(websocket)
    try:
        first_msg = await asyncio.wait_for(websocket.receive(), timeout=15.0)
        if first_msg["type"] == "websocket.disconnect":
            return
        first_chunk = first_msg.get("bytes") or (first_msg.get("text") or "").encode()
        if not first_chunk:
            return

        try:
            command, address, port, initial_payload = await parse_vless_header(first_chunk)
        except ValueError as e:
            logger.warning(f"Invalid VLESS header: {e}")
            await websocket.close(code=1008, reason="invalid header")
            return

        conn_id = secrets.token_urlsafe(8)
        async with connections_lock:
            connections[conn_id] = {
                "uuid": uuid, "ip": client_ip,
                "connected_at": datetime.now(timezone.utc).isoformat(),
                "bytes": 0,
            }
            connection_sockets[conn_id] = websocket
            link_ip_map[uuid].add(client_ip)

        size = len(first_chunk)
        stats["total_bytes"] += size
        stats["total_requests"] += 1
        async with connections_lock:
            if conn_id in connections:
                connections[conn_id]["bytes"] += size
        hourly_traffic[datetime.now(timezone.utc).strftime("%H:00")] += size
        daily_traffic[datetime.now(timezone.utc).strftime("%Y-%m-%d")] += size
        await add_usage(uuid, size)

        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(address, port), timeout=10.0
        )

        if initial_payload:
            p_size = len(initial_payload)
            stats["total_bytes"] += p_size
            async with connections_lock:
                if conn_id in connections:
                    connections[conn_id]["bytes"] += p_size
            hourly_traffic[datetime.now(timezone.utc).strftime("%H:00")] += p_size
            daily_traffic[datetime.now(timezone.utc).strftime("%Y-%m-%d")] += p_size
            await add_usage(uuid, p_size)
            try:
                writer.write(initial_payload)
                await writer.drain()
            except Exception:
                pass

        task_up = asyncio.create_task(ws_to_tcp(websocket, writer, conn_id, uuid))
        task_down = asyncio.create_task(tcp_to_ws(websocket, reader, conn_id, uuid))
        done, pending = await asyncio.wait({task_up, task_down}, return_when=asyncio.FIRST_COMPLETED)
        for t in pending:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass

    except WebSocketDisconnect:
        pass
    except Exception as exc:
        stats["total_errors"] += 1
        error_logs.append({"error": str(exc), "time": datetime.now(timezone.utc).isoformat()})
        logger.exception("WebSocket error")
    finally:
        if writer:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
        if conn_id:
            async with connections_lock:
                info = connections.pop(conn_id, None)
                connection_sockets.pop(conn_id, None)
                if info:
                    uid = info.get("uuid")
                    ip = info.get("ip")
                    if uid and ip:
                        has_other = any(
                            c.get("uuid") == uid and c.get("ip") == ip
                            for c in connections.values()
                        )
                        if not has_other:
                            if uid in link_ip_map:
                                link_ip_map[uid].discard(ip)
                                if not link_ip_map[uid]:
                                    link_ip_map.pop(uid, None)

# ===== PANEL HTML =====
PANEL_HTML = r"""<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Wind Panel</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Inter:opsz,wght@14..32,300;14..32,400;14..32,500;14..32,600;14..32,700&display=swap" rel="stylesheet">
    <script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"></script>
    <style>
        *,
        *::before,
        *::after {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        :root {
            --bg-primary: #f5f7fa;
            --bg-secondary: #ffffff;
            --bg-card: #ffffff;
            --bg-sidebar: rgba(255, 255, 255, 0.72);
            --bg-header: rgba(255, 255, 255, 0.64);
            --bg-input: rgba(255, 255, 255, 0.56);
            --bg-modal: rgba(0, 0, 0, 0.35);

            --text-primary: #1d1d1f;
            --text-secondary: #6e6e73;
            --text-tertiary: #aeaeb2;
            --text-inverse: #ffffff;

            --border-color: rgba(0, 0, 0, 0.06);
            --border-strong: rgba(0, 0, 0, 0.10);

            --shadow-sm: 0 1px 3px rgba(0, 0, 0, 0.02), 0 1px 2px rgba(0, 0, 0, 0.03);
            --shadow-md: 0 8px 30px rgba(0, 0, 0, 0.04), 0 2px 8px rgba(0, 0, 0, 0.02);
            --shadow-lg: 0 20px 60px rgba(0, 0, 0, 0.06), 0 8px 20px rgba(0, 0, 0, 0.03);

            --radius-sm: 10px;
            --radius-md: 16px;
            --radius-lg: 24px;
            --radius-xl: 28px;

            --accent: #007aff;
            --accent-hover: #0066d9;
            --accent-light: rgba(0, 122, 255, 0.10);
            --accent-dark: #0055b3;

            --green: #34c759;
            --red: #ff3b30;
            --orange: #ff9500;
            --purple: #af52de;

            --transition: 0.25s cubic-bezier(0.25, 0.46, 0.45, 0.94);
            --transition-slow: 0.4s cubic-bezier(0.25, 0.46, 0.45, 0.94);

            --font: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
            --font-mono: 'SF Mono', 'Menlo', 'Monaco', 'Consolas', monospace;
        }

        [data-theme="dark"] {
            --bg-primary: #0a0a0c;
            --bg-secondary: #1c1c1e;
            --bg-card: #1c1c1e;
            --bg-sidebar: rgba(28, 28, 30, 0.78);
            --bg-header: rgba(28, 28, 30, 0.70);
            --bg-input: rgba(44, 44, 46, 0.70);
            --bg-modal: rgba(0, 0, 0, 0.55);

            --text-primary: #f5f5f7;
            --text-secondary: #8e8e93;
            --text-tertiary: #48484a;
            --text-inverse: #1d1d1f;

            --border-color: rgba(255, 255, 255, 0.06);
            --border-strong: rgba(255, 255, 255, 0.10);

            --shadow-sm: 0 1px 3px rgba(0, 0, 0, 0.20);
            --shadow-md: 0 8px 30px rgba(0, 0, 0, 0.30);
            --shadow-lg: 0 20px 60px rgba(0, 0, 0, 0.40);

            --accent: #0a84ff;
            --accent-hover: #409cff;
            --accent-light: rgba(10, 132, 255, 0.15);
        }

        html {
            font-size: 16px;
            -webkit-font-smoothing: antialiased;
            -moz-osx-font-smoothing: grayscale;
        }

        body {
            font-family: var(--font);
            background: var(--bg-primary);
            color: var(--text-primary);
            min-height: 100vh;
            transition: background var(--transition), color var(--transition);
            overflow-x: hidden;
        }

        ::-webkit-scrollbar {
            width: 5px;
            height: 5px;
        }
        ::-webkit-scrollbar-track {
            background: transparent;
        }
        ::-webkit-scrollbar-thumb {
            background: var(--text-tertiary);
            border-radius: 10px;
        }
        ::-webkit-scrollbar-thumb:hover {
            background: var(--text-secondary);
        }

        .glass {
            background: var(--bg-sidebar);
            backdrop-filter: blur(24px) saturate(1.4);
            -webkit-backdrop-filter: blur(24px) saturate(1.4);
            border: 1px solid var(--border-color);
        }

        .glass-light {
            background: var(--bg-header);
            backdrop-filter: blur(20px) saturate(1.3);
            -webkit-backdrop-filter: blur(20px) saturate(1.3);
            border: 1px solid var(--border-color);
        }

        .app-container {
            display: flex;
            min-height: 100vh;
        }

        .sidebar {
            position: fixed;
            top: 0;
            right: 0;
            bottom: 0;
            width: 220px;
            padding: 20px 12px;
            display: flex;
            flex-direction: column;
            z-index: 100;
            background: var(--bg-sidebar);
            backdrop-filter: blur(24px) saturate(1.4);
            -webkit-backdrop-filter: blur(24px) saturate(1.4);
            border-left: 1px solid var(--border-color);
            transition: transform var(--transition-slow), background var(--transition), border var(--transition);
        }

        .sidebar-brand {
            display: flex;
            align-items: center;
            padding: 4px 8px 16px 8px;
            border-bottom: 1px solid var(--border-color);
            margin-bottom: 16px;
            justify-content: center;
        }

        .sidebar-brand .wind-logo {
            width: 56px;
            flex-shrink: 0;
            display: flex;
            align-items: center;
            justify-content: center;
        }
        .sidebar-brand .wind-logo svg {
            width: 100%;
            height: auto;
            display: block;
        }

        .sidebar-nav {
            flex: 1;
            display: flex;
            flex-direction: column;
            gap: 2px;
            overflow-y: auto;
            padding: 0 4px;
        }

        .nav-item {
            display: flex;
            align-items: center;
            gap: 12px;
            padding: 10px 14px;
            border-radius: var(--radius-sm);
            font-size: 14px;
            font-weight: 500;
            color: var(--text-secondary);
            background: transparent;
            border: none;
            width: 100%;
            cursor: pointer;
            transition: all var(--transition);
            text-align: right;
            position: relative;
        }

        .nav-item .icon {
            width: 20px;
            height: 20px;
            flex-shrink: 0;
            stroke: currentColor;
            stroke-width: 1.5;
            fill: none;
            transition: stroke var(--transition);
        }

        .nav-item .badge {
            margin-right: auto;
            font-size: 11px;
            font-weight: 600;
            color: var(--text-inverse);
            background: var(--accent);
            padding: 0 8px;
            border-radius: 12px;
            line-height: 18px;
            min-width: 18px;
            text-align: center;
        }

        .nav-item:hover {
            color: var(--text-primary);
            background: var(--border-color);
        }

        .nav-item.active {
            color: var(--accent);
            background: var(--accent-light);
        }

        .nav-item.active .icon {
            stroke: var(--accent);
        }

        .nav-item.logout {
            color: var(--red);
            margin-top: 8px;
            border-top: 1px solid var(--border-color);
            border-radius: 0;
            padding-top: 16px;
        }
        .nav-item.logout:hover {
            background: rgba(255, 59, 48, 0.08);
        }

        .main-content {
            margin-right: 220px;
            flex: 1;
            padding: 20px 32px 40px 32px;
            min-height: 100vh;
            transition: margin var(--transition-slow);
        }

        .header {
            display: flex;
            align-items: center;
            justify-content: space-between;
            padding: 12px 20px;
            margin-bottom: 24px;
            border-radius: var(--radius-md);
            background: var(--bg-header);
            backdrop-filter: blur(20px) saturate(1.3);
            -webkit-backdrop-filter: blur(20px) saturate(1.3);
            border: 1px solid var(--border-color);
            transition: background var(--transition), border var(--transition);
        }

        .header-left {
            display: flex;
            align-items: center;
            gap: 14px;
        }

        .header-left .page-title {
            font-size: 20px;
            font-weight: 600;
            letter-spacing: -0.3px;
            color: var(--text-primary);
        }

        .header-left .page-title .accent {
            color: var(--accent);
        }

        .header-right {
            display: flex;
            align-items: center;
            gap: 12px;
        }

        .header-right .version-tag {
            font-size: 11px;
            font-weight: 500;
            color: var(--text-tertiary);
            background: var(--bg-input);
            padding: 4px 12px;
            border-radius: 20px;
            border: 1px solid var(--border-color);
            letter-spacing: 0.2px;
        }

        .theme-toggle {
            background: none;
            border: none;
            font-size: 18px;
            cursor: pointer;
            color: var(--text-secondary);
            padding: 6px;
            border-radius: 8px;
            transition: background var(--transition), color var(--transition);
            display: flex;
            align-items: center;
            justify-content: center;
        }
        .theme-toggle:hover {
            background: var(--border-color);
            color: var(--text-primary);
        }

        .mobile-menu-btn {
            display: none;
            background: none;
            border: none;
            font-size: 22px;
            cursor: pointer;
            color: var(--text-primary);
            padding: 4px;
        }

        .page {
            display: none;
            animation: fadeSlide 0.35s ease both;
        }
        .page.active {
            display: block;
        }

        @keyframes fadeSlide {
            from {
                opacity: 0;
                transform: translateY(8px);
            }
            to {
                opacity: 1;
                transform: translateY(0);
            }
        }

        .card {
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: var(--radius-md);
            padding: 20px 24px;
            box-shadow: var(--shadow-sm);
            transition: background var(--transition), border var(--transition), box-shadow var(--transition);
            margin-bottom: 12px;
        }
        .card:hover {
            box-shadow: var(--shadow-md);
        }

        .card-title {
            font-size: 13px;
            font-weight: 600;
            color: var(--text-secondary);
            letter-spacing: 0.2px;
            margin-bottom: 12px;
            text-transform: uppercase;
        }

        .stats-grid {
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 12px;
            margin-bottom: 16px;
        }

        .stat-card {
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: var(--radius-md);
            padding: 16px 18px;
            display: flex;
            align-items: center;
            gap: 14px;
            box-shadow: var(--shadow-sm);
            transition: all var(--transition);
        }
        .stat-card:hover {
            box-shadow: var(--shadow-md);
            border-color: var(--border-strong);
        }

        .stat-icon {
            width: 44px;
            height: 44px;
            background: var(--accent-light);
            border-radius: var(--radius-sm);
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 18px;
            flex-shrink: 0;
            color: var(--accent);
        }

        .stat-content {
            flex: 1;
            min-width: 0;
        }
        .stat-label {
            font-size: 12px;
            font-weight: 500;
            color: var(--text-secondary);
            letter-spacing: 0.1px;
        }
        .stat-value {
            font-size: 22px;
            font-weight: 600;
            letter-spacing: -0.3px;
            color: var(--text-primary);
            line-height: 1.2;
        }
        .stat-unit {
            font-size: 14px;
            font-weight: 400;
            color: var(--text-tertiary);
        }

        .actions-row {
            display: flex;
            gap: 8px;
            flex-wrap: wrap;
        }

        .action-btn {
            display: inline-flex;
            align-items: center;
            gap: 6px;
            padding: 8px 18px;
            border-radius: var(--radius-sm);
            border: 1px solid var(--border-color);
            background: var(--bg-secondary);
            color: var(--text-primary);
            font-size: 13px;
            font-weight: 500;
            cursor: pointer;
            transition: all var(--transition);
        }
        .action-btn:hover {
            border-color: var(--border-strong);
            box-shadow: var(--shadow-sm);
            transform: translateY(-1px);
        }
        .action-btn.primary {
            background: var(--accent);
            color: #fff;
            border-color: var(--accent);
        }
        .action-btn.primary:hover {
            background: var(--accent-hover);
            border-color: var(--accent-hover);
        }
        .action-btn.green {
            background: rgba(52, 199, 89, 0.10);
            color: var(--green);
            border-color: rgba(52, 199, 89, 0.20);
        }
        .action-btn.green:hover {
            background: rgba(52, 199, 89, 0.18);
        }
        .action-btn.purple {
            background: rgba(175, 82, 222, 0.10);
            color: var(--purple);
            border-color: rgba(175, 82, 222, 0.20);
        }
        .action-btn.purple:hover {
            background: rgba(175, 82, 222, 0.18);
        }

        .activity-item {
            display: flex;
            align-items: center;
            gap: 12px;
            padding: 10px 0;
            border-bottom: 1px solid var(--border-color);
        }
        .activity-item:last-child {
            border-bottom: none;
        }
        .activity-dot {
            width: 8px;
            height: 8px;
            border-radius: 50%;
            flex-shrink: 0;
        }
        .activity-dot.green {
            background: var(--green);
        }
        .activity-dot.blue {
            background: var(--accent);
        }
        .activity-dot.orange {
            background: var(--orange);
        }
        .activity-text {
            flex: 1;
            font-size: 14px;
            color: var(--text-primary);
        }
        .activity-time {
            font-size: 12px;
            color: var(--text-tertiary);
            font-weight: 400;
        }

        .chart-container {
            height: 160px;
            width: 100%;
        }
        .chart-container.tall {
            height: 220px;
        }

        .table-wrap {
            overflow-x: auto;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            font-size: 14px;
        }
        th {
            text-align: right;
            padding: 10px 12px;
            color: var(--text-secondary);
            font-weight: 600;
            font-size: 12px;
            letter-spacing: 0.2px;
            text-transform: uppercase;
            border-bottom: 1px solid var(--border-color);
        }
        td {
            padding: 10px 12px;
            border-bottom: 1px solid var(--border-color);
            vertical-align: middle;
            color: var(--text-primary);
        }

        .tag {
            display: inline-block;
            padding: 2px 12px;
            border-radius: 20px;
            font-size: 11px;
            font-weight: 600;
            letter-spacing: 0.1px;
        }
        .tag-vless {
            background: var(--accent-light);
            color: var(--accent);
        }
        .tag-on {
            background: rgba(52, 199, 89, 0.12);
            color: var(--green);
        }
        .tag-off {
            background: rgba(255, 59, 48, 0.10);
            color: var(--red);
        }

        .toggle {
            width: 34px;
            height: 20px;
            border-radius: 12px;
            background: var(--text-tertiary);
            position: relative;
            cursor: pointer;
            transition: all var(--transition);
            border: none;
            flex-shrink: 0;
        }
        .toggle.on {
            background: var(--green);
        }
        .toggle::after {
            content: '';
            position: absolute;
            width: 16px;
            height: 16px;
            background: #fff;
            border-radius: 50%;
            top: 2px;
            right: 2px;
            transition: all var(--transition);
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.12);
        }
        .toggle.on::after {
            right: 16px;
        }

        .pill {
            display: flex;
            align-items: center;
            gap: 10px;
        }
        .pill-bar {
            flex: 1;
            height: 4px;
            background: var(--border-color);
            border-radius: 4px;
            min-width: 40px;
            overflow: hidden;
        }
        .pill-fill {
            height: 100%;
            border-radius: 4px;
            transition: width var(--transition);
        }

        .btn {
            display: inline-flex;
            align-items: center;
            gap: 6px;
            padding: 8px 20px;
            border-radius: var(--radius-sm);
            border: none;
            font-size: 14px;
            font-weight: 500;
            cursor: pointer;
            transition: all var(--transition);
            font-family: inherit;
            background: var(--bg-secondary);
            color: var(--text-primary);
            border: 1px solid var(--border-color);
        }
        .btn:hover {
            border-color: var(--border-strong);
            box-shadow: var(--shadow-sm);
            transform: translateY(-1px);
        }
        .btn-primary {
            background: var(--accent);
            color: #fff;
            border-color: var(--accent);
        }
        .btn-primary:hover {
            background: var(--accent-hover);
            border-color: var(--accent-hover);
        }
        .btn-danger {
            background: rgba(255, 59, 48, 0.08);
            color: var(--red);
            border-color: rgba(255, 59, 48, 0.15);
        }
        .btn-danger:hover {
            background: rgba(255, 59, 48, 0.15);
        }
        .btn-sm {
            padding: 4px 14px;
            font-size: 12px;
        }

        .input-group {
            display: flex;
            flex-direction: column;
            gap: 4px;
            margin-bottom: 14px;
        }
        .input-group label {
            font-size: 13px;
            font-weight: 500;
            color: var(--text-secondary);
        }
        .input-group input,
        .input-group select,
        .input-group textarea {
            padding: 10px 14px;
            border: 1px solid var(--border-color);
            border-radius: var(--radius-sm);
            font-size: 14px;
            font-family: inherit;
            background: var(--bg-input);
            color: var(--text-primary);
            transition: all var(--transition);
            outline: none;
            backdrop-filter: blur(4px);
            -webkit-backdrop-filter: blur(4px);
        }
        .input-group input:focus,
        .input-group select:focus,
        .input-group textarea:focus {
            border-color: var(--accent);
            box-shadow: 0 0 0 3px var(--accent-light);
        }
        .input-group input::placeholder,
        .input-group textarea::placeholder {
            color: var(--text-tertiary);
        }
        .input-group select option {
            background: var(--bg-secondary);
            color: var(--text-primary);
        }

        .toolbar {
            display: flex;
            align-items: center;
            justify-content: space-between;
            flex-wrap: wrap;
            gap: 10px;
            margin-bottom: 16px;
        }
        .toolbar-left {
            display: flex;
            align-items: center;
            gap: 8px;
            flex-wrap: wrap;
        }
        .toolbar-right {
            display: flex;
            align-items: center;
            gap: 8px;
            flex-wrap: wrap;
        }

        .search-box {
            display: flex;
            align-items: center;
            background: var(--bg-input);
            border: 1px solid var(--border-color);
            border-radius: var(--radius-sm);
            padding: 0 12px;
            transition: all var(--transition);
            backdrop-filter: blur(4px);
            -webkit-backdrop-filter: blur(4px);
        }
        .search-box:focus-within {
            border-color: var(--accent);
            box-shadow: 0 0 0 3px var(--accent-light);
        }
        .search-box input {
            border: none;
            outline: none;
            padding: 8px 8px;
            font-size: 14px;
            background: transparent;
            width: 150px;
            color: var(--text-primary);
            font-family: inherit;
        }
        .search-box input::placeholder {
            color: var(--text-tertiary);
        }
        .search-box .icon {
            color: var(--text-tertiary);
            font-size: 16px;
        }

        .filter-chips {
            display: flex;
            gap: 4px;
            background: var(--bg-input);
            border-radius: var(--radius-sm);
            padding: 3px;
            border: 1px solid var(--border-color);
            backdrop-filter: blur(4px);
            -webkit-backdrop-filter: blur(4px);
        }
        .chip {
            padding: 5px 14px;
            border-radius: 6px;
            font-size: 12px;
            font-weight: 500;
            color: var(--text-tertiary);
            cursor: pointer;
            border: none;
            background: none;
            transition: all var(--transition);
            font-family: inherit;
        }
        .chip.active {
            background: var(--bg-secondary);
            color: var(--text-primary);
            box-shadow: var(--shadow-sm);
        }
        .chip:hover:not(.active) {
            color: var(--text-secondary);
        }

        .modal-overlay {
            position: fixed;
            inset: 0;
            background: var(--bg-modal);
            backdrop-filter: blur(12px);
            -webkit-backdrop-filter: blur(12px);
            display: none;
            align-items: center;
            justify-content: center;
            z-index: 200;
            padding: 20px;
        }
        .modal-overlay.show {
            display: flex;
        }
        .modal-box {
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: var(--radius-lg);
            padding: 28px 28px 32px;
            max-width: 480px;
            width: 100%;
            box-shadow: var(--shadow-lg);
            position: relative;
            max-height: 90vh;
            overflow-y: auto;
        }
        .modal-close {
            position: absolute;
            top: 16px;
            left: 20px;
            font-size: 22px;
            color: var(--text-tertiary);
            cursor: pointer;
            background: none;
            border: none;
            transition: color var(--transition);
        }
        .modal-close:hover {
            color: var(--text-primary);
        }
        .modal-title {
            font-size: 20px;
            font-weight: 600;
            color: var(--text-primary);
            margin-bottom: 20px;
            letter-spacing: -0.3px;
        }

        .toast {
            position: fixed;
            bottom: 28px;
            left: 50%;
            transform: translateX(-50%) translateY(16px);
            background: var(--bg-secondary);
            color: var(--text-primary);
            border: 1px solid var(--border-color);
            border-radius: var(--radius-sm);
            padding: 12px 28px;
            font-size: 14px;
            font-weight: 500;
            box-shadow: var(--shadow-lg);
            opacity: 0;
            transition: all var(--transition);
            z-index: 999;
            backdrop-filter: blur(12px);
            -webkit-backdrop-filter: blur(12px);
            pointer-events: none;
        }
        .toast.show {
            opacity: 1;
            transform: translateX(-50%) translateY(0);
        }

        .empty-state {
            text-align: center;
            padding: 48px 20px;
            color: var(--text-tertiary);
            font-size: 15px;
            font-weight: 400;
        }

        .live-logs-container {
            background: var(--bg-input);
            border-radius: var(--radius-sm);
            padding: 14px;
            font-family: var(--font-mono);
            font-size: 12px;
            height: 180px;
            overflow-y: auto;
            white-space: pre-wrap;
            color: var(--text-primary);
            border: 1px solid var(--border-color);
            line-height: 1.6;
            transition: background var(--transition), border var(--transition);
        }

        .m-cards {
            display: none;
            flex-direction: column;
            gap: 12px;
            padding: 12px;
        }
        .m-card {
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: var(--radius-sm);
            padding: 14px;
            transition: background var(--transition), border var(--transition);
        }

        .status-settings {
            margin-top: 12px;
            padding-top: 12px;
            border-top: 1px solid var(--border-color);
        }
        .status-settings .select-group {
            display: flex;
            align-items: center;
            gap: 10px;
            margin-bottom: 10px;
        }
        .status-settings .select-group select {
            padding: 6px 12px;
            border-radius: var(--radius-sm);
            border: 1px solid var(--border-color);
            background: var(--bg-input);
            color: var(--text-primary);
            font-size: 13px;
            font-family: inherit;
            outline: none;
        }
        .status-settings .input-group {
            margin-bottom: 8px;
        }

        @media (max-width: 1024px) {
            .stats-grid {
                grid-template-columns: repeat(2, 1fr);
            }
        }

        @media (max-width: 768px) {
            .sidebar {
                transform: translateX(100%);
                width: 260px;
                padding: 16px;
                border-left: none;
                border-right: 1px solid var(--border-color);
            }
            .sidebar.open {
                transform: translateX(0);
            }
            .main-content {
                margin-right: 0;
                padding: 16px;
            }
            .mobile-menu-btn {
                display: block;
            }
            .stats-grid {
                grid-template-columns: 1fr 1fr;
                gap: 10px;
            }
            .header {
                flex-wrap: wrap;
                gap: 10px;
                padding: 12px 16px;
            }
            .header-left .page-title {
                font-size: 17px;
            }
            .toolbar {
                flex-direction: column;
                align-items: stretch;
            }
            .toolbar-left,
            .toolbar-right {
                justify-content: center;
            }
            .search-box input {
                width: 120px;
            }
            .tbl-wrap {
                display: none;
            }
            .m-cards {
                display: flex;
            }
            .stat-card {
                padding: 14px;
            }
            .stat-value {
                font-size: 18px;
            }
        }

        @media (max-width: 480px) {
            .stats-grid {
                grid-template-columns: 1fr;
            }
            .modal-box {
                padding: 20px;
            }
            .header-right .version-tag {
                display: none;
            }
            .sidebar-brand .wind-logo {
                width: 40px;
            }
        }

        @media (prefers-reduced-motion: reduce) {
            *,
            *::before,
            *::after {
                animation-duration: 0.01ms !important;
                transition-duration: 0.01ms !important;
            }
        }
    </style>
</head>
<body>

    <div class="toast" id="toast">کپی شد!</div>

    <aside class="sidebar" id="sidebar">
        <div class="sidebar-brand">
            <div class="wind-logo">
                <svg viewBox="0 0 512 460" xmlns="http://www.w3.org/2000/svg">
                    <defs>
                        <linearGradient id="wind-grad-sidebar" x1="0" y1="0" x2="1" y2="1">
                            <stop offset="0%" stop-color="#f8fbff"/>
                            <stop offset="45%" stop-color="#d9e1ec"/>
                            <stop offset="100%" stop-color="#9aa9bd"/>
                        </linearGradient>
                        <filter id="shadow-sidebar">
                            <feDropShadow dx="0" dy="7" stdDeviation="9" flood-opacity=".18"/>
                        </filter>
                    </defs>
                    <g fill="url(#wind-grad-sidebar)" filter="url(#shadow-sidebar)">
                        <path d="M65 160 C125 91 197 72 282 76 C345 79 400 68 438 34 C438 91 412 135 360 151 C310 166 257 153 205 150 C145 146 101 158 65 191 C69 179 71 169 65 160Z"/>
                        <path d="M61 235 C112 181 169 170 229 176 C280 181 324 197 370 188 C399 182 419 166 439 143 C427 199 388 229 341 236 C291 244 247 224 200 219 C144 213 99 228 61 265 C65 253 67 243 61 235Z"/>
                        <path d="M58 310 C108 257 159 247 216 253 C265 258 303 276 342 272 C376 269 404 251 429 225 C413 280 376 308 329 316 C281 324 239 304 195 299 C140 293 96 307 58 343 C62 331 64 320 58 310Z"/>
                    </g>
                    <text x="256" y="415" text-anchor="middle" font-family="-apple-system, BlinkMacSystemFont, 'SF Pro Display', Inter, Arial, sans-serif" font-size="55" font-weight="400" letter-spacing="-2" fill="#15171b">Wind</text>
                </svg>
            </div>
        </div>

        <nav class="sidebar-nav">
            <button class="nav-item active" data-page="dashboard">
                <svg class="icon" viewBox="0 0 24 24" stroke="currentColor"><rect x="3" y="3" width="7" height="7" rx="1.5"/><rect x="14" y="3" width="7" height="7" rx="1.5"/><rect x="3" y="14" width="7" height="7" rx="1.5"/><rect x="14" y="14" width="7" height="7" rx="1.5"/></svg>
                <span>نمای کلی</span>
            </button>
            <button class="nav-item" data-page="inbounds">
                <svg class="icon" viewBox="0 0 24 24" stroke="currentColor"><path d="M17 21v-2a4 4 0 00-4-4H5a4 4 0 00-4 4v2"/><circle cx="9" cy="7" r="4"/><line x1="23" y1="11" x2="17" y2="11"/><line x1="20" y1="8" x2="20" y2="14"/></svg>
                <span>نودها</span>
                <span class="badge" id="nb">0</span>
            </button>
            <button class="nav-item" data-page="groups">
                <svg class="icon" viewBox="0 0 24 24" stroke="currentColor"><path d="M17 21v-2a4 4 0 00-4-4H5a4 4 0 00-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 00-3-3.87"/><path d="M16 3.13a4 4 0 010 7.75"/></svg>
                <span>گروه‌ها</span>
                <span class="badge" id="gb">0</span>
            </button>
            <button class="nav-item" data-page="traffic">
                <svg class="icon" viewBox="0 0 24 24" stroke="currentColor"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>
                <span>ترافیک</span>
            </button>
            <button class="nav-item" data-page="status">
                <svg class="icon" viewBox="0 0 24 24" stroke="currentColor"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/><polyline points="9 12 11 14 15 10"/></svg>
                <span>وضعیت</span>
            </button>
            <button class="nav-item" data-page="cleanips">
                <svg class="icon" viewBox="0 0 24 24" stroke="currentColor"><circle cx="12" cy="12" r="10"/><line x1="2" y1="12" x2="22" y2="12"/><path d="M12 2a15.3 15.3 0 014 10 15.3 15.3 0 01-4 10 15.3 15.3 0 01-4-10 15.3 15.3 0 014-10z"/></svg>
                <span>آی‌پی تمیز</span>
                <span class="badge" id="cleanip-badge">0</span>
            </button>
            <button class="nav-item" data-page="security">
                <svg class="icon" viewBox="0 0 24 24" stroke="currentColor"><rect x="3" y="11" width="18" height="11" rx="2"/><path d="M7 11V7a5 5 0 0110 0v4"/></svg>
                <span>تنظیمات</span>
            </button>
            <button class="nav-item logout" onclick="doLogout()">
                <svg class="icon" viewBox="0 0 24 24" stroke="currentColor"><path d="M9 21H5a2 2 0 01-2-2V5a2 2 0 012-2h4"/><polyline points="16 17 21 12 16 7"/><line x1="21" y1="12" x2="9" y2="12"/></svg>
                <span>خروج</span>
            </button>
        </nav>
    </aside>

    <main class="main-content" id="mainContent">

        <header class="header">
            <div class="header-left">
                <button class="mobile-menu-btn" onclick="document.getElementById('sidebar').classList.toggle('open')">☰</button>
                <span class="page-title" id="pageTitle"><span class="accent">Wind</span> · نمای کلی</span>
            </div>
            <div class="header-right">
                <span class="version-tag">v3.0</span>
                <button class="theme-toggle" onclick="toggleTheme()" id="themeBtn" aria-label="تغییر تم">🌙</button>
            </div>
        </header>

        <!-- Dashboard -->
        <section class="page active" id="page-dashboard">
            <div class="stats-grid">
                <div class="stat-card">
                    <div class="stat-icon">📊</div>
                    <div class="stat-content">
                        <div class="stat-label">ترافیک کل</div>
                        <div class="stat-value" id="sv-traffic">– <span class="stat-unit">MB</span></div>
                    </div>
                </div>
                <div class="stat-card">
                    <div class="stat-icon">🔗</div>
                    <div class="stat-content">
                        <div class="stat-label">نودها</div>
                        <div class="stat-value" id="sv-links">–</div>
                    </div>
                </div>
                <div class="stat-card">
                    <div class="stat-icon">⏱</div>
                    <div class="stat-content">
                        <div class="stat-label">آپ‌تایم</div>
                        <div class="stat-value" id="sv-uptime" style="font-size:18px;">–</div>
                    </div>
                </div>
                <div class="stat-card">
                    <div class="stat-icon">🌐</div>
                    <div class="stat-content">
                        <div class="stat-label">دامنه</div>
                        <div class="stat-value" id="sv-domain" style="font-size:14px;font-weight:500;">–</div>
                    </div>
                </div>
            </div>

            <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:12px;">
                <div class="card">
                    <div class="card-title">CPU</div>
                    <div style="display:flex;align-items:center;gap:14px;">
                        <span style="font-size:22px;font-weight:600;color:var(--accent);" id="cpu-v">–%</span>
                        <div style="flex:1;height:4px;background:var(--border-color);border-radius:4px;overflow:hidden;">
                            <div id="cpu-b" style="height:100%;background:var(--accent);border-radius:4px;width:0%;transition:width var(--transition);"></div>
                        </div>
                    </div>
                </div>
                <div class="card">
                    <div class="card-title">حافظه</div>
                    <div style="display:flex;align-items:center;gap:14px;">
                        <span style="font-size:22px;font-weight:600;color:var(--green);" id="mem-v">–%</span>
                        <div style="flex:1;height:4px;background:var(--border-color);border-radius:4px;overflow:hidden;">
                            <div id="mem-b" style="height:100%;background:var(--green);border-radius:4px;width:0%;transition:width var(--transition);"></div>
                        </div>
                    </div>
                </div>
            </div>

            <div class="card">
                <div class="card-title">عملیات سریع</div>
                <div class="actions-row">
                    <button class="action-btn primary" onclick="showAddMo()">➕ افزودن نود</button>
                    <button class="action-btn purple" onclick="switchPage('inbounds')">👥 مدیریت نودها</button>
                    <button class="action-btn green" onclick="switchPage('groups')">📦 گروه‌های اشتراک</button>
                </div>
            </div>

            <div class="card">
                <div class="card-title">فعالیت‌های اخیر</div>
                <div id="activityLog">
                    <div class="activity-item"><div class="activity-dot green"></div><span class="activity-text">ورود موفق</span><span class="activity-time">دقیقه پیش</span></div>
                    <div class="activity-item"><div class="activity-dot blue"></div><span class="activity-text">نود جدید ساخته شد</span><span class="activity-time">۲ ساعت پیش</span></div>
                    <div class="activity-item"><div class="activity-dot orange"></div><span class="activity-text">ترافیک روزانه بروز شد</span><span class="activity-time">۳ ساعت پیش</span></div>
                </div>
            </div>

            <div class="card">
                <div class="card-title">ترافیک ساعتی</div>
                <div class="chart-container"><canvas id="tc"></canvas></div>
            </div>
        </section>

        <!-- Inbounds -->
        <section class="page" id="page-inbounds">
            <div class="toolbar">
                <div class="toolbar-left"><span style="font-weight:600;font-size:15px;">فهرست نودها</span></div>
                <div class="toolbar-right">
                    <div class="search-box"><span class="icon">🔍</span><input id="srch" placeholder="جستجوی نام..." oninput="filterLinks()"></div>
                    <button class="btn btn-primary" onclick="showAddMo()">➕ افزودن</button>
                </div>
            </div>
            <div class="card" style="padding:0;overflow:hidden;">
                <div class="table-wrap">
                    <table>
                        <thead><tr><th>#</th><th>نام</th><th>نوع</th><th>مصرف</th><th>IP</th><th>انقضا</th><th>وضعیت</th><th>عملیات</th></tr></thead>
                        <tbody id="ltb"></tbody>
                    </table>
                </div>
                <div class="m-cards" id="mcards"></div>
                <div class="empty-state" id="lempty" style="display:none;">هیچ نودی یافت نشد</div>
            </div>
        </section>

        <!-- Groups -->
        <section class="page" id="page-groups">
            <div class="toolbar">
                <div class="toolbar-left"><span style="font-weight:600;font-size:15px;">گروه‌های اشتراک</span></div>
                <div class="toolbar-right"><button class="btn btn-primary" onclick="showGroupMo()">➕ گروه جدید</button></div>
            </div>
            <div class="card" id="groups-list"><div class="empty-state">در حال بارگذاری...</div></div>
        </section>

        <!-- Traffic -->
        <section class="page" id="page-traffic">
            <div class="card">
                <div style="display:flex;justify-content:space-between;padding:8px 0;border-bottom:1px solid var(--border-color);"><span style="color:var(--text-secondary);">کل ترافیک</span><span style="font-weight:500;" id="t-tr">–</span></div>
                <div style="display:flex;justify-content:space-between;padding:8px 0;border-bottom:1px solid var(--border-color);"><span style="color:var(--text-secondary);">تعداد درخواست‌ها</span><span style="font-weight:500;" id="t-rq">–</span></div>
                <div style="display:flex;justify-content:space-between;padding:8px 0;"><span style="color:var(--text-secondary);">آپ‌تایم</span><span style="font-weight:500;" id="t-up">–</span></div>
            </div>
            <div class="card">
                <div class="card-title">سهم ترافیک نودها</div>
                <div class="chart-container tall"><canvas id="inbound-chart"></canvas></div>
            </div>
        </section>

        <!-- Status Page -->
        <section class="page" id="page-status">
            <div class="card">
                <div class="card-title">تنظیمات کانفیگ وضعیت</div>
                <div class="status-settings">
                    <div class="select-group">
                        <label style="font-size:13px;color:var(--text-secondary);">حالت نمایش:</label>
                        <select id="statusDisplayMode" onchange="updateCustomField()">
                            <option value="separate">جدا (حجم + زمان)</option>
                            <option value="combined">ترکیبی (حجم و زمان)</option>
                            <option value="custom">متن دلخواه</option>
                            <option value="separate+custom">جدا + متن دلخواه</option>
                            <option value="combined+custom">ترکیبی + متن دلخواه</option>
                        </select>
                    </div>
                    <div class="input-group" id="customTextGroup">
                        <label for="customStatusText">متن سفارشی:</label>
                        <input type="text" id="customStatusText" placeholder="مثلاً: 🌟 اشتراک شما فعال است" onchange="saveStatusSettings()">
                    </div>
                    <button class="btn btn-primary btn-sm" onclick="saveStatusSettings()">ذخیره تنظیمات وضعیت</button>
                </div>
            </div>
            <div class="card">
                <div class="card-title">پیش‌نمایش کانفیگ‌های وضعیت</div>
                <div style="background:var(--bg-input);border-radius:var(--radius-sm);padding:12px;font-family:var(--font-mono);font-size:12px;border:1px solid var(--border-color);">
                    <div id="statusPreview">بارگذاری...</div>
                </div>
            </div>
        </section>

        <!-- Clean IP Page -->
        <section class="page" id="page-cleanips">
            <div class="toolbar">
                <div class="toolbar-left"><span style="font-weight:600;font-size:15px;">مدیریت آی‌پی تمیز</span></div>
                <div class="toolbar-right">
                    <button class="btn btn-primary btn-sm" onclick="openAddCleanIP()">➕ افزودن</button>
                </div>
            </div>
            <div class="card" id="cleanip-list">
                <div class="empty-state">هیچ آی‌پی تمیزی اضافه نشده</div>
            </div>
        </section>

        <!-- Security -->
        <section class="page" id="page-security">
            <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;">
                <div class="card">
                    <div class="card-title">تنظیمات ربات تلگرام</div>
                    <div class="input-group"><label>توکن ربات</label><input id="tg-token" placeholder="123456:ABC-DEF..."></div>
                    <div class="input-group"><label>شناسه ادمین</label><input id="tg-admin-id" placeholder="987654321"></div>
                    <button class="btn btn-primary" onclick="saveSettings()">ذخیره تنظیمات</button>
                </div>
                <div class="card">
                    <div class="card-title">تغییر رمز عبور</div>
                    <div class="input-group"><label>رمز فعلی</label><input type="password" id="cpw" placeholder="رمز فعلی"></div>
                    <div class="input-group"><label>رمز جدید</label><input type="password" id="npw" placeholder="حداقل ۴ کاراکتر"></div>
                    <button class="btn btn-primary" onclick="chgPw()">بروزرسانی رمز</button>
                </div>
            </div>
            <div class="card">
                <div class="card-title">لاگ‌های زنده</div>
                <div class="live-logs-container" id="log-container">در حال اتصال...</div>
            </div>
        </section>

    </main>

    <!-- Add Clean IP Modal -->
    <div class="modal-overlay" id="mo-cleanip-add" onclick="if(event.target===this)this.classList.remove('show')">
        <div class="modal-box" onclick="event.stopPropagation()">
            <button class="modal-close" onclick="document.getElementById('mo-cleanip-add').classList.remove('show')">✕</button>
            <div class="modal-title">افزودن آی‌پی تمیز</div>
            <div class="input-group"><label>آی‌پی</label><input class="input-dark" id="cleanip-input" placeholder="مثال: 1.2.3.4"></div>
            <button class="btn btn-primary" onclick="addCleanIP()" style="width:100%;justify-content:center;">افزودن</button>
        </div>
    </div>

    <!-- Other Modals -->
    <div class="modal-overlay" id="mo-add" onclick="if(event.target===this)this.classList.remove('show')">
        <div class="modal-box" onclick="event.stopPropagation()">
            <button class="modal-close" onclick="document.getElementById('mo-add').classList.remove('show')">✕</button>
            <div class="modal-title">افزودن نود</div>
            <div class="input-group"><label>نام</label><input id="nl" placeholder="مثلاً کاربر ۱"></div>
            <div style="display:flex;gap:8px;">
                <div class="input-group" style="flex:1;"><label>محدودیت</label><input id="nv" type="number" min="0" step="0.1" placeholder="۰ = ∞"></div>
                <div class="input-group" style="max-width:100px;"><label>واحد</label><select id="nu"><option>GB</option></select></div>
            </div>
            <div class="input-group"><label>حداکثر IP</label><input id="nc" type="number" min="0" placeholder="۰ = ∞"></div>
            <div class="input-group"><label>روزهای اعتبار</label><input id="nd" type="number" min="0" placeholder="۰ = بدون انقضا"></div>
            <button class="btn btn-primary" onclick="createLink()" style="width:100%;justify-content:center;">ایجاد</button>
        </div>
    </div>

    <div class="modal-overlay" id="mo-edit" onclick="if(event.target===this)this.classList.remove('show')">
        <div class="modal-box" onclick="event.stopPropagation()">
            <button class="modal-close" onclick="document.getElementById('mo-edit').classList.remove('show')">✕</button>
            <div class="modal-title" id="et">ویرایش نود</div>
            <input type="hidden" id="eu">
            <div class="input-group"><label>نام</label><input id="en2" readonly style="opacity:0.5;cursor:not-allowed;"></div>
            <div style="display:flex;gap:8px;">
                <div class="input-group" style="flex:1;"><label>محدودیت</label><input id="el" type="number" min="0" step="0.1" placeholder="۰ = ∞"></div>
                <div class="input-group" style="max-width:100px;"><label>واحد</label><select id="eu2"><option>GB</option></select></div>
            </div>
            <div class="input-group"><label>حداکثر IP</label><input id="ec" type="number" min="0" placeholder="۰ = ∞"></div>
            <div class="input-group"><label>افزایش روزها</label><input id="ed" type="number" min="0" placeholder="۰ = بدون تغییر"></div>
            <div style="display:flex;gap:8px;margin-top:8px;">
                <button class="btn btn-primary" onclick="saveEdit()" style="flex:1;">ذخیره</button>
                <button class="btn btn-danger" onclick="resetTraf()">بازنشانی</button>
            </div>
        </div>
    </div>

    <div class="modal-overlay" id="mo-group" onclick="if(event.target===this)this.classList.remove('show')">
        <div class="modal-box" onclick="event.stopPropagation()">
            <button class="modal-close" onclick="document.getElementById('mo-group').classList.remove('show')">✕</button>
            <div class="modal-title" id="group-mo-title">گروه اشتراک جدید</div>
            <input type="hidden" id="group-edit-id">
            <div class="input-group"><label>نام گروه</label><input id="group-name" placeholder="مثلاً کاربران ویژه"></div>
            <div class="input-group"><label>انتخاب نودها (Ctrl برای چندتایی)</label><select id="group-inbounds" multiple style="height:80px;"></select></div>
            <div style="display:flex;gap:8px;">
                <div class="input-group" style="flex:1;"><label>محدودیت کل (GB)</label><input id="group-limit" type="number" min="0" step="0.1" placeholder="۰ = ∞"></div>
                <div class="input-group" style="max-width:100px;"><label>روز</label><input id="group-days" type="number" min="0" placeholder="۰"></div>
            </div>
            <button class="btn btn-primary" onclick="saveGroup()" style="width:100%;justify-content:center;">ذخیره گروه</button>
        </div>
    </div>

    <div class="modal-overlay" id="mo-qr" onclick="if(event.target===this)this.classList.remove('show')">
        <div class="modal-box" style="max-width:360px;" onclick="event.stopPropagation()">
            <button class="modal-close" onclick="document.getElementById('mo-qr').classList.remove('show')">✕</button>
            <div class="modal-title">کد QR</div>
            <div style="text-align:center;"><img id="qr-img" src="" alt="QR" style="max-width:200px;border-radius:var(--radius-sm);border:1px solid var(--border-color);"></div>
            <div style="display:flex;gap:8px;margin-top:16px;justify-content:center;">
                <button class="btn btn-primary btn-sm" onclick="dlQR()">دانلود</button>
                <button class="btn btn-sm" onclick="document.getElementById('mo-qr').classList.remove('show')">بستن</button>
            </div>
        </div>
    </div>

    <script>
        // ===== Helper Functions =====
        function $(s) { return document.querySelector(s); }
        function $m(id) { return document.getElementById(id); }
        function esc(s) { return String(s).replace(/</g, '&lt;').replace(/>/g, '&gt;'); }

        // ===== State =====
        let lang = 'fa', theme = 'light';
        let allLinks = [], allGroups = [], sData = {};
        let cf = 'all', tChart = null, iChart = null;
        let isAuthenticated = false, logsWS = null;
        let statusDisplayMode = 'separate';
        let customStatusText = '🌟 اشتراک شما فعال است';
        let cleanIPs = {};

        // ===== Theme =====
        function setTheme(t) {
            theme = t;
            document.documentElement.setAttribute('data-theme', t);
            localStorage.setItem('theme', t);
            const btn = $m('themeBtn');
            if (btn) btn.textContent = t === 'dark' ? '☀️' : '🌙';
        }

        function toggleTheme() {
            setTheme(theme === 'dark' ? 'light' : 'dark');
        }

        const savedTheme = localStorage.getItem('theme') || 'light';
        setTheme(savedTheme);

        // ===== Toast =====
        function toast(msg) {
            const t = $m('toast');
            t.textContent = msg;
            t.classList.add('show');
            clearTimeout(t._hide);
            t._hide = setTimeout(() => t.classList.remove('show'), 2800);
        }

        // ===== Status Settings =====
        async function saveStatusSettings() {
            const mode = document.getElementById('statusDisplayMode').value;
            const text = document.getElementById('customStatusText').value.trim();
            try {
                const r = await authFetch('/api/settings', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        status_display_mode: mode,
                        custom_status_text: text
                    })
                });
                if (r.ok) {
                    statusDisplayMode = mode;
                    customStatusText = text;
                    toast('تنظیمات وضعیت ذخیره شد');
                    updateStatusPreview();
                } else {
                    toast('خطا در ذخیره', true);
                }
            } catch (e) {
                toast('خطا در ذخیره', true);
            }
        }

        function loadStatusSettings() {
            authFetch('/api/settings')
                .then(r => r.json())
                .then(d => {
                    statusDisplayMode = d.status_display_mode || 'separate';
                    customStatusText = d.custom_status_text || '🌟 اشتراک شما فعال است';
                    document.getElementById('statusDisplayMode').value = statusDisplayMode;
                    document.getElementById('customStatusText').value = customStatusText;
                    updateCustomField();
                    updateStatusPreview();
                })
                .catch(() => {});
        }

        function updateCustomField() {
            const mode = document.getElementById('statusDisplayMode').value;
            const customGroup = document.getElementById('customTextGroup');
            if (mode === 'custom' || mode === 'separate+custom' || mode === 'combined+custom') {
                customGroup.style.display = 'block';
            } else {
                customGroup.style.display = 'none';
            }
        }

        function updateStatusPreview() {
            const preview = document.getElementById('statusPreview');
            if (!preview) return;
            const mode = statusDisplayMode;
            const text = customStatusText;
            let lines = [];
            
            if (mode === 'separate+custom') {
                lines.push(`vless://status@0.0.0.0:443?encryption=none&security=tls&type=ws&host=...&path=/ws/status#📊%20User%20%7C%20Usage`);
                lines.push(`vless://status@0.0.0.0:443?encryption=none&security=tls&type=ws&host=...&path=/ws/status#⏳%20User%20%7C%20Expiry`);
                lines.push(`vless://status@0.0.0.0:443?encryption=none&security=tls&type=ws&host=...&path=/ws/status#${encodeURIComponent(text)}`);
            } else if (mode === 'combined+custom') {
                lines.push(`vless://status@0.0.0.0:443?encryption=none&security=tls&type=ws&host=...&path=/ws/status#📊%20User%20%7C%20Usage%20%7C%20⏳%20Expiry`);
                lines.push(`vless://status@0.0.0.0:443?encryption=none&security=tls&type=ws&host=...&path=/ws/status#${encodeURIComponent(text)}`);
            } else if (mode === 'custom') {
                lines.push(`vless://status@0.0.0.0:443?encryption=none&security=tls&type=ws&host=...&path=/ws/status#${encodeURIComponent(text)}`);
            } else if (mode === 'separate') {
                lines.push(`vless://status@0.0.0.0:443?encryption=none&security=tls&type=ws&host=...&path=/ws/status#📊%20User%20%7C%20Usage`);
                lines.push(`vless://status@0.0.0.0:443?encryption=none&security=tls&type=ws&host=...&path=/ws/status#⏳%20User%20%7C%20Expiry`);
            } else { // combined
                lines.push(`vless://status@0.0.0.0:443?encryption=none&security=tls&type=ws&host=...&path=/ws/status#📊%20User%20%7C%20Usage%20%7C%20⏳%20Expiry`);
            }
            
            preview.innerHTML = lines.join('<br>');
        }

        // ===== Navigation =====
        document.querySelectorAll('.nav-item[data-page]').forEach(el => {
            el.addEventListener('click', () => switchPage(el.dataset.page));
        });

        function switchPage(id) {
            document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
            const target = $m('page-' + id);
            if (target) target.classList.add('active');
            document.querySelectorAll('.nav-item').forEach(n => n.classList.toggle('active', n.dataset.page === id));
            const titles = {
                dashboard: 'نمای کلی',
                inbounds: 'نودها',
                groups: 'گروه‌ها',
                traffic: 'ترافیک',
                status: 'وضعیت',
                cleanips: 'آی‌پی تمیز',
                security: 'تنظیمات'
            };
            const titleEl = $m('pageTitle');
            if (titleEl) titleEl.innerHTML = '<span class="accent">Wind</span> · ' + (titles[id] || id);
            document.getElementById('sidebar').classList.remove('open');
            if (id === 'groups') renderGroups();
            if (id === 'cleanips') loadCleanIPs();
        }

        // ===== AUTH =====
        async function checkAuth() {
            try {
                const token = localStorage.getItem('token');
                const headers = {};
                if (token) headers['Authorization'] = 'Bearer ' + token;
                const r = await fetch('/api/me', { headers, credentials: 'include' });
                const d = await r.json();
                if (d.authenticated) {
                    isAuthenticated = true;
                    initAll();
                } else {
                    window.location.href = '/login';
                }
            } catch (e) {
                window.location.href = '/login';
            }
        }

        async function doLogout() {
            localStorage.removeItem('token');
            await fetch('/api/logout', { method: 'POST' });
            window.location.href = '/login';
        }

        // ===== FETCH =====
        async function authFetch(url, options = {}) {
            const token = localStorage.getItem('token');
            const headers = options.headers || {};
            if (token) { headers['Authorization'] = 'Bearer ' + token; }
            return fetch(url, { ...options, headers, credentials: 'include' });
        }

        // ===== INIT =====
        function initAll() {
            initChart();
            loadStats();
            loadLinks();
            loadGroups();
            loadSettings();
            loadStatusSettings();
            loadCleanIPs();
            connectLogsWS();
            setInterval(() => {
                if (isAuthenticated) { loadStats();
                    loadLinks();
                    loadGroups(); }
            }, 12000);
        }

        // ===== LOGS WS =====
        function connectLogsWS() {
            if (logsWS) try { logsWS.close(); } catch (e) {}
            const token = localStorage.getItem('token');
            if (!token) return;
            const wsUrl = (location.protocol === 'https:' ? 'wss:' : 'ws:') + '//' + location.host + '/ws/live-logs?token=' +
                token;
            logsWS = new WebSocket(wsUrl);
            logsWS.onmessage = e => {
                const c = $m('log-container');
                if (c) { c.textContent += e.data + '\n';
                    c.scrollTop = c.scrollHeight; }
            };
            logsWS.onerror = () => { $m('log-container').textContent = 'خطا در اتصال لاگ...'; };
            logsWS.onclose = () => setTimeout(connectLogsWS, 5000);
        }

        // ===== STATS =====
        async function loadStats() {
            try {
                const r = await authFetch('/stats');
                if (r.status === 401) { window.location.href = '/login'; return; }
                if (!r.ok) throw new Error();
                sData = await r.json();
                $m('sv-traffic').innerHTML = (sData.total_traffic_mb || 0) +
                    ' <span class="stat-unit">MB</span>';
                $m('sv-links').textContent = sData.links_count || 0;
                $m('sv-uptime').textContent = sData.uptime || '–';
                $m('sv-domain').textContent = sData.domain || '–';
                $m('nb').textContent = sData.links_count || 0;
                if ($m('t-tr')) $m('t-tr').textContent = (sData.total_traffic_mb || 0) + ' MB';
                if ($m('t-rq')) $m('t-rq').textContent = (sData.total_requests || 0).toLocaleString();
                if ($m('t-up')) $m('t-up').textContent = sData.uptime || '–';
                if (sData.cpu_percent !== undefined) {
                    const c = sData.cpu_percent;
                    $m('cpu-v').textContent = c.toFixed(1) + '%';
                    $m('cpu-b').style.width = c + '%';
                }
                if (sData.memory_percent !== undefined) {
                    const m = sData.memory_percent;
                    $m('mem-v').textContent = m.toFixed(1) + '%';
                    $m('mem-b').style.width = m + '%';
                }
                updChart();
            } catch (e) {}
        }

        // ===== LINKS =====
        async function loadLinks() {
            try {
                const r = await authFetch('/api/links');
                if (r.status === 401) { window.location.href = '/login'; return; }
                if (!r.ok) throw new Error();
                const d = await r.json();
                allLinks = d.links || [];
                filterLinks();
                populateGroupSelect();
            } catch (e) {}
        }

        function filterLinks() {
            const q = ($m('srch')?.value || '').toLowerCase();
            let r = allLinks;
            if (cf === 'active') r = r.filter(l => l.active);
            else if (cf === 'off') r = r.filter(l => !l.active);
            if (q) r = r.filter(l => l.label.toLowerCase().includes(q) || l.uuid.toLowerCase().includes(q));
            renderLinks(r);
        }

        function renderLinks(links) {
            const tb = $m('ltb'),
                em = $m('lempty'),
                mc = $m('mcards');
            if (!links || !links.length) { tb.innerHTML = '';
                mc.innerHTML = '';
                em.style.display = 'block'; return; }
            em.style.display = 'none';
            let idx = links.length;
            const rows = links.map(l => {
                const u = l.used_bytes || 0;
                const lim = l.limit_bytes || 0;
                const pct = lim > 0 ? Math.min(100, (u / lim) * 100) : 0;
                const col = pct > 90 ? 'var(--red)' : pct > 70 ? 'var(--orange)' : 'var(--accent)';
                const ex = fmtExp(l.expires_at);
                const ec = ex === 'Expired' ? 'var(--red)' : ex === '∞' ? 'var(--text-tertiary)' : 'var(--text-primary)';
                const i = idx--;
                const cc = l.current_connections || 0;
                const mc2 = l.max_connections || 0;
                return { l, pct, col, ex, ec, i, cc, mc2, u, lim };
            });
            tb.innerHTML = rows.map(r =>
                `<tr><td style="color:var(--text-tertiary);font-size:13px;">${r.i}</td><td style="font-weight:500;">${esc(r.l.label)}</td><td><span class="tag tag-vless">VLESS</span></td><td><div class="pill"><span style="font-weight:500;">${fmtB(r.u)}</span><div class="pill-bar"><div class="pill-fill" style="width:${r.pct}%;background:${r.col};"></div></div><span style="color:var(--text-tertiary);font-size:12px;">${fmtLim(r.lim)}</span></div></td><td style="font-weight:500;color:${r.mc2>0&&r.cc>=r.mc2?'var(--red)':'var(--text-primary)'};">${r.cc}/${r.mc2||'∞'}</td><td style="color:${r.ec};">${r.ex}</td><td><span class="tag ${r.l.active?'tag-on':'tag-off'}">${r.l.active?'فعال':'غیرفعال'}</span></td><td><div style="display:flex;gap:4px;flex-wrap:wrap;"><button class="toggle ${r.l.active?'on':''}" data-uid="${r.l.uuid}" onclick="togLink(this)"></button><button class="btn btn-sm" style="background:var(--accent-light);color:var(--accent);border:none;" onclick="showEditMo('${r.l.uuid}')">ویرایش</button><button class="btn btn-sm" style="background:var(--accent-light);color:var(--accent);border:none;" onclick="cpLink('${esc(r.l.vless_link||'')}')">کپی</button><button class="btn btn-sm" style="background:rgba(52,199,89,0.10);color:var(--green);border:none;" onclick="cpSub('${r.l.uuid}')">اشتراک</button><button class="btn btn-sm" style="background:rgba(175,82,222,0.10);color:var(--purple);border:none;" onclick="showQR('${esc(r.l.vless_link||'')}')">QR</button><button class="btn btn-sm btn-danger" onclick="delLink('${r.l.uuid}')">حذف</button></div></td></tr>`
            ).join('');
            mc.innerHTML = rows.map(r =>
                `<div class="m-card"><div style="display:flex;justify-content:space-between;align-items:center;"><span style="font-weight:500;">${esc(r.l.label)}</span><button class="toggle ${r.l.active?'on':''}" data-uid="${r.l.uuid}" onclick="togLink(this)"></button></div><div class="pill" style="margin-top:6px;"><span style="font-weight:500;">${fmtB(r.u)}</span><div class="pill-bar"><div class="pill-fill" style="width:${r.pct}%;background:${r.col};"></div></div><span style="color:var(--text-tertiary);font-size:12px;">${fmtLim(r.lim)}</span></div><div style="font-size:13px;color:var(--text-secondary);margin-top:4px;">⏳ ${r.ex} · ${r.cc}/${r.mc2||'∞'} IP</div><div style="display:flex;gap:4px;flex-wrap:wrap;margin-top:8px;"><button class="btn btn-sm" style="background:var(--accent-light);color:var(--accent);border:none;" onclick="showEditMo('${r.l.uuid}')">ویرایش</button><button class="btn btn-sm" style="background:var(--accent-light);color:var(--accent);border:none;" onclick="cpLink('${esc(r.l.vless_link||'')}')">کپی</button><button class="btn btn-sm" style="background:rgba(52,199,89,0.10);color:var(--green);border:none;" onclick="cpSub('${r.l.uuid}')">اشتراک</button><button class="btn btn-sm" style="background:rgba(175,82,222,0.10);color:var(--purple);border:none;" onclick="showQR('${esc(r.l.vless_link||'')}')">QR</button><button class="btn btn-sm btn-danger" onclick="delLink('${r.l.uuid}')">حذف</button></div></div>`
            ).join('');
        }

        function fmtB(b) { if (!b || b === 0) return '0 B'; return b >= 1073741824 ? (b / 1073741824).toFixed(2) + ' GB' : b >=
                1048576 ? (b / 1048576).toFixed(2) + ' MB' : (b / 1024).toFixed(1) + ' KB'; }

        function fmtLim(b) { if (!b || b === 0) return '∞'; const g = b / 1073741824; return (g % 1 === 0 ? g.toFixed(0) : g
                .toFixed(1)) + ' GB'; }

        function fmtExp(ea) { if (!ea) return '∞'; const d = new Date(ea) - new Date(); if (d <= 0) return 'Expired'; const days =
                Math.floor(d / 86400000); if (days > 0) return days + 'd'; const hours = Math.floor(d / 3600000); if (hours >
                0) return hours + 'h'; return Math.floor(d / 60000) + 'm'; }

        async function togLink(el) { const uid = el.dataset.uid; const l = allLinks.find(x => x.uuid === uid); if (!l) return;
            const na = !l.active; try { const r = await authFetch('/api/links/' + uid, { method: 'PATCH', headers: {
                        'Content-Type': 'application/json' }, body: JSON.stringify({ active: na }) }); if (!r.ok) throw new Error();
                l.active = na;
                filterLinks();
                loadStats(); } catch (e) { toast('خطا در تغییر وضعیت'); } }

        async function createLink() { const label = $m('nl').value.trim() || 'New Link'; if (!/^[a-zA-Z0-9\-_. ]+$/.test(label)) {
                toast('فقط حروف انگلیسی مجاز'); return; } const v = parseFloat($m('nv').value) || 0; const mc = parseInt($m('nc')
                    .value) || 0; const days = parseInt($m('nd').value) || 0; try { const r = await authFetch('/api/links', {
                    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ label,
                        limit_value: v, limit_unit: 'GB', max_connections: mc, days_valid: days }) }); if (!r.ok) throw new
                    Error(); toast('ساخته شد');
                $m('nl').value = '';
                $m('nv').value = '';
                $m('nc').value = '';
                $m('nd').value = '';
                $m('mo-add').classList.remove('show');
                await loadLinks();
                await loadStats();
                await loadGroups(); } catch (e) { toast('خطا در ساخت'); } }

        async function delLink(uid) { if (!confirm('حذف شود؟')) return; try { const r = await authFetch('/api/links/' + uid, {
                    method: 'DELETE' }); if (!r.ok) throw new Error(); toast('حذف شد');
                await loadLinks();
                await loadStats();
                await loadGroups(); } catch (e) { toast('خطا در حذف'); } }

        function cpLink(txt) { if (!txt) { toast('لینکی وجود ندارد'); return; } navigator.clipboard.writeText(txt).then(() =>
                toast('کپی شد!')).catch(() => toast('خطا در کپی')); }

        async function cpSub(uid) { try { await navigator.clipboard.writeText('https://' + location.host + '/sub/' + uid);
                toast('آدرس اشتراک کپی شد!'); } catch (e) { toast('خطا در کپی'); } }

        function showQR(txt) { if (!txt) { toast('داده‌ای برای QR وجود ندارد'); return; }
            $m('qr-img').src = 'https://api.qrserver.com/v1/create-qr-code/?size=280x280&data=' + encodeURIComponent(txt);
            $m('mo-qr').classList.add('show'); }

        function dlQR() { const a = document.createElement('a');
            a.href = $m('qr-img').src;
            a.download = 'wind-qr.png';
            a.click(); }

        function showAddMo() {
            $m('nl').value = '';
            $m('nv').value = '';
            $m('nc').value = '';
            $m('nd').value = '';
            $m('mo-add').classList.add('show');
        }

        function showEditMo(uid) { const l = allLinks.find(x => x.uuid === uid); if (!l) return;
            $m('eu').value = uid;
            $m('en2').value = l.label;
            $m('el').value = l.limit_bytes > 0 ? (l.limit_bytes / 1073741824) : '';
            $m('ec').value = l.max_connections > 0 ? l.max_connections : '';
            $m('ed').value = '';
            $m('et').textContent = 'ویرایش: ' + l.label;
            $m('mo-edit').classList.add('show'); }

        async function saveEdit() { const uid = $m('eu').value; const v = parseFloat($m('el').value) || 0; const mc = parseInt($m(
                'ec').value) || 0; const days = parseInt($m('ed').value) || 0; const body = { limit_value: v, limit_unit: 'GB',
                max_connections: mc }; if (days > 0) body.days_valid = days; try { const r = await authFetch('/api/links/' +
                    uid, { method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }); if (!
                    r.ok) throw new Error(); toast('بروز شد');
                $m('mo-edit').classList.remove('show');
                await loadLinks();
                await loadGroups(); } catch (e) { toast('خطا در بروزرسانی'); } }

        async function resetTraf() { const uid = $m('eu').value; if (!confirm('ترافیک بازنشانی شود؟')) return; try { const r =
                    await authFetch('/api/links/' + uid, { method: 'PATCH', headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ reset_usage: true }) }); if (!r.ok) throw new Error(); toast('ترافیک بازنشانی شد');
                await loadLinks(); } catch (e) { toast('خطا در بازنشانی'); } }

        // ===== GROUPS =====
        async function loadGroups() { try { const r = await authFetch('/api/subscription-groups'); if (!r.ok) throw new Error();
                const d = await r.json();
                allGroups = d.groups || [];
                $m('gb').textContent = allGroups.length;
                renderGroups(); } catch (e) {} }

        // ===== RENDER GROUPS WITH QR BUTTON =====
        function renderGroups() {
            const el = $m('groups-list');
            if (!el) return;
            if (!allGroups || !allGroups.length) {
                el.innerHTML = '<div class="empty-state">گروهی تعریف نشده</div>';
                return;
            }
            let html = '';
            allGroups.forEach(g => {
                const expiry = g.expires_at ? fmtExp(g.expires_at) : '∞';
                const limitStr = g.limit_bytes ? fmtLim(g.limit_bytes) : '∞';
                const usedStr = fmtB(g.total_used_bytes || 0);
                const subUrl = 'https://' + location.host + '/sub/group/' + g.id;
                html += `
                    <div style="background:var(--bg-secondary);border:1px solid var(--border-color);border-radius:var(--radius-sm);padding:16px;margin-bottom:10px;">
                        <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:6px;">
                            <div>
                                <span style="font-weight:600;font-size:16px;color:var(--accent);">${esc(g.name)}</span>
                                <span style="color:var(--text-tertiary);font-size:12px;margin-right:8px;">#${g.id.substring(0,6)}</span>
                            </div>
                            <div style="display:flex;gap:4px;flex-wrap:wrap;">
                                <button class="btn btn-sm" style="background:var(--accent-light);color:var(--accent);border:none;" onclick="cpGroupSub('${g.id}')">کپی اشتراک</button>
                                <button class="btn btn-sm" style="background:rgba(52,199,89,0.10);color:var(--green);border:none;" onclick="window.open('${subUrl}','_blank')">مشاهده</button>
                                <button class="btn btn-sm" style="background:var(--accent-light);color:var(--accent);border:none;" onclick="editGroup('${g.id}')">ویرایش</button>
                                <button class="btn btn-sm" style="background:rgba(175,82,222,0.10);color:var(--purple);border:none;" onclick="showGroupQR('${g.id}')">📱 QR</button>
                                <button class="btn btn-sm btn-danger" onclick="delGroup('${g.id}')">حذف</button>
                            </div>
                        </div>
                        <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-top:10px;font-size:13px;">
                            <div><span style="color:var(--text-secondary);">نودها:</span> <span style="font-weight:500;">${g.inbounds?.length||0}</span></div>
                            <div><span style="color:var(--text-secondary);">مصرف:</span> <span style="font-weight:500;">${usedStr} / ${limitStr}</span></div>
                            <div><span style="color:var(--text-secondary);">انقضا:</span> <span style="font-weight:500;color:${expiry==='Expired'?'var(--red)':'var(--text-primary)'};">${expiry}</span></div>
                        </div>
                        <div style="display:flex;flex-wrap:wrap;gap:4px;margin-top:8px;">
                            ${g.inbounds?.map(uid => `<span class="tag tag-vless">${esc(uid)}</span>`).join('') || '<span style="color:var(--text-tertiary);font-size:13px;">هیچ نودی انتخاب نشده</span>'}
                        </div>
                    </div>
                `;
            });
            el.innerHTML = html;
        }

        // ===== GROUP QR =====
        function showGroupQR(gid) {
            const group = allGroups.find(x => x.id === gid);
            if (!group) {
                toast('گروه یافت نشد');
                return;
            }
            const url = 'https://' + location.host + '/sub/group/' + gid;
            $m('qr-img').src = 'https://api.qrserver.com/v1/create-qr-code/?size=280x280&data=' + encodeURIComponent(url);
            $m('mo-qr').classList.add('show');
        }

        function showGroupMo() {
            $m('group-edit-id').value = '';
            $m('group-name').value = '';
            $m('group-limit').value = '';
            $m('group-days').value = '';
            $m('group-mo-title').textContent = 'گروه اشتراک جدید';
            populateGroupSelect();
            $m('mo-group').classList.add('show');
        }

        function populateGroupSelect() { const sel = $m('group-inbounds'); if (!sel) return; sel.innerHTML = '';
            allLinks.forEach(l => { const opt = document.createElement('option');
                opt.value = l.uuid;
                opt.textContent = l.label + ' (' + fmtLim(l.limit_bytes) + ')';
                sel.appendChild(opt); }); }

        function editGroup(gid) { const g = allGroups.find(x => x.id === gid); if (!g) return;
            $m('group-edit-id').value = gid;
            $m('group-name').value = g.name;
            $m('group-limit').value = g.limit_bytes > 0 ? (g.limit_bytes / 1073741824) : '';
            $m('group-days').value = '';
            $m('group-mo-title').textContent = 'ویرایش گروه';
            populateGroupSelect(); const sel = $m('group-inbounds'); for (let opt of sel.options) { if (g.inbounds?.includes(opt
                        .value)) opt.selected = true; }
            $m('mo-group').classList.add('show'); }

        async function saveGroup() { const id = $m('group-edit-id').value; const name = $m('group-name').value.trim(); if (!name ||
                !/^[a-zA-Z0-9\-_. ]+$/.test(name)) { toast('نام گروه نامعتبر'); return; } const sel = $m('group-inbounds');
            const inbounds = []; for (let opt of sel.options) { if (opt.selected) inbounds.push(opt.value); } if (!inbounds
                .length) { toast('حداقل یک نود انتخاب کنید'); return; } const limit = parseFloat($m('group-limit').value) || 0;
            const days = parseInt($m('group-days').value) || 0; try { let url = '/api/subscription-groups'; let method =
                    'POST'; let body = { name, inbounds, limit_gb: limit, days }; if (id) { url += '/' + id;
                    method = 'PATCH';
                    body = { name, inbounds, limit_gb: limit, days }; } const r = await authFetch(url, { method, headers: {
                            'Content-Type': 'application/json' }, body: JSON.stringify(body) }); if (!r.ok) throw new Error();
                toast(id ? 'بروز شد' : 'ساخته شد');
                $m('mo-group').classList.remove('show');
                await loadGroups();
                await loadStats(); } catch (e) { toast('خطا در ذخیره گروه'); } }

        async function delGroup(gid) { if (!confirm('حذف گروه؟')) return; try { const r = await authFetch(
                    '/api/subscription-groups/' + gid, { method: 'DELETE' }); if (!r.ok) throw new Error(); toast(
                    'گروه حذف شد');
                await loadGroups(); } catch (e) { toast('خطا در حذف گروه'); } }

        function cpGroupSub(gid) { const url = 'https://' + location.host + '/sub/group/' + gid; navigator.clipboard.writeText(
                url).then(() => toast('آدرس اشتراق گروه کپی شد!')).catch(() => toast('خطا در کپی')); }

        // ===== SETTINGS =====
        async function loadSettings() { try { const r = await authFetch('/api/settings'); if (r.ok) { const d = await r.json();
                    $m('tg-token').value = d.telegram_token || '';
                    $m('tg-admin-id').value = d.telegram_admin_id || ''; } } catch (e) {} }

        async function saveSettings() { const tok = $m('tg-token').value.trim(); const adm = $m('tg-admin-id').value.trim();
            try { const r = await authFetch('/api/settings', { method: 'POST', headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ telegram_token: tok, telegram_admin_id: adm }) }); if (r.ok) toast(
                    'تنظیمات ذخیره شد');
                else toast('خطا در ذخیره', true); } catch (e) { toast('خطا در ذخیره', true); } }

        async function chgPw() { const cur = $m('cpw').value; const nw = $m('npw').value; if (!cur || !nw) { toast(
                    'همه فیلدها را پر کنید', true); return; } if (nw.length < 4) { toast('رمز جدید حداقل ۴ کاراکتر', true); return; }
            try { const r = await authFetch('/api/change-password', { method: 'POST', headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ current_password: cur, new_password: nw }) }); if (!r.ok) { const d = await r
                        .json().catch(() => ({})); throw new Error(d.detail || 'خطا'); } toast('رمز بروز شد');
                $m('cpw').value = '';
                $m('npw').value = ''; } catch (e) { toast(e.message, true); } }

        // ===== CLEAN IPS =====
        async function loadCleanIPs() {
            try {
                const r = await authFetch('/api/clean-ips');
                if (!r.ok) throw new Error();
                const d = await r.json();
                cleanIPs = d.clean_ips || {};
                renderCleanIPs();
                document.getElementById('cleanip-badge').textContent = Object.keys(cleanIPs).length;
            } catch (e) {
                console.error('Error loading clean IPs:', e);
            }
        }

        function renderCleanIPs() {
            const container = document.getElementById('cleanip-list');
            const keys = Object.keys(cleanIPs);
            if (!keys.length) {
                container.innerHTML = '<div class="empty-state">هیچ آی‌پی تمیزی اضافه نشده</div>';
                return;
            }
            let html = '';
            keys.forEach(ip => {
                const data = cleanIPs[ip];
                const status = data.active ? 'فعال' : 'غیرفعال';
                const statusClass = data.active ? 'tag-on' : 'tag-off';
                const assigned = data.assigned_to || '—';
                html += `
                    <div style="display:flex;align-items:center;justify-content:space-between;padding:10px 14px;background:var(--bg-secondary);border:1px solid var(--border-color);border-radius:var(--radius-sm);margin-bottom:6px;">
                        <div style="display:flex;align-items:center;gap:12px;">
                            <span style="font-weight:600;">${ip}</span>
                            <span class="tag ${statusClass}">${status}</span>
                            <span style="font-size:12px;color:var(--text-secondary);">نود: ${assigned}</span>
                        </div>
                        <div style="display:flex;gap:4px;">
                            ${!data.assigned_to ? `<button class="btn btn-sm btn-primary" onclick="assignCleanIP('${ip}')">اختصاص به نود</button>` : ''}
                            <button class="toggle ${data.active?'on':''}" onclick="toggleCleanIP('${ip}')"></button>
                            <button class="btn btn-sm btn-danger" onclick="deleteCleanIP('${ip}')">حذف</button>
                        </div>
                    </div>
                `;
            });
            container.innerHTML = html;
        }

        function openAddCleanIP() {
            document.getElementById('cleanip-input').value = '';
            document.getElementById('mo-cleanip-add').classList.add('show');
        }

        async function addCleanIP() {
            const ip = document.getElementById('cleanip-input').value.trim();
            if (!ip) { toast('لطفاً آی‌پی را وارد کنید', true); return; }
            try {
                const r = await authFetch('/api/clean-ips', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ ip })
                });
                const data = await r.json();
                if (data.success) {
                    toast('آی‌پی تمیز اضافه شد');
                    document.getElementById('mo-cleanip-add').classList.remove('show');
                    document.getElementById('cleanip-input').value = '';
                    loadCleanIPs();
                } else {
                    toast(data.detail || 'خطا در افزودن', true);
                }
            } catch (e) {
                toast('خطا در افزودن', true);
                console.error(e);
            }
        }

        async function toggleCleanIP(ip) {
            try {
                const r = await authFetch(`/api/clean-ips/${ip}/toggle`, {
                    method: 'PATCH'
                });
                if (!r.ok) throw new Error();
                loadCleanIPs();
                loadLinks();
                toast('وضعیت تغییر کرد');
            } catch (e) {
                toast('خطا در تغییر وضعیت', true);
            }
        }

        async function deleteCleanIP(ip) {
            if (!confirm(`آی‌پی ${ip} حذف شود؟`)) return;
            try {
                const r = await authFetch(`/api/clean-ips/${ip}`, {
                    method: 'DELETE'
                });
                if (!r.ok) throw new Error();
                toast('حذف شد');
                loadCleanIPs();
                loadLinks();
            } catch (e) {
                toast('خطا در حذف', true);
            }
        }

        async function assignCleanIP(ip) {
            const nodeNames = allLinks.map(l => l.label);
            if (!nodeNames.length) { toast('هیچ نودی وجود ندارد', true); return; }
            const nodeLabel = prompt(`انتخاب نود برای آی‌پی ${ip}:\n${nodeNames.join('\n')}`);
            if (!nodeLabel) return;
            if (!nodeNames.includes(nodeLabel)) {
                toast('نود نامعتبر است', true);
                return;
            }
            try {
                const r = await authFetch('/api/clean-ips/assign', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ ip, node_label: nodeLabel })
                });
                if (!r.ok) throw new Error();
                toast('آی‌پی به نود اختصاص داده شد');
                loadCleanIPs();
                loadLinks();
            } catch (e) {
                toast('خطا در اختصاص', true);
            }
        }

        // ===== CHARTS =====
        function initChart() {
            const ctx = $m('tc');
            if (!ctx || tChart) return;
            const isDark = document.documentElement.getAttribute('data-theme') === 'dark';
            const textColor = isDark ? 'rgba(255,255,255,0.3)' : 'rgba(0,0,0,0.25)';
            const gridColor = isDark ? 'rgba(255,255,255,0.04)' : 'rgba(0,0,0,0.04)';

            tChart = new Chart(ctx, {
                type: 'bar',
                data: {
                    labels: [],
                    datasets: [{ label: 'MB', data: [], backgroundColor: 'rgba(0,122,255,0.25)',
                        borderColor: '#007aff', borderWidth: 1.5, borderRadius: 4 }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: { legend: { display: false } },
                    scales: {
                        x: { grid: { display: false }, ticks: { color: textColor, font: { size: 10 } } },
                        y: { grid: { color: gridColor }, ticks: { color: textColor, font: { size: 10 },
                                callback: v => v + ' MB' }, beginAtZero: true }
                    }
                }
            });
            const ctx2 = $m('inbound-chart');
            if (ctx2 && !iChart) {
                iChart = new Chart(ctx2, {
                    type: 'doughnut',
                    data: {
                        labels: [],
                        datasets: [{ data: [], backgroundColor: ['#007aff', '#34c759', '#ff9500', '#ff3b30',
                                '#af52de', '#5ac8fa', '#ff2d92', '#30b0c0'
                            ], borderWidth: 0 }]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        plugins: {
                            legend: { display: true, position: 'right', labels: { color: isDark ?
                                        'rgba(255,255,255,0.5)' : 'rgba(0,0,0,0.5)', font: { size: 10 } } }
                        }
                    }
                });
            }
            updChartColors();
        }

        function updChartColors() {
            if (!tChart) return;
            const isDark = document.documentElement.getAttribute('data-theme') === 'dark';
            const textColor = isDark ? 'rgba(255,255,255,0.3)' : 'rgba(0,0,0,0.25)';
            const gridColor = isDark ? 'rgba(255,255,255,0.04)' : 'rgba(0,0,0,0.04)';
            tChart.options.scales.x.ticks.color = textColor;
            tChart.options.scales.y.ticks.color = textColor;
            tChart.options.scales.y.grid.color = gridColor;
            tChart.update();
            if (iChart) {
                iChart.options.plugins.legend.labels.color = isDark ? 'rgba(255,255,255,0.5)' : 'rgba(0,0,0,0.5)';
                iChart.update();
            }
        }

        function updChart() {
            if (!tChart || !sData.hourly_traffic) return;
            const entries = Object.entries(sData.hourly_traffic).sort((a, b) => a[0].localeCompare(b[0])).slice(-12);
            tChart.data.labels = entries.map(x => x[0]);
            tChart.data.datasets[0].data = entries.map(x => Math.round(x[1] / 1048576));
            tChart.update();
            if (iChart) {
                const sorted = [...allLinks].sort((a, b) => (b.used_bytes || 0) - (a.used_bytes || 0)).slice(0, 8);
                iChart.data.labels = sorted.map(x => x.label);
                iChart.data.datasets[0].data = sorted.map(x => Math.round((x.used_bytes || 0) / (1024 * 1024)));
                iChart.update();
            }
        }

        // ===== START =====
        checkAuth();

        const observer = new MutationObserver(() => {
            updChartColors();
        });
        observer.observe(document.documentElement, { attributes: true, attributeFilter: ['data-theme'] });
    </script>

</body>
</html>"""

# ===== LOGIN PAGE =====
LOGIN_HTML = """<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Wind — ورود</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Inter:opsz,wght@14..32,300;14..32,400;14..32,500;14..32,600;14..32,700&display=swap" rel="stylesheet">
    <style>
        *,
        *::before,
        *::after {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        :root {
            --bg: #f5f7fa;
            --card: rgba(255, 255, 255, 0.72);
            --text: #1d1d1f;
            --text-secondary: #6e6e73;
            --border: rgba(0, 0, 0, 0.06);
            --border-strong: rgba(0, 0, 0, 0.10);
            --accent: #007aff;
            --accent-hover: #0066d9;
            --shadow: 0 8px 40px rgba(0, 0, 0, 0.04), 0 2px 10px rgba(0, 0, 0, 0.02);
            --radius: 20px;
            --radius-sm: 12px;
            --font: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
            --transition: 0.3s cubic-bezier(0.25, 0.46, 0.45, 0.94);
        }

        [data-theme="dark"] {
            --bg: #0a0a0c;
            --card: rgba(28, 28, 30, 0.80);
            --text: #f5f5f7;
            --text-secondary: #8e8e93;
            --border: rgba(255, 255, 255, 0.06);
            --border-strong: rgba(255, 255, 255, 0.10);
            --shadow: 0 8px 40px rgba(0, 0, 0, 0.30);
        }

        body {
            font-family: var(--font);
            background: var(--bg);
            color: var(--text);
            min-height: 100vh;
            display: flex;
            align-items: center;
            justify-content: center;
            padding: 20px;
            transition: background var(--transition), color var(--transition);
            -webkit-font-smoothing: antialiased;
            -moz-osx-font-smoothing: grayscale;
        }

        .login-container {
            width: 100%;
            max-width: 400px;
            margin: 0 auto;
        }

        .login-card {
            background: var(--card);
            backdrop-filter: blur(24px) saturate(1.4);
            -webkit-backdrop-filter: blur(24px) saturate(1.4);
            border: 1px solid var(--border);
            border-radius: var(--radius);
            padding: 40px 32px 36px;
            box-shadow: var(--shadow);
            transition: background var(--transition), border var(--transition), box-shadow var(--transition);
        }

        .login-brand {
            text-align: center;
            margin-bottom: 32px;
        }

        .login-brand .wind-logo {
            display: flex;
            align-items: center;
            justify-content: center;
            margin: 0 auto 12px auto;
            width: 72px;
        }
        .login-brand .wind-logo svg {
            width: 100%;
            height: auto;
            display: block;
        }

        .login-brand h1 {
            font-size: 24px;
            font-weight: 600;
            letter-spacing: -0.3px;
        }
        .login-brand p {
            font-size: 15px;
            color: var(--text-secondary);
            font-weight: 400;
            margin-top: 4px;
        }

        .login-form .input-group {
            margin-bottom: 18px;
        }
        .login-form .input-group label {
            display: block;
            font-size: 13px;
            font-weight: 500;
            color: var(--text-secondary);
            margin-bottom: 4px;
        }
        .login-form .input-group input {
            width: 100%;
            padding: 12px 16px;
            border: 1px solid var(--border);
            border-radius: var(--radius-sm);
            font-size: 16px;
            font-family: inherit;
            background: rgba(255, 255, 255, 0.40);
            backdrop-filter: blur(4px);
            -webkit-backdrop-filter: blur(4px);
            color: var(--text);
            transition: all var(--transition);
            outline: none;
        }
        [data-theme="dark"] .login-form .input-group input {
            background: rgba(44, 44, 46, 0.60);
        }
        .login-form .input-group input:focus {
            border-color: var(--accent);
            box-shadow: 0 0 0 3px rgba(0, 122, 255, 0.15);
        }
        .login-form .input-group input::placeholder {
            color: var(--text-secondary);
            opacity: 0.6;
        }

        .login-form .btn-login {
            width: 100%;
            padding: 14px;
            background: var(--accent);
            color: #fff;
            border: none;
            border-radius: var(--radius-sm);
            font-size: 17px;
            font-weight: 600;
            font-family: inherit;
            cursor: pointer;
            transition: all var(--transition);
            margin-top: 6px;
            letter-spacing: 0.2px;
        }
        .login-form .btn-login:hover {
            background: var(--accent-hover);
            transform: scale(1.01);
            box-shadow: 0 4px 16px rgba(0, 122, 255, 0.25);
        }
        .login-form .btn-login:active {
            transform: scale(0.98);
        }

        .login-error {
            color: #ff3b30;
            font-size: 14px;
            text-align: center;
            margin-top: 14px;
            display: none;
            font-weight: 500;
        }

        .login-footer {
            text-align: center;
            margin-top: 20px;
            font-size: 13px;
            color: var(--text-secondary);
        }
        .login-footer .theme-toggle {
            background: none;
            border: none;
            color: var(--text-secondary);
            font-size: 20px;
            cursor: pointer;
            transition: color var(--transition);
            padding: 4px;
        }
        .login-footer .theme-toggle:hover {
            color: var(--text);
        }

        @media (max-width: 480px) {
            .login-card {
                padding: 28px 20px 28px;
            }
            .login-brand h1 {
                font-size: 20px;
            }
            .login-form .input-group input {
                font-size: 15px;
                padding: 10px 14px;
            }
            .login-form .btn-login {
                font-size: 15px;
                padding: 12px;
            }
            .login-brand .wind-logo {
                width: 56px;
            }
        }

        @media (prefers-reduced-motion: reduce) {
            *,
            *::before,
            *::after {
                transition-duration: 0.01ms !important;
                animation-duration: 0.01ms !important;
            }
        }
    </style>
</head>
<body>

    <div class="login-container">
        <div class="login-card">
            <div class="login-brand">
                <div class="wind-logo">
                    <svg viewBox="0 0 512 460" xmlns="http://www.w3.org/2000/svg">
                        <defs>
                            <linearGradient id="wind-grad-login" x1="0" y1="0" x2="1" y2="1">
                                <stop offset="0%" stop-color="#f8fbff"/>
                                <stop offset="45%" stop-color="#d9e1ec"/>
                                <stop offset="100%" stop-color="#9aa9bd"/>
                            </linearGradient>
                            <filter id="shadow-login">
                                <feDropShadow dx="0" dy="7" stdDeviation="9" flood-opacity=".18"/>
                            </filter>
                        </defs>
                        <g fill="url(#wind-grad-login)" filter="url(#shadow-login)">
                            <path d="M65 160 C125 91 197 72 282 76 C345 79 400 68 438 34 C438 91 412 135 360 151 C310 166 257 153 205 150 C145 146 101 158 65 191 C69 179 71 169 65 160Z"/>
                            <path d="M61 235 C112 181 169 170 229 176 C280 181 324 197 370 188 C399 182 419 166 439 143 C427 199 388 229 341 236 C291 244 247 224 200 219 C144 213 99 228 61 265 C65 253 67 243 61 235Z"/>
                            <path d="M58 310 C108 257 159 247 216 253 C265 258 303 276 342 272 C376 269 404 251 429 225 C413 280 376 308 329 316 C281 324 239 304 195 299 C140 293 96 307 58 343 C62 331 64 320 58 310Z"/>
                        </g>
                        <text x="256" y="415" text-anchor="middle" font-family="-apple-system, BlinkMacSystemFont, 'SF Pro Display', Inter, Arial, sans-serif" font-size="55" font-weight="400" letter-spacing="-2" fill="#15171b">Wind</text>
                    </svg>
                </div>
                <h1>Wind</h1>
                <p>برای ورود رمز عبور را وارد کنید</p>
            </div>

            <form class="login-form" onsubmit="event.preventDefault(); doLogin();">
                <div class="input-group">
                    <label for="login-pw">رمز عبور</label>
                    <input id="login-pw" type="password" placeholder="••••••••" autocomplete="current-password" autofocus>
                </div>
                <button type="submit" class="btn-login">ورود</button>
                <div class="login-error" id="login-err">رمز عبور اشتباه است</div>
            </form>

            <div class="login-footer">
                <button class="theme-toggle" onclick="toggleTheme()" id="themeBtn" aria-label="تغییر تم">🌙</button>
            </div>
        </div>
    </div>

    <script>
        // ===== Theme =====
        function setTheme(t) {
            document.documentElement.setAttribute('data-theme', t);
            localStorage.setItem('login-theme', t);
            const btn = document.getElementById('themeBtn');
            if (btn) btn.textContent = t === 'dark' ? '☀️' : '🌙';
        }

        function toggleTheme() {
            const current = document.documentElement.getAttribute('data-theme') || 'light';
            setTheme(current === 'dark' ? 'light' : 'dark');
        }

        const savedTheme = localStorage.getItem('login-theme') || 'light';
        setTheme(savedTheme);

        // ===== Login =====
        async function doLogin() {
            const pw = document.getElementById('login-pw').value;
            const err = document.getElementById('login-err');
            err.style.display = 'none';

            if (!pw) {
                err.textContent = 'لطفاً رمز عبور را وارد کنید';
                err.style.display = 'block';
                return;
            }

            try {
                const r = await fetch('/api/login', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ password: pw })
                });

                if (r.ok) {
                    const data = await r.json();
                    if (data.token) {
                        localStorage.setItem('token', data.token);
                    }
                    window.location.href = '/dashboard';
                } else {
                    err.textContent = 'رمز عبور اشتباه است';
                    err.style.display = 'block';
                    document.getElementById('login-pw').value = '';
                    document.getElementById('login-pw').focus();
                }
            } catch (e) {
                err.textContent = 'خطا در ارتباط با سرور';
                err.style.display = 'block';
            }
        }

        document.getElementById('login-pw').addEventListener('keydown', (e) => {
            if (e.key === 'Enter') {
                e.preventDefault();
                doLogin();
            }
        });

        document.getElementById('login-pw').focus();
    </script>

</body>
</html>"""

# ===== ROUTES =====
@app.get("/login", response_class=HTMLResponse)
async def login_page():
    return HTMLResponse(content=LOGIN_HTML)

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard_page():
    return HTMLResponse(content=PANEL_HTML)

@app.get("/panel", response_class=HTMLResponse)
async def panel_page():
    return HTMLResponse(content=PANEL_HTML)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=CONFIG["port"])