#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
╔══════════════════════════════════════════════════════════════════════════╗
║  rexco.py  —  Telegram Ads Bot  (single-file build)                       ║
║                                                                           ║
║  Everything the original texco-ad-bot repo spread across 13 files,        ║
║  merged into one file and with every bug fixed:                           ║
║                                                                           ║
║   1. no more `from Nexa.plugins import test` (module never existed)       ║
║   2. logging no longer needs python-telegram-bot (plain httpx)            ║
║   3. Telethon clients are re-used instead of being disconnected every     ║
║      round, so broadcasting works past the first cycle                    ║
║   4. "My Accounts" builds the real session path (<uid>_<phone>.session)   ║
║   5. the Delete button emits `delete_<phone>` and has a handler           ║
║   6. force-join is OFF unless MUST_JOIN_CHANNEL/GROUP are set             ║
║   7. one event loop is installed before bot.run() (no asyncio.run())      ║
║   8. MAX_ACCOUNTS is honoured (was hard-coded to 5)                       ║
║   9. logging never dies silently on a missing LOGGER_BOT_TOKEN            ║
║                                                                           ║
║  Run:   python rexco.py                                                   ║
║  Needs: pip install -r requirements.txt                                   ║
╚══════════════════════════════════════════════════════════════════════════╝
"""

import asyncio
import html
import logging
import os
import random
import re
import sys
import traceback
from datetime import datetime
from typing import Dict, List, Optional

import httpx
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import DuplicateKeyError
from pyrogram import Client, filters
from pyrogram.enums import ChatType, ParseMode
from pyrogram.errors import (
    ChatAdminRequired,
    ChatWriteForbidden,
    MessageNotModified,
    RPCError,
    UserNotParticipant,
)
from pyrogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)
from telethon import TelegramClient, functions
from telethon.errors import (
    AuthKeyUnregisteredError,
    ChatWriteForbiddenError,
    FloodWaitError,
    PasswordHashInvalidError,
    PeerFloodError,
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
    SessionPasswordNeededError,
)

# ══════════════════════════════════════════════════════════════════════════
# 1. CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════

load_dotenv()


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


API_ID = _env_int("API_ID",32853799)
API_HASH = os.getenv("API_HASH", "7618973ca6fa27183f6df27dfba88f26")
BOT_TOKEN = os.getenv("BOT_TOKEN", "8960949500:AAHOBqGmP_O8iDB8cZILBerTiWsVfPFcQqc")
LOGGER_BOT_TOKEN = os.getenv("LOGGER_BOT_TOKEN", "8983071296:AAFYJ7_w1OKmC-yE6zxtEFOqyW0wyZDem8U")

if not API_ID or not API_HASH or not BOT_TOKEN:
    sys.exit("❌  API_ID, API_HASH and BOT_TOKEN must be set (see .env).")

# Localhost MongoDB by default — no Atlas allow-listing needed.
MONGO_URI = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017")
DB_NAME = os.getenv("DB_NAME", "nexa_bot")

MAX_ACCOUNTS = _env_int("MAX_ACCOUNTS", 5)
DEFAULT_DELAY = _env_int("DEFAULT_DELAY", 300)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SESSION_DIR = os.path.join(BASE_DIR, "sessions")
os.makedirs(SESSION_DIR, exist_ok=True)

# Force-join: disabled unless BOTH are set.
MUST_JOIN_CHANNEL = os.getenv("MUST_JOIN_CHANNEL", "").strip().lstrip("@")
MUST_JOIN_GROUP = os.getenv("MUST_JOIN_GROUP", "").strip().lstrip("@")

START_IMAGE = os.getenv("START_IMAGE", "https://files.catbox.moe/43767f.jpg")

START_TEXT = (
    "╰_╯ Welcome to **Free Ads Bot** — Telegram Automation\n"
    "• Premium Ad Broadcasting\n"
    "• Smart Delays\n"
    "• Multi-Account Support\n"
)

DASHBOARD_TEXT = (
    "╰_╯ **Ads DASHBOARD**\n"
    "• Hosted Accounts: `{account_count}/{max_accounts}`\n"
    "• Ad Message: {ad_status}\n"
    "• Cycle Interval: {delay}s\n"
    "• Advertising Status: {running_status}\n\n"
    "╰_╯ Choose an action below to continue"
)

CUSTOM_LAST_NAME = os.getenv("PROFILE_LAST_NAME", "★ By @NexaMeetup")
CUSTOM_BIO = os.getenv("BIO_TEXT", "⚡ Powered By @NexaCoders")

DEBUG_UPDATES = os.getenv("DEBUG_UPDATES", "0") == "1"

# ══════════════════════════════════════════════════════════════════════════
# 2. LOGGING
# ══════════════════════════════════════════════════════════════════════════

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.FileHandler(os.path.join("logs", "broadcast.log")),
              logging.StreamHandler()],
)
logger = logging.getLogger("rexco")

# ══════════════════════════════════════════════════════════════════════════
# 3. DATABASE
# ══════════════════════════════════════════════════════════════════════════

mongo_client = AsyncIOMotorClient(MONGO_URI, serverSelectionTimeoutMS=8000)
db = mongo_client[DB_NAME]
users_db = db["users"]


async def check_connection() -> bool:
    try:
        await mongo_client.admin.command("ping")
        print("✅ MongoDB Connected Successfully")
        return True
    except Exception as e:
        print(f"[DB ERROR] Connection failed: {type(e).__name__}: {e}")
        print("           → is mongod running?  (MONGO_URI=%s)" % MONGO_URI)
        return False


async def ensure_indexes() -> None:
    try:
        if "user_id_1" in await users_db.index_information():
            await users_db.drop_index("user_id_1")
            print("✅ Removed stale 'user_id_1' index")
    except Exception as e:
        print(f"[DB WARNING] index cleanup skipped: {e}")


# ---- user helpers ---------------------------------------------------------

def valid_user_id(uid) -> bool:
    return isinstance(uid, int) and uid > 0


async def get_user(uid: int) -> Optional[dict]:
    if not valid_user_id(uid):
        return None
    try:
        return await users_db.find_one({"_id": uid})
    except Exception as e:
        logger.error(f"get_user: {e}")
        return None


async def get_or_create_user(uid: int) -> Optional[dict]:
    if not valid_user_id(uid):
        return None
    try:
        await users_db.update_one(
            {"_id": uid},
            {"$setOnInsert": {
                "accounts": [],
                "max_accounts": MAX_ACCOUNTS,   # honours .env (was hard-coded 5)
                "ad_message": None,
                "delay": DEFAULT_DELAY,
                "advertising": False,
                "messages_sent": 0,
                "messages_failed": 0,
                "broadcast_completed": 0,
                "created_at": datetime.utcnow(),
            }},
            upsert=True,
        )
    except DuplicateKeyError:
        pass
    except Exception as e:
        logger.error(f"get_or_create_user: {e}")
        return None
    return await get_user(uid)


async def get_accounts(uid: int) -> list:
    u = await get_user(uid)
    return u.get("accounts", []) if u else []


async def get_ad_message(uid: int) -> Optional[str]:
    u = await get_user(uid)
    return u.get("ad_message") if u else None


async def set_ad_message(uid: int, text: str) -> None:
    await users_db.update_one({"_id": uid}, {"$set": {"ad_message": text}}, upsert=True)


async def set_delay(uid: int, delay: int) -> None:
    await users_db.update_one({"_id": uid}, {"$set": {"delay": int(delay)}}, upsert=True)


async def set_broadcast_status(uid: int, status: bool) -> None:
    await users_db.update_one({"_id": uid}, {"$set": {"advertising": bool(status)}})


async def add_account(uid: int, phone: str) -> None:
    await users_db.update_one({"_id": uid}, {"$addToSet": {"accounts": phone}}, upsert=True)


async def remove_account(uid: int, phone: str) -> None:
    await users_db.update_one(
        {"_id": uid},
        {"$pull": {"accounts": phone},
         "$unset": {f"account_status.{phone}": ""}},
    )


# ══════════════════════════════════════════════════════════════════════════
# 4. CLIENTS  (Pyrogram bot + httpx logger)
# ══════════════════════════════════════════════════════════════════════════

bot = Client(name="rexco", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)


async def send_log(user_id: int, text: str) -> None:
    """Send a log line to the user through the logger bot (Bot API over httpx).

    The original used python-telegram-bot; httpx is already a dependency and
    removes a whole library for what is one HTTP call.
    """
    if not LOGGER_BOT_TOKEN:
        return
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            await client.post(
                f"https://api.telegram.org/bot{LOGGER_BOT_TOKEN}/sendMessage",
                json={"chat_id": user_id, "text": text, "parse_mode": "HTML",
                      "disable_web_page_preview": True},
            )
    except Exception as e:
        logger.warning(f"send_log failed for {user_id}: {e}")


# ══════════════════════════════════════════════════════════════════════════
# 5. SESSIONS
# ══════════════════════════════════════════════════════════════════════════

def normalize_phone(phone: str) -> str:
    return str(phone).replace("+", "").replace(".session", "").strip()


def session_base(uid: int, phone: str) -> str:
    """Real on-disk path: <SESSION_DIR>/<user_id>_<phone_without_+>"""
    return os.path.join(SESSION_DIR, f"{uid}_{normalize_phone(phone)}")


def session_file(uid: int, phone: str) -> str:
    return session_base(uid, phone) + ".session"


def list_user_sessions(uid: int) -> List[str]:
    prefix = f"{uid}_"
    try:
        return [f[:-len(".session")] for f in os.listdir(SESSION_DIR)
                if f.startswith(prefix) and f.endswith(".session")]
    except Exception:
        return []


def phone_of(session_name: str) -> str:
    phone = session_name.split("_")[-1]
    return phone if phone.startswith("+") else "+" + phone


# ══════════════════════════════════════════════════════════════════════════
# 6. BROADCAST ENGINE
# ══════════════════════════════════════════════════════════════════════════

running_tasks: Dict[int, asyncio.Task] = {}
running_delays: Dict[int, int] = {}
clients: Dict[str, TelegramClient] = {}


def set_user_delay(uid: int, delay: int) -> None:
    running_delays[uid] = int(delay)


async def update_profile_for_session(uid: int, session_name: str) -> None:
    path = session_file(uid, phone_of(session_name).lstrip("+"))
    if not os.path.exists(path):
        return
    client = TelegramClient(path.replace(".session", ""), API_ID, API_HASH)
    phone = phone_of(session_name)
    try:
        await client.connect()
        if not await client.is_user_authorized():
            return
        await client(functions.account.UpdateProfileRequest(
            last_name=CUSTOM_LAST_NAME, about=CUSTOM_BIO))
        await send_log(uid, f"📝 <b>Profile updated for:</b> {phone}")
    except Exception as e:
        await send_log(uid, f"⚠️ <b>Profile update failed for {phone}</b>: {e}")
    finally:
        await client.disconnect()


async def start_broadcast(uid: int) -> bool:
    if uid in running_tasks and not running_tasks[uid].done():
        return True

    user = await get_user(uid)
    if not user or not user.get("advertising") or not user.get("ad_message"):
        return False

    sessions = list_user_sessions(uid)
    if not sessions:
        return False

    for name in sessions:
        await update_profile_for_session(uid, name)

    running_delays[uid] = int(user.get("delay", DEFAULT_DELAY))
    running_tasks[uid] = asyncio.create_task(broadcast_loop(uid))
    await send_log(uid, "🚀 <b>Broadcast started!</b> Logs will appear below:")
    return True


async def stop_broadcast(uid: int) -> None:
    await set_broadcast_status(uid, False)

    task = running_tasks.pop(uid, None)
    if task and not task.done():
        task.cancel()

    running_delays.pop(uid, None)

    for name in list_user_sessions(uid):
        key = f"{uid}_{name}"
        client = clients.get(key)
        if client:
            try:
                await client.disconnect()
            except Exception:
                pass
            clients.pop(key, None)

    await send_log(uid, "🛑 <b>Broadcast stopped</b>")


async def broadcast_loop(uid: int) -> None:
    try:
        while True:
            user = await get_user(uid)
            if not user or not user.get("advertising"):
                break

            message = (user.get("ad_message") or "").strip()
            if not message:
                await asyncio.sleep(10)
                continue

            sessions = list_user_sessions(uid)
            if not sessions:
                await asyncio.sleep(10)
                continue

            await asyncio.gather(*[send_from_session(uid, s, message) for s in sessions])

            await users_db.update_one({"_id": uid}, {"$inc": {"broadcast_completed": 1}})
            await asyncio.sleep(running_delays.get(uid, int(user.get("delay", DEFAULT_DELAY))))

    except asyncio.CancelledError:
        pass
    except Exception:
        logger.error(traceback.format_exc())
    finally:
        running_tasks.pop(uid, None)
        running_delays.pop(uid, None)
        await send_log(uid, "🛑 <b>Broadcast stopped</b>")


async def send_from_session(uid: int, session_name: str, message: str) -> None:
    path = os.path.join(SESSION_DIR, f"{session_name}.session")
    if not os.path.exists(path):
        return

    key = f"{uid}_{session_name}"
    client = clients.get(key)

    if client is None:
        client = TelegramClient(path.replace(".session", ""), API_ID, API_HASH)
        clients[key] = client

    # FIX: a disconnected Telethon client raises
    # "Cannot send requests while disconnected" on reuse. The original
    # disconnected in `finally` but kept the client cached, so only the very
    # first round ever sent anything.
    if not client.is_connected():
        await client.connect()

    success = failed = 0
    phone = phone_of(session_name)

    try:
        if not await client.is_user_authorized():
            await client.disconnect()
            clients.pop(key, None)
            return

        chats = [d.entity for d in await client.get_dialogs(limit=None)
                 if d.is_group or d.is_channel]

        for chat in chats:
            try:
                await client.send_message(chat, message)
                success += 1
                await users_db.update_one({"_id": uid}, {"$inc": {"messages_sent": 1}})
                await send_log(uid, f"✅ <b>Sent to:</b> "
                                    f"{getattr(chat, 'title', chat)} using {phone}")
                await asyncio.sleep(random.randint(3, 5))

            except FloodWaitError as e:
                failed += 1
                await users_db.update_one({"_id": uid}, {"$inc": {"messages_failed": 1}})
                await send_log(uid, f"⏳ <b>FloodWait {e.seconds}s</b> using {phone}")
                await asyncio.sleep(e.seconds)
            except (PeerFloodError, ChatWriteForbiddenError):
                failed += 1
                await users_db.update_one({"_id": uid}, {"$inc": {"messages_failed": 1}})
                await send_log(uid, f"❌ <b>Cannot send with {phone}, skipping chat.</b>")
            except Exception:
                failed += 1
                await users_db.update_one({"_id": uid}, {"$inc": {"messages_failed": 1}})

        await send_log(uid, f"🕸 <u>Round complete</u>\n✅ {success}\n❌ {failed}\n👤 {phone}")

    except AuthKeyUnregisteredError:
        await send_log(uid, f"❌ <b>Session revoked for {phone}</b> — re-add the account.")
        await client.disconnect()
        clients.pop(key, None)
    except Exception:
        logger.error(traceback.format_exc())


# ══════════════════════════════════════════════════════════════════════════
# 7. KEYBOARDS
# ══════════════════════════════════════════════════════════════════════════

def kb_start() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Dashboard", callback_data="dashboard")],
        [InlineKeyboardButton("Updates", url="https://t.me/NexaCoders"),
         InlineKeyboardButton("Support", url="https://t.me/NexaCoders")],
    ])


def kb_dashboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Add Accounts", callback_data="host_account"),
         InlineKeyboardButton("My Accounts", callback_data="view_accounts")],
        [InlineKeyboardButton("Set Ad Message", callback_data="set_msg"),
         InlineKeyboardButton("Set Time Interval", callback_data="set_delay")],
        [InlineKeyboardButton("Start Ads ▶️", callback_data="start_broadcast"),
         InlineKeyboardButton("Stop Ads ⏸️", callback_data="stop_broadcast")],
        [InlineKeyboardButton("Delete Accounts", callback_data="delete_accounts"),
         InlineKeyboardButton("Analytics", callback_data="analytics")],
        [InlineKeyboardButton("Auto Reply", callback_data="auto_reply")],
    ])


def kb_back(target: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=target)]])


def kb_otp() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(str(n), callback_data=f"otp_{n}") for n in (1, 2, 3)],
        [InlineKeyboardButton(str(n), callback_data=f"otp_{n}") for n in (4, 5, 6)],
        [InlineKeyboardButton(str(n), callback_data=f"otp_{n}") for n in (7, 8, 9)],
        [InlineKeyboardButton("⌫", callback_data="otp_del"),
         InlineKeyboardButton("0", callback_data="otp_0"),
         InlineKeyboardButton("❌", callback_data="otp_cancel")],
    ])


# ══════════════════════════════════════════════════════════════════════════
# 8. SHARED UI HELPERS
# ══════════════════════════════════════════════════════════════════════════

async def safe_edit(query: CallbackQuery, text: str,
                    markup: InlineKeyboardMarkup = None, html_mode: bool = True) -> None:
    try:
        if query.message.photo:
            await query.message.edit_caption(text, reply_markup=markup,
                                             parse_mode=ParseMode.HTML if html_mode else None)
        else:
            await query.message.edit_text(text, reply_markup=markup,
                                          parse_mode=ParseMode.HTML if html_mode else None)
    except (MessageNotModified, RPCError):
        pass
    except Exception as e:
        logger.warning(f"safe_edit: {e}")


async def send_start_menu(message: Message) -> None:
    try:
        await message.reply_photo(photo=START_IMAGE, caption=START_TEXT,
                                  reply_markup=kb_start(), parse_mode=ParseMode.MARKDOWN)
    except Exception:
        await message.reply_text(START_TEXT, reply_markup=kb_start(),
                                 parse_mode=ParseMode.MARKDOWN)


# ══════════════════════════════════════════════════════════════════════════
# 9. HANDLERS  —  defined top-to-bottom; Pyrogram runs the first match in a
#    group, so the more specific handlers are registered first.
# ══════════════════════════════════════════════════════════════════════════

# ---- force-join (group -1, disabled unless configured) --------------------

@bot.on_message(filters.incoming & filters.private, group=-1)
async def must_join_handler(client: Client, msg: Message):
    if not MUST_JOIN_CHANNEL or not MUST_JOIN_GROUP or not msg.from_user:
        return
    try:
        await client.get_chat_member(MUST_JOIN_CHANNEL, msg.from_user.id)
        await client.get_chat_member(MUST_JOIN_GROUP, msg.from_user.id)
    except UserNotParticipant:
        await msg.reply_text(
            "╰_╯ Please join our channel and group first, then try again.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Join Channel", url=f"https://t.me/{MUST_JOIN_CHANNEL}")],
                [InlineKeyboardButton("Join Group", url=f"https://t.me/{MUST_JOIN_GROUP}")],
                [InlineKeyboardButton("Try Again", callback_data="mustjoin_retry")],
            ]))
        raise msg.stop_propagation()
    except ChatAdminRequired:
        print(f"❌ Make the bot admin in @{MUST_JOIN_CHANNEL} and @{MUST_JOIN_GROUP}")
    except Exception:
        pass  # bad username / deleted chat — never block users


@bot.on_callback_query(filters.regex("^mustjoin_retry$"))
async def mustjoin_retry(client: Client, query: CallbackQuery):
    try:
        await client.get_chat_member(MUST_JOIN_CHANNEL, query.from_user.id)
        await client.get_chat_member(MUST_JOIN_GROUP, query.from_user.id)
        await query.answer("✅ Access granted!", show_alert=True)
        await send_start_menu(query.message)
    except UserNotParticipant:
        await query.answer("❌ Join both channel and group first!", show_alert=True)
    except Exception:
        pass


# ---- /start ---------------------------------------------------------------

@bot.on_message(filters.command("start") & filters.private)
async def start_cmd(client: Client, message: Message):
    await get_or_create_user(message.from_user.id)
    await send_start_menu(message)


@bot.on_callback_query(filters.regex("^back$"))
async def back_callback(client: Client, query: CallbackQuery):
    await query.answer()
    await send_start_menu(query.message)


# ---- dashboard ------------------------------------------------------------

@bot.on_callback_query(filters.regex("^dashboard$"))
async def dashboard_callback(client: Client, query: CallbackQuery):
    await query.answer()
    uid = query.from_user.id

    # Also drop any half-finished "add account" flow for this user.
    await cleanup_host_state(uid)

    user = await get_or_create_user(uid)
    if not user:
        return await query.answer("❌ Could not fetch user data.", show_alert=True)

    accounts = await get_accounts(uid)
    text = DASHBOARD_TEXT.format(
        account_count=len(accounts),
        max_accounts=user.get("max_accounts", MAX_ACCOUNTS),
        ad_status="Set ✅" if user.get("ad_message") else "Not Set ⭕",
        delay=user.get("delay", DEFAULT_DELAY),
        running_status="Running 🚀" if user.get("advertising") else "Paused ⏸️",
    )
    await safe_edit(query, text, kb_dashboard())


# ---- set ad message -------------------------------------------------------

waiting_for_ad: set = set()


@bot.on_callback_query(filters.regex("^set_msg$"))
async def set_msg_ui(client: Client, query: CallbackQuery):
    await query.answer()
    uid = query.from_user.id
    current = await get_ad_message(uid)
    preview = f"`{current}`" if current else "_No message set yet._"
    waiting_for_ad.add(uid)
    await safe_edit(query,
                    "╰_╯ <b>SET YOUR AD MESSAGE</b>\n\n"
                    f"<b>Current:</b> {html.escape(str(current)) if current else '—'}\n\n"
                    "Send your ad message now (max 4096 chars).",
                    kb_back("dashboard"))


@bot.on_message(filters.private & filters.text & ~filters.command(["start"]), group=1)
async def receive_ad_message(client: Client, message: Message):
    uid = message.from_user.id
    if uid not in waiting_for_ad:
        return

    ad_text = (message.text or "").strip()
    if not ad_text:
        return await message.reply("⚠ Ad message cannot be empty.")
    if len(ad_text) > 4096:
        return await message.reply("⚠ Message too long — keep it under 4096 characters.")

    await set_ad_message(uid, ad_text)
    waiting_for_ad.discard(uid)
    await message.reply(f"╰_╯ <b>AD MESSAGE SET!</b> ✅\n\n{html.escape(ad_text)}",
                        reply_markup=kb_back("dashboard"))
    await send_log(uid, f"📝 <b>Ad message updated</b>\n\n<code>{html.escape(ad_text[:400])}</code>")


# ---- set delay ------------------------------------------------------------

waiting_delay: set = set()


def delay_mode(delay: int) -> str:
    if delay <= 300:
        return "Aggressive 🔴"
    if delay <= 600:
        return "Safe & Balanced 🟡"
    return "Conservative 🟢"


@bot.on_callback_query(filters.regex("^set_delay$"))
async def set_delay_ui(client: Client, query: CallbackQuery):
    await query.answer()
    uid = query.from_user.id
    user = await get_user(uid)
    current = user.get("delay", DEFAULT_DELAY) if user else DEFAULT_DELAY
    waiting_delay.add(uid)
    await safe_edit(query,
                    "╰_╯ <b>SET BROADCAST CYCLE INTERVAL</b>\n\n"
                    f"Current: <b>{current}s</b>\n\n"
                    "• 300s – Aggressive 🔴\n• 600s – Safe & Balanced 🟡\n"
                    "• 1200s – Conservative 🟢\n\n"
                    "Or send a number in seconds (minimum 60).",
                    InlineKeyboardMarkup([
                        [InlineKeyboardButton("5min 🔴", callback_data="delay_300"),
                         InlineKeyboardButton("10min 🟡", callback_data="delay_600"),
                         InlineKeyboardButton("20min 🟢", callback_data="delay_1200")],
                        [InlineKeyboardButton("🔙 Back", callback_data="dashboard")],
                    ]))


@bot.on_callback_query(filters.regex(r"^delay_(\d+)$"))
async def preset_delay(client: Client, query: CallbackQuery):
    await query.answer()
    uid = query.from_user.id
    delay = int(query.data.split("_")[1])
    await set_delay(uid, delay)
    set_user_delay(uid, delay)
    waiting_delay.discard(uid)
    await safe_edit(query,
                    "╰_╯ <b>INTERVAL UPDATED!</b>\n\n"
                    f"New interval: <b>{delay}s</b>\nMode: <b>{delay_mode(delay)}</b>",
                    kb_back("set_delay"))


@bot.on_message(filters.private & filters.text & ~filters.command(["start"]), group=2)
async def custom_delay(client: Client, message: Message):
    uid = message.from_user.id
    if uid not in waiting_delay:
        return
    if not (message.text or "").strip().isdigit():
        return await message.reply("⚠ Send a valid number of seconds.")

    delay = int(message.text.strip())
    if delay < 60:
        return await message.reply("⚠ Minimum 60 seconds allowed.")

    await set_delay(uid, delay)
    set_user_delay(uid, delay)
    waiting_delay.discard(uid)
    await message.reply(f"╰_╯ <b>INTERVAL UPDATED!</b>\n\nNew interval: <b>{delay}s</b>\n"
                        f"Mode: <b>{delay_mode(delay)}</b>",
                        reply_markup=kb_back("set_delay"))


# ---- host (add) accounts --------------------------------------------------

user_states: Dict[int, dict] = {}
user_locks: Dict[int, asyncio.Lock] = {}


def get_lock(uid: int) -> asyncio.Lock:
    if uid not in user_locks:
        user_locks[uid] = asyncio.Lock()
    return user_locks[uid]


async def cleanup_host_state(uid: int) -> None:
    """Cancel a pending login flow for this user (if any)."""
    state = user_states.get(uid)
    if not state:
        return
    if state.get("timeout"):
        state["timeout"].cancel()
    if state.get("client") and state.get("step") != "SUCCESS":
        try:
            await state["client"].disconnect()
        except Exception:
            pass
    user_states.pop(uid, None)
    user_locks.pop(uid, None)


def otp_text(phone: str, otp: str = "", extra: str = "") -> str:
    text = (f"╰_╯ OTP sent to {phone}! ✅\n\n"
            "Enter the OTP using the keypad below\n\n"
            f"Current : {'*' * len(otp) if otp else '_____'}\n\n"
            "Valid for: 5 minutes")
    return text + (f"\n\n{extra}" if extra else "")


async def otp_timeout(uid: int, seconds: int = 300) -> None:
    try:
        await asyncio.sleep(seconds)
    except asyncio.CancelledError:
        return
    state = user_states.get(uid)
    if state and state.get("step") in ("OTP", "2FA"):
        try:
            await bot.send_message(state["chat_id"], "⏰ OTP expired — process cancelled.")
        except Exception:
            pass
        await cleanup_host_state(uid)


@bot.on_callback_query(filters.regex("^host_account$"))
async def start_host(client: Client, query: CallbackQuery):
    uid = query.from_user.id
    await query.answer()
    await cleanup_host_state(uid)

    # FIX: the MAX_ACCOUNTS limit was never checked in the original.
    user = await get_user(uid)
    limit = int((user or {}).get("max_accounts", MAX_ACCOUNTS))
    if len((user or {}).get("accounts", [])) >= limit:
        return await query.answer(f"❌ Limit reached ({limit}). Delete one first.",
                                  show_alert=True)

    await safe_edit(query,
                    "<b>╰_╯ HOST NEW ACCOUNT</b>\n\n"
                    "Enter your phone number with country code:\n\n"
                    "Example: <code>+1234567890</code>",
                    kb_back("dashboard"))
    user_states[uid] = {"step": "PHONE", "phone": None, "client": None, "otp": "",
                        "timeout": None, "process_msg_id": None,
                        "chat_id": query.message.chat.id}


@bot.on_message(filters.private & filters.text, group=0)
async def host_text_flow(client: Client, message: Message):
    uid = message.from_user.id
    if uid not in user_states:
        return

    async with get_lock(uid):
        state = user_states.get(uid)
        if not state:
            return
        try:
            await message.delete()
        except Exception:
            pass

        # ---- step 1: phone number ----
        if state["step"] == "PHONE":
            phone = (message.text or "").strip()
            if not phone.startswith("+"):
                return await bot.send_message(
                    state["chat_id"],
                    "❌ <b>Invalid phone number!</b>\n\nUse international format, e.g. "
                    "<code>+1234567890</code>",
                    reply_markup=kb_back("dashboard"))

            wait = await bot.send_message(state["chat_id"],
                                          f"⏳ Sending OTP to <code>{phone}</code> …")
            state["process_msg_id"] = wait.id

            try:
                path = session_base(uid, phone)
                client_ = TelegramClient(path, API_ID, API_HASH)
                await client_.connect()
                sent = await client_.send_code_request(phone)
                state.update({"phone": phone, "client": client_, "step": "OTP",
                              "otp": "", "phone_code_hash": sent.phone_code_hash})
                state["timeout"] = asyncio.create_task(otp_timeout(uid))
                await bot.edit_message_text(state["chat_id"], wait.id,
                                            otp_text(phone), reply_markup=kb_otp())
                await send_log(uid, f"🔐 <b>OTP requested for</b> <code>{phone}</code>")
            except FloodWaitError as e:
                await bot.edit_message_text(
                    state["chat_id"], state["process_msg_id"],
                    f"⏳ <b>Telegram rate limit</b>\n\nWait {e.seconds}s before retrying.",
                    reply_markup=kb_back("dashboard"))
                await cleanup_host_state(uid)
            except Exception as e:
                await bot.edit_message_text(
                    state["chat_id"], state.get("process_msg_id") or wait.id,
                    f"❌ <b>Failed to send OTP</b>\n\n<code>{html.escape(str(e))}</code>",
                    reply_markup=kb_back("dashboard"))
                await cleanup_host_state(uid)

        # ---- step 2: 2FA password ----
        elif state["step"] == "2FA":
            try:
                await state["client"].sign_in(password=(message.text or "").strip())
                await finalize_success(uid, state)
            except PasswordHashInvalidError:
                await bot.edit_message_text(
                    state["chat_id"], state["process_msg_id"],
                    otp_text(state["phone"], state["otp"], "🔐 Invalid password — try again."))


@bot.on_callback_query(filters.regex("^otp_"))
async def otp_handler(client: Client, query: CallbackQuery):
    uid = query.from_user.id
    state = user_states.get(uid)
    if not state:
        return await query.answer("Session expired", show_alert=True)

    await query.answer()
    data = query.data

    if data == "otp_cancel":
        await bot.edit_message_text(state["chat_id"], state["process_msg_id"],
                                    "OTP entry cancelled.")
        return await cleanup_host_state(uid)

    async with get_lock(uid):
        if data == "otp_del":
            state["otp"] = state["otp"][:-1]
        elif len(state["otp"]) < 5:
            state["otp"] += data.split("_")[1]

        if len(state["otp"]) == 5:
            await bot.edit_message_text(state["chat_id"], state["process_msg_id"],
                                        otp_text(state["phone"], state["otp"],
                                                 "Verifying…"), reply_markup=kb_otp())
            await asyncio.sleep(1)
            await verify_otp(uid, state)
        else:
            await bot.edit_message_text(state["chat_id"], state["process_msg_id"],
                                        otp_text(state["phone"], state["otp"]),
                                        reply_markup=kb_otp())


async def verify_otp(uid: int, state: dict) -> None:
    try:
        await state["client"].sign_in(phone=state["phone"], code=state["otp"],
                                      phone_code_hash=state["phone_code_hash"])
        await finalize_success(uid, state)
    except PhoneCodeInvalidError:
        state["otp"] = ""
        await bot.edit_message_text(state["chat_id"], state["process_msg_id"],
                                    otp_text(state["phone"], "", "❌ Invalid OTP — try again."),
                                    reply_markup=kb_otp())
    except PhoneCodeExpiredError:
        await bot.edit_message_text(state["chat_id"], state["process_msg_id"],
                                    "⏰ OTP expired. Send the phone number again.")
        await cleanup_host_state(uid)
    except SessionPasswordNeededError:
        state["step"] = "2FA"
        await bot.edit_message_text(state["chat_id"], state["process_msg_id"],
                                    otp_text(state["phone"], state["otp"],
                                             "🔐 2FA detected — send your cloud password:"))


async def finalize_success(uid: int, state: dict) -> None:
    if state.get("timeout"):
        state["timeout"].cancel()
    phone = state["phone"]
    await add_account(uid, phone)
    await bot.edit_message_text(
        state["chat_id"], state["process_msg_id"],
        f"╰_╯ Account added! ✅\n\nPhone: {phone}\nReady for broadcasting.",
        reply_markup=kb_back("dashboard"))
    await send_log(uid, f"✅ <b>Account logged in:</b> <code>{phone}</code>")
    await cleanup_host_state(uid)


# ---- view accounts --------------------------------------------------------

async def account_status(uid: int, phone: str) -> tuple:
    path = session_file(uid, phone)          # FIX: was looking for "<phone>.session"
    if not os.path.isfile(path):
        return "Inactive", "❌"
    client = TelegramClient(path.replace(".session", ""), API_ID, API_HASH)
    try:
        await client.connect()
        ok = await client.is_user_authorized()
        await client.disconnect()
        return ("Active", "✅") if ok else ("Inactive", "❌")
    except Exception:
        return "Inactive", "❌"


@bot.on_callback_query(filters.regex("^view_accounts$"))
async def view_accounts(client: Client, query: CallbackQuery):
    await query.answer()
    uid = query.from_user.id
    accounts = await get_accounts(uid)

    if not accounts:
        return await safe_edit(query,
                               "╰_╯ <b>NO ACCOUNTS YET</b>\n\nAdd an account to start advertising.",
                               InlineKeyboardMarkup([
                                   [InlineKeyboardButton("Add Account", callback_data="host_account")],
                                   [InlineKeyboardButton("Back", callback_data="dashboard")]]))

    lines, buttons = [], []
    for i, phone in enumerate(accounts, 1):
        status, emoji = await account_status(uid, phone)
        shown = f"+{normalize_phone(phone)}"
        lines.append(f"{i}. {shown} — {status} {emoji}")
        buttons.append([InlineKeyboardButton(f"{shown} ({status})", callback_data="ignore"),
                        InlineKeyboardButton("Delete", callback_data=f"delete_{phone}")])

    buttons.append([InlineKeyboardButton("Add Account", callback_data="host_account")])
    buttons.append([InlineKeyboardButton("Back", callback_data="dashboard")])
    await safe_edit(query, "╰_╯ <b>HOSTED ACCOUNTS</b>\n\n" + "\n".join(lines),
                    InlineKeyboardMarkup(buttons))


@bot.on_callback_query(filters.regex("^ignore$"))
async def ignore_cb(client: Client, query: CallbackQuery):
    await query.answer()


# ---- delete accounts (registered BEFORE the generic delete_ handler) -------

@bot.on_callback_query(filters.regex("^delete_accounts$"))
async def delete_accounts_menu(client: Client, query: CallbackQuery):
    await query.answer()
    uid = query.from_user.id
    accounts = await get_accounts(uid)

    if not accounts:
        return await safe_edit(query,
                               "╰_╯ <b>NO ACCOUNTS TO DELETE</b>",
                               InlineKeyboardMarkup([
                                   [InlineKeyboardButton("Add Account", callback_data="host_account")],
                                   [InlineKeyboardButton("Back", callback_data="dashboard")]]))

    buttons = [[InlineKeyboardButton(f"+{normalize_phone(p)}", callback_data="ignore"),
                InlineKeyboardButton("Delete", callback_data=f"delete_{p}")]
               for p in accounts]
    buttons.append([InlineKeyboardButton("Back", callback_data="dashboard")])
    await safe_edit(query, "╰_╯ <b>DELETE ACCOUNTS</b>\n\nPick an account to remove:",
                    InlineKeyboardMarkup(buttons))


@bot.on_callback_query(filters.regex(r"^delete_(.+)$"))
async def delete_account(client: Client, query: CallbackQuery):
    await query.answer()
    uid = query.from_user.id
    phone = query.data.split("_", 1)[1]

    path = session_file(uid, phone)
    if os.path.exists(path):
        os.remove(path)

    await remove_account(uid, phone)
    await safe_edit(query, "<b>Account deleted!</b> ✅",
                    kb_back("view_accounts"))


# ---- broadcast control ----------------------------------------------------

@bot.on_callback_query(filters.regex("^start_broadcast$"))
async def start_broadcast_cb(client: Client, query: CallbackQuery):
    uid = query.from_user.id
    user = await get_or_create_user(uid)

    if not user:
        return await query.answer("User not found ❌", show_alert=True)
    if not user.get("accounts"):
        return await query.answer("No accounts hosted ❌", show_alert=True)
    if not user.get("ad_message"):
        return await query.answer("Set the ad message first ❌", show_alert=True)
    if user.get("advertising"):
        return await query.answer("Already running 🚀", show_alert=True)

    await set_broadcast_status(uid, True)
    if not await start_broadcast(uid):
        await set_broadcast_status(uid, False)
        return await query.answer("Failed to start ❌ — no valid session file.", show_alert=True)

    await query.answer("🚀 Broadcast started")
    await safe_edit(query, "╰_╯ <b>BROADCAST STARTED</b>\n\n"
                           "Ads are being sent to your groups/channels.",
                    kb_back("dashboard"))


@bot.on_callback_query(filters.regex("^stop_broadcast$"))
async def stop_broadcast_cb(client: Client, query: CallbackQuery):
    uid = query.from_user.id
    user = await get_user(uid)

    if not user:
        return await query.answer("User not found ❌", show_alert=True)
    if not user.get("advertising"):
        return await query.answer("Not running ❌", show_alert=True)

    await stop_broadcast(uid)
    await query.answer("Stopped 🛑", show_alert=True)
    await safe_edit(query, "╰_╯ <b>BROADCAST STOPPED</b>\n\nAdvertising has been stopped.",
                    kb_back("dashboard"))


# ---- analytics ------------------------------------------------------------

def progress_bar(pct: int) -> str:
    pct = max(0, min(pct, 100))
    filled = pct // 10
    return "░" * filled + "▓" * (10 - filled)


@bot.on_callback_query(filters.regex("^analytics$"))
async def analytics_cb(client: Client, query: CallbackQuery):
    await query.answer()
    uid = query.from_user.id
    user = await get_or_create_user(uid)
    if not user:
        return await query.answer("No data found.", show_alert=True)

    sent = int(user.get("messages_sent", 0))
    failed = int(user.get("messages_failed", 0))
    rounds = int(user.get("broadcast_completed", 0))
    total = sent + failed
    rate = int(sent / total * 100) if total else 0

    await safe_edit(query,
                    "<b>╰_╯ ANALYTICS</b>\n\n"
                    f"Broadcast cycles: <b>{rounds}</b>\n"
                    f"Messages sent: <b>{sent}</b>\n"
                    f"Failed sends: <b>{failed}</b>\n"
                    f"Accounts: <b>{len(user.get('accounts', []))}</b>\n"
                    f"Interval: <code>{user.get('delay', DEFAULT_DELAY)}s</code>\n\n"
                    f"<b>Success rate:</b>\n{progress_bar(rate)} {rate}%",
                    kb_back("dashboard"))


@bot.on_callback_query(filters.regex("^auto_reply$"))
async def auto_reply_cb(client: Client, query: CallbackQuery):
    await query.answer()
    await safe_edit(query,
                    "<b>╰_╯ AUTO REPLY</b>\n\nComing soon — this feature is not available yet.",
                    kb_back("dashboard"))


# ---- optional update logging ---------------------------------------------

if DEBUG_UPDATES:
    @bot.on_message(group=-2)
    async def _debug_message(client: Client, message: Message):
        print(f"[DEBUG MSG] from={message.from_user.id if message.from_user else None} "
              f"text={(message.text or '')!r}", flush=True)

    @bot.on_callback_query(group=-2)
    async def _debug_callback(client: Client, query: CallbackQuery):
        print(f"[DEBUG CB] from={query.from_user.id} data={query.data!r}", flush=True)


# ══════════════════════════════════════════════════════════════════════════
# 10. ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════

def main() -> None:
    print("💡 Starting rexco ad bot…")
    print(f"🔌 MongoDB → {MONGO_URI} (db: {DB_NAME})")

    # Install ONE loop and keep it. asyncio.run() would unset it and Pyrogram's
    # run() (which calls asyncio.get_event_loop()) would then crash with
    # "There is no current event loop in thread 'MainThread'".
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        if not loop.run_until_complete(check_connection()):
            print("❌ Bot stopped: MongoDB unreachable.")
            return
        loop.run_until_complete(ensure_indexes())
    except Exception as e:
        print(f"❌ Startup check failed: {type(e).__name__}: {e}")
        return

    try:
        print("🚀 Bot is now running… (Ctrl+C to stop)")
        bot.run()
    except KeyboardInterrupt:
        print("🛑 Stopped manually")
    except Exception as e:
        print(f"❌ Bot crashed: {e}")
        traceback.print_exc()
    finally:
        print("⏹ Shutdown complete.")


if __name__ == "__main__":
    main()
