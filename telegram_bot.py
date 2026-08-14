
# Fully optimized Redis-based Telegram bot script with Analytics
# Implements single and batch link sharing, auto-delete, button and channel management,
# admin commands, channel re-verification with 'Try Again' button, **plus analytics**.
# Uses `redis.asyncio` for persistence. Requires python-telegram-bot v20+ and a running Redis instance.

import os
import json
import uuid
import asyncio
import logging
import re
from datetime import datetime, timedelta
from functools import wraps
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler, filters,
    CallbackQueryHandler, ContextTypes
)
from redis.asyncio import Redis
from flask import Flask
from threading import Thread

# --- Config ---
TOKEN = os.getenv("BOT_TOKEN")

ADMIN_IDS = {
    int(x.strip())
    for x in os.getenv("ADMIN_IDS", "").split(",")
    if x.strip()
}

if not ADMIN_IDS:
    raise ValueError("ADMIN_IDS is not configured.")

# Primary admin remains the first ID for existing bot behavior that
# relies on one primary source chat.
ADMIN_ID = next(iter(ADMIN_IDS))

CHANNEL_ID = os.getenv("CHANNEL_ID")
CHANNEL_IMAGE_FILE_ID = os.getenv("CHANNEL_IMAGE_FILE_ID")

PAGE_SIZE = 10  # links per page
REDIS_URL = os.getenv("REDIS_URL")
redis = Redis.from_url(REDIS_URL, decode_responses=True)

# --- Logging ---
logging.basicConfig(level=logging.INFO)

# --- Render health check ---
web_app = Flask(__name__)

@web_app.route("/")
def health_check():
    return "Bot is running!", 200

def run_web():
    port = int(os.environ.get("PORT", 10000))
    web_app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)

# --- Helpers ---
def admin_only(func):
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id not in ADMIN_IDS:
            await update.message.reply_text("❌ You are not authorized to use this bot.")
            return
        return await func(update, context)
    return wrapper

def format_seconds(seconds: int) -> str:
    h, m = divmod(seconds, 3600)
    m, s = divmod(m, 60)
    parts = []
    if h:
        parts.append(f"{h} hour{'s' if h != 1 else ''}")
    if m:
        parts.append(f"{m} minute{'s' if m != 1 else ''}")
    if s:
        parts.append(f"{s} second{'s' if s != 1 else ''}")
    return " ".join(parts) if parts else "0 seconds"

def generate_token() -> str:
    return uuid.uuid4().hex[:8]

# === Access limit / full-access helpers ===
DEFAULT_ACCESS_LIMIT = 6
DEFAULT_ACCESS_CYCLE_HOURS = 48
DEFAULT_LIMIT_MESSAGE = "⚠️ You have reached your free access limit.\n\nGet full access to continue."
DEFAULT_FULL_ACCESS_MESSAGE = "🔓 Please follow the instructions below to get full access."

async def get_access_limit() -> int:
    try:
        return max(0, int(await redis.get("access:limit") or DEFAULT_ACCESS_LIMIT))
    except (TypeError, ValueError):
        return DEFAULT_ACCESS_LIMIT

async def get_access_cycle_hours() -> int:
    try:
        return max(1, int(await redis.get("access:cycle_hours") or DEFAULT_ACCESS_CYCLE_HOURS))
    except (TypeError, ValueError):
        return DEFAULT_ACCESS_CYCLE_HOURS

async def get_limit_message() -> str:
    return await redis.get("access:limit_message") or DEFAULT_LIMIT_MESSAGE

async def get_full_access_message() -> str:
    return await redis.get("access:full_access_message") or DEFAULT_FULL_ACCESS_MESSAGE

async def remember_user(user):
    if not user:
        return
    if user.username:
        await redis.set(f"access:username:{user.username.lower()}", user.id)
    await redis.set(f"access:user:{user.id}:username", (user.username or "").lower())

def normalize_user_reference(value: str):
    value = value.strip()
    if value.startswith("tg://user?id="):
        match = re.search(r"id=(\d+)", value)
        return ("id", int(match.group(1))) if match else (None, None)
    if value.startswith(("https://t.me/", "http://t.me/", "t.me/")):
        value = value.split("/", 3)[-1]
        if "?" in value:
            query = value.split("?", 1)[1]
            match = re.search(r"(?:^|&)id=(\d+)", query)
            if match:
                return "id", int(match.group(1))
            value = value.split("?", 1)[0]
    if value.startswith("@"):
        value = value[1:]
    if value.isdigit():
        return "id", int(value)
    if re.fullmatch(r"[A-Za-z0-9_]{5,32}", value):
        return "username", value.lower()
    return None, None

async def resolve_user_reference(value: str):
    ref_type, ref_value = normalize_user_reference(value)
    if ref_type == "id":
        return ref_value, str(ref_value)
    if ref_type == "username":
        user_id = await redis.get(f"access:username:{ref_value}")
        return (int(user_id), f"@{ref_value}") if user_id else (None, f"@{ref_value}")
    return None, value

async def has_full_access(user_id: int) -> bool:
    value = await redis.get(f"access:full:{user_id}")
    if not value:
        return False
    if value == "lifetime":
        return True
    try:
        expires_at = int(value)
    except (TypeError, ValueError):
        await redis.delete(f"access:full:{user_id}")
        return False
    if expires_at <= int(datetime.utcnow().timestamp()):
        await redis.delete(f"access:full:{user_id}")
        return False
    return True

async def get_usage_state(user_id: int):
    key = f"access:usage:{user_id}"
    state = await get_data(key, {})
    now = int(datetime.utcnow().timestamp())
    cycle_seconds = (await get_access_cycle_hours()) * 3600
    started_at = int(state.get("started_at", 0) or 0) if isinstance(state, dict) else 0
    count = int(state.get("count", 0) or 0) if isinstance(state, dict) else 0
    if not started_at or now >= started_at + cycle_seconds:
        return {"count": 0, "started_at": now}
    return {"count": count, "started_at": started_at}

async def can_receive_file(user_id: int) -> bool:
    if await has_full_access(user_id):
        return True
    limit = await get_access_limit()
    if limit <= 0:
        return False
    state = await get_usage_state(user_id)
    return state["count"] < limit

async def record_file_delivery(user_id: int):
    if await has_full_access(user_id):
        return
    state = await get_usage_state(user_id)
    state["count"] += 1
    await set_data(f"access:usage:{user_id}", state)

async def send_limit_reached(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cycle_hours = await get_access_cycle_hours()

    message = (
    f"⚠️ You've reached your free access limit.\n\n"
    f"Your free access will reset in {cycle_hours} hours.\n\n"
    f"🔓 Get full access to continue without waiting."
)

    channel_url = os.getenv("LIMIT_CHANNEL_URL")

    buttons = [
        [InlineKeyboardButton("🔓 Get Full Access", callback_data="fullaccess")]
    ]

    if channel_url:
        buttons.append(
            [InlineKeyboardButton("📢 Get Dark Content", url=channel_url)]
        )

    keyboard = InlineKeyboardMarkup(buttons)

    await update.effective_chat.send_message(
        message,
        reply_markup=keyboard
    )

async def send_full_access_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Send the saved text first
    await update.effective_chat.send_message(await get_full_access_message())

    # Then send the saved image separately
    image_file_id = await redis.get("access:full_access_image")
    if image_file_id:
        await update.effective_chat.send_photo(photo=image_file_id)

async def get_data(key, default=None):
    val = await redis.get(key)
    return json.loads(val) if val else default

async def set_data(key, val):
    await redis.set(key, json.dumps(val))


async def post_link_to_channel(context: ContextTypes.DEFAULT_TYPE, link: str):
    """Post the fixed channel image with the generated link button."""
    if not CHANNEL_ID or not CHANNEL_IMAGE_FILE_ID:
        logging.warning("CHANNEL_ID or CHANNEL_IMAGE_FILE_ID is not configured; skipping channel post.")
        return

    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("🔗 Open Video", url=link)]])

    await context.bot.send_photo(
        chat_id=CHANNEL_ID,
        photo=CHANNEL_IMAGE_FILE_ID,
        caption="🎬 New Content\n\nClick the button below to access.",
        reply_markup=keyboard,
    )


async def is_user_joined(user_id, context):
    channels = await get_data("required_channels", [])
    for ch in channels:
        try:
            member = await context.bot.get_chat_member(ch["chat_id"], user_id)
            if member.status not in ["member", "administrator", "creator"]:
                return False
        except Exception:
            return False
    return True

async def schedule_deletion(context: ContextTypes.DEFAULT_TYPE, chat_id: int, msg_ids: list):
    delay = int(await redis.get("delete_time") or 1800)
    await asyncio.sleep(delay)
    for msg_id in msg_ids:
        try:
            await context.bot.delete_message(chat_id, msg_id)
        except Exception:
            pass

# === Analytics helpers ===
def _daystamp(ts: datetime | None = None) -> str:
    """UTC date as YYYY-MM-DD (used in Redis keys)."""
    return (ts or datetime.utcnow()).strftime("%Y-%m-%d")

async def incr(key: str, amount: int = 1):
    await redis.incrby(key, amount)

async def sadd(key: str, member):
    await redis.sadd(key, member)

# --- Admin Commands (existing + analytics) ---
@admin_only
async def setjointitle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("❌ Usage: /setjointitle <your message>")
        return
    txt = " ".join(context.args)
    await redis.set("join_text", txt)
    await update.message.reply_text(f"✅ Join prompt updated to:\n\n{txt}")



@admin_only
async def resetjointitle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await redis.set("join_text", "📢 Please join all required channels:")
    await update.message.reply_text("🔄 Join prompt reset to default.")

@admin_only
async def batch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    admin_id = update.effective_user.id
    await redis.delete(f"batch:{admin_id}")
    await redis.set(f"batch_active:{admin_id}", 1)
    await update.message.reply_text("📦 Batch mode ON. Send your messages.")

@admin_only
async def batchoff(update: Update, context: ContextTypes.DEFAULT_TYPE):
    admin_id = update.effective_user.id
    await redis.delete(f"batch:{admin_id}")
    await redis.delete(f"batch_active:{admin_id}")
    await update.message.reply_text("❌ Batch mode cancelled.")

@admin_only
async def generatebatch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    admin_id = update.effective_user.id
    chat_id = update.effective_chat.id

    session = await redis.lrange(f"batch:{admin_id}", 0, -1)
    if not session:
        await update.message.reply_text("❌ No inputs in batch.")
        return

    token = generate_token()
    await set_data(f"link:{token}", {"type": "batch", "chat_id": chat_id, "messages": session})
    await redis.delete(f"batch:{admin_id}")
    await redis.delete(f"batch_active:{admin_id}")

    await incr("metrics:links:batch:total")
    await sadd(f"metrics:links:batch:{_daystamp()}", token)

    link = f"https://t.me/{context.bot.username}?start={token}"
    await update.message.reply_text(f"✅ Batch link generated:\n{link}")
    await post_link_to_channel(context, link)

@admin_only
async def setchannels(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("📥 Send channel/Group usernames (one per line):")
    context.user_data["awaiting_channels"] = True

@admin_only
async def cancelsetchannels(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.user_data.get("awaiting_channels"):
        context.user_data["awaiting_channels"] = False
        await update.message.reply_text("❌ Channel setup cancelled.")
    else:
        await update.message.reply_text("ℹ️ No channel setup in progress.")

@admin_only
async def clearsetchannels(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await set_data("required_channels", [])
    await update.message.reply_text("✅ Required channel/Group list has been cleared.")

@admin_only
async def setbutton(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("📝 Send the new button text:")
    context.user_data["awaiting_button_text"] = True

@admin_only
async def cancelsetbutton(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keys = ["awaiting_button_text", "awaiting_button_url", "new_button_text"]
    if any(context.user_data.pop(k, None) for k in keys):
        await update.message.reply_text("❌ Button setup cancelled.")
    else:
        await update.message.reply_text("ℹ️ No button setup in progress.")

@admin_only
async def promotext(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("❌ Usage: /promotext <button caption> or /promotext clear")
        return
    if context.args[0].lower() == "clear":
        await redis.delete("button_caption")
        await update.message.reply_text("✅ Button caption reset to default.")
    else:
        txt = " ".join(context.args)
        await redis.set("button_caption", txt)
        await update.message.reply_text(f"✅ Button caption set to:\n\n{txt}")

async def listlinks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keys = await redis.keys("link:*")
    if not keys:
        await update.message.reply_text("ℹ️ No links found.")
        return

    items = []
    for k in keys:
        tok = k.split(":", 1)[-1]
        val = await get_data(k)
        label = "Batch" if val.get("type") == "batch" else "Single"
        items.append(f"`{tok}` ({label})")
    items.sort()

    # Store for pagination callback
    context.user_data["listlinks_items"] = items
    await send_links_page(update.effective_chat.id, context, 0)

async def send_links_page(chat_id: int, context: ContextTypes.DEFAULT_TYPE, page: int):
    items = context.user_data.get("listlinks_items", [])
    if not items:
        return
    total_pages = (len(items) - 1) // PAGE_SIZE + 1
    page = max(0, min(page, total_pages - 1))
    start_idx = page * PAGE_SIZE
    end_idx = start_idx + PAGE_SIZE
    page_items = items[start_idx:end_idx]

    text_lines = [f"🔗 *Links* _(page {page + 1}/{total_pages})_:"]
    text_lines.extend(f"- {i}" for i in page_items)

    buttons = []
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("« Previous", callback_data=f"listlinks_{page - 1}"))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton("Next »", callback_data=f"listlinks_{page + 1}"))
    if nav_row:
        buttons.append(nav_row)

    reply_markup = InlineKeyboardMarkup(buttons) if buttons else None
    await context.bot.send_message(chat_id, "\n".join(text_lines), parse_mode="Markdown", reply_markup=reply_markup)

async def listlinks_pagination(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    match = re.match(r"^listlinks_(\d+)$", query.data)
    if not match:
        return
    page = int(match.group(1))
    items = context.user_data.get("listlinks_items", [])
    if not items:
        # fallback: recall command
        await query.edit_message_text("⚠️ List expired. Please run /listlinks again.")
        return
    total_pages = (len(items) - 1) // PAGE_SIZE + 1
    page = max(0, min(page, total_pages - 1))
    start_idx = page * PAGE_SIZE
    end_idx = start_idx + PAGE_SIZE
    page_items = items[start_idx:end_idx]

    text_lines = [f"🔗 *Links* _(page {page + 1}/{total_pages})_:"]
    text_lines.extend(f"- {i}" for i in page_items)

    buttons = []
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("« Previous", callback_data=f"listlinks_{page - 1}"))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton("Next »", callback_data=f"listlinks_{page + 1}"))
    if nav_row:
        buttons.append(nav_row)

    reply_markup = InlineKeyboardMarkup(buttons) if buttons else None
    await query.edit_message_text("\n".join(text_lines), parse_mode="Markdown", reply_markup=reply_markup)

@admin_only
async def deletelink(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("❌ Usage: /deletelink <token>")
        return
    tok = context.args[0]
    if await redis.exists(f"link:{tok}"):
        await redis.delete(f"link:{tok}")
        await update.message.reply_text(f"✅ Link `{tok}` deleted.", parse_mode="Markdown")
    else:
        await update.message.reply_text("❌ Token not found.")

@admin_only
async def deletealllinks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keys = await redis.keys("link:*")
    for k in keys:
        await redis.delete(k)
    await redis.delete(f"batch:{ADMIN_ID}")
    await redis.delete(f"batch_active:{ADMIN_ID}")
    await update.message.reply_text("🗑️ All links (single & batch) have been deleted.")

@admin_only
async def settime(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("❌ Usage: /settime <seconds>\nExample: `/settime 600`", parse_mode="Markdown")
        return
    sec = int(context.args[0])
    if sec < 30:
        await update.message.reply_text("⚠️ Auto-delete time must be at least 30 seconds.")
        return
    await redis.set("delete_time", sec)
    await update.message.reply_text(f"⏱️ Auto-delete time set to {format_seconds(sec)}.")

@admin_only
async def showconfig(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /config — show all admin‑set configuration.
    Admin‑only command that fetches current settings from Redis and displays them neatly.
    """
    join_text     = await redis.get("join_text")        or "📢 Please join all required channels:"
    channels      = await get_data("required_channels", [])
    button_text   = await redis.get("button_text")      or "Open"
    button_url    = await redis.get("button_url")       or "https://example.com"
    button_caption= await redis.get("button_caption")   or "🔘 Tap below to continue"
    promo_text    = await redis.get("promo_text")       or "Not set"
    delete_time   = int(await redis.get("delete_time")  or 1800)

    lines = [
        "*Current Configuration*",
        f"*Join Prompt*: {join_text}",
        f"*Required Channels* ({len(channels)}): " + (", ".join(ch.get("chat_id") for ch in channels) if channels else "None"),
        f"*Button Text*: {button_text}",
        f"*Button URL*: {button_url}",
        f"*Promo Text*: {promo_text}",
        f"*Auto‑delete Time*: {format_seconds(delete_time)}"
    ]
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


# === Access limit / full-access admin commands ===
@admin_only
async def setlimit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("❌ Usage: /setlimit <number of files>\nExample: /setlimit 6")
        return
    limit = int(context.args[0])
    if limit < 0:
        await update.message.reply_text("❌ Limit cannot be negative.")
        return
    await redis.set("access:limit", limit)
    await update.message.reply_text(f"✅ Free file limit set to {limit} files per cycle.")

@admin_only
async def setcycle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("❌ Usage: /setcycle <hours>\nExample: /setcycle 48")
        return
    hours = int(context.args[0])
    if hours < 1:
        await update.message.reply_text("❌ Cycle must be at least 1 hour.")
        return
    await redis.set("access:cycle_hours", hours)
    await update.message.reply_text(f"✅ Free access cycle set to {hours} hours.")

@admin_only
async def setlimitmsg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["awaiting_limit_message"] = True
    await update.message.reply_text("📝 Send the message to show when a user reaches the free limit.")

@admin_only
async def setaccessmsg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["awaiting_full_access_message"] = True
    context.user_data.pop("awaiting_full_access_image", None)
    await update.message.reply_text(
        "📝 Send the text to show after the user taps Get Full Access."
    )

@admin_only
async def addaccess(update: Update, context: ContextTypes.DEFAULT_TYPE):
    reference = context.args[0] if context.args else None
    duration_arg = context.args[1].lower() if len(context.args) > 1 else None
    if update.message.reply_to_message and not reference:
        reference = str(update.message.reply_to_message.from_user.id)
    if not reference or not duration_arg:
        await update.message.reply_text(
            "❌ Usage: /addaccess <@username/profile link/user ID> <hours|lifetime>\n"
            "Examples:\n"
            "/addaccess @username 24\n"
            "/addaccess @username 48\n"
            "/addaccess @username lifetime"
        )
        return

    is_lifetime = duration_arg in {"lifetime", "forever", "permanent"}
    if not is_lifetime and not duration_arg.isdigit():
        await update.message.reply_text("❌ Duration must be a number of hours or `lifetime`.", parse_mode="Markdown")
        return

    hours = int(duration_arg) if not is_lifetime else 0
    if not is_lifetime and hours < 1:
        await update.message.reply_text("❌ Access duration must be at least 1 hour.")
        return

    user_id, display = await resolve_user_reference(reference)
    if not user_id:
        await update.message.reply_text(
            f"❌ I couldn't resolve {display}. The user must have opened this bot at least once, "
            "or you can use their numeric Telegram user ID / t.me/user?id=... link."
        )
        return

    username = await redis.get(f"access:user:{user_id}:username")
    shown = f"@{username}" if username else str(user_id)

    if is_lifetime:
        await redis.set(f"access:full:{user_id}", "lifetime")
        await update.message.reply_text(
            f"✅ Lifetime full access granted.\n\nUser: {shown}\nDuration: Lifetime"
        )
        return

    expires_at = int(datetime.utcnow().timestamp()) + hours * 3600
    await redis.set(f"access:full:{user_id}", expires_at)
    expires_text = datetime.utcfromtimestamp(expires_at).strftime("%Y-%m-%d %H:%M UTC")
    await update.message.reply_text(
        f"✅ Full access granted.\n\nUser: {shown}\nDuration: {hours} hours\nExpires: {expires_text}"
    )

@admin_only
async def removeaccess(update: Update, context: ContextTypes.DEFAULT_TYPE):
    reference = context.args[0] if context.args else None
    if update.message.reply_to_message and not reference:
        reference = str(update.message.reply_to_message.from_user.id)
    if not reference:
        await update.message.reply_text("❌ Usage: /removeaccess <@username/profile link/user ID>")
        return
    user_id, display = await resolve_user_reference(reference)
    if not user_id:
        await update.message.reply_text(f"❌ I couldn't resolve {display}.")
        return
    deleted = await redis.delete(f"access:full:{user_id}")
    await update.message.reply_text(
        "✅ Full access removed." if deleted else "ℹ️ This user does not currently have full access."
    )

@admin_only
async def accesslist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keys = await redis.keys("access:full:*")
    if not keys:
        await update.message.reply_text("ℹ️ No users currently have full access.")
        return
    now = int(datetime.utcnow().timestamp())
    lines = ["🔓 *Full Access Users*", ""]
    active = 0
    for key in sorted(keys):
        try:
            user_id = int(key.rsplit(":", 1)[-1])
            access_value = await redis.get(key) or ""
        except (TypeError, ValueError):
            await redis.delete(key)
            continue

        if access_value == "lifetime":
            username = await redis.get(f"access:user:{user_id}:username")
            shown = f"@{username}" if username else str(user_id)
            active += 1
            lines.append(f"{active}. `{shown}` — ♾️ Lifetime")
            continue

        try:
            expires_at = int(access_value)
        except (TypeError, ValueError):
            await redis.delete(key)
            continue

        if expires_at <= now:
            await redis.delete(key)
            continue

        username = await redis.get(f"access:user:{user_id}:username")
        shown = f"@{username}" if username else str(user_id)
        expires_text = datetime.utcfromtimestamp(expires_at).strftime("%Y-%m-%d %H:%M UTC")
        active += 1
        lines.append(f"{active}. `{shown}` — expires {expires_text}")
    if not active:
        await update.message.reply_text("ℹ️ No users currently have full access.")
        return
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

@admin_only
async def limitconfig(update: Update, context: ContextTypes.DEFAULT_TYPE):
    limit = await get_access_limit()
    hours = await get_access_cycle_hours()
    limit_message = await get_limit_message()
    access_message = await get_full_access_message()
    text = (
        "⚙️ *Access Configuration*\n\n"
        f"*Free file limit:* {limit}\n"
        f"*Cycle:* {hours} hours\n\n"
        f"*Limit message:*\n{limit_message}\n\n"
        f"*Full access message:*\n{access_message}"
    )
    await update.message.reply_text(text, parse_mode="Markdown")

# === NEW ANALYTICS COMMANDS ===
@admin_only
async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/stats [days] — show totals & DAU"""
    days = int(context.args[0]) if context.args and context.args[0].isdigit() else 7
    today = datetime.utcnow().date()
    start_date = today - timedelta(days=days - 1)

    dau_lines, union_keys = [], []
    for i in range(days):
        day_key = (start_date + timedelta(days=i)).strftime("%Y-%m-%d")
        cnt = await redis.scard(f"metrics:users:{day_key}")
        dau_lines.append(f"{day_key}:  {cnt}")
        union_keys.append(f"metrics:users:{day_key}")

    # Unique users in range (via SUNIONSTORE)
    total_users = await redis.sunionstore("tmp:users:range", *union_keys)
    await redis.delete("tmp:users:range")

    single_total = int(await redis.get("metrics:links:single:total") or 0)
    batch_total  = int(await redis.get("metrics:links:batch:total") or 0)

    text = (
        f"📊 *Stats — last {days} days*\n"
        f"• *Unique users in range:* {total_users}\n"
        f"• *Links generated:* {single_total:,} single  |  {batch_total:,} batch\n\n"
        f"*DAU*\n" + "\n".join(dau_lines)
    )
    await update.message.reply_text(text, parse_mode="Markdown")

@admin_only
async def toplinks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/toplinks [N] — most viewed links"""
    n = int(context.args[0]) if context.args and context.args[0].isdigit() else 10
    pairs = await redis.zrevrange("metrics:linkviews:z", 0, n - 1, withscores=True)
    if not pairs:
        return await update.message.reply_text("No view data yet.")
    lines = [f"{i+1}. `{tok}` — {int(cnt)} views" for i, (tok, cnt) in enumerate(pairs)]
    await update.message.reply_text("*Most viewed links:*\n" + "\n".join(lines), parse_mode="Markdown")

@admin_only
async def invalidlinks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/invalidlinks — counts + top offenders"""
    total = int(await redis.get("metrics:invalid:total") or 0)
    offenders = await redis.zrevrange("metrics:invalid:z", 0, 4, withscores=True)

    txt = [f"❌ *Invalid/Expired attempts:* {total}"]
    if offenders:
        txt.append("\n*Top failed tokens*")
        txt += [f"- `{t}` ({int(c)} hits)" for t, c in offenders]
    await update.message.reply_text("\n".join(txt), parse_mode="Markdown")

@admin_only
async def allcommands(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cmds = [
        "/batch - Start batch mode",
        "/generatebatch - Generate batch link",
        "/batchoff - Cancel batch",
        "/setchannels - Set required channels/Groups",
        "/cancelsetchannels - Cancel channel setup",
        "/clearsetchannels - Clear required channel list",
        "/setbutton - Set button text and link",
        "/cancelsetbutton - Cancel button setup",
        "/promotext - Set or clear promo message",
        "/listlinks - List all active links",
        "/deletelink <token> - Delete a specific link",
        "/deletealllinks - Delete all links",
        "/setjointitle - Set the join prompt message",
        "/resetjointitle - Reset join prompt to default",
        "/settime <seconds> - Set auto-delete time in seconds",
        "/setlimit <files> - Set free file limit per cycle",
        "/setcycle <hours> - Set free access cycle length",
        "/setlimitmsg - Set limit-reached message",
        "/setaccessmsg - Set Get Full Access response",
        "/addaccess <user> <hours|lifetime> - Grant timed or lifetime full access",
        "/removeaccess <user> - Remove full access",
        "/accesslist - List full-access users",
        "/limitconfig - Show access-limit configuration",
        "/stats [days] - Show usage statistics",
        "/toplinks [N] - Most viewed links",
        "/invalidlinks - Invalid / expired link attempts",
        "/config - Show current configuration",
        "/allcommands - Show all commands"
    ]
    await update.message.reply_text("\n".join(cmds))

# --- User Start Handler (/start <token>) ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await remember_user(update.effective_user)
    args = context.args
    if not args:
        await update.message.reply_text("👋 Welcome!")
        return

    token = args[0]
    record = await get_data(f"link:{token}")
    if not record:
        # Analytics: invalid attempt
        await incr("metrics:invalid:total")
        await incr(f"metrics:invalid:{_daystamp()}")
        await redis.zincrby("metrics:invalid:z", 1, token)

        await update.message.reply_text("❌ Invalid or expired link.")
        return

    if await get_data("required_channels") and not await is_user_joined(update.effective_user.id, context):
        channels = await get_data("required_channels", [])
        buttons = [[InlineKeyboardButton("Join", url=ch["url"])] for ch in channels]
        buttons.append([InlineKeyboardButton("✅ Try Again", callback_data=f"tryagain|{token}")])
        prompt = await redis.get("join_text") or "📢 Please join all required channels:"
        await update.message.reply_text(prompt, reply_markup=InlineKeyboardMarkup(buttons))
        return

    if not await can_receive_file(update.effective_user.id):
        await send_limit_reached(update, context)
        return

    # === Analytics: successful link usage ===
    await sadd(f"metrics:users:{_daystamp()}", update.effective_user.id)
    await incr(f"metrics:linkviews:{token}")
    await redis.zincrby("metrics:linkviews:z", 1, token)

    sent_ids = []
    wait_msg = await context.bot.send_message(update.effective_chat.id, "⏳ Please wait …")

    if record["type"] == "single":
        msg = await context.bot.copy_message(update.effective_chat.id, record["chat_id"], record["message_id"])
        sent_ids.append(msg.message_id)
    else:
        for msg_id in record["messages"]:
            msg = await context.bot.copy_message(update.effective_chat.id, record["chat_id"], msg_id)
            sent_ids.append(msg.message_id)

    await record_file_delivery(update.effective_user.id)
    await wait_msg.delete()

    promo = await redis.get("promo_text")
    if promo and promo.lower() != "null":
        promo_msg = await update.message.reply_text(promo)
        sent_ids.append(promo_msg.message_id)

    caption = await redis.get("button_caption")
    btext = await redis.get("button_text") or "Open"
    burl = await redis.get("button_url") or "https://example.com"

    if caption:
        button_msg = await update.message.reply_text(
            caption,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(btext, url=burl)]])
        )
    else:
    button_msg = await update.message.reply_text(
        "\u200b",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(btext, url=burl)]])
    )
    sent_ids.append(button_msg.message_id)

    delay = int(await redis.get("delete_time") or 1800)
    note = await update.message.reply_text(f"_⚠️ Important!\n\nAll the messages will be auto-deleted after {format_seconds(delay)}_", parse_mode="Markdown")
    sent_ids.append(note.message_id)

    context.application.create_task(schedule_deletion(context, update.effective_chat.id, sent_ids))

# --- Callback Handler for "🔓 Get Full Access" ---
async def fullaccess_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await send_full_access_message(update, context)


# --- Callback Handler for "✅ Try Again" ---
async def tryagain_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private":
        await update.callback_query.answer("❌ Not available in group/channel chats.", show_alert=True)
        return

    query = update.callback_query
    await remember_user(query.from_user)
    _, token = query.data.split("|")
    user_id = query.from_user.id
    chat_id = query.message.chat.id

    if await get_data("required_channels") and not await is_user_joined(user_id, context):
        await query.answer("❌ You haven't joined all required channels.", show_alert=True)
        return

    record = await get_data(f"link:{token}")
    if not record:
        # Analytics: invalid attempt
        await incr("metrics:invalid:total")
        await incr(f"metrics:invalid:{_daystamp()}")
        await redis.zincrby("metrics:invalid:z", 1, token)

        await query.message.reply_text("❌ Invalid or expired link.")
        await query.answer()
        return

    if not await can_receive_file(user_id):
        await query.answer()
        await send_limit_reached(update, context)
        await query.message.delete()
        return

    # === Analytics: successful link usage ===
    await sadd(f"metrics:users:{_daystamp()}", user_id)
    await incr(f"metrics:linkviews:{token}")
    await redis.zincrby("metrics:linkviews:z", 1, token)

    sent_ids = []
    wait_msg = await context.bot.send_message(chat_id, "⏳ Please wait …")

    if record["type"] == "single":
        msg = await context.bot.copy_message(chat_id, record["chat_id"], record["message_id"])
        sent_ids.append(msg.message_id)
    else:
        for msg_id in record["messages"]:
            msg = await context.bot.copy_message(chat_id, record["chat_id"], msg_id)
            sent_ids.append(msg.message_id)

    await record_file_delivery(user_id)
    await wait_msg.delete()

    promo = await redis.get("promo_text")
    if promo and promo.lower() != "null":
        promo_note = await context.bot.send_message(chat_id, promo)
        sent_ids.append(promo_note.message_id)

    caption = await redis.get("button_caption")
    btext = await redis.get("button_text") or "Open"
    burl = await redis.get("button_url") or "https://example.com"

    if caption:
        bmsg = await context.bot.send_message(
            chat_id,
            caption,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(btext, url=burl)]])
        )
    else:
        bmsg = await context.bot.send_message(
            chat_id,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(btext, url=burl)]])
        )
    sent_ids.append(bmsg.message_id)

    delay = int(await redis.get("delete_time") or 1800)
    del_note = await context.bot.send_message(chat_id, f"_This will be auto-deleted after {format_seconds(delay)}_", parse_mode="Markdown")
    sent_ids.append(del_note.message_id)

    context.application.create_task(schedule_deletion(context, chat_id, sent_ids))
    await query.answer()
    await query.message.delete()

# --- Admin Message Handler ---
async def handle_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private":
        return

    user_id = update.effective_user.id
    if user_id != ADMIN_ID:
        await update.message.reply_text("❌ You are not authorized to use this bot.")
        return

    if context.user_data.get("awaiting_limit_message"):
        await redis.set("access:limit_message", update.message.text)
        context.user_data.pop("awaiting_limit_message", None)
        await update.message.reply_text("✅ Limit-reached message updated.")
        return

    if context.user_data.get("awaiting_full_access_message"):
        if not update.message.text:
            await update.message.reply_text("❌ Please send the text first.")
            return

        await redis.set("access:full_access_message", update.message.text)
        context.user_data.pop("awaiting_full_access_message", None)
        context.user_data["awaiting_full_access_image"] = True

        await update.message.reply_text(
            "✅ Text saved.\n\n"
            "🖼️ Now send the image that should appear below the text."
        )
        return

    if context.user_data.get("awaiting_full_access_image"):
        if not update.message.photo:
            await update.message.reply_text("❌ Please send an image/photo.")
            return

        image_file_id = update.message.photo[-1].file_id
        await redis.set("access:full_access_image", image_file_id)

        context.user_data.pop("awaiting_full_access_image", None)

        await update.message.reply_text(
            "✅ Full-access text and image updated successfully."
        )
        return

    if context.user_data.get("awaiting_channels"):
        usernames = update.message.text.splitlines()
        channels = []
        for u in usernames:
            u = u.strip()
            if u.startswith("@"):
                channels.append({"chat_id": u, "url": f"https://t.me/{u[1:]}"})
        await set_data("required_channels", channels)
        context.user_data["awaiting_channels"] = False
        await update.message.reply_text("✅ Required channels updated.")
        return

    if context.user_data.get("awaiting_button_text"):
        context.user_data["new_button_text"] = update.message.text.strip()
        context.user_data["awaiting_button_url"] = True
        context.user_data.pop("awaiting_button_text")
        await update.message.reply_text("🔗 Now send the new button URL:")
        return

    if context.user_data.get("awaiting_button_url"):
        url = update.message.text.strip()
        text = context.user_data.pop("new_button_text")
        await redis.set("button_text", text)
        await redis.set("button_url", url)
        context.user_data.pop("awaiting_button_url")
        await update.message.reply_text(f"✅ Button updated to: [{text}]({url})", parse_mode="Markdown")
        return

    admin_id = update.effective_user.id

    if await redis.exists(f"batch_active:{admin_id}"):
        await redis.rpush(f"batch:{admin_id}", update.message.message_id)
        return

    token = generate_token()
    await set_data(f"link:{token}", {
        "type": "single",
        "chat_id": update.effective_chat.id,
        "message_id": update.message.message_id
    })

    await incr("metrics:links:single:total")
    await sadd(f"metrics:links:single:{_daystamp()}", token)

    link = f"https://t.me/{context.bot.username}?start={token}"
    await update.message.reply_text(f"🔗 Link generated:\n{link}")
    await post_link_to_channel(context, link)

# --- Fallback ---
async def fallback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private":
        return
    await update.message.reply_text("❓ Unknown command. Use /allcommands to see available commands.")

# --- Error Handler ---
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logging.error("Exception while handling an update:", exc_info=context.error)

# --- Main ---
if __name__ == '__main__':
    Thread(target=run_web, daemon=True).start()

    app = ApplicationBuilder().token(TOKEN).build()

    # Admin command handlers
    app.add_handler(CommandHandler("setjointitle", setjointitle))
    app.add_handler(CommandHandler("resetjointitle", resetjointitle))
    app.add_handler(CommandHandler("batch", batch))
    app.add_handler(CommandHandler("batchoff", batchoff))
    app.add_handler(CommandHandler("generatebatch", generatebatch))
    app.add_handler(CommandHandler("setchannels", setchannels))
    app.add_handler(CommandHandler("cancelsetchannels", cancelsetchannels))
    app.add_handler(CommandHandler("clearsetchannels", clearsetchannels))
    app.add_handler(CommandHandler("setbutton", setbutton))
    app.add_handler(CommandHandler("cancelsetbutton", cancelsetbutton))
    app.add_handler(CommandHandler("promotext", promotext))
    app.add_handler(CommandHandler("listlinks", listlinks))
    app.add_handler(CallbackQueryHandler(listlinks_pagination, pattern=r"^listlinks_"))
    app.add_handler(CommandHandler("deletelink", deletelink))
    app.add_handler(CommandHandler("deletealllinks", deletealllinks))
    app.add_handler(CommandHandler("settime", settime))
    app.add_handler(CommandHandler("config", showconfig))

    # Access limit / full-access command handlers
    app.add_handler(CommandHandler("setlimit", setlimit))
    app.add_handler(CommandHandler("setcycle", setcycle))
    app.add_handler(CommandHandler("setlimitmsg", setlimitmsg))
    app.add_handler(CommandHandler("setaccessmsg", setaccessmsg))
    app.add_handler(CommandHandler("addaccess", addaccess))
    app.add_handler(CommandHandler("removeaccess", removeaccess))
    app.add_handler(CommandHandler("accesslist", accesslist))
    app.add_handler(CommandHandler("limitconfig", limitconfig))

    # Analytics command handlers
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("toplinks", toplinks))
    app.add_handler(CommandHandler("invalidlinks", invalidlinks))

    app.add_handler(CommandHandler("allcommands", allcommands))
    app.add_handler(CommandHandler("start", start))


    # Callback and message handlers
    app.add_handler(CallbackQueryHandler(fullaccess_callback, pattern=r"^fullaccess$"))
    app.add_handler(CallbackQueryHandler(tryagain_callback, pattern=r"^tryagain\|"))
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & ~filters.COMMAND, handle_input))
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.COMMAND, fallback))

    # Error handler
    app.add_error_handler(error_handler)

    app.run_polling(drop_pending_updates=True)
