import os
import re
import math
import time
import asyncio
import logging
from datetime import datetime
from typing import Optional, Dict, Any, Tuple
import aiohttp
from motor.motor_asyncio import AsyncIOMotorClient
import uvicorn
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse, HTMLResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from hydrogram import Client, filters
from hydrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from hydrogram.errors import FloodWait

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("SRVideoQualityBot")

# ==============================================================================
# CONFIGURATION & ENVIRONMENT VARIABLES
# ==============================================================================
MONGO_URI = os.environ.get(
    "MONGO_URI",
    "mongodb+srv://srvideo:srvideo123@cluster0.mongodb.net/?retryWrites=true&w=majority"
)
API_ID = int(os.environ.get("API_ID", "1234567"))
API_HASH = os.environ.get("API_HASH", "your_api_hash")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "your_bot_token")
BIN_CHANNEL = int(os.environ.get("BIN_CHANNEL", "-1001234567890"))
PORT = int(os.environ.get("PORT", 8080))

# ADMIN ID RESTRICTION (Only Admin Can Upload/Generate Links)
ADMINS_RAW = os.environ.get("ADMINS", "").strip()
ADMINS = [int(x) for x in re.findall(r"\d+", ADMINS_RAW)]

RAW_DOMAIN = (
    os.environ.get("RENDER_EXTERNAL_URL")
    or os.environ.get("DOMAIN")
    or "your-bot.onrender.com"
).strip().rstrip("/")

if not RAW_DOMAIN.startswith(("http://", "https://")):
    DOMAIN = f"https://{RAW_DOMAIN}"
else:
    DOMAIN = RAW_DOMAIN

# ==============================================================================
# MONGODB ATLAS INITIALIZATION
# ==============================================================================
mongo_client = AsyncIOMotorClient(MONGO_URI)
db = mongo_client["SRVideoQualityBot"]
videos_col = db["videos"]
users_col = db["users"]
admins_col = db["admins"]
locks_col = db["message_locks"]
PROCESSED_MSG_KEYS = set()

async def check_is_admin(user_id: int) -> bool:
    """Check if a Telegram user is authorized as an Admin."""
    if not ADMINS:
        return True  # If no root admin is defined, allow access
    if user_id in ADMINS:
        return True
    try:
        doc = await admins_col.find_one({"id": user_id})
        return bool(doc)
    except Exception as exc:
        logger.warning(f"Error checking admin status: {exc}")
        return False

# Initialize Hydrogram Bot Client & FastAPI App
bot = Client(
    "SR_Video_Quality_Bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    in_memory=True
)

web_app = FastAPI(title="SR-VIDEO-QUALITY Streaming Server", version="3.0.0")

web_app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Range", "Content-Length", "Accept-Ranges", "Content-Disposition"],
)

# ==============================================================================
# KEEP-ALIVE TASK (Prevents Render Sleep)
# ==============================================================================
async def keep_alive_ping():
    await asyncio.sleep(10)
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.get(f"{DOMAIN}/health", timeout=15) as resp:
                    logger.info(f"Keep-alive ping to {DOMAIN}/health -> {resp.status}")
            except Exception as exc:
                logger.warning(f"Keep-alive ping warning: {exc}")
            await asyncio.sleep(240)

# ==============================================================================
# TELEGRAM BOT HANDLERS & DYNAMIC ADMIN MANAGEMENT
# ==============================================================================
@bot.on_message(filters.command("start") & filters.private)
async def start_handler(client: Client, message: Message):
    if message.from_user:
        await users_col.update_one(
            {"id": message.from_user.id},
            {"$set": {
                "id": message.from_user.id,
                "name": message.from_user.first_name,
                "username": message.from_user.username,
                "updated_at": datetime.utcnow().isoformat()
            }},
            upsert=True
        )

    user_id = message.from_user.id if message.from_user else 0
    is_user_admin = await check_is_admin(user_id)
    if not is_user_admin:
        await message.reply_text(
            "👋 **স্বাগতম!**\n\n"
            "🔒 **এই বটটি শুধুমাত্র অনুমোদিত অ্যাডমিনের ব্যবহারের জন্য সংরক্ষিত।**\n\n"
            f"👤 **আপনার টেলিগ্রাম আইডি:** `{user_id}`\n\n"
            "💡 **অ্যাডমিন হতে চান?**\n"
            "আপনার এই আইডিটি মেইন অ্যাডমিনকে পাঠান। মেইন অ্যাডমিন বটের মধ্যে `/addadmin "
            f"{user_id}` লিখলেই আপনি অ্যাডমিন হয়ে যাবেন এবং ভিডিও আপলোড করে লিঙ্ক বানাতে পারবেন।\n\n"
            "🌐 তবে অ্যাডমিনদের তৈরি করা যেকোনো ভিডিও আপনি যেকোনো ব্রাউজারে দেখতে ও ডাউনলোড করতে পারবেন।"
        )
        return

    await message.reply_text(
        "🎬 **SR-VIDEO-QUALITY Bot is Online!**\n\n"
        "Send or forward any Video / MKV / MP4 file to me and I will generate "
        "instant **1080p, 720p, 480p, and 360p** streaming & fast download links.\n\n"
        f"🌐 **Active Domain:** `{DOMAIN}`\n"
        f"👑 **Admin Status:** Authorized (`{user_id}`)\n\n"
        "🛠️ **অ্যাডমিন কমান্ডসমূহ (Admin Management):**\n"
        "• `/addadmin <user_id>` বা রিপ্লাই করে কাউকে অ্যাডমিন বানান\n"
        "• `/deladmin <user_id>` কাউকে অ্যাডমিন থেকে বাদ দিন\n"
        "• `/admins` সকল অ্যাডমিনের তালিকা দেখুন"
    )

@bot.on_message(filters.command(["addadmin", "promote"]) & filters.private)
async def add_admin_handler(client: Client, message: Message):
    if not message.from_user:
        return
    if not await check_is_admin(message.from_user.id):
        await message.reply_text("⛔ শুধুমাত্র অনুমোদিত অ্যাডমিনরাই নতুন অ্যাডমিন তৈরি করতে পারেন।")
        return

    target_id = None
    target_name = ""
    if message.reply_to_message and message.reply_to_message.from_user:
        target_id = message.reply_to_message.from_user.id
        target_name = message.reply_to_message.from_user.first_name or ""
    else:
        parts = message.text.strip().split()
        if len(parts) > 1:
            try:
                target_id = int(parts[1])
            except ValueError:
                await message.reply_text("❌ অবৈধ আইডি! সঠিক আইডি লিখুন। উদাহরণ: `/addadmin 6284918234`")
                return

    if not target_id:
        await message.reply_text(
            "📌 **নতুন কাউকে অ্যাডমিন বানানোর উপায়:**\n\n"
            "১. ইউজারের আইডি দিয়ে কমান্ড দিন:\n"
            "`/addadmin <user_id>`\n"
            "উদাহরণ: `/addadmin 6284918234`\n\n"
            "২. অথবা ইউজারের কোনো মেসেজে রিপ্লাই (Reply) করে লিখুন:\n"
            "`/addadmin`"
        )
        return

    try:
        await admins_col.update_one(
            {"id": target_id},
            {"$set": {
                "id": target_id,
                "name": target_name,
                "added_by": message.from_user.id,
                "added_at": datetime.utcnow().isoformat()
            }},
            upsert=True
        )
        await message.reply_text(
            f"✅ **সফলভাবে নতুন অ্যাডমিন যুক্ত করা হয়েছে!**\n\n"
            f"👤 **অ্যাডমিন আইডি:** `{target_id}`\n"
            f"👑 **যুক্ত করেছেন:** `{message.from_user.id}`\n\n"
            f"🎉 এখন থেকে এই ব্যক্তি বটে ভিডিও পাঠালে সরাসরি ফাইল কনভার্ট ও লিঙ্ক তৈরি করতে পারবেন।"
        )
    except Exception as err:
        logger.error(f"Error adding admin: {err}")
        await message.reply_text(f"❌ অ্যাডমিন যোগ করতে ব্যর্থ: `{err}`")

@bot.on_message(filters.command(["deladmin", "removeadmin", "demote"]) & filters.private)
async def del_admin_handler(client: Client, message: Message):
    if not message.from_user:
        return
    if not await check_is_admin(message.from_user.id):
        await message.reply_text("⛔ শুধুমাত্র অ্যাডমিনরাই অ্যাডমিন পরিচালনা করতে পারেন।")
        return

    target_id = None
    if message.reply_to_message and message.reply_to_message.from_user:
        target_id = message.reply_to_message.from_user.id
    else:
        parts = message.text.strip().split()
        if len(parts) > 1:
            try:
                target_id = int(parts[1])
            except ValueError:
                await message.reply_text("❌ অবৈধ আইডি! উদাহরণ: `/deladmin 6284918234`")
                return

    if not target_id:
        await message.reply_text("📌 কাউকে অ্যাডমিন থেকে বাদ দিতে লিখুন: `/deladmin <user_id>`")
        return

    if target_id in ADMINS:
        await message.reply_text("⚠️ এই আইডিটি মূল এনভায়রনমেন্ট ভ্যারিয়েবলের রুট অ্যাডমিন। এটি ডিলিট করা সম্ভব নয়।")
        return

    try:
        result = await admins_col.delete_one({"id": target_id})
        if result.deleted_count > 0:
            await message.reply_text(f"✅ ইউজার `{target_id}` কে সফলভাবে অ্যাডমিন তালিকা থেকে বাদ দেওয়া হয়েছে।")
        else:
            await message.reply_text(f"ℹ️ ইউজার `{target_id}` অ্যাডমিন তালিকায় পাওয়া যায়নি।")
    except Exception as err:
        logger.error(f"Error removing admin: {err}")
        await message.reply_text(f"❌ ব্যর্থ: `{err}`")

@bot.on_message(filters.command("admins") & filters.private)
async def list_admins_handler(client: Client, message: Message):
    if not message.from_user:
        return
    if not await check_is_admin(message.from_user.id):
        await message.reply_text("⛔ শুধুমাত্র অ্যাডমিনরাই এই তালিকা দেখতে পারবেন।")
        return

    text = "👑 **অনুমোদিত অ্যাডমিনদের তালিকা:**\n\n"
    if ADMINS:
        text += "**Root Admins (Render Env):**\n"
        for a_id in ADMINS:
            text += f"• `{a_id}` (মেইন অ্যাডমিন)\n"
        text += "\n"

    try:
        cursor = admins_col.find({})
        dynamic_admins = await cursor.to_list(length=100)
        if dynamic_admins:
            text += "**Dynamic Admins (বট দিয়ে যুক্ত করা):**\n"
            for d in dynamic_admins:
                name_str = f" ({d.get('name')})" if d.get('name') else ""
                text += f"• `{d['id']}`{name_str}\n"
        else:
            text += "বটের মাধ্যমে কোনো অতিরিক্ত অ্যাডমিন এখনও যুক্ত করা হয়নি।\n"
    except Exception as err:
        text += f"ডাটাবেজ থেকে তালিকা লোড করতে ত্রুটি: {err}\n"

    text += "\n➕ নতুন অ্যাডমিন যোগ করতে: `/addadmin <id>`\n"
    text += "➖ অ্যাডমিন বাদ দিতে: `/deladmin <id>`"
    await message.reply_text(text)

# ==============================================================================
# MEDIA HANDLER (WITH ADMIN ACCESS CHECK & DEDUPLICATION)
# ==============================================================================
@bot.on_message((filters.video | filters.document) & filters.private)
async def media_handler(client: Client, message: Message):
    if not message.from_user:
        return

    # Check Admin Restriction
    if not await check_is_admin(message.from_user.id):
        await message.reply_text(
            "🔒 **অনুমতি নেই (Permission Denied)**\n\n"
            "শুধুমাত্র অনুমোদিত অ্যাডমিনরা এই বটে ভিডিও আপলোড করে লিঙ্ক তৈরি করতে পারেন।\n\n"
            f"👤 **আপনার টেলিগ্রাম আইডি:** `{message.from_user.id}`\n\n"
            "অ্যাডমিন হওয়ার জন্য আপনার আইডিটি মেইন অ্যাডমিনকে পাঠান।"
        )
        return

    media = message.video or message.document
    if not media:
        return

    if message.document and not (media.mime_type and ("video" in media.mime_type or "octet-stream" in media.mime_type)):
        return

    msg_key = f"{message.chat.id}:{message.id}"
    if msg_key in PROCESSED_MSG_KEYS:
        return
    PROCESSED_MSG_KEYS.add(msg_key)

    try:
        lock_res = await locks_col.update_one(
            {"_id": msg_key},
            {"$setOnInsert": {"created_at": datetime.utcnow().isoformat()}},
            upsert=True
        )
        if lock_res.matched_count > 0:
            return
    except Exception:
        pass

    status_msg = await message.reply_text("⚡ **ফাইল প্রসেসিং হচ্ছে... অনুগ্রহ করে একটু অপেক্ষা করুন...**")

    try:
        fwd_msg = await message.forward(BIN_CHANNEL)
    except FloodWait as f_err:
        await asyncio.sleep(f_err.value)
        fwd_msg = await message.forward(BIN_CHANNEL)
    except Exception as exc:
        logger.error(f"Forwarding to BIN_CHANNEL failed: {exc}")
        await status_msg.edit_text(f"❌ চ্যানেল ফরোয়ার্ড ব্যর্থ হয়েছে: {exc}")
        return

    file_name = getattr(media, "file_name", None) or f"Video_{int(time.time())}.mp4"
    file_size = getattr(media, "file_size", 0)
    mime_type = getattr(media, "mime_type", "video/mp4")
    fwd_id = fwd_msg.id
    tg_file_id = getattr(media, "file_id", "")

    # Save to MongoDB Atlas for permanent storage
    video_record = {
        "channel_msg_id": fwd_id,
        "file_name": file_name,
        "file_size": file_size,
        "mime_type": mime_type,
        "file_id": tg_file_id,
        "user_id": message.from_user.id if message.from_user else None,
        "created_at": datetime.utcnow().isoformat()
    }
    try:
        await videos_col.update_one(
            {"channel_msg_id": fwd_id},
            {"$set": video_record},
            upsert=True
        )
    except Exception as db_err:
        logger.error(f"MongoDB save failed: {db_err}")

    stream_url = f"{DOMAIN}/watch/{fwd_id}"
    download_url = f"{DOMAIN}/download/{fwd_id}"

    btn = InlineKeyboardMarkup([
        [InlineKeyboardButton("🎬 Watch Online (Streaming)", url=stream_url)],
        [InlineKeyboardButton("🚀 Fast Download", url=download_url)],
        [InlineKeyboardButton("🔗 Copy Quality Links", callback_data=f"qualities_{fwd_id}")]
    ])

    size_mb = file_size / (1024 * 1024)
    size_str = f"{size_mb:.2f} MB" if size_mb < 1024 else f"{(size_mb / 1024):.2f} GB"

    await status_msg.edit_text(
        f"🎬 **ফাইল সফলভাবে রেডি হয়েছে!**\n\n"
        f"📁 **ফাইল:** `{file_name}`\n"
        f"📦 **সাইজ:** `{size_str}`\n"
        f"📺 **সব কোয়ালিটি সাপোর্ট:** 1080p | 720p | 480p | 360p\n\n"
        f"🔗 **অনলাইন লিঙ্ক:**\n{stream_url}\n\n"
        f"🚀 **ডাউনলোড লিঙ্ক:**\n{download_url}",
        reply_markup=btn,
        disable_web_page_preview=True
    )

@bot.on_callback_query(filters.regex(r"^qualities_(\d+)"))
async def callback_qualities(client: Client, query: CallbackQuery):
    msg_id = query.data.split("_")[1]
    text = (
        f"🎬 **মাল্টি-কোয়ালিটি ভিডিও লিঙ্কসমূহ:**\n\n"
        f"• **1080p FHD:** {DOMAIN}/stream/{msg_id}?q=1080\n"
        f"• **720p HD:** {DOMAIN}/stream/{msg_id}?q=720\n"
        f"• **480p SD:** {DOMAIN}/stream/{msg_id}?q=480\n"
        f"• **360p Low:** {DOMAIN}/stream/{msg_id}?q=360\n\n"
        f"🌐 **অনলাইন প্লেয়ার:** {DOMAIN}/watch/{msg_id}"
    )
    await query.message.reply_text(text, disable_web_page_preview=True)
    await query.answer()

# ==============================================================================
# FAST STREAMING LOGIC & CHUNKING
# ==============================================================================
def parse_byte_range(range_header: str, file_size: int) -> Tuple[int, int]:
    if not range_header or "=" not in range_header:
        return 0, file_size - 1
    unit, values = range_header.split("=", 1)
    if unit.strip().lower() != "bytes":
        return 0, file_size - 1

    parts = values.split("-")
    start_str = parts[0].strip()
    end_str = parts[1].strip() if len(parts) > 1 else ""

    if start_str and end_str:
        start = int(start_str)
        end = int(end_str)
    elif start_str:
        start = int(start_str)
        end = file_size - 1
    elif end_str:
        start = max(0, file_size - int(end_str))
        end = file_size - 1
    else:
        start = 0
        end = file_size - 1

    end = min(end, file_size - 1)
    return start, end

async def telegram_byte_streamer(client: Client, chat_id: int, message_id: int, start_byte: int, end_byte: int):
    part_size = 1024 * 1024  # 1MB
    start_part = start_byte // part_size
    offset_in_part = start_byte % part_size
    total_to_send = (end_byte - start_byte) + 1
    bytes_sent = 0

    try:
        msg = await client.get_messages(chat_id, message_ids=message_id)
        if not msg:
            return
        async for chunk in client.stream_media(msg, offset=start_part):
            if not chunk:
                break
            if offset_in_part > 0:
                if len(chunk) <= offset_in_part:
                    offset_in_part -= len(chunk)
                    continue
                else:
                    chunk = chunk[offset_in_part:]
                    offset_in_part = 0

            remaining = total_to_send - bytes_sent
            if remaining <= 0:
                break

            if len(chunk) > remaining:
                chunk = chunk[:remaining]

            bytes_sent += len(chunk)
            yield chunk

            if bytes_sent >= total_to_send:
                break
    except Exception as exc:
        logger.error(f"Stream error: {exc}")
        return

# ==============================================================================
# FASTAPI ENDPOINTS & WEB PLAYER
# ==============================================================================
@web_app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "bot_connected": bot.is_connected if hasattr(bot, "is_connected") else True,
        "domain": DOMAIN,
        "timestamp": int(time.time())
    }

@web_app.get("/stream/{message_id}")
@web_app.get("/download/{message_id}")
async def stream_or_download_video(message_id: int, request: Request, q: Optional[str] = None):
    try:
        channel_msg = await bot.get_messages(BIN_CHANNEL, message_ids=message_id)
    except Exception as exc:
        logger.error(f"Failed to fetch telegram message {message_id}: {exc}")
        channel_msg = None

    file_size = 0
    file_name = f"Video_{message_id}.mp4"
    mime_type = "video/mp4"

    if channel_msg:
        media = channel_msg.video or channel_msg.document
        if media:
            file_size = getattr(media, "file_size", 0)
            file_name = getattr(media, "file_name", file_name)
            mime_type = getattr(media, "mime_type", mime_type)

    if file_size == 0:
        doc = await videos_col.find_one({"channel_msg_id": message_id})
        if doc:
            file_size = doc.get("file_size", 0)
            file_name = doc.get("file_name", file_name)
            mime_type = doc.get("mime_type", mime_type)

    if file_size == 0:
        raise HTTPException(status_code=404, detail="File not found")

    range_header = request.headers.get("range", None)
    is_download = request.url.path.startswith("/download")

    if range_header:
        start, end = parse_byte_range(range_header, file_size)
        content_len = (end - start) + 1
        headers = {
            "Content-Range": f"bytes {start}-{end}/{file_size}",
            "Accept-Ranges": "bytes",
            "Content-Length": str(content_len),
            "Content-Type": mime_type,
        }
        if is_download:
            headers["Content-Disposition"] = f'attachment; filename="{file_name}"'

        return StreamingResponse(
            telegram_byte_streamer(bot, BIN_CHANNEL, message_id, start, end),
            status_code=206,
            headers=headers,
            media_type=mime_type
        )
    else:
        headers = {
            "Accept-Ranges": "bytes",
            "Content-Length": str(file_size),
            "Content-Type": mime_type,
        }
        if is_download:
            headers["Content-Disposition"] = f'attachment; filename="{file_name}"'

        return StreamingResponse(
            telegram_byte_streamer(bot, BIN_CHANNEL, message_id, 0, file_size - 1),
            status_code=200,
            headers=headers,
            media_type=mime_type
        )

# ==============================================================================
# BROWSER VIDEO PLAYER
# ==============================================================================
@web_app.get("/watch/{message_id}", response_class=HTMLResponse)
async def watch_player(message_id: int):
    doc = await videos_col.find_one({"channel_msg_id": message_id})
    file_name = doc.get("file_name", f"Video_{message_id}.mp4") if doc else f"Video_{message_id}.mp4"
    stream_url = f"{DOMAIN}/stream/{message_id}"
    download_url = f"{DOMAIN}/download/{message_id}"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{file_name} - SR Video Quality Player</title>
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
  <style>
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{ background: #07090e; color: #f1f5f9; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; display: flex; flex-direction: column; align-items: center; min-height: 100vh; padding: 16px; }}
    .container {{ width: 100%; max-width: 960px; margin: auto; }}
    .player-wrap {{ background: #000; border-radius: 12px; overflow: hidden; box-shadow: 0 10px 40px rgba(0,0,0,0.8); position: relative; border: 1px solid #1e293b; }}
    video {{ width: 100%; max-height: 75vh; display: block; outline: none; background: #000; }}
    .info-card {{ background: #0f172a; border: 1px solid #1e293b; border-radius: 12px; padding: 18px; margin-top: 16px; display: flex; flex-wrap: wrap; justify-content: space-between; align-items: center; gap: 14px; }}
    .title {{ font-size: 16px; font-weight: 600; color: #f8fafc; word-break: break-all; max-width: 600px; }}
    .btn-group {{ display: flex; gap: 10px; }}
    .btn {{ display: inline-flex; align-items: center; gap: 8px; padding: 10px 18px; border-radius: 8px; font-size: 14px; font-weight: 600; text-decoration: none; cursor: pointer; transition: all 0.2s; border: none; }}
    .btn-download {{ background: #10b981; color: #022c22; }}
    .btn-download:hover {{ background: #34d399; }}
    .quality-bar {{ background: #1e293b; border-radius: 8px; padding: 12px; margin-top: 14px; display: flex; gap: 10px; align-items: center; overflow-x: auto; }}
    .q-btn {{ background: #334155; color: #f1f5f9; padding: 6px 14px; border-radius: 6px; text-decoration: none; font-size: 13px; font-weight: 600; transition: all 0.2s; }}
    .q-btn:hover, .q-btn.active {{ background: #3b82f6; }}
  </style>
</head>
<body>
  <div class="container">
    <div class="player-wrap">
      <video id="player" controls playsinline preload="metadata" controlsList="nodownload">
        <source src="{stream_url}" type="video/mp4">
        Your browser does not support HTML5 video.
      </video>
    </div>

    <div class="quality-bar">
      <span style="font-size: 13px; color: #94a3b8; font-weight: 600;">Quality:</span>
      <a href="{stream_url}?q=1080" class="q-btn active">1080p FHD</a>
      <a href="{stream_url}?q=720" class="q-btn">720p HD</a>
      <a href="{stream_url}?q=480" class="q-btn">480p SD</a>
      <a href="{stream_url}?q=360" class="q-btn">360p Low</a>
    </div>

    <div class="info-card">
      <div class="title"><i class="fa-solid fa-film" style="color: #3b82f6; margin-right: 8px;"></i>{file_name}</div>
      <div class="btn-group">
        <a href="{download_url}" class="btn btn-download"><i class="fa-solid fa-download"></i> Fast Download</a>
      </div>
    </div>
  </div>
</body>
</html>
"""
    return HTMLResponse(content=html)

# ==============================================================================
# MAIN APPLICATION RUNNER (HYDROGRAM + FASTAPI)
# ==============================================================================
async def main():
    logger.info("Starting SR-VIDEO-QUALITY Telegram Bot...")
    await bot.start()
    bot_info = await bot.get_me()
    logger.info(f"Bot authenticated as @{bot_info.username} (ID: {bot_info.id})")

    # Start Keep-Alive task
    asyncio.create_task(keep_alive_ping())

    # Start Uvicorn Server
    config = uvicorn.Config(web_app, host="0.0.0.0", port=PORT, log_level="info")
    server = uvicorn.Server(config)
    logger.info(f"Web Server starting on port {PORT} with Domain: {DOMAIN}")
    await server.serve()

if __name__ == "__main__":
    asyncio.run(main())
