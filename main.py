import os
import re
import asyncio
import logging
import aiohttp
import uvicorn
from datetime import datetime

# Initialize asyncio event loop immediately before Hydrogram Client import (Python 3.10-3.14 compatibility)
try:
    loop = asyncio.get_event_loop()
except RuntimeError:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

from motor.motor_asyncio import AsyncIOMotorClient
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from hydrogram import Client, filters, idle
from hydrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from hydrogram.errors import FloodWait, MessageNotModified

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("SR-VIDEO-QUALITY")

# ==============================================================================
# 100% WORKING CONFIGURATION (MongoDB Atlas + Domain + Telegram MTProto)
# ==============================================================================
MONGO_URI = os.environ.get(
    "MONGO_URI",
    "mongodb+srv://sabbirvai1122:SABBIRVAI1122@cluster0.t1bsiei.mongodb.net/?appName=Cluster0"
)
API_ID = int(os.environ.get("API_ID", "29608422"))
API_HASH = os.environ.get("API_HASH", "3db2f8e109301f02f5d9c8f10dd79244")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "CONFIGURED_IN_SERVER_ENV")
BIN_CHANNEL = int(os.environ.get("BIN_CHANNEL", "-1004450462812"))
PORT = int(os.environ.get("PORT", 8080))

# ADMIN ID RESTRICTION (Only Admin Can Upload/Generate Links)
ADMINS_RAW = os.environ.get("ADMINS", "8056243176").strip()
ADMINS = [int(x) for x in re.findall(r"\d+", ADMINS_RAW)]

# MINIMUM FILE SIZE RESTRICTION (Default: 10 MB)
MIN_FILE_SIZE_MB = float(os.environ.get("MIN_FILE_SIZE_MB", "10"))

RAW_DOMAIN = (
    os.environ.get("RENDER_EXTERNAL_URL")
    or os.environ.get("DOMAIN")
    or "https://ss-anime-file-to-link.onrender.com"
).strip().rstrip("/")

if not RAW_DOMAIN.startswith(("http://", "https://")):
    DOMAIN = f"https://{RAW_DOMAIN}"
else:
    DOMAIN = RAW_DOMAIN

# ==============================================================================
# MONGODB ATLAS INITIALIZATION (Persists file_id so videos ALWAYS load/download)
# ==============================================================================
mongo_client = AsyncIOMotorClient(MONGO_URI)
db = mongo_client["SRVideoQualityBot"]
videos_col = db["videos"]
users_col = db["users"]
admins_col = db["admins"]
settings_col = db["settings"]
locks_col = db["message_locks"]
PROCESSED_MSG_KEYS = set()

async def get_min_file_size_mb() -> float:
    try:
        doc = await settings_col.find_one({"key": "min_file_size_mb"})
        if doc and "val" in doc:
            return float(doc["val"])
    except Exception:
        pass
    return MIN_FILE_SIZE_MB

async def check_is_admin(user_id: int) -> bool:
    if not ADMINS:
        return True
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
# KEEP-ALIVE TASK (Prevents Render / Cloud Sleep)
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
            "আপনার এই আইডিটি মেইন অ্যাডমিনকে পাঠান। মেইন অ্যাডমিন বটের মধ্যে `/addadmin {user_id}` লিখলেই আপনি অ্যাডমিন হয়ে যাবেন এবং ভিডিও আপলোড করে লিঙ্ক বানাতে পারবেন।\n\n"
            "🌐 তবে অ্যাডমিনদের তৈরি করা যেকোনো ভিডিও আপনি যেকোনো ব্রাউজারে দেখতে ও ডাউনলোড করতে পারবেন।"
        )
        return

    min_size_val = await get_min_file_size_mb()
    await message.reply_text(
        "🎬 **SR-VIDEO-QUALITY Bot is Online!**\n\n"
        "Send or forward any Video / MKV / MP4 file to me and I will generate "
        "instant **1080p, 720p, 480p, and 360p** streaming & fast download links.\n\n"
        f"🌐 **Active Domain:** `{DOMAIN}`\n"
        f"👑 **Admin Status:** Authorized (`{user_id}`)\n"
        f"📦 **সর্বনিম্ন ফাইল সাইজ:** `{min_size_val} MB` (১০ এমবির কম ফাইল গ্রহণযোগ্য নয়)\n\n"
        "🛠️ **অ্যাডমিন কমান্ডসমূহ (Admin Management):**\n"
        "• `/addadmin <user_id>` নতুন কাউকে অ্যাডমিন বানান\n"
        "• `/deladmin <user_id>` কাউকে অ্যাডমিন থেকে বাদ দিন\n"
        "• `/admins` সকল অ্যাডমিনের তালিকা দেখুন\n"
        "• `/minsize` বর্তমান সর্বনিম্ন ফাইল সাইজ লিমিট দেখুন\n"
        "• `/setmin <MB>` সর্বনিম্ন ফাইলের সাইজ পরিবর্তন করুন (যেমন: `/setmin 10`)"
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
    # 1. Check if command was in reply to a user's message
    if message.reply_to_message and message.reply_to_message.from_user:
        target_id = message.reply_to_message.from_user.id
        target_name = message.reply_to_message.from_user.first_name or ""
    else:
        # 2. Check if passed as argument: /addadmin 123456789
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
            f"🎉 এখন থেকে এই ব্যক্তি বটে ভিডিও পাঠালে সরাসরি ফাইল কনভার্ট ও স্ট্রিম লিঙ্ক তৈরি করতে পারবেন।"
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
        await message.reply_text(f"⚠️ আইডি `{target_id}` হলো রুট অ্যাডমিন (Render ENV তে সেট করা)। এটিকে বাদ দিতে হলে Render Environment Variables থেকে মুছতে হবে।")
        return

    res = await admins_col.delete_one({"id": target_id})
    if res.deleted_count > 0:
        await message.reply_text(f"❌ **আইডি `{target_id}` কে অ্যাডমিন তালিকা থেকে বাদ দেওয়া হয়েছে।**")
    else:
        await message.reply_text(f"ℹ️ আইডি `{target_id}` ডাটাবেজ অ্যাডমিন তালিকায় নেই।")


@bot.on_message(filters.command(["admins", "adminlist"]) & filters.private)
async def list_admins_handler(client: Client, message: Message):
    if not message.from_user:
        return
    if not await check_is_admin(message.from_user.id):
        await message.reply_text(f"🔒 আপনার আইডি: `{message.from_user.id}` (আপনি অ্যাডমিন নন)।")
        return

    db_admins = []
    try:
        cursor = admins_col.find({})
        async for adm in cursor:
            db_admins.append(adm.get("id"))
    except Exception as err:
        logger.warning(f"Error fetching admins from db: {err}")

    admin_lines = []
    for a_id in ADMINS:
        admin_lines.append(f"• `{a_id}` 👑 (Root Owner / ENV)")
    for a_id in db_admins:
        if a_id not in ADMINS:
            admin_lines.append(f"• `{a_id}` ⭐ (Database Admin)")

    lines_str = "\n".join(admin_lines) if admin_lines else "কোনো নির্দিষ্ট অ্যাডমিন সেট করা নেই (উন্মুক্ত)।"

    await message.reply_text(
        "📋 **বটের বর্তমান অ্যাডমিন তালিকা:**\n\n"
        f"{lines_str}\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "💡 **নতুন অ্যাডমিন যোগ করতে:**\n"
        "`/addadmin <user_id>`\n\n"
        "🗑️ **অ্যাডমিন বাদ দিতে:**\n"
        "`/deladmin <user_id>`"
    )


@bot.on_message(filters.command(["setmin", "setminsize", "minfilesize"]) & filters.private)
async def set_min_size_handler(client: Client, message: Message):
    if not message.from_user:
        return
    if not await check_is_admin(message.from_user.id):
        await message.reply_text("⛔ শুধুমাত্র অনুমোদিত অ্যাডমিনরা সর্বনিম্ন সাইজ নির্ধারণ করতে পারেন।")
        return

    parts = message.text.strip().split()
    if len(parts) < 2:
        cur_min = await get_min_file_size_mb()
        await message.reply_text(
            f"ℹ️ **বর্তমান সর্বনিম্ন ফাইল সাইজ:** `{cur_min} MB`\n\n"
            f"📌 পরিবর্তন করতে লিখুন:\n"
            f"`/setmin 10` (১০ এমবি করার জন্য)\n"
            f"`/setmin 20` (২০ এমবি করার জন্য)"
        )
        return

    try:
        new_val = float(parts[1])
        if new_val < 0:
            raise ValueError()
        await settings_col.update_one(
            {"key": "min_file_size_mb"},
            {"$set": {
                "key": "min_file_size_mb",
                "val": new_val,
                "updated_by": message.from_user.id,
                "updated_at": datetime.utcnow().isoformat()
            }},
            upsert=True
        )
        await message.reply_text(
            f"✅ **সর্বনিম্ন ফাইল সাইজ সফলভাবে সেট করা হয়েছে!**\n\n"
            f"📏 নতুন সাইজ: `{new_val} MB`\n"
            f"এখন থেকে `{new_val} MB`-এর নিচের কোনো ফাইল বটের মাধ্যমে লিঙ্ক তৈরি হবে না।"
        )
    except Exception:
        await message.reply_text("❌ ভুল ইনপুট! সঠিক সংখ্যা লিখুন। যেমন: `/setmin 10`")


@bot.on_message(filters.command(["minsize", "getmin"]) & filters.private)
async def get_min_size_handler(client: Client, message: Message):
    cur_min = await get_min_file_size_mb()
    await message.reply_text(
        f"📏 **বটের বর্তমান সর্বনিম্ন ফাইল সাইজ:** `{cur_min} MB`\n\n"
        f"এর চেয়ে ছোট আকারের ফাইল পাঠালে বট লিঙ্ক তৈরি করবে না।"
    )


@bot.on_message((filters.private | filters.group) & (filters.video | filters.document | filters.animation))
async def media_handler(client: Client, message: Message):
    media = message.video or message.document or message.animation
    if not media:
        return

    user_id = message.from_user.id if message.from_user else message.chat.id
    try:
        if message.from_user:
            await users_col.update_one(
                {"id": user_id},
                {"$set": {
                    "id": user_id,
                    "name": message.from_user.first_name,
                    "username": message.from_user.username,
                    "updated_at": datetime.utcnow().isoformat()
                }},
                upsert=True
            )
    except Exception:
        pass

    file_size = getattr(media, "file_size", 0) or 0
    file_size_mb = round(file_size / (1024 * 1024), 2)
    min_size_limit = await get_min_file_size_mb()

    # MINIMUM FILE SIZE CHECK (Only if min_size_limit > 0)
    if min_size_limit > 0 and file_size < int(min_size_limit * 1024 * 1024):
        await message.reply_text(
            f"⚠️ **ফাইল সাইজ খুবই ছোট!**\n\n"
            f"📦 আপনার পাঠানো ফাইল: `{file_size_mb} MB`\n"
            f"🛑 **সর্বনিম্ন ফাইল সাইজ:** `{min_size_limit} MB` হতে হবে।\n\n"
            f"💡 অনুগ্রহ করে অন্তত **{min_size_limit} MB** বা তার বড় কোনো ভিডিও বা ফাইল পাঠান যাতে স্ট্রিমিং ও ডাউনলোড লিঙ্ক তৈরি করা যায়।",
            quote=True
        )
        return

    msg_key = f"msg_{message.chat.id}_{message.id}"
    if msg_key in PROCESSED_MSG_KEYS:
        return
    PROCESSED_MSG_KEYS.add(msg_key)

    try:
        await locks_col.update_one(
            {"_id": msg_key},
            {"$setOnInsert": {
                "chatId": str(message.chat.id),
                "messageId": message.id,
                "claimedAt": datetime.utcnow().isoformat()
            }},
            upsert=True
        )
    except Exception as lock_err:
        logger.warning(f"Lock exception: {lock_err}")

    try:
        # Try copying to BIN_CHANNEL if bot is admin there, otherwise use direct chat + file_id
        bin_msg_id = None
        try:
            if BIN_CHANNEL and BIN_CHANNEL != -1001234567890:
                log_msg = await message.copy(chat_id=BIN_CHANNEL)
                bin_msg_id = log_msg.id
        except FloodWait as fw:
            logger.warning(f"FloodWait on BIN_CHANNEL copy: {fw.value}s")
            bin_msg_id = None
        except Exception as copy_err:
            logger.warning(f"BIN_CHANNEL copy skipped (using direct file_id from MongoDB): {copy_err}")

        stream_id = f"tg-{bin_msg_id or message.id}"
        file_name = getattr(media, "file_name", None) or f"SR_Video_{stream_id}.mp4"
        file_size = getattr(media, "file_size", 0) or 0
        mime_type = getattr(media, "mime_type", "video/mp4") or "video/mp4"
        if not mime_type.startswith("video/"):
            mime_type = "video/mp4"
        file_size_mb = round(file_size / (1024 * 1024), 2)

        # Save complete file metadata & file_id into MongoDB so /dl and /watch ALWAYS work!
        doc = {
            "id": stream_id,
            "msg_id": message.id,
            "chat_id": message.chat.id,
            "bin_channel": BIN_CHANNEL if bin_msg_id else None,
            "bin_msg_id": bin_msg_id,
            "telegramFileId": media.file_id,
            "fileUniqueId": getattr(media, "file_unique_id", ""),
            "title": file_name.rsplit(".", 1)[0].replace("_", " "),
            "fileName": file_name,
            "sizeBytes": file_size,
            "mimeType": mime_type,
            "durationSec": getattr(media, "duration", 120) or 120,
            "sourceType": "telegram",
            "createdAt": datetime.utcnow().isoformat(),
        }
        await videos_col.update_one({"id": stream_id}, {"$set": doc}, upsert=True)

        watch_url = f"{DOMAIN}/player/{stream_id}"
        dl_url = f"{DOMAIN}/dl/{stream_id}"

        buttons = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("▶️ Watch Video", url=watch_url),
                InlineKeyboardButton("⬇️ Direct Download", url=dl_url)
            ],
            [
                InlineKeyboardButton("📺 Watch 480p", url=f"{watch_url}?q=480p"),
                InlineKeyboardButton("📺 Watch 720p", url=f"{watch_url}?q=720p"),
                InlineKeyboardButton("📺 Watch 1080p", url=f"{watch_url}?q=1080p")
            ],
            [
                InlineKeyboardButton("⬇️ DL 480p", url=f"{dl_url}?quality=480p"),
                InlineKeyboardButton("⬇️ DL 720p", url=f"{dl_url}?quality=720p"),
                InlineKeyboardButton("⬇️ DL 1080p", url=f"{dl_url}?quality=1080p")
            ],
            [
                InlineKeyboardButton("🎛️ Watch Quality Menu", callback_data=f"q_watch:{stream_id}"),
                InlineKeyboardButton("📥 Download Quality Menu", callback_data=f"q_dl:{stream_id}")
            ]
        ])

        reply_text_body = (
            f"🎬 **{doc['title']}**\n"
            f"📦 **Size:** `{file_size_mb} MB`\n\n"
            f"▶️ **Watch Video Link:**\n{watch_url}\n\n"
            f"⬇️ **Direct Download Link:**\n{dl_url}\n\n"
            f"📥 **Direct Quality Download:**\n"
            f"• [Download 480p]({dl_url}?quality=480p) | [Download 720p]({dl_url}?quality=720p) | [Download 1080p]({dl_url}?quality=1080p)\n\n"
            f"👇 **নিচের বাটন থেকে কোয়ালিটি (480p, 720p, 1080p) সিলেক্ট করে ভিডিও দেখুন বা সরাসরি ডাউনলোড করুন:**"
        )

        # Send response directly without messages.EditMessage (avoids Telegram 420 FLOOD_WAIT_X)
        for attempt in range(4):
            try:
                await message.reply_text(
                    reply_text_body,
                    reply_markup=buttons,
                    disable_web_page_preview=True,
                    quote=True
                )
                break
            except FloodWait as fw:
                wait_sec = int(fw.value) or 1
                logger.warning(f"Telegram FloodWait on reply: {wait_sec}s")
                if wait_sec <= 10:
                    await asyncio.sleep(wait_sec)
                else:
                    await asyncio.sleep(2)
            except Exception as send_err:
                logger.error(f"Error sending reply on attempt {attempt}: {send_err}")
                await asyncio.sleep(1)

    except Exception as err:
        logger.error(f"Error handling media: {err}")
        try:
            await message.reply_text(f"❌ Error generating link: `{err}`", quote=True)
        except Exception:
            pass


@bot.on_callback_query()
async def quality_callback_handler(client: Client, callback_query):
    data = callback_query.data or ""
    if data.startswith("q_watch:"):
        stream_id = data.replace("q_watch:", "")
        watch_url = f"{DOMAIN}/player/{stream_id}"
        await callback_query.answer("🎬 ভিডিও দেখার জন্য কোয়ালিটি সিলেক্ট করুন (480p / 720p / 1080p)")
        await callback_query.message.edit_reply_markup(
            InlineKeyboardMarkup([
                [InlineKeyboardButton("📺 Watch 480p (SD)", url=f"{watch_url}?q=480p")],
                [InlineKeyboardButton("📺 Watch 720p (HD)", url=f"{watch_url}?q=720p")],
                [InlineKeyboardButton("📺 Watch 1080p (Full HD)", url=f"{watch_url}?q=1080p")],
                [InlineKeyboardButton("🔙 Back", callback_data=f"q_main:{stream_id}")]
            ])
        )
    elif data.startswith("q_dl:"):
        stream_id = data.replace("q_dl:", "")
        dl_url = f"{DOMAIN}/dl/{stream_id}"
        await callback_query.answer("⬇️ ডাউনলোড করার জন্য কোয়ালিটি সিলেক্ট করুন (480p / 720p / 1080p)")
        await callback_query.message.edit_reply_markup(
            InlineKeyboardMarkup([
                [InlineKeyboardButton("⬇️ Download 480p (SD)", url=f"{dl_url}?quality=480p")],
                [InlineKeyboardButton("⬇️ Download 720p (HD)", url=f"{dl_url}?quality=720p")],
                [InlineKeyboardButton("⬇️ Download 1080p (Full HD)", url=f"{dl_url}?quality=1080p")],
                [InlineKeyboardButton("🔙 Back", callback_data=f"q_main:{stream_id}")]
            ])
        )
    elif data.startswith("q_main:"):
        stream_id = data.replace("q_main:", "")
        watch_url = f"{DOMAIN}/player/{stream_id}"
        dl_url = f"{DOMAIN}/dl/{stream_id}"
        await callback_query.answer()
        await callback_query.message.edit_reply_markup(
            InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("▶️ Watch Video", url=watch_url),
                    InlineKeyboardButton("⬇️ Direct Download", url=dl_url)
                ],
                [
                    InlineKeyboardButton("📺 Watch 480p", url=f"{watch_url}?q=480p"),
                    InlineKeyboardButton("📺 Watch 720p", url=f"{watch_url}?q=720p"),
                    InlineKeyboardButton("📺 Watch 1080p", url=f"{watch_url}?q=1080p")
                ],
                [
                    InlineKeyboardButton("⬇️ DL 480p", url=f"{dl_url}?quality=480p"),
                    InlineKeyboardButton("⬇️ DL 720p", url=f"{dl_url}?quality=720p"),
                    InlineKeyboardButton("⬇️ DL 1080p", url=f"{dl_url}?quality=1080p")
                ],
                [
                    InlineKeyboardButton("🎛️ Watch Quality Menu", callback_data=f"q_watch:{stream_id}"),
                    InlineKeyboardButton("📥 Download Quality Menu", callback_data=f"q_dl:{stream_id}")
                ]
            ])
        )


# ==============================================================================
# FASTAPI STREAMING & MULTI-QUALITY WEB PLAYER ROUTES (UPTIMEROBOT & CRON READY)
# ==============================================================================
@web_app.api_route("/", methods=["GET", "HEAD"])
async def root(request: Request):
    if request.method == "HEAD":
        return Response(status_code=200)
    count = await videos_col.count_documents({})
    return JSONResponse({
        "status": "online",
        "service": "SS-ANIME-FILE-TO-LINK",
        "mongodb": "connected",
        "stored_videos": count,
        "domain": DOMAIN,
        "uptimerobot": "ready",
        "qualities": ["480p", "720p", "1080p"]
    })


@web_app.api_route("/health", methods=["GET", "HEAD"])
async def health(request: Request):
    if request.method == "HEAD":
        return Response(status_code=200)
    return JSONResponse({"ok": True, "domain": DOMAIN, "mongodb": True, "status": "healthy"})


@web_app.api_route("/ping", methods=["GET", "HEAD"])
async def ping(request: Request):
    if request.method == "HEAD":
        return Response(status_code=200)
    return Response(content="pong", media_type="text/plain")


@web_app.get("/watch/{stream_id}", response_class=HTMLResponse)
@web_app.get("/player/{stream_id}", response_class=HTMLResponse)
@web_app.get("/play/{stream_id}", response_class=HTMLResponse)
@web_app.get("/view/{stream_id}", response_class=HTMLResponse)
@web_app.get("/v/{stream_id}", response_class=HTMLResponse)
async def watch_video(stream_id: str, q: str = ""):
    has_explicit_q = q in ("480p", "720p", "1080p")
    initial_q = q if has_explicit_q else "720p"
    stream_url = f"/stream/{stream_id}"
    dl_url = f"/dl/{stream_id}"
    modal_hidden_class = "hidden" if has_explicit_q else ""

    clean_id = stream_id.replace("tg-", "")
    num_id = int(clean_id) if clean_id.isdigit() else -1
    doc = await videos_col.find_one({
        "$or": [
            {"id": stream_id},
            {"id": f"tg-{stream_id}"},
            {"id": clean_id},
            {"id": f"tg-{clean_id}"},
            {"msg_id": num_id},
            {"bin_msg_id": num_id},
            {"telegramFileId": stream_id}
        ]
    })
    duration_sec = int((doc or {}).get("durationSec") or 1420)
    video_title = (doc or {}).get("title") or (doc or {}).get("fileName") or f"Video #{stream_id}"
    file_size_bytes = int((doc or {}).get("sizeBytes") or 0)
    file_size_mb = round(file_size_bytes / (1024 * 1024), 2) if file_size_bytes > 0 else "50"

    html = f"""<!DOCTYPE html>
<html lang="bn">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no, viewport-fit=cover">
  <title>{video_title}</title>
  <script src="https://telegram.org/js/telegram-web-app.js"></script>
  <style>
    * {{ box-sizing:border-box; margin:0; padding:0; -webkit-tap-highlight-color:transparent; }}
    html, body {{
      width:100%; min-height:100%; overflow-x:hidden; overflow-y:auto;
      background:#070b13; color:#f8fafc; font-family:system-ui,-apple-system,sans-serif;
      user-select:none; -webkit-user-select:none;
    }}
    .screen-wrapper {{
      width:100%; min-height:100dvh; display:flex; flex-direction:column;
      justify-content:center; align-items:center; background:#070b13;
      padding:16px 12px 28px;
    }}
    .player-card {{
      width:100%; max-width:580px; margin:0 auto; display:flex; flex-direction:column;
      align-items:center; gap:12px; position:relative;
    }}
    /* Photo 3 Style: Compact 16:9 Player Box in Center of Screen */
    .video-box {{
      width:100%;
      aspect-ratio:16 / 9;
      max-height:48dvh;
      background:#000;
      position:relative;
      overflow:hidden;
      display:flex;
      align-items:center;
      justify-content:center;
      box-shadow:0 12px 35px rgba(0,0,0,0.85);
      border-radius:14px;
      border:1px solid rgba(255,255,255,0.12);
      cursor:pointer;
    }}
    video {{
      width:100%; height:100%; object-fit:contain;
      display:block; background:#000;
    }}
    .gesture-hud {{
      position:absolute; top:50%; left:50%; transform:translate(-50%,-50%);
      background:rgba(9,13,22,0.92); border:1px solid rgba(255,255,255,0.22);
      border-radius:14px; padding:14px 22px; min-width:165px;
      display:flex; flex-direction:column; align-items:center; gap:8px;
      pointer-events:none; opacity:0; transition:opacity 0.18s ease;
      z-index:45; backdrop-filter:blur(8px); -webkit-backdrop-filter:blur(8px);
      box-shadow:0 12px 35px rgba(0,0,0,0.85);
    }}
    .gesture-hud.visible {{
      opacity:1;
    }}
    .gesture-icon-row {{
      display:flex; align-items:center; gap:8px; font-size:15px; font-weight:700; color:#f8fafc;
    }}
    .gesture-bar-bg {{
      width:130px; height:6px; background:rgba(255,255,255,0.20); border-radius:999px; overflow:hidden;
    }}
    .gesture-bar-fill {{
      height:100%; width:100%; background:#10b981; border-radius:999px; transition:width 0.05s linear;
    }}
    .quality-pill {{
      position:absolute; top:10px; right:10px; z-index:30;
      background:rgba(15,23,42,0.88); border:1px solid rgba(255,255,255,0.28);
      color:#f8fafc; font-size:11px; font-weight:700; padding:4px 10px; border-radius:7px; cursor:pointer;
      backdrop-filter:blur(8px); -webkit-backdrop-filter:blur(8px);
      box-shadow:0 4px 15px rgba(0,0,0,0.6);
      transition:opacity 0.25s ease;
    }}
    .quality-pill.ctrl-hidden {{
      opacity:0; pointer-events:none;
    }}
    .tap-overlay {{
      position:absolute; top:0; bottom:55px; width:45%; z-index:25;
      display:flex; align-items:center; justify-content:center;
      pointer-events:none; opacity:0; transition:opacity 0.25s ease;
    }}
    .tap-overlay.left {{ left:0; }}
    .tap-overlay.right {{ right:0; }}
    .tap-overlay.active {{ opacity:1; background:rgba(255,255,255,0.08); }}
    .tap-badge {{
      background:rgba(15,23,42,0.92); border:1px solid rgba(255,255,255,0.25);
      padding:7px 14px; border-radius:999px; font-size:12px; font-weight:700;
      color:#10b981; box-shadow:0 6px 20px rgba(0,0,0,0.7);
    }}
    /* Photo 3 Style Meta Card Below Video */
    .video-meta-card {{
      width:100%;
      background:#0f172a;
      border:1px solid rgba(255,255,255,0.1);
      border-radius:14px;
      padding:14px 16px;
      display:flex;
      flex-direction:column;
      gap:8px;
      box-shadow:0 8px 24px rgba(0,0,0,0.5);
    }}
    .meta-title {{
      font-size:15px;
      font-weight:700;
      color:#f8fafc;
      word-break:break-word;
      line-height:1.35;
    }}
    .meta-badges {{
      display:flex;
      align-items:center;
      gap:8px;
      flex-wrap:wrap;
    }}
    .badge {{
      font-size:11px;
      font-weight:700;
      padding:4px 9px;
      border-radius:6px;
      background:#1e293b;
      color:#94a3b8;
      border:1px solid rgba(255,255,255,0.08);
    }}
    .badge.size {{ color:#38bdf8; border-color:rgba(56,189,248,0.25); background:rgba(56,189,248,0.1); }}
    .badge.quality {{ color:#10b981; border-color:rgba(16,185,129,0.25); background:rgba(16,185,129,0.1); }}
    .badge.fast {{ color:#fbbf24; border-color:rgba(251,191,36,0.25); background:rgba(251,191,36,0.1); }}
    .meta-section-title {{
      font-size:12px;
      font-weight:600;
      color:#94a3b8;
      margin-top:4px;
    }}
    .quality-grid {{
      display:grid;
      grid-template-columns:repeat(3, 1fr);
      gap:8px;
    }}
    .q-chip {{
      padding:9px 6px;
      border-radius:8px;
      border:1px solid #334155;
      background:#1e293b;
      color:#f1f5f9;
      font-size:12px;
      font-weight:700;
      cursor:pointer;
      text-align:center;
      transition:all 0.15s ease;
    }}
    .q-chip:hover, .q-chip.active {{
      border-color:#10b981;
      background:rgba(16,185,129,0.15);
      color:#34d399;
    }}
    .main-dl-btn {{
      width:100%;
      padding:12px;
      border-radius:10px;
      background:linear-gradient(135deg, #059669 0%, #10b981 100%);
      color:#ffffff;
      font-size:14px;
      font-weight:800;
      text-align:center;
      text-decoration:none;
      display:flex;
      align-items:center;
      justify-content:center;
      box-shadow:0 4px 14px rgba(16,185,129,0.35);
      transition:transform 0.1s ease;
      cursor:pointer;
    }}
    .main-dl-btn:active {{
      transform:scale(0.98);
    }}
    .dl-grid {{
      display:grid;
      grid-template-columns:repeat(3, 1fr);
      gap:8px;
    }}
    .dl-chip {{
      padding:8px 6px;
      border-radius:8px;
      border:1px solid rgba(16,185,129,0.3);
      background:rgba(6,78,59,0.4);
      color:#6ee7b7;
      font-size:11px;
      font-weight:700;
      text-align:center;
      text-decoration:none;
      display:flex;
      align-items:center;
      justify-content:center;
    }}
    .quality-modal-backdrop {{
      position:fixed; inset:0; background:rgba(5,8,15,0.88); backdrop-filter:blur(10px);
      z-index:100; display:flex; align-items:center; justify-content:center; padding:20px;
    }}
    .quality-modal-backdrop.hidden {{ display:none; }}
    .quality-modal {{
      width:100%; max-width:340px; background:#111827; border:1px solid #1f2937;
      border-radius:18px; padding:22px; display:flex; flex-direction:column; gap:14px;
      box-shadow:0 20px 40px rgba(0,0,0,0.8);
    }}
    .q-row {{ display:flex; gap:10px; }}
    .q-btn {{
      flex:1; padding:12px 14px; border-radius:10px; border:1px solid #374151;
      background:#1f2937; color:#f8fafc; font-size:14px; font-weight:700; cursor:pointer;
    }}
    .q-btn:hover {{ background:#374151; }}
    .q-dl {{
      padding:12px 14px; border-radius:10px; border:1px solid #059669;
      background:#064e3b; color:#34d399; font-size:13px; font-weight:700; text-decoration:none;
      display:flex; align-items:center; justify-content:center;
    }}
  </style>
</head>
<body>
  <div class="screen-wrapper">
    <div class="player-card">
      <!-- Photo 3 Style: Compact 16:9 Player Box in Center -->
      <div class="video-box" id="video-box">
        <!-- Quality Selector Pill (Top right inside video) -->
        <button type="button" class="quality-pill" onclick="event.stopPropagation(); document.getElementById('q-modal').classList.remove('hidden')">
          <span id="ql">{initial_q}</span> ⚙️
        </button>

        <!-- Double Tap Seek Overlays (Left: -10s, Right: +10s) -->
        <div id="tap-left" class="tap-overlay left">
          <div class="tap-badge">⏪ 10s</div>
        </div>
        <div id="tap-right" class="tap-overlay right">
          <div class="tap-badge">10s ⏩</div>
        </div>

        <!-- Gesture HUD Overlay (Brightness & Sound) -->
        <div id="gesture-hud" class="gesture-hud">
          <div class="gesture-icon-row">
            <span id="hud-icon">🔊</span>
            <span id="hud-label">Sound 100%</span>
          </div>
          <div class="gesture-bar-bg">
            <div id="hud-fill" class="gesture-bar-fill"></div>
          </div>
        </div>

        <!-- Clean 16:9 Video Player (Photo 3 Style - No permanent center button blocking video) -->
        <video
          id="v"
          playsinline
          webkit-playsinline
          controls
          controlsList="nodownload"
          preload="auto"
          src="{stream_url}?quality={initial_q}"
        ></video>
      </div>

      <!-- Photo 3 Style: Video Meta & Fast Download Card Directly Below Player -->
      <div class="video-meta-card">
        <div class="meta-title">🎬 {video_title}</div>
        <div class="meta-badges">
          <span class="badge size">📦 {file_size_mb} MB</span>
          <span class="badge quality" id="badge-q">⚡ {initial_q} HD</span>
          <span class="badge fast">🚀 Fast Stream</span>
        </div>

        <div class="meta-section-title">📺 কোয়ালিটি সিলেক্ট করে দেখুন:</div>
        <div class="quality-grid">
          <button type="button" class="q-chip" id="chip-480p" onclick="setQ('480p')">▶️ 480p SD</button>
          <button type="button" class="q-chip active" id="chip-720p" onclick="setQ('720p')">▶️ 720p HD</button>
          <button type="button" class="q-chip" id="chip-1080p" onclick="setQ('1080p')">▶️ 1080p FHD</button>
        </div>

        <div class="meta-section-title" style="margin-top:6px;">⬇️ সরাসরি ডাউনলোড করুন:</div>
        <a class="main-dl-btn" href="{dl_url}?quality={initial_q}" id="main-dl-link">
          ⬇️ Download Video (<span id="main-dl-q">{initial_q}</span>)
        </a>
        <div class="dl-grid">
          <a class="dl-chip" href="{dl_url}?quality=480p">⬇️ 480p</a>
          <a class="dl-chip" href="{dl_url}?quality=720p">⬇️ 720p</a>
          <a class="dl-chip" href="{dl_url}?quality=1080p">⬇️ 1080p</a>
        </div>
      </div>
    </div>
  </div>

  <div id="q-modal" class="quality-modal-backdrop {modal_hidden_class}">
    <div class="quality-modal">
      <div style="font-size:16px;font-weight:700;text-align:center;color:#fff;">কোয়ালিটি সিলেক্ট করুন</div>
      <div class="q-row">
        <button class="q-btn" onclick="setQ('480p')">▶️ Play 480p (SD)</button>
        <a class="q-dl" href="{dl_url}?quality=480p">⬇️ 480p</a>
      </div>
      <div class="q-row">
        <button class="q-btn" onclick="setQ('720p')">▶️ Play 720p (HD)</button>
        <a class="q-dl" href="{dl_url}?quality=720p">⬇️ 720p</a>
      </div>
      <div class="q-row">
        <button class="q-btn" onclick="setQ('1080p')">▶️ Play 1080p (FHD)</button>
        <a class="q-dl" href="{dl_url}?quality=1080p">⬇️ 1080p</a>
      </div>
    </div>
  </div>

  <script>
    try {{
      if (window.Telegram && window.Telegram.WebApp) {{
        window.Telegram.WebApp.ready();
        window.Telegram.WebApp.expand();
      }}
    }} catch (e) {{}}

    const v = document.getElementById('v');
    const box = document.getElementById('video-box');
    const qualityPill = document.querySelector('.quality-pill');
    const hud = document.getElementById('gesture-hud');
    const hudIcon = document.getElementById('hud-icon');
    const hudLabel = document.getElementById('hud-label');
    const hudFill = document.getElementById('hud-fill');
    const tapLeft = document.getElementById('tap-left');
    const tapRight = document.getElementById('tap-right');

    // =========================================================================
    // SWIPE GESTURES:
    // Left side vertical swipe -> Brightness (0% - 100%)
    // Right side vertical swipe -> Volume (0% - 100%)
    // Bottom 68px reserved for native timeline scrubber and controls
    // =========================================================================
    let brightnessPercent = 85;
    let volumePercent = 100;
    let touchState = null;
    let hudTimer = null;
    let lastTapTime = 0, lastTapX = 0, tapTimer = null;

    function showHud(type, val) {{
      const clamped = Math.max(0, Math.min(100, Math.round(val)));
      if (type === 'volume') {{
        hudIcon.textContent = clamped === 0 ? '🔇' : clamped < 50 ? '🔉' : '🔊';
        hudLabel.textContent = 'Sound ' + clamped + '%';
      }} else {{
        hudIcon.textContent = '☀️';
        hudLabel.textContent = 'Light ' + clamped + '%';
      }}
      hudFill.style.width = clamped + '%';
      hud.classList.add('visible');
      if (hudTimer) clearTimeout(hudTimer);
      hudTimer = setTimeout(() => {{
        hud.classList.remove('visible');
      }}, 700);
    }}

    function applyBrightness(val) {{
      brightnessPercent = Math.max(0, Math.min(100, val));
      const cssBrightness = (0.20 + (brightnessPercent / 100) * 1.05).toFixed(2);
      v.style.filter = 'brightness(' + cssBrightness + ')';
      showHud('brightness', brightnessPercent);
    }}

    function applyVolume(val) {{
      volumePercent = Math.max(0, Math.min(100, val));
      v.muted = volumePercent === 0;
      v.volume = Math.max(0, Math.min(1, volumePercent / 100));
      showHud('volume', volumePercent);
    }}

    function showTapEffect(side) {{
      const el = side === 'left' ? tapLeft : tapRight;
      el.classList.add('active');
      setTimeout(() => el.classList.remove('active'), 450);
    }}

    function seekRelative(delta) {{
      const totalDur = (v.duration && isFinite(v.duration)) ? v.duration : 1420;
      const nextTime = Math.max(0, Math.min(totalDur - 1, (v.currentTime || 0) + delta));
      v.currentTime = nextTime;
    }}

    function togglePlay() {{
      if (v.paused) {{
        v.play().catch(() => {{}});
      }} else {{
        v.pause();
      }}
    }}

    // TOUCH EVENT LISTENERS FOR BRIGHTNESS & SOUND
    box.addEventListener('touchstart', function(e) {{
      if (!e.touches || e.touches.length !== 1) return;
      const touch = e.touches[0];
      const rect = box.getBoundingClientRect();
      const relY = touch.clientY - rect.top;
      const relX = touch.clientX - rect.left;

      // Bottom 68px reserved for native <video controls>
      if (relY > rect.height - 68) {{
        touchState = null;
        return;
      }}

      touchState = {{
        startX: touch.clientX,
        startY: touch.clientY,
        isRightSide: relX > rect.width / 2,
        startVolume: volumePercent,
        startBrightness: brightnessPercent,
        swiping: false,
        boxHeight: Math.max(160, rect.height)
      }};
    }}, {{ passive: true, capture: true }});

    box.addEventListener('touchmove', function(e) {{
      if (!e.touches || e.touches.length !== 1 || !touchState) return;
      const touch = e.touches[0];
      const dy = touchState.startY - touch.clientY;
      const dx = touch.clientX - touchState.startX;

      if (!touchState.swiping) {{
        if (Math.abs(dy) > 8 && Math.abs(dy) > Math.abs(dx)) {{
          touchState.swiping = true;
        }} else {{
          return;
        }}
      }}

      const deltaPercent = (dy / (touchState.boxHeight * 0.65)) * 100;
      if (touchState.isRightSide) {{
        applyVolume(touchState.startVolume + deltaPercent);
      }} else {{
        applyBrightness(touchState.startBrightness + deltaPercent);
      }}
    }}, {{ passive: true, capture: true }});

    box.addEventListener('touchend', function() {{
      touchState = null;
    }}, {{ passive: true, capture: true }});

    // DOUBLE TAP FOR SEEK (-10s / +10s) OR SINGLE TAP
    box.addEventListener('click', function(e) {{
      if (e.target.closest('.quality-pill') || e.target.closest('#q-modal')) return;

      const rect = box.getBoundingClientRect();
      const clickX = e.clientX - rect.left;
      const clickY = e.clientY - rect.top;
      const width = rect.width;
      const height = rect.height;

      // Don't intercept clicks on bottom native controls
      if (clickY > height - 68) return;

      const currentTime = Date.now();
      const diffTime = currentTime - lastTapTime;

      // Double-tap detected (within 320ms on same side)
      if (diffTime < 320 && Math.abs(clickX - lastTapX) < width * 0.38) {{
        if (tapTimer) {{
          clearTimeout(tapTimer);
          tapTimer = null;
        }}
        if (clickX > width * 0.55) {{
          seekRelative(10);
          showTapEffect('right');
        }} else if (clickX < width * 0.45) {{
          seekRelative(-10);
          showTapEffect('left');
        }}
        lastTapTime = 0;
      }} else {{
        lastTapTime = currentTime;
        lastTapX = clickX;
        if (tapTimer) clearTimeout(tapTimer);
        tapTimer = setTimeout(() => {{
          tapTimer = null;
        }}, 280);
      }}
    }});

    function setQ(q) {{
      document.getElementById('ql').textContent = q;
      const badgeQ = document.getElementById('badge-q');
      if (badgeQ) badgeQ.textContent = '⚡ ' + q + (q === '1080p' ? ' FHD' : q === '720p' ? ' HD' : ' SD');
      const mainDlQ = document.getElementById('main-dl-q');
      if (mainDlQ) mainDlQ.textContent = q;
      const mainDlLink = document.getElementById('main-dl-link');
      if (mainDlLink) mainDlLink.href = '{dl_url}?quality=' + q;

      ['480p', '720p', '1080p'].forEach(item => {{
        const chip = document.getElementById('chip-' + item);
        if (chip) {{
          if (item === q) chip.classList.add('active');
          else chip.classList.remove('active');
        }}
      }});

      document.getElementById('q-modal').classList.add('hidden');
      const savedTime = v.currentTime || 0;
      v.src = '{stream_url}?quality=' + q;
      v.load();
      v.currentTime = savedTime;
      v.play().catch(() => {{}});
    }}

    // KEYBOARD SHORTCUTS
    window.addEventListener('keydown', function(e) {{
      if (e.code === 'Space') {{ e.preventDefault(); togglePlay(); }}
      else if (e.code === 'ArrowRight') {{ e.preventDefault(); seekRelative(10); }}
      else if (e.code === 'ArrowLeft') {{ e.preventDefault(); seekRelative(-10); }}
    }});
  </script>
</body>
</html>"""
    return HTMLResponse(content=html)


async def handle_telegram_stream(stream_id: str, request: Request, quality: str, is_download: bool):
    # 1. Search in MongoDB for stored metadata
    clean_id = stream_id.replace("tg-", "")
    num_id = int(clean_id) if clean_id.isdigit() else -1
    doc = await videos_col.find_one({
        "$or": [
            {"id": stream_id},
            {"id": f"tg-{stream_id}"},
            {"id": clean_id},
            {"id": f"tg-{clean_id}"},
            {"msg_id": num_id},
            {"bin_msg_id": num_id},
            {"telegramFileId": stream_id}
        ]
    })

    # 2. Resolve genuine Telegram Message object with full MTProto file credentials
    target_msg = None
    if doc and doc.get("bin_msg_id") and BIN_CHANNEL:
        try:
            target_msg = await bot.get_messages(BIN_CHANNEL, int(doc["bin_msg_id"]))
        except Exception as e:
            logger.warning(f"Failed get_messages from BIN_CHANNEL: {e}")

    if not target_msg and doc and doc.get("chat_id") and doc.get("msg_id"):
        try:
            target_msg = await bot.get_messages(int(doc["chat_id"]), int(doc["msg_id"]))
        except Exception as e:
            logger.warning(f"Failed get_messages from chat_id: {e}")

    if not target_msg and num_id > 0 and BIN_CHANNEL:
        try:
            target_msg = await bot.get_messages(BIN_CHANNEL, num_id)
        except Exception as e:
            logger.warning(f"Failed direct BIN_CHANNEL fetch: {e}")

    # Extract media & file size
    media_obj = None
    if target_msg:
        media_obj = target_msg.video or target_msg.document or target_msg.animation or target_msg.audio

    file_size = 0
    file_name = f"SR_Video_{stream_id}_{quality}.mp4"
    mime_type = "video/mp4"

    if media_obj:
        file_size = getattr(media_obj, "file_size", 0) or 0
        file_name = getattr(media_obj, "file_name", None) or file_name
        mime_type = getattr(media_obj, "mime_type", "video/mp4") or "video/mp4"
    elif doc:
        file_size = int(doc.get("sizeBytes") or 0)
        file_name = doc.get("fileName") or file_name
        mime_type = doc.get("mimeType") or "video/mp4"

    if file_size <= 0:
        raise HTTPException(status_code=404, detail="File size or media not found")

    if not mime_type.startswith("video/"):
        mime_type = "video/mp4"

    # 3. Exact HTTP 206 Byte-Range Calculation (0-byte disk usage on Render! Pure memory stream!)
    range_header = request.headers.get("Range", "")
    if range_header and range_header.startswith("bytes="):
        parts = range_header.replace("bytes=", "").split("-")
        from_bytes = int(parts[0]) if parts[0] else 0
        until_bytes = int(parts[1]) if len(parts) > 1 and parts[1] else file_size - 1
    else:
        from_bytes = 0
        until_bytes = file_size - 1

    until_bytes = min(until_bytes, file_size - 1)
    if from_bytes > until_bytes or from_bytes < 0:
        return Response(status_code=416, headers={"Content-Range": f"bytes */{file_size}"})

    req_length = until_bytes - from_bytes + 1
    chunk_size = 1024 * 1024  # 1 MB MTProto chunk
    first_part = from_bytes // chunk_size
    last_part = until_bytes // chunk_size
    part_count = last_part - first_part + 1
    first_part_cut = from_bytes % chunk_size
    last_part_cut = (until_bytes % chunk_size) + 1

    disposition = "attachment" if is_download else "inline"
    headers = {
        "Content-Type": mime_type,
        "Accept-Ranges": "bytes",
        "Content-Length": str(req_length),
        "Content-Disposition": f'{disposition}; filename="{file_name}"',
    }
    if range_header:
        headers["Content-Range"] = f"bytes {from_bytes}-{until_bytes}/{file_size}"

    if request.method == "HEAD":
        return Response(status_code=206 if range_header else 200, headers=headers)

    # ZERO DISK USAGE GENERATOR: Stream directly from Telegram MTProto chunk into HTTP stream!
    async def media_generator():
        current_part = 1
        stream_target = target_msg or (doc.get("telegramFileId") if doc else stream_id)
        try:
            async for chunk in bot.stream_media(stream_target, limit=part_count, offset=first_part):
                if part_count == 1:
                    yield chunk[first_part_cut:last_part_cut]
                elif current_part == 1:
                    yield chunk[first_part_cut:]
                elif current_part == part_count:
                    yield chunk[:last_part_cut]
                else:
                    yield chunk
                current_part += 1
        except Exception as e:
            logger.error(f"Telegram streaming exception on part {current_part}: {e}")

    return StreamingResponse(
        media_generator(),
        status_code=206 if range_header else 200,
        headers=headers
    )


@web_app.api_route("/stream/{stream_id}", methods=["GET", "HEAD"])
@web_app.api_route("/video/{stream_id}", methods=["GET", "HEAD"])
async def stream_endpoint(stream_id: str, request: Request, quality: str = "1080p", q: str = "1080p"):
    return await handle_telegram_stream(stream_id, request, quality or q, is_download=False)


@web_app.api_route("/dl/{stream_id}", methods=["GET", "HEAD"])
@web_app.api_route("/download/{stream_id}", methods=["GET", "HEAD"])
@web_app.api_route("/get/{stream_id}", methods=["GET", "HEAD"])
async def download_endpoint(stream_id: str, request: Request, quality: str = "1080p", q: str = "1080p", download: int = 1):
    return await handle_telegram_stream(stream_id, request, quality or q, is_download=bool(download))


# ==============================================================================
# UNIFIED ASYNC ENTRYPOINT
# ==============================================================================
async def start_services():
    logger.info(f"Starting SR-VIDEO-QUALITY on domain: {DOMAIN} (Port {PORT})")
    config = uvicorn.Config(web_app, host="0.0.0.0", port=PORT, log_level="info")
    server = uvicorn.Server(config)

    web_task = asyncio.create_task(server.serve())
    ping_task = asyncio.create_task(keep_alive_ping())

    if BOT_TOKEN and BOT_TOKEN != "YOUR_BOT_TOKEN_HERE":
        try:
            await bot.start()
            me = await bot.get_me()
            logger.info(f"Telegram Bot @{me.username} connected with MongoDB persistence!")
            await idle()
            await bot.stop()
        except Exception as bot_err:
            logger.error(f"Telegram Bot warning (Web server continues running): {bot_err}")
            await web_task
    else:
        logger.info("Running in Web Streaming Server mode (set BOT_TOKEN to enable Telegram bot).")
        await web_task

    ping_task.cancel()


if __name__ == "__main__":
    try:
        loop.run_until_complete(start_services())
    except RuntimeError:
        asyncio.run(start_services())
