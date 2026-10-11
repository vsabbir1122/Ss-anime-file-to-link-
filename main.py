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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("SR-VIDEO-QUALITY")

# ==============================================================================
# CONFIGURATION
# ==============================================================================
MONGO_URI = os.environ.get(
    "MONGO_URI",
    "mongodb+srv://sabbirvai1122:SABBIRVAI1122@cluster0.t1bsiei.mongodb.net/?appName=Cluster0"
)
API_ID = int(os.environ.get("API_ID", "29608422"))
API_HASH = os.environ.get("API_HASH", "3db2f8e109301f02f5d9c8f10dd79244")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
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
# MONGODB ATLAS INITIALIZATION
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
    expose_headers=["Content-Range", "Content-Length", "Accept-Ranges"]
)

# 24/7 Keep-Alive Background Worker
async def keep_alive_ping():
    await asyncio.sleep(20)
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                ping_url = f"{DOMAIN}/health"
                async with session.get(ping_url, timeout=10) as resp:
                    logger.info(f"Keep-Alive ping to {ping_url} => HTTP {resp.status}")
            except Exception as e:
                logger.debug(f"Keep-Alive ping skipped: {e}")
            await asyncio.sleep(300)

# ==============================================================================
# TELEGRAM BOT HANDLERS
# ==============================================================================
@bot.on_message(filters.command("start") & filters.private)
async def start_handler(client: Client, message: Message):
    user_id = message.from_user.id if message.from_user else 0
    if message.from_user:
        await users_col.update_one(
            {"id": message.from_user.id},
            {"$set": {
                "id": message.from_user.id,
                "first_name": message.from_user.first_name,
                "username": message.from_user.username,
                "last_active": datetime.utcnow().isoformat()
            }},
            upsert=True
        )

    is_user_admin = await check_is_admin(user_id)
    if not is_user_admin:
        await message.reply_text(
            "👋 **স্বাগতম!**\n\n"
            "🔒 **এই বটটি শুধুমাত্র অনুমোদিত অ্যাডমিনের ব্যবহারের জন্য সংরক্ষিত।**\n\n"
            f"👤 **আপনার টেলিগ্রাম আইডি:** `{user_id}`\n\n"
            "💡 **অ্যাডমিন হতে চান?**\n"
            f"আপনার এই আইডিটি মেইন অ্যাডমিনকে পাঠান। মেইন অ্যাডমিন বটের মধ্যে `/addadmin {user_id}` লিখলেই আপনি অ্যাডমিন হয়ে যাবেন এবং ভিডিও আপলোড করে লিঙ্ক বানাতে পারবেন।\n\n"
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
        await message.reply_text(f"⚠️ আইডি `{target_id}` হলো রুট অ্যাডমিন (Render ENV তে সেট করা)।")
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

    lines_str = "\n".join(admin_lines) if admin_lines else "কোনো নির্দিষ্ট অ্যাডমিন সেট করা নেই।"

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


@bot.on_message(filters.private & (filters.video | filters.document | filters.animation))
async def media_handler(client: Client, message: Message):
    # ADMIN PROTECTION CHECK: Only allowed Admins can process video files
    user_id = message.from_user.id if message.from_user else 0
    if not await check_is_admin(user_id):
        await message.reply_text(
            "⛔ **অ্যাক্সেস ডিনাইড (Access Denied)!**\n\n"
            "এই বটটি শুধুমাত্র **অনুমোদিত অ্যাডমিনদের ব্যবহারের জন্য সংরক্ষিত**।\n"
            f"👤 **আপনার টেলিগ্রাম আইডি:** `{user_id}`\n\n"
            f"💡 মেইন অ্যাডমিনকে এই আইডি পাঠিয়ে বলুন বটের মধ্যে `/addadmin {user_id}` লিখে আপনাকে অনুমোদন দিতে।"
        )
        return

    media = message.video or message.document or message.animation
    if not media:
        return

    file_size = getattr(media, "file_size", 0) or 0
    file_size_mb = round(file_size / (1024 * 1024), 2)
    min_size_limit = await get_min_file_size_mb()

    # 10 MB MINIMUM FILE SIZE RESTRICTION (১০ এমবির কম ফাইল গৃহীত হবে না)
    if file_size < int(min_size_limit * 1024 * 1024):
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
        lock_res = await locks_col.update_one(
            {"_id": msg_key},
            {"$setOnInsert": {
                "chatId": str(message.chat.id),
                "messageId": message.id,
                "claimedAt": datetime.utcnow().isoformat()
            }},
            upsert=True
        )
        if lock_res.upserted_id is None and lock_res.matched_count > 0:
            return
    except Exception:
        return

    status_msg = await message.reply_text("⏳ **Saving to MongoDB & Generating Stream Links...**", quote=True)

    try:
        bin_msg_id = None
        try:
            if BIN_CHANNEL and BIN_CHANNEL != -1001234567890:
                log_msg = await message.copy(chat_id=BIN_CHANNEL)
                bin_msg_id = log_msg.id
        except Exception as copy_err:
            logger.warning(f"BIN_CHANNEL copy skipped (using direct file_id from MongoDB): {copy_err}")

        stream_id = f"tg-{bin_msg_id or message.id}"
        file_name = getattr(media, "file_name", None) or f"SR_Video_{stream_id}.mp4"
        mime_type = getattr(media, "mime_type", "video/mp4") or "video/mp4"
        if not mime_type.startswith("video/"):
            mime_type = "video/mp4"

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

        await status_msg.edit_text(
            f"🎬 **{doc['title']}**\n"
            f"📦 **Size:** `{file_size_mb} MB`\n\n"
            f"▶️ **Watch Video Link:**\n{watch_url}\n\n"
            f"⬇️ **Direct Download Link:**\n{dl_url}\n\n"
            f"📥 **Direct Quality Download:**\n"
            f"• [Download 480p]({dl_url}?quality=480p) | [Download 720p]({dl_url}?quality=720p) | [Download 1080p]({dl_url}?quality=1080p)\n\n"
            f"👇 **নিচের বাটন থেকে কোয়ালিটি (480p, 720p, 1080p) সিলেক্ট করে ভিডিও দেখুন বা সরাসরি ডাউনলোড করুন:**",
            reply_markup=buttons,
            disable_web_page_preview=True
        )
    except Exception as err:
        logger.error(f"Error handling media: {err}")
        await status_msg.edit_text(f"❌ Error generating link: `{err}`")


@bot.on_callback_query()
async def quality_callback_handler(client: Client, callback_query):
    data = callback_query.data or ""
    if data.startswith("q_watch:"):
        vid_id = data.split(":", 1)[1]
        watch_url = f"{DOMAIN}/player/{vid_id}"
        await callback_query.message.reply_text(
            f"🎬 **Watch Multi-Quality Stream Links:**\n\n"
            f"• 📺 [Watch 480p (SD)]({watch_url}?q=480p)\n"
            f"• 📺 [Watch 720p (HD)]({watch_url}?q=720p)\n"
            f"• 📺 [Watch 1080p (FHD)]({watch_url}?q=1080p)\n\n"
            f"🌐 [Open Interactive Player]({watch_url})",
            disable_web_page_preview=True
        )
        await callback_query.answer()
    elif data.startswith("q_dl:"):
        vid_id = data.split(":", 1)[1]
        dl_url = f"{DOMAIN}/dl/{vid_id}"
        await callback_query.message.reply_text(
            f"📥 **Download Multi-Quality Links:**\n\n"
            f"• ⬇️ [Download 480p]({dl_url}?quality=480p)\n"
            f"• ⬇️ [Download 720p]({dl_url}?quality=720p)\n"
            f"• ⬇️ [Download 1080p]({dl_url}?quality=1080p)\n\n"
            f"⚡ Direct High-Speed Download (Resume Supported)",
            disable_web_page_preview=True
        )
        await callback_query.answer()


# ==============================================================================
# FASTAPI STREAMING & WATCH ROUTES
# ==============================================================================
@web_app.get("/health")
async def health_check():
    return JSON_Response({"status": "healthy", "service": "SR-VIDEO-QUALITY", "domain": DOMAIN})

@web_app.get("/")
async def root_endpoint():
    return JSON_Response({
        "status": "online",
        "domain": DOMAIN,
        "admins": ADMINS,
        "minFileSizeMb": await get_min_file_size_mb(),
        "database": "MongoDB Atlas Connected",
        "features": ["HTTP 206 Byte-Range", "Multi-Quality (1080p, 720p, 480p)", "Tap-to-Show Controls", "Double-Tap Seek 10s"]
    })

def JSON_Response(content):
    return JSONResponse(content)

async def resolve_video_doc(video_id: str):
    clean_id = video_id.strip()
    doc = await videos_col.find_one({"id": clean_id})
    if not doc and clean_id.startswith("tg-"):
        num_str = clean_id.replace("tg-", "")
        if num_str.isdigit():
            doc = await videos_col.find_one({"$or": [{"msg_id": int(num_str)}, {"bin_msg_id": int(num_str)}]})
    return doc

@web_app.get("/player/{video_id}", response_class=HTMLResponse)
@web_app.get("/watch/{video_id}", response_class=HTMLResponse)
async def watch_player(request: Request, video_id: str):
    doc = await resolve_video_doc(video_id)
    video_title = doc.get("title", f"SR Video {video_id}") if doc else f"Video {video_id}"
    duration_sec = doc.get("durationSec", 120) if doc else 120
    q_param = request.query_params.get("q", "720p").lower()
    initial_q = q_param if q_param in ["1080p", "720p", "480p", "360p"] else "720p"
    modal_hidden_class = "" if "q" not in request.query_params else "hidden"
    stream_url = f"{DOMAIN}/stream/{video_id}"
    dl_url = f"{DOMAIN}/dl/{video_id}"

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
      width:100%; height:100%; max-width:100vw; max-height:100dvh;
      overflow:hidden !important; position:fixed !important; inset:0 !important;
      background:#000; color:#f8fafc; font-family:system-ui,-apple-system,sans-serif;
      overscroll-behavior:none !important;
      user-select:none; -webkit-user-select:none;
    }}
    .screen-wrapper {{
      width:100vw; height:100dvh; display:flex; flex-direction:column;
      justify-content:center; align-items:center; overflow:hidden; background:#000;
    }}
    .player-card {{ width:100%; max-width:960px; height:100%; display:flex; flex-direction:column; justify-content:center; align-items:center; }}
    .video-box {{
      width:100%; height:100%; max-height:100dvh; background:#000;
      position:relative; overflow:hidden; display:flex; align-items:center; justify-content:center;
      cursor:pointer;
    }}
    video {{
      width:100%; height:100%; max-height:100dvh; object-fit:contain;
      display:block; background:#000;
    }}
    .center-action-btn {{
      position:absolute; top:50%; left:50%; transform:translate(-50%,-50%) scale(1);
      width:80px; height:80px; border-radius:50%;
      background:rgba(15,23,42,0.85); border:2.5px solid rgba(255,255,255,0.4);
      display:flex; align-items:center; justify-content:center; font-size:36px;
      color:#fff; cursor:pointer; opacity:1;
      transition:opacity 0.28s ease, transform 0.22s cubic-bezier(0.4,0,0.2,1);
      z-index:35; box-shadow:0 10px 35px rgba(0,0,0,0.75);
      backdrop-filter:blur(10px); -webkit-backdrop-filter:blur(10px);
    }}
    .center-action-btn:hover {{
      background:rgba(15,23,42,0.95); border-color:#10b981; transform:translate(-50%,-50%) scale(1.08);
    }}
    .center-action-btn:active {{
      transform:translate(-50%,-50%) scale(0.92);
    }}
    .center-action-btn.ctrl-hidden {{
      opacity:0; pointer-events:none; transform:translate(-50%,-50%) scale(0.7);
    }}
    .quality-pill {{
      position:absolute; top:16px; right:16px; z-index:30;
      background:rgba(15,23,42,0.85); border:1px solid rgba(255,255,255,0.25);
      color:#f8fafc; font-size:12px; font-weight:700; padding:6px 12px; border-radius:8px; cursor:pointer;
      backdrop-filter:blur(8px);
      transition:opacity 0.25s ease;
    }}
    .quality-pill.ctrl-hidden {{
      opacity:0; pointer-events:none;
    }}
    .custom-controls {{
      position:absolute; left:0; right:0; bottom:0; z-index:28;
      padding:24px 16px 12px;
      background:linear-gradient(to top, rgba(0,0,0,0.92) 0%, rgba(0,0,0,0.6) 65%, transparent 100%);
      display:flex; flex-direction:column; gap:8px;
      transition:opacity 0.25s ease;
    }}
    .custom-controls.ctrl-hidden {{
      opacity:0; pointer-events:none;
    }}
    .seek-range {{
      width:100%; accent-color:#10b981; cursor:pointer; height:8px;
      border-radius:999px; -webkit-appearance:none; background:rgba(255,255,255,0.25);
      touch-action:manipulation;
    }}
    .seek-range::-webkit-slider-thumb {{
      -webkit-appearance:none; width:16px; height:16px; border-radius:50%;
      background:#10b981; cursor:pointer; box-shadow:0 0 10px rgba(16,185,129,0.8);
    }}
    .tap-overlay {{
      position:absolute; top:0; bottom:60px; width:45%; z-index:25;
      display:flex; align-items:center; justify-content:center;
      pointer-events:none; opacity:0; transition:opacity 0.25s ease;
    }}
    .tap-overlay.left {{ left:0; }}
    .tap-overlay.right {{ right:0; }}
    .tap-overlay.active {{ opacity:1; background:rgba(255,255,255,0.08); }}
    .tap-badge {{
      background:rgba(15,23,42,0.9); border:1px solid rgba(255,255,255,0.25);
      padding:10px 18px; border-radius:999px; font-size:15px; font-weight:700;
      color:#10b981; box-shadow:0 8px 24px rgba(0,0,0,0.7);
    }}
    .ctrl-row {{ display:flex; align-items:center; justify-content:space-between; font-size:13px; font-weight:700; }}
    .ctrl-btn {{
      background:rgba(255,255,255,0.14); border:1px solid rgba(255,255,255,0.18);
      color:#fff; border-radius:8px; padding:6px 10px; font-size:13px; font-weight:700; cursor:pointer;
    }}
    .ctrl-btn:hover {{ background:rgba(255,255,255,0.22); }}
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
      <div class="video-box" id="video-box">
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

        <!-- Central Action Button: অফ করার বাটন (Pause) / অন করার বাটন (Play) -->
        <button type="button" id="center-play-btn" class="center-action-btn" aria-label="Play or Pause">
          <span id="center-btn-icon">▶️</span>
        </button>

        <video id="v" playsinline webkit-playsinline preload="auto" src="{stream_url}?quality={initial_q}"></video>

        <div class="custom-controls" id="custom-ctrls" onclick="event.stopPropagation();">
          <input id="seekbar" class="seek-range" type="range" min="0" max="{duration_sec}" step="1" value="0" />
          <div class="ctrl-row">
            <div style="display:flex;align-items:center;gap:12px;">
              <button type="button" class="ctrl-btn" id="btn-play" onclick="togglePlay()">▶️ Play</button>
              <span id="tl" style="color:#94a3b8;font-size:13px;">0:00 / 0:00</span>
            </div>
            <button type="button" class="ctrl-btn" onclick="toggleFullscreen()">⛶ Fullscreen</button>
          </div>
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
    const seekbar = document.getElementById('seekbar');
    const tl = document.getElementById('tl');
    const centerBtn = document.getElementById('center-play-btn');
    const centerIcon = document.getElementById('center-btn-icon');
    const customCtrls = document.getElementById('custom-ctrls');
    const qualityPill = document.querySelector('.quality-pill');
    const btnPlay = document.getElementById('btn-play');

    let totalDur = {duration_sec}, isScrubbing = false;
    let controlsVisible = true;
    let autoHideTimer = null;

    function fmt(s) {{
      s = Math.max(0, Math.floor(s || 0));
      const m = Math.floor(s / 60), r = s % 60;
      return m + ':' + (r < 10 ? '0' : '') + r;
    }}

    function resetAutoHide() {{
      if (autoHideTimer) clearTimeout(autoHideTimer);
      if (!v.paused && controlsVisible) {{
        autoHideTimer = setTimeout(() => {{
          hideControls();
        }}, 3200);
      }}
    }}

    function showControls() {{
      controlsVisible = true;
      centerBtn.classList.remove('ctrl-hidden');
      customCtrls.classList.remove('ctrl-hidden');
      if (qualityPill) qualityPill.classList.remove('ctrl-hidden');
      resetAutoHide();
    }}

    function hideControls() {{
      if (!v.paused) {{
        controlsVisible = false;
        centerBtn.classList.add('ctrl-hidden');
        customCtrls.classList.add('ctrl-hidden');
        if (qualityPill) qualityPill.classList.add('ctrl-hidden');
      }}
    }}

    function toggleControls() {{
      if (controlsVisible && !v.paused) {{
        hideControls();
      }} else {{
        showControls();
      }}
    }}

    function syncPlayState(isPlaying) {{
      if (isPlaying) {{
        centerIcon.textContent = '⏸️';
        btnPlay.textContent = '⏸️ Pause';
        resetAutoHide();
      }} else {{
        centerIcon.textContent = '▶️';
        btnPlay.textContent = '▶️ Play';
        showControls();
      }}
    }}

    function togglePlay() {{
      if (v.paused) {{
        v.play().then(() => {{
          syncPlayState(true);
        }}).catch(() => {{
          syncPlayState(false);
        }});
      }} else {{
        v.pause();
        syncPlayState(false);
      }}
    }}

    // Center button click directly plays or pauses
    centerBtn.addEventListener('click', function(e) {{
      e.stopPropagation();
      togglePlay();
    }});

    const tapLeft = document.getElementById('tap-left');
    const tapRight = document.getElementById('tap-right');
    let lastTapTime = 0, lastTapX = 0, tapTimer = null;

    function showTapEffect(side) {
      const el = side === 'left' ? tapLeft : tapRight;
      el.classList.add('active');
      setTimeout(() => el.classList.remove('active'), 450);
    }

    // SMART SCREEN GESTURE:
    // Single-Tap on screen: Shows the Pause/Play button (অফ/অন করার বাটন) or hides controls if already shown!
    // Double-Tap Right: +10s
    // Double-Tap Left: -10s
    box.addEventListener('click', function(e) {
      if (e.target.closest('#custom-ctrls') || e.target.closest('.quality-pill') || e.target.closest('#q-modal') || e.target.closest('#center-play-btn')) return;

      const rect = box.getBoundingClientRect();
      const clickX = e.clientX - rect.left;
      const width = rect.width;
      const currentTime = Date.now();
      const diffTime = currentTime - lastTapTime;

      // Double-tap detected (within 320ms and within the same half of screen)
      if (diffTime < 320 && Math.abs(clickX - lastTapX) < width * 0.35) {
        if (tapTimer) {
          clearTimeout(tapTimer);
          tapTimer = null;
        }
        if (clickX > width * 0.55) {
          // Double-tap on Right half: Seek 10s Forward
          seekRelative(10);
          showTapEffect('right');
        } else if (clickX < width * 0.45) {
          // Double-tap on Left half: Seek 10s Backward
          seekRelative(-10);
          showTapEffect('left');
        } else {
          toggleControls();
        }
        lastTapTime = 0;
      } else {
        // Single tap: Schedule toggleControls after 280ms if not followed by a second tap
        lastTapTime = currentTime;
        lastTapX = clickX;
        if (tapTimer) clearTimeout(tapTimer);
        tapTimer = setTimeout(() => {
          toggleControls();
          tapTimer = null;
        }, 280);
      }
    });

    v.addEventListener('play', () => {{ syncPlayState(true); }});
    v.addEventListener('pause', () => {{ syncPlayState(false); }});

    v.addEventListener('loadedmetadata', function() {{
      if (v.duration && isFinite(v.duration) && v.duration > 1) {{
        totalDur = Math.floor(v.duration);
        seekbar.max = String(totalDur);
      }}
    }});

    v.addEventListener('timeupdate', function() {{
      if (!isScrubbing) {{
        const cur = v.currentTime || 0;
        seekbar.value = String(Math.floor(cur));
        tl.textContent = fmt(cur) + ' / ' + fmt(totalDur);
      }}
    }});

    // SMOOTH TIMELINE DRAGGING (টানাপোড়েন)
    seekbar.addEventListener('input', function() {{
      isScrubbing = true;
      tl.textContent = fmt(Number(seekbar.value)) + ' / ' + fmt(totalDur);
    }});

    seekbar.addEventListener('change', function() {{
      isScrubbing = false;
      const target = Number(seekbar.value);
      if (isFinite(v.duration) && v.duration > 0) {{
        v.currentTime = target;
      }} else {{
        v.currentTime = target;
      }}
    }});

    function seekRelative(delta) {{
      const nextTime = Math.max(0, Math.min(totalDur - 1, (v.currentTime || 0) + delta));
      v.currentTime = nextTime;
    }}

    function toggleFullscreen() {{
      if (document.fullscreenElement) {{
        document.exitFullscreen().catch(() => {{}});
      }} else if (box.requestFullscreen) {{
        box.requestFullscreen().catch(() => {{}});
      }} else if (v.webkitEnterFullscreen) {{
        v.webkitEnterFullscreen();
      }}
    }}

    function setQ(q) {{
      document.getElementById('ql').textContent = q;
      document.getElementById('q-modal').classList.add('hidden');
      const savedTime = v.currentTime || 0;
      v.src = '{stream_url}?quality=' + q;
      v.currentTime = savedTime;
      v.play().catch(() => {{}});
    }}
  </script>
</body>
</html>"""
    return HTMLResponse(content=html)

async def stream_telegram_file(doc: dict, range_header: str | None = None):
    try:
        bin_channel = doc.get("bin_channel") or BIN_CHANNEL
        bin_msg_id = doc.get("bin_msg_id") or doc.get("msg_id")
        tg_msg = None
        if bin_channel and bin_msg_id:
            try:
                tg_msg = await bot.get_messages(chat_id=bin_channel, message_ids=bin_msg_id)
            except Exception as e:
                logger.warning(f"Could not fetch message from bin channel: {e}")

        media = getattr(tg_msg, "video", None) or getattr(tg_msg, "document", None) if tg_msg else None
        file_size = doc.get("sizeBytes") or (getattr(media, "file_size", 0) if media else 0) or 0
        mime_type = doc.get("mimeType", "video/mp4") or "video/mp4"

        start_byte = 0
        end_byte = file_size - 1 if file_size > 0 else 0

        if range_header and range_header.startswith("bytes="):
            parts = range_header.replace("bytes=", "").split("-")
            if parts[0].isdigit():
                start_byte = int(parts[0])
            if len(parts) > 1 and parts[1].isdigit():
                end_byte = int(parts[1])

        if end_byte >= file_size and file_size > 0:
            end_byte = file_size - 1
        content_length = (end_byte - start_byte + 1) if file_size > 0 else 0

        async def tg_chunk_generator():
            try:
                if tg_msg:
                    async for chunk in bot.stream_media(tg_msg, offset=start_byte, limit=content_length):
                        yield chunk
                else:
                    file_id = doc.get("telegramFileId")
                    if file_id:
                        async for chunk in bot.stream_media(file_id, offset=start_byte, limit=content_length):
                            yield chunk
            except Exception as s_err:
                logger.error(f"Streaming chunk error: {s_err}")

        headers = {
            "Content-Type": mime_type,
            "Accept-Ranges": "bytes",
            "Content-Range": f"bytes {start_byte}-{end_byte}/{file_size}",
            "Content-Length": str(content_length),
            "Access-Control-Allow-Origin": "*",
            "Cache-Control": "public, max-age=3600"
        }
        return StreamingResponse(tg_chunk_generator(), status_code=206, headers=headers)
    except Exception as err:
        logger.error(f"Stream error: {err}")
        raise HTTPException(status_code=500, detail=str(err))

@web_app.get("/stream/{video_id}")
async def stream_route(request: Request, video_id: str):
    doc = await resolve_video_doc(video_id)
    if not doc:
        raise HTTPException(status_code=404, detail="Video stream not found in MongoDB")
    range_header = request.headers.get("range")
    return await stream_telegram_file(doc, range_header)

@web_app.get("/dl/{video_id}")
async def download_route(request: Request, video_id: str):
    doc = await resolve_video_doc(video_id)
    if not doc:
        raise HTTPException(status_code=404, detail="Video not found in MongoDB")
    range_header = request.headers.get("range")
    resp = await stream_telegram_file(doc, range_header)
    file_name = doc.get("fileName") or f"{video_id}.mp4"
    resp.headers["Content-Disposition"] = f'attachment; filename="{file_name}"'
    return resp

# ==============================================================================
# SERVER & BOT RUNNER
# ==============================================================================
async def start_services():
    config = uvicorn.Config(web_app, host="0.0.0.0", port=PORT, log_level="info")
    server = uvicorn.Server(config)

    web_task = asyncio.create_task(server.serve())
    ping_task = asyncio.create_task(keep_alive_ping())

    if BOT_TOKEN and BOT_TOKEN != "YOUR_BOT_TOKEN_HERE":
        try:
            await bot.start()
            me = await bot.get_me()
            logger.info(f"Telegram Bot @{me.username} connected with MongoDB persistence & Root Admin: 8056243176!")
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
