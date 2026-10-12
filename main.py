import os
import re
import asyncio
import logging
import aiohttp
import uvicorn
from datetime import datetime

# Initialize asyncio event loop immediately before Hydrogram Client import
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
# ZERO SERVER BANDWIDTH (0GB USAGE) & TELEGRAM CLOUD STREAMING ENGINE
# ==============================================================================
MONGO_URI = os.environ.get("MONGO_URI", "mongodb+srv://sabbirvai1122:SABBIRVAI1122@cluster0.t1bsiei.mongodb.net/?appName=Cluster0")
API_ID = int(os.environ.get("API_ID", "29608422"))
API_HASH = os.environ.get("API_HASH", "3db2f8e109301f02f5d9c8f10dd79244")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "CONFIGURED_IN_SERVER_ENV")
BIN_CHANNEL = int(os.environ.get("BIN_CHANNEL", "-1004450462812"))
# Public Telegram Channel username (without @) where video links come from
CHANNEL_USERNAME = os.environ.get("CHANNEL_USERNAME", "ssanimebd").strip().lstrip("@")
PORT = int(os.environ.get("PORT", 8080))

# ADMIN ID RESTRICTION
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

async def get_bin_channel() -> int:
    try:
        doc = await settings_col.find_one({"key": "bin_channel"})
        if doc and "val" in doc:
            return int(doc["val"])
    except Exception:
        pass
    return BIN_CHANNEL

async def get_channel_username() -> str:
    try:
        doc = await settings_col.find_one({"key": "channel_username"})
        if doc and "val" in doc:
            return str(doc["val"]).lstrip("@")
    except Exception:
        pass
    return CHANNEL_USERNAME

def normalize_channel_id(raw: str) -> int:
    import re
    clean = re.sub(r"[^d-]", "", str(raw).strip())
    if not clean:
        return 0
    val = int(clean)
    if val > 0:
        val = -int(f"100{val}") if not str(val).startswith("100") else -val
    elif val < 0 and not str(val).startswith("-100"):
        val = -int(f"100{abs(val)}")
    return val

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

bot = Client(
    "SR_Video_Quality_Bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    in_memory=True
)

web_app = FastAPI(title="SR-VIDEO-QUALITY Streaming Server", version="3.1.0")

web_app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Range", "Content-Length", "Accept-Ranges", "Content-Disposition"],
)

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
            "⚠️ **অননুমোদিত অ্যাক্সেস!**\n\n"
            "এই বটটি শুধুমাত্র অনুমোদিত অ্যাডমিনদের ব্যবহারের জন্য সংরক্ষিত।\n\n"
            f"**আপনার Telegram ID:** `{user_id}`\n\n"
            "অ্যাডমিন এক্সেস পেতে `/addadmin {user_id}` কমান্ড ব্যবহার করুন।"
        )
        return

    min_size_val = await get_min_file_size_mb()
    cur_bin = await get_bin_channel()
    cur_ch = await get_channel_username()
    await message.reply_text(
        "👋 **SR-VIDEO-QUALITY Bot Online (0GB Bandwidth & Bin Channel Engine)!**\n\n"
        "বটে যে কোনো ভিডিও পাঠালে তা **স্বয়ংক্রিয়ভাবে আপনার Bin Channel-এ কপি হবে** এবং স্ট্রিমিং ও Lovable লিংক দিবে:\n"
        "⚡ **Telegram Native Embed (0 Server Bandwidth - কখনো লিমিট শেষ হবে না)**\n"
        "🎬 **Web Player & Multi-Quality Stream (1080p, 720p, 480p)**\n"
        "📥 **Fast Direct Download Link**\n\n"
        f"🌐 **Active Domain:** `{DOMAIN}`\n"
        f"📦 **Bin Channel ID:** `{cur_bin}`\n"
        f"📢 **Public Channel:** `@{cur_ch}`\n"
        f"👑 **Admin Status:** Authorized (`{user_id}`)\n"
        f"📦 **ন্যূনতম সাইজ সীমা:** `{min_size_val} MB`\n\n"
        "🛠 **কমান্ড সমূহ:**\n"
        "📦 `/setbin <channel_id>` - বিন চ্যানেল সেট করুন\n"
        "🔍 `/bin` - বর্তমান বিন চ্যানেল দেখুন\n"
        "📢 `/setchannel <username>` - পাবলিক চ্যানেল ইউজারনেম সেট করুন\n"
        "🔗 `/convert <link>` - যে কোনো টেলিগ্রাম বা স্ট্রিম লিংক রূপান্তর করুন\n"
        "➕ `/addadmin <user_id>` - নতুন অ্যাডমিন যুক্ত করুন\n"
        "➖ `/deladmin <user_id>` - অ্যাডমিন বাতিল করুন\n"
        "📋 `/admins` - সকল অ্যাডমিন তালিকা দেখুন\n"
        "📏 `/minsize` - বর্তমান সাইজ লিমিট দেখুন\n"
        "⚙️ `/setmin <MB>` - সাইজ লিমিট পরিবর্তন করুন"
    )

@bot.on_message(filters.command(["setbin", "binchannel"]) & filters.private)
async def set_bin_channel_handler(client: Client, message: Message):
    if not message.from_user or not await check_is_admin(message.from_user.id):
        await message.reply_text("⛔ শুধুমাত্র অ্যাডমিনরা Bin Channel সেট করতে পারেন।")
        return
    parts = message.text.strip().split()
    if len(parts) < 2:
        cur_bin = await get_bin_channel()
        await message.reply_text(
            f"📦 **বর্তমান Bin Channel ID:** `{cur_bin}`\n\n"
            "পরিবর্তন করতে লিখুন:\n"
            "`/setbin -1004450462812`\n\n"
            "💡 অথবা আপনার Bin Channel থেকে যেকোনো পোস্ট এই বটে ফরোয়ার্ড করুন।"
        )
        return
    norm_id = normalize_channel_id(parts[1])
    if not norm_id:
        await message.reply_text("❌ সঠিক চ্যানেল আইডি লিখুন (যেমন: `/setbin -1004450462812`)")
        return
    global BIN_CHANNEL
    BIN_CHANNEL = norm_id
    await settings_col.update_one(
        {"key": "bin_channel"},
        {"$set": {"val": norm_id, "updated_at": datetime.utcnow().isoformat()}},
        upsert=True
    )
    ch_title = "Unknown"
    try:
        chat_info = await bot.get_chat(norm_id)
        ch_title = chat_info.title or "Channel"
    except Exception as exc:
        ch_title = f"(সতর্কতা: বটটি এই চ্যানেলে অ্যাডমিন কিনা নিশ্চিত করুন: {exc})"

    await message.reply_text(
        f"✅ **Bin Channel সফলভাবে আপডেট করা হয়েছে!**\n\n"
        f"📢 **চ্যানেল:** `{ch_title}`\n"
        f"🆔 **ID:** `{norm_id}`\n\n"
        f"এখন থেকে বটে পাঠানো যেকোনো ভিডিও অটো এই চ্যানেলে যাবে এবং সেখান থেকে লিংক জেনারেট হবে!"
    )

@bot.on_message(filters.command(["bin", "getbin"]) & filters.private)
async def get_bin_channel_handler(client: Client, message: Message):
    cur_bin = await get_bin_channel()
    await message.reply_text(
        f"📦 **বর্তমান Bin Channel ID:** `{cur_bin}`\n\n"
        "নতুন আইডি সেট করতে: `/setbin -1004450462812`"
    )

@bot.on_message(filters.command(["setchannel", "channel"]) & filters.private)
async def set_channel_handler(client: Client, message: Message):
    if not message.from_user or not await check_is_admin(message.from_user.id):
        await message.reply_text("⛔ শুধুমাত্র অ্যাডমিনরা চ্যানেল ইউজারনেম পরিবর্তন করতে পারেন।")
        return
    parts = message.text.strip().split()
    if len(parts) < 2:
        cur_ch = await get_channel_username()
        await message.reply_text(f"ব্যবহার: `/setchannel ssanimebd`\nবর্তমান চ্যানেল: `@{cur_ch}`")
        return
    new_ch = parts[1].strip().lstrip("@")
    global CHANNEL_USERNAME
    CHANNEL_USERNAME = new_ch
    await settings_col.update_one(
        {"key": "channel_username"},
        {"$set": {"val": new_ch, "updated_at": datetime.utcnow().isoformat()}},
        upsert=True
    )
    await message.reply_text(f"✅ **চ্যানেল ইউজারনেম আপডেট সফল:** `@{new_ch}`\nএখন সকল ভিডিওতে সরাসরি 0-Bandwidth Telegram Embed লিংক তৈরি হবে!")

@bot.on_message(filters.command(["addadmin", "promote"]) & filters.private)
async def add_admin_handler(client: Client, message: Message):
    if not message.from_user or not await check_is_admin(message.from_user.id):
        await message.reply_text("⛔ শুধুমাত্র অ্যাডমিনরা নতুন অ্যাডমিন যুক্ত করতে পারেন।")
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
                await message.reply_text("ব্যবহার: `/addadmin 6284918234`")
                return

    if not target_id:
        await message.reply_text("ব্যবহার: `/addadmin <user_id>`")
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
            f"✅ **নতুন অ্যাডমিন যুক্ত হয়েছে!**\n\n"
            f"👤 **অ্যাডমিন ID:** `{target_id}`\n"
            f"👑 **যুক্ত করেছেন:** `{message.from_user.id}`"
        )
    except Exception as err:
        logger.error(f"Error adding admin: {err}")
        await message.reply_text(f"ত্রুটি: `{err}`")

@bot.on_message(filters.command(["deladmin", "removeadmin"]) & filters.private)
async def del_admin_handler(client: Client, message: Message):
    if not message.from_user or not await check_is_admin(message.from_user.id):
        await message.reply_text("⛔ শুধুমাত্র অ্যাডমিনরা অ্যাডমিন বাতিল করতে পারেন।")
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
                await message.reply_text("ব্যবহার: `/deladmin 6284918234`")
                return

    if not target_id:
        await message.reply_text("ব্যবহার: `/deladmin <user_id>`")
        return

    if target_id in ADMINS:
        await message.reply_text(f"⛔ মূল ওনার `{target_id}` কে বাদ দেওয়া যাবে না।")
        return

    res = await admins_col.delete_one({"id": target_id})
    if res.deleted_count > 0:
        await message.reply_text(f"✅ `{target_id}` কে অ্যাডমিন তালিকা থেকে বাদ দেওয়া হয়েছে।")
    else:
        await message.reply_text(f"⚠️ `{target_id}` তালিকায় পাওয়া যায়নি।")

@bot.on_message(filters.command(["admins", "adminlist"]) & filters.private)
async def list_admins_handler(client: Client, message: Message):
    if not message.from_user or not await check_is_admin(message.from_user.id):
        await message.reply_text(f"আপনার ID: `{message.from_user.id}` (অননুমোদিত)")
        return

    db_admins = []
    try:
        cursor = admins_col.find({})
        async for adm in cursor:
            db_admins.append(adm.get("id"))
    except Exception as err:
        logger.warning(f"Error fetching admins: {err}")

    admin_lines = []
    for a_id in ADMINS:
        admin_lines.append(f"👑 `{a_id}` (Root Owner / ENV)")
    for a_id in db_admins:
        if a_id not in ADMINS:
            admin_lines.append(f"⭐ `{a_id}` (Database Admin)")

    lines_str = "\n".join(admin_lines) if admin_lines else "কোনো অ্যাডমিন পাওয়া যায়নি"
    await message.reply_text(
        "📋 **অনুমোদিত অ্যাডমিন তালিকা:**\n\n"
        f"{lines_str}\n\n"
        "➕ যুক্ত করতে: `/addadmin <user_id>`\n"
        "➖ বাতিল করতে: `/deladmin <user_id>`"
    )

@bot.on_message(filters.command(["setmin", "minfilesize"]) & filters.private)
async def set_min_size_handler(client: Client, message: Message):
    if not message.from_user or not await check_is_admin(message.from_user.id):
        await message.reply_text("⛔ অনুমোদিত নয়।")
        return

    parts = message.text.strip().split()
    if len(parts) < 2:
        cur_min = await get_min_file_size_mb()
        await message.reply_text(f"বর্তমান ন্যূনতম সাইজ: `{cur_min} MB`\nব্যবহার: `/setmin 10`")
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
        await message.reply_text(f"✅ **সাইজ লিমিট আপডেট সফল:** `{new_val} MB`")
    except Exception:
        await message.reply_text("ব্যবহার: `/setmin 10`")

@bot.on_message(filters.command(["minsize", "getmin"]) & filters.private)
async def get_min_size_handler(client: Client, message: Message):
    cur_min = await get_min_file_size_mb()
    await message.reply_text(f"📏 **বর্তমান ন্যূনতম ফাইল সাইজ লিমিট:** `{cur_min} MB`")

@bot.on_message(filters.command(["convert", "embed", "link"]) & filters.private)
async def convert_command_handler(client: Client, message: Message):
    user_id = message.from_user.id if message.from_user else 0
    if not await check_is_admin(user_id):
        await message.reply_text("⛔ অনুমোদিত নয়।")
        return
    parts = message.text.strip().split(maxsplit=1)
    if len(parts) < 2:
        await message.reply_text("ব্যবহার: `/convert https://t.me/ssanimebd/123` বা যেকোনো লিংক")
        return
    raw_url = parts[1].strip()
    await process_and_reply_link(message, raw_url)

@bot.on_message(filters.private & filters.text & ~filters.command(["start", "convert", "embed", "link", "addadmin", "deladmin", "admins", "minsize", "setmin", "setchannel", "channel", "promote"]))
async def text_link_auto_converter(client: Client, message: Message):
    user_id = message.from_user.id if message.from_user else 0
    if not await check_is_admin(user_id):
        return
    text = (message.text or "").strip()
    if "t.me/" in text or "/player/" in text or "/watch/" in text or text.isdigit():
        await process_and_reply_link(message, text)

async def process_and_reply_link(message: Message, raw_text: str):
    import re
    msg_id = None
    channel = CHANNEL_USERNAME
    m = re.search(r"t.me/([a-zA-Z0-9_]+)/(d+)", raw_text)
    if m and m.group(1).lower() != "c":
        channel = m.group(1)
        msg_id = int(m.group(2))
    else:
        m2 = re.search(r"t.me/c/(d+)/(d+)", raw_text)
        if m2:
            msg_id = int(m2.group(2))
        else:
            m3 = re.search(r"(?:/player/|/watch/|/stream/)?(?:tg-)?(d+)", raw_text)
            if m3:
                msg_id = int(m3.group(1))

    if not msg_id:
        await message.reply_text("❌ লিংকের মধ্যে কোনো টেলিগ্রাম মেসেজ আইডি পাওয়া যায়নি।")
        return

    stream_id = f"tg-{msg_id}"
    tg_embed = f"https://t.me/{channel}/{msg_id}?embed=1" if channel else ""
    watch_url = f"{DOMAIN}/player/{stream_id}"
    dl_url = f"{DOMAIN}/dl/{stream_id}"

    btn_rows = []
    if tg_embed:
        btn_rows.append([
            InlineKeyboardButton("⚡ Telegram Embed (0GB)", url=tg_embed),
            InlineKeyboardButton("📱 Telegram Post", url=f"https://t.me/{channel}/{msg_id}")
        ])
    btn_rows.append([
        InlineKeyboardButton("🎬 Watch Player", url=watch_url),
        InlineKeyboardButton("📥 Download", url=dl_url)
    ])
    markup = InlineKeyboardMarkup(btn_rows)

    rep = (
        f"⚡ **0-Bandwidth Converted Link Ready!**\n\n"
        f"🌐 **Lovable Player Link (ওয়েবসাইটে সরাসরি বসান):**\n"
        f"`{watch_url}`\n\n"
        f"📋 **Lovable Iframe Embed কোড:**\n"
        f"`<iframe src=\"{watch_url}\" width=\"100%\" height=\"450\" frameborder=\"0\" allowfullscreen allow=\"autoplay; fullscreen\"></iframe>`\n\n"
        f"⚡ **Direct Telegram CDN:**\n<{tg_embed or watch_url}>\n\n"
        f"✅ **সুবিধা:** এই লিংকটি ওয়েবসাইটে ব্যবহার করলে টেলিগ্রামের নিজস্ব ক্লাউড থেকে ভিডিও প্লে হবে, ফলে আপনার হোস্টিংয়ের ৫ জিবি লিমিট একটুও কাটবে না!"
    )
    await message.reply_text(rep, reply_markup=markup, disable_web_page_preview=True)

@bot.on_message(filters.private & (filters.video | filters.document | filters.animation))
async def media_handler(client: Client, message: Message):
    user_id = message.from_user.id if message.from_user else 0
    if not await check_is_admin(user_id):
        await message.reply_text(
            "⛔ **অননুমোদিত অ্যাক্সেস!**\n\n"
            f"আপনার Telegram ID: `{user_id}`\n\n"
            "শুধুমাত্র অ্যাডমিনরা ভিডিও প্রসেস করতে পারবেন।"
        )
        return

    media = message.video or message.document or message.animation
    if not media:
        return

    file_size = getattr(media, "file_size", 0) or 0
    file_size_mb = round(file_size / (1024 * 1024), 2)
    min_size_limit = await get_min_file_size_mb()

    if file_size < int(min_size_limit * 1024 * 1024):
        await message.reply_text(
            f"⚠️ **ফাইলটি খুব ছোট!**\n\n"
            f"ফাইলের সাইজ: `{file_size_mb} MB`\n"
            f"ন্যূনতম প্রয়োজন: `{min_size_limit} MB`",
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

    target_bin = await get_bin_channel()
    bin_msg_id = None
    copy_err_text = None
    if target_bin:
        try:
            log_msg = await message.copy(chat_id=target_bin)
            bin_msg_id = log_msg.id
            logger.info(f"Successfully copied media to BIN_CHANNEL {target_bin} with msg #{bin_msg_id}")
        except FloodWait as fw:
            logger.warning(f"FloodWait on BIN_CHANNEL copy: {fw.value}s")
            bin_msg_id = None
            copy_err_text = f"FloodWait ({fw.value}s)"
        except Exception as copy_err:
            copy_err_text = str(copy_err)
            logger.warning(f"BIN_CHANNEL copy error: {copy_err}")

    stream_id = f"tg-{bin_msg_id or message.id}"
    file_name = getattr(media, "file_name", None) or f"SR_Video_{stream_id}.mp4"
    file_size = getattr(media, "file_size", 0) or 0
    mime_type = getattr(media, "mime_type", "video/mp4") or "video/mp4"
    if not mime_type.startswith("video/"):
        mime_type = "video/mp4"
    file_size_mb = round(file_size / (1024 * 1024), 2)

    effective_channel = await get_channel_username()

    tg_embed_url = ""
    tg_post_url = ""
    if effective_channel and bin_msg_id:
        tg_embed_url = f"https://t.me/{effective_channel}/{bin_msg_id}?embed=1"
        tg_post_url = f"https://t.me/{effective_channel}/{bin_msg_id}"
    elif bin_msg_id and target_bin:
        clean_bin_num = str(target_bin).replace("-100", "").replace("-", "")
        tg_post_url = f"https://t.me/c/{clean_bin_num}/{bin_msg_id}"

    doc = {
        "id": stream_id,
        "msg_id": message.id,
        "chat_id": message.chat.id,
        "bin_channel": target_bin if bin_msg_id else None,
        "bin_msg_id": bin_msg_id,
        "channel_username": effective_channel,
        "telegramEmbedUrl": tg_embed_url,
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

    button_rows = []
    if tg_embed_url:
        button_rows.append([
            InlineKeyboardButton("⚡ Telegram Embed (0GB Bandwidth)", url=tg_embed_url),
            InlineKeyboardButton("📱 Telegram Channel Post", url=tg_post_url)
        ])
    elif tg_post_url:
        button_rows.append([
            InlineKeyboardButton("📦 Bin Channel Post", url=tg_post_url)
        ])

    button_rows.extend([
        [
            InlineKeyboardButton("🎬 Watch Video", url=watch_url),
            InlineKeyboardButton("⚡ Direct Download", url=dl_url)
        ],
        [
            InlineKeyboardButton("▶️ Watch 480p", url=f"{watch_url}?q=480p"),
            InlineKeyboardButton("▶️ Watch 720p", url=f"{watch_url}?q=720p"),
            InlineKeyboardButton("▶️ Watch 1080p", url=f"{watch_url}?q=1080p")
        ],
        [
            InlineKeyboardButton("📥 DL 480p", url=f"{dl_url}?quality=480p"),
            InlineKeyboardButton("📥 DL 720p", url=f"{dl_url}?quality=720p"),
            InlineKeyboardButton("📥 DL 1080p", url=f"{dl_url}?quality=1080p")
        ],
        [
            InlineKeyboardButton("⚙️ Watch Quality Menu", callback_data=f"q_watch:{stream_id}"),
            InlineKeyboardButton("⚙️ Download Quality Menu", callback_data=f"q_dl:{stream_id}")
        ]
    ])

    buttons = InlineKeyboardMarkup(button_rows)

    bin_status_banner = ""
    if bin_msg_id:
        bin_status_banner = f"✅ **স্বয়ংক্রিয়ভাবে আপনার Bin Channel-এ ভিডিও পাঠানো হয়েছে!**\n📦 **Bin Msg ID:** `#{bin_msg_id}`\n\n"
    elif copy_err_text:
        bin_status_banner = (
            f"⚠️ **সতর্কতা: ভিডিওটি Bin Channel (`{target_bin}`) এ কপি করা যায়নি!**\n"
            f"কারণ: `{copy_err_text}`\n"
            "👉 দয়া করে নিশ্চিত করুন বটটি আপনার Bin Channel-এ **Administrator** হিসেবে যুক্ত আছে এবং **'Post Messages'** পারমিশন দেওয়া আছে।\n"
            "সঠিক চ্যানেল আইডি সেট করতে লিখুন: `/setbin <channel_id>`\n\n"
        )

    reply_text_body = (
        f"🎬 **{doc['title']}**\n"
        f"📦 **Size:** `{file_size_mb} MB`\n\n"
        + bin_status_banner +
        f"🌐 **Lovable-এ বসানোর লিংক (সরাসরি পেস্ট করলেই চলবে):**\n"
        f"`{watch_url}`\n\n"
        f"⚡ **সুবিধা:** এই লিংকটি ওয়েবসাইটে বসালে স্বয়ংক্রিয়ভাবে টেলিগ্রাম থেকে ০ ব্যান্ডউইথে চলবে (আপনার হোস্টিংয়ের ৫ জিবি লিমিট একটুও কাটবে না!)\n\n"
        f"📋 **রেডি Iframe কোড (যদি iframe হিসেবে বসাতে চান):**\n"
        f"`<iframe src=\"{watch_url}\" width=\"100%\" height=\"450\" frameborder=\"0\" allowfullscreen allow=\"autoplay; fullscreen\"></iframe>`\n\n"
        f"⚡ **Direct Telegram CDN:**\n<{tg_embed_url or watch_url}>\n\n"
        f"📥 **Direct Download Link:**\n<{dl_url}>\n\n"
        f"✨ **অন্যান্য কোয়ালিটি বা অপশন পেতে নিচের বাটন চাপুন:**"
    )

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
            if wait_sec <= 10:
                await asyncio.sleep(wait_sec)
            else:
                await asyncio.sleep(2)
        except Exception as send_err:
            logger.error(f"Error sending reply: {send_err}")
            await asyncio.sleep(1)

# ==============================================================================
# CHANNEL POST HANDLER (সরাসরি BIN CHANNEL-এ ভিডিও পোস্ট করলে অটো ক্যাপচার)
# ==============================================================================
@bot.on_message(filters.channel & (filters.video | filters.document | filters.animation))
async def channel_media_handler(client: Client, message: Message):
    media = message.video or message.document or message.animation
    if not media:
        return

    bin_msg_id = message.id
    stream_id = f"tg-{bin_msg_id}"
    file_name = getattr(media, "file_name", None) or f"SR_Video_{stream_id}.mp4"
    file_size = getattr(media, "file_size", 0) or 0
    file_size_mb = round(file_size / (1024 * 1024), 2)
    mime_type = getattr(media, "mime_type", "video/mp4") or "video/mp4"

    effective_channel = await get_channel_username() or (message.chat.username or "").lstrip("@")
    tg_embed_url = f"https://t.me/{effective_channel}/{bin_msg_id}?embed=1" if effective_channel else ""
    tg_post_url = f"https://t.me/{effective_channel}/{bin_msg_id}" if effective_channel else f"https://t.me/c/{str(message.chat.id).replace('-100', '')}/{bin_msg_id}"

    doc = {
        "id": stream_id,
        "msg_id": message.id,
        "chat_id": message.chat.id,
        "bin_channel": message.chat.id,
        "bin_msg_id": bin_msg_id,
        "channel_username": effective_channel,
        "telegramEmbedUrl": tg_embed_url,
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

    logger.info(f"Channel video indexed from {message.chat.id} as msg #{bin_msg_id}: {doc['title']}")

    admin_notify = (
        f"🎬 **আপনার Bin Channel থেকে নতুন ভিডিও প্রসেস সম্পন্ন!**\n\n"
        f"📌 **শিরোনাম:** `{doc['title']}`\n"
        f"📦 **সাইজ:** `{file_size_mb} MB`\n"
        f"🆔 **Bin Msg ID:** `#{bin_msg_id}`\n\n"
        f"🌐 **Lovable-এ বসানোর লিংক:**\n"
        f"`{watch_url}`\n\n"
        f"📋 **রেডি Lovable Iframe কোড:**\n"
        f"`<iframe src=\"{watch_url}\" width=\"100%\" height=\"450\" frameborder=\"0\" allowfullscreen allow=\"autoplay; fullscreen\"></iframe>`\n\n"
        + (f"⚡ **Telegram 0-Bandwidth Embed:**\n<{tg_embed_url}>\n\n" if tg_embed_url else "")
        + f"📥 **Download:**\n<{dl_url}>\n\n"
        f"✅ ভিডিওটি স্বয়ংক্রিয়ভাবে ০-ব্যান্ডউইথ স্ট্রিমিংয়ের জন্য প্রস্তুত!"
    )

    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("🎬 Watch Player", url=watch_url), InlineKeyboardButton("📥 Direct Download", url=dl_url)],
        [InlineKeyboardButton("📱 Channel Post", url=tg_post_url)]
    ])

    for admin_id in ADMINS:
        try:
            await bot.send_message(admin_id, admin_notify, reply_markup=markup, disable_web_page_preview=True)
        except Exception:
            pass

# ==============================================================================
# FORWARDED MESSAGE HANDLER (চ্যানেল থেকে মেসেজ ফরোয়ার্ড করলে অটো সেট করবে)
# ==============================================================================
@bot.on_message(filters.private & filters.forwarded)
async def forwarded_channel_post_detector(client: Client, message: Message):
    user_id = message.from_user.id if message.from_user else 0
    if not await check_is_admin(user_id):
        return
    fw_chat = message.forward_from_chat
    if fw_chat and fw_chat.type in ("channel", "supergroup"):
        fw_id = fw_chat.id
        fw_title = fw_chat.title or "Channel"
        fw_username = (fw_chat.username or "").lstrip("@")
        fw_msg_id = message.forward_from_message_id

        btns = [
            [InlineKeyboardButton("✅ সেট করুন Bin Channel হিসেবে", callback_data=f"set_bin:{fw_id}")]
        ]
        if fw_username:
            btns.append([InlineKeyboardButton(f"📢 সেট করুন @{fw_username} পাবলিক চ্যানেল", callback_data=f"set_ch:{fw_username}")])

        await message.reply_text(
            f"📢 **ফরোয়ার্ড করা চ্যানেল শনাক্ত হয়েছে!**\n\n"
            f"📌 **নাম:** {fw_title}\n"
            f"🆔 **চ্যানেল আইডি:** `{fw_id}`\n"
            + (f"🌐 **ইউজারনেম:** `@{fw_username}`\n" if fw_username else "")
            + (f"🔢 **মেসেজ আইডি:** `#{fw_msg_id}`\n\n" if fw_msg_id else "\n")
            + "👉 এটিকে আপনার **Bin Channel** হিসেবে সেট করতে নিচের বাটনে চাপুন অথবা লিখুন:\n"
            f"`/setbin {fw_id}`",
            reply_markup=InlineKeyboardMarkup(btns)
        )

@bot.on_callback_query()
async def quality_callback_handler(client: Client, callback_query):
    data = callback_query.data or ""
    if data.startswith("q_watch:"):
        stream_id = data.replace("q_watch:", "")
        watch_url = f"{DOMAIN}/player/{stream_id}"
        await callback_query.answer("ভিডিও কোয়ালিটি মেনু (480p / 720p / 1080p)")
        await callback_query.message.edit_reply_markup(
            InlineKeyboardMarkup([
                [InlineKeyboardButton("🎬 Watch 480p (SD)", url=f"{watch_url}?q=480p")],
                [InlineKeyboardButton("🎬 Watch 720p (HD)", url=f"{watch_url}?q=720p")],
                [InlineKeyboardButton("🎬 Watch 1080p (Full HD)", url=f"{watch_url}?q=1080p")],
                [InlineKeyboardButton("🔙 Back", callback_data=f"q_main:{stream_id}")]
            ])
        )
    elif data.startswith("q_dl:"):
        stream_id = data.replace("q_dl:", "")
        dl_url = f"{DOMAIN}/dl/{stream_id}"
        await callback_query.answer("ডাউনলোড কোয়ালিটি মেনু (480p / 720p / 1080p)")
        await callback_query.message.edit_reply_markup(
            InlineKeyboardMarkup([
                [InlineKeyboardButton("📥 Download 480p (SD)", url=f"{dl_url}?quality=480p")],
                [InlineKeyboardButton("📥 Download 720p (HD)", url=f"{dl_url}?quality=720p")],
                [InlineKeyboardButton("📥 Download 1080p (Full HD)", url=f"{dl_url}?quality=1080p")],
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
                    InlineKeyboardButton("🎬 Watch Video", url=watch_url),
                    InlineKeyboardButton("⚡ Direct Download", url=dl_url)
                ],
                [
                    InlineKeyboardButton("▶️ Watch 480p", url=f"{watch_url}?q=480p"),
                    InlineKeyboardButton("▶️ Watch 720p", url=f"{watch_url}?q=720p"),
                    InlineKeyboardButton("▶️ Watch 1080p", url=f"{watch_url}?q=1080p")
                ],
                [
                    InlineKeyboardButton("📥 DL 480p", url=f"{dl_url}?quality=480p"),
                    InlineKeyboardButton("📥 DL 720p", url=f"{dl_url}?quality=720p"),
                    InlineKeyboardButton("📥 DL 1080p", url=f"{dl_url}?quality=1080p")
                ],
                [
                    InlineKeyboardButton("⚙️ Watch Quality Menu", callback_data=f"q_watch:{stream_id}"),
                    InlineKeyboardButton("⚙️ Download Quality Menu", callback_data=f"q_dl:{stream_id}")
                ]
            ])
        )
    elif data.startswith("set_bin:"):
        chan_id = int(data.replace("set_bin:", ""))
        global BIN_CHANNEL
        BIN_CHANNEL = chan_id
        await settings_col.update_one(
            {"key": "bin_channel"},
            {"$set": {"val": chan_id, "updated_at": datetime.utcnow().isoformat()}},
            upsert=True
        )
        await callback_query.answer("✅ Bin Channel সফলভাবে সেট করা হয়েছে!", show_alert=True)
        await callback_query.message.edit_text(
            f"✅ **Bin Channel সফলভাবে সেট করা হয়েছে!**\n🆔 আইডি: `{chan_id}`\n\nএখন থেকে ভিডিও আপলোড করলে অটো এই চ্যানেলে যাবে এবং সেখান থেকে লিংক দিবে।"
        )
    elif data.startswith("set_ch:"):
        ch_user = data.replace("set_ch:", "").lstrip("@")
        global CHANNEL_USERNAME
        CHANNEL_USERNAME = ch_user
        await settings_col.update_one(
            {"key": "channel_username"},
            {"$set": {"val": ch_user, "updated_at": datetime.utcnow().isoformat()}},
            upsert=True
        )
        await callback_query.answer(f"✅ পাবলিক চ্যানেল @{ch_user} সেট করা হয়েছে!", show_alert=True)
        await callback_query.message.edit_text(
            f"✅ **পাবলিক চ্যানেল আপডেট সফল:** `@{ch_user}`\nএখন সকল ভিডিওতে 0-Bandwidth Telegram Embed লিংক চালু থাকবে!"
        )

# ==============================================================================
# FASTAPI STREAMING & DUAL-ENGINE WEB PLAYER (TELEGRAM EMBED VS SR CUSTOM)
# ==============================================================================
@web_app.api_route("/", methods=["GET", "HEAD"])
async def root(request: Request):
    if request.method == "HEAD":
        return Response(status_code=200)
    count = await videos_col.count_documents({})
    return JSONResponse({
        "status": "online",
        "service": "SS-ANIME-FILE-TO-LINK",
        "zero_bandwidth_telegram_embed": True,
        "public_channel": CHANNEL_USERNAME,
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
async def watch_video(stream_id: str, q: str = "", engine: str = ""):
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
    bin_msg = (doc or {}).get("bin_msg_id") or num_id
    effective_channel = (doc or {}).get("channel_username") or CHANNEL_USERNAME
    telegram_embed_url = (doc or {}).get("telegramEmbedUrl") or ""
    if not telegram_embed_url and effective_channel and bin_msg > 0:
        telegram_embed_url = f"https://t.me/{effective_channel}/{bin_msg}?embed=1"

    use_telegram_native = engine == "telegram" or (bool(telegram_embed_url) and not has_explicit_q and not engine)

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
      background:#070b13; color:#f8fafc; font-family:system-ui,-apple-system,sans-serif;
      overscroll-behavior:none !important; user-select:none; -webkit-user-select:none;
    }}
    .screen-wrapper {{
      width:100vw; height:100dvh; display:flex; flex-direction:column;
      justify-content:center; align-items:center; overflow:hidden; background:#070b13; padding:0;
    }}
    .player-card {{
      width:100%; max-width:850px; display:flex; flex-direction:column;
      justify-content:center; align-items:center; position:relative;
    }}
    /* Mode Switcher Banner: 0 Server Bandwidth Telegram CDN vs SR Custom Stream */
    .engine-switcher {{
      width:100%; display:flex; align-items:center; justify-content:space-between;
      padding:6px 12px; background:#0f172a; border-bottom:1px solid #1e293b;
      font-size:11px; z-index:40;
    }}
    .engine-btn {{
      padding:4px 10px; border-radius:6px; border:1px solid #334155;
      background:#1e293b; color:#94a3b8; font-size:11px; font-weight:600; cursor:pointer;
    }}
    .engine-btn.active {{
      border-color:#10b981; background:rgba(16,185,129,0.15); color:#34d399;
    }}
    .video-box {{
      width:100%; aspect-ratio:16 / 9; max-height:56.25vw;
      background:#000; position:relative; overflow:hidden;
      display:flex; align-items:center; justify-content:center;
      box-shadow:0 12px 40px rgba(0,0,0,0.85);
    }}
    @media (min-width:640px) {{ .video-box {{ border-radius:12px; }} }}
    video, iframe {{ width:100%; height:100%; object-fit:contain; display:block; background:#000; border:none; }}
    .center-action-btn {{
      position:absolute; top:50%; left:50%; transform:translate(-50%,-50%);
      width:66px; height:66px; border-radius:50%;
      background:rgba(15,23,42,0.85); border:2px solid rgba(255,255,255,0.35);
      display:flex; align-items:center; justify-content:center; font-size:28px;
      color:#fff; cursor:pointer; z-index:35; backdrop-filter:blur(8px);
      box-shadow:0 8px 30px rgba(0,0,0,0.75);
    }}
    .center-action-btn.ctrl-hidden {{ opacity:0; pointer-events:none; transform:translate(-50%,-50%) scale(0.7); }}
    .quality-pill {{
      position:absolute; top:12px; right:12px; z-index:30;
      background:rgba(15,23,42,0.85); border:1px solid rgba(255,255,255,0.28);
      color:#f8fafc; font-size:11px; font-weight:700; padding:5px 11px; border-radius:7px; cursor:pointer;
      backdrop-filter:blur(8px); box-shadow:0 4px 15px rgba(0,0,0,0.6);
    }}
    .quality-modal-backdrop {{
      position:fixed; inset:0; background:rgba(5,8,15,0.88); backdrop-filter:blur(10px);
      z-index:100; display:flex; align-items:center; justify-content:center; padding:20px;
    }}
    .quality-modal-backdrop.hidden {{ display:none; }}
    .quality-modal {{
      width:100%; max-width:340px; background:#111827; border:1px solid #1f2937;
      border-radius:18px; padding:22px; display:flex; flex-direction:column; gap:14px;
    }}
    .q-row {{ display:flex; gap:10px; }}
    .q-btn {{
      flex:1; padding:12px 14px; border-radius:10px; border:1px solid #374151;
      background:#1f2937; color:#f8fafc; font-size:14px; font-weight:700; cursor:pointer;
    }}
    .q-dl {{
      padding:12px 14px; border-radius:10px; border:1px solid #059669;
      background:#064e3b; color:#34d399; font-size:13px; font-weight:700; text-decoration:none;
      display:flex; align-items:center; justify-content:center;
    }}
    .embed-box {{
      margin-top:8px; padding:8px 12px; background:#0f172a; border:1px solid #334155;
      border-radius:8px; font-size:11px; color:#cbd5e1; display:flex; align-items:center; justify-content:space-between;
    }}
  </style>
</head>
<body>
  <div class="screen-wrapper">
    <div class="player-card">
      <div class="engine-switcher">
        <span style="color:#10b981;font-weight:700;">⚡ 0GB Server Bandwidth Active</span>
        <div style="display:flex;gap:6px;">
          {"<button type='button' class='engine-btn " + ("active" if use_telegram_native else "") + "' onclick='switchEngine("telegram")'>⚡ Telegram Native (0GB)</button>" if telegram_embed_url else ""}
          <button type="button" class="engine-btn {'active' if not use_telegram_native else ''}" onclick="switchEngine('custom')">🎬 SR Player</button>
        </div>
      </div>
      <div class="video-box" id="video-box">
        {f'<iframe id="tg-frame" src="{telegram_embed_url}" allowfullscreen allow="autoplay; fullscreen"></iframe>' if (use_telegram_native and telegram_embed_url) else f'''
        <button type="button" class="quality-pill" onclick="event.stopPropagation(); document.getElementById('q-modal').classList.remove('hidden')">
          <span id="ql">{initial_q}</span> ⚙️
        </button>
        <button type="button" id="center-play-btn" class="center-action-btn">▶️</button>
        <video id="v" playsinline webkit-playsinline controls preload="auto" src="{stream_url}?quality={initial_q}"></video>
        '''}
      </div>
    </div>
  </div>
  <div id="q-modal" class="quality-modal-backdrop {modal_hidden_class}">
    <div class="quality-modal">
      <div style="font-size:16px;font-weight:700;text-align:center;color:#fff;">কোয়ালিটি সিলেক্ট করুন</div>
      <div class="q-row">
        <button class="q-btn" onclick="setQ('480p')">▶️ Play 480p (SD)</button>
        <a class="q-dl" href="{dl_url}?quality=480p">📥 480p</a>
      </div>
      <div class="q-row">
        <button class="q-btn" onclick="setQ('720p')">▶️ Play 720p (HD)</button>
        <a class="q-dl" href="{dl_url}?quality=720p">📥 720p</a>
      </div>
      <div class="q-row">
        <button class="q-btn" onclick="setQ('1080p')">▶️ Play 1080p (FHD)</button>
        <a class="q-dl" href="{dl_url}?quality=1080p">📥 1080p</a>
      </div>
      <div class="embed-box">
        <span>📋 Lovable Iframe Embed:</span>
        <button type="button" class="engine-btn" onclick="copyEmbed()">Copy Iframe</button>
      </div>
    </div>
  </div>
  <script>
    function switchEngine(type) {{
      const url = new URL(window.location.href);
      url.searchParams.set('engine', type);
      window.location.href = url.toString();
    }}
    function setQ(q) {{
      const url = new URL(window.location.href);
      url.searchParams.set('q', q);
      url.searchParams.set('engine', 'custom');
      window.location.href = url.toString();
    }}
    function copyEmbed() {{
      const code = '{("<iframe src=\"" + telegram_embed_url + "\" width=\"100%\" height=\"450\" frameborder=\"0\" allowfullscreen></iframe>") if telegram_embed_url else ("<iframe src=\"" + DOMAIN + "/player/" + stream_id + "\" width=\"100%\" height=\"450\" frameborder=\"0\" allowfullscreen></iframe>")}';
      navigator.clipboard.writeText(code);
      alert('Iframe Embed Code Copied! Paste this directly into Lovable.');
    }}
  </script>
</body>
</html>"""
    return HTMLResponse(content=html)

async def handle_telegram_stream(stream_id: str, request: Request, quality: str, is_download: bool):
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
    media_target = None
    file_size = 0
    file_name = f"SR_Video_{stream_id}_{quality}.mp4"
    mime_type = "video/mp4"

    if doc and doc.get("telegramFileId"):
        media_target = doc["telegramFileId"]
        file_size = int(doc.get("sizeBytes") or 0)
        file_name = doc.get("fileName") or file_name
        mime_type = doc.get("mimeType") or "video/mp4"

    if not media_target or file_size <= 0:
        try:
            target_bin = int((doc or {}).get("bin_channel") or await get_bin_channel())
            bin_msg = int((doc or {}).get("bin_msg_id") or 0)
            if bin_msg > 0:
                msg = await bot.get_messages(target_bin, bin_msg)
            elif doc and doc.get("chat_id") and doc.get("msg_id"):
                msg = await bot.get_messages(int(doc["chat_id"]), int(doc["msg_id"]))
            else:
                numeric_id = int(stream_id.replace("tg-", ""))
                msg = await bot.get_messages(target_bin, numeric_id)
            media = msg.video or msg.document or msg.animation
            if not media:
                raise HTTPException(status_code=404, detail="Media not found")
            media_target = msg
            file_size = getattr(media, "file_size", 0)
            file_name = getattr(media, "file_name", file_name)
            mime_type = getattr(media, "mime_type", "video/mp4") or "video/mp4"
        except Exception as exc:
            raise HTTPException(status_code=404, detail=f"Video #{stream_id} not found: {exc}")

    if not mime_type.startswith("video/"):
        mime_type = "video/mp4"

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
    chunk_size = 1024 * 1024
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
        # Cache control so Cloudflare and browsers cache video data without hitting server
        "Cache-Control": "public, max-age=604800, s-maxage=2592000, stale-while-revalidate=86400",
    }
    if range_header:
        headers["Content-Range"] = f"bytes {from_bytes}-{until_bytes}/{file_size}"

    if request.method == "HEAD":
        return Response(status_code=206 if range_header else 200, headers=headers)

    async def media_generator():
        nonlocal media_target
        current_part = 1
        try:
            async for chunk in bot.stream_media(media_target, offset=first_part, limit=part_count):
                if part_count == 1:
                    yield chunk[first_part_cut:last_part_cut]
                elif current_part == 1:
                    yield chunk[first_part_cut:]
                elif current_part == part_count:
                    yield chunk[:last_part_cut]
                else:
                    yield chunk
                current_part += 1
        except Exception:
            target_bin = int((doc or {}).get("bin_channel") or await get_bin_channel())
            if doc and doc.get("bin_msg_id"):
                fresh_msg = await bot.get_messages(target_bin, int(doc["bin_msg_id"]))
            elif doc and doc.get("chat_id") and doc.get("msg_id"):
                fresh_msg = await bot.get_messages(int(doc["chat_id"]), int(doc["msg_id"]))
            else:
                numeric_id = int(stream_id.replace("tg-", ""))
                fresh_msg = await bot.get_messages(target_bin, numeric_id)
            fresh_media = fresh_msg.video or fresh_msg.document or fresh_msg.animation
            if fresh_media and getattr(fresh_media, "file_id", None):
                await videos_col.update_one({"id": stream_id}, {"$set": {"telegramFileId": fresh_media.file_id}})
            current_part = 1
            async for chunk in bot.stream_media(fresh_msg, offset=first_part, limit=part_count):
                if part_count == 1:
                    yield chunk[first_part_cut:last_part_cut]
                elif current_part == 1:
                    yield chunk[first_part_cut:]
                elif current_part == part_count:
                    yield chunk[:last_part_cut]
                else:
                    yield chunk
                current_part += 1

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
            logger.error(f"Telegram Bot warning: {bot_err}")
            await web_task
    else:
        logger.info("Running in Web Streaming Server mode.")
        await web_task

    ping_task.cancel()

if __name__ == "__main__":
    try:
        loop.run_until_complete(start_services())
    except RuntimeError:
        asyncio.run(start_services())
