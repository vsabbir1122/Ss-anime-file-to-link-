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
from fastapi.responses import StreamingResponse, HTMLResponse, Response, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from hydrogram import Client, filters, idle
from hydrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from hydrogram.errors import FloodWait

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("SRVideoQualityBot")

# ==============================================================================
# CONFIGURATION & YOUR ROOT ADMIN ID (8056243176)
# ==============================================================================
MONGO_URI = os.environ.get(
    "MONGO_URI",
    "mongodb+srv://sabbirvai1122:SABBIRVAI1122@cluster0.t1bsiei.mongodb.net/?appName=Cluster0"
)
API_ID = int(os.environ.get("API_ID", "29608422"))
API_HASH = os.environ.get("API_HASH", "3db2f8e109301f02f5d9c8f10dd79244")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
BIN_CHANNEL = int(os.environ.get("BIN_CHANNEL", "-1004450462812"))
PORT = int(os.environ.get("PORT", 8080))

# ROOT ADMINS (Includes your ID: 8056243176)
ADMINS_RAW = os.environ.get("ADMINS", "8056243176").strip()
ADMINS = [int(x) for x in re.findall(r"\d+", ADMINS_RAW)]
if 8056243176 not in ADMINS:
    ADMINS.append(8056243176)

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
# MONGODB ATLAS
# ==============================================================================
mongo_client = AsyncIOMotorClient(MONGO_URI)
db = mongo_client["SRVideoQualityBot"]
videos_col = db["videos"]
users_col = db["users"]
admins_col = db["admins"]
locks_col = db["message_locks"]
PROCESSED_KEYS = set()

async def check_is_admin(user_id: int) -> bool:
    if not ADMINS:
        return True
    if user_id in ADMINS:
        return True
    try:
        doc = await admins_col.find_one({"id": user_id})
        return bool(doc)
    except Exception as exc:
        logger.warning(f"Error checking admin: {exc}")
        return False

# Initialize Hydrogram Client
bot = Client(
    "SR_Video_Quality_Bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN if BOT_TOKEN else None,
    in_memory=True
)

web_app = FastAPI(title="SR-VIDEO-QUALITY Streaming Server", version="3.3.0")

web_app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Range", "Content-Length", "Accept-Ranges", "Content-Disposition"],
)

async def keep_alive_ping():
    await asyncio.sleep(15)
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.get(f"{DOMAIN}/health", timeout=10) as resp:
                    logger.info(f"Keep-alive ping to {DOMAIN}/health -> {resp.status}")
            except Exception:
                pass
            await asyncio.sleep(240)

# ==============================================================================
# BOT HANDLERS & ADMIN MANAGEMENT
# ==============================================================================
@bot.on_message(filters.command("start") & filters.private)
async def start_handler(client: Client, message: Message):
    user_id = message.from_user.id if message.from_user else 0
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

    is_user_admin = await check_is_admin(user_id)
    if not is_user_admin:
        await message.reply_text(
            "👋 **স্বাগতম!**\n\n"
            "🔒 **এই বটটি শুধুমাত্র অনুমোদিত অ্যাডমিনদের ব্যবহারের জন্য সংরক্ষিত।**\n\n"
            f"👤 **আপনার টেলিগ্রাম আইডি:** `{user_id}`\n\n"
            "💡 আপনি যদি অ্যাডমিন হতে চান তবে আপনার এই আইডিটি মেইন অ্যাডমিনকে পাঠান।"
        )
        return

    await message.reply_text(
        "🎬 **SR GROUP VIDEO QUALITY Bot Online!**\n\n"
        "ভিডিও ফাইল পাঠালে সাথে সাথে **1080p, 720p, 480p** স্ট্রিমিং ও ফাস্ট ডাউনলোড লিঙ্ক তৈরি হবে।\n\n"
        f"🌐 **অ্যাক্টিভ ডোমেইন:** `{DOMAIN}`\n"
        f"👑 **আপনার অ্যাডমিন স্ট্যাটাস:** Authorized (`{user_id}`)\n\n"
        "🛠️ **অ্যাডমিন কমান্ডসমূহ:**\n"
        "• `/addadmin <id>` - নতুন কাউকে অ্যাডমিন বানান\n"
        "• `/deladmin <id>` - কাউকে অ্যাডমিন থেকে বাদ দিন\n"
        "• `/admins` - সকল অ্যাডমিনের তালিকা দেখুন"
    )

@bot.on_message(filters.command(["addadmin", "promote"]) & filters.private)
async def add_admin_handler(client: Client, message: Message):
    if not message.from_user or not await check_is_admin(message.from_user.id):
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
                await message.reply_text("❌ আইডি সঠিক নয়! উদাহরণ: `/addadmin 12345678`")
                return

    if not target_id:
        await message.reply_text("📌 ব্যবহার: `/addadmin <user_id>` অথবা মেসেজে রিপ্লাই করে `/addadmin`")
        return

    await admins_col.update_one(
        {"id": target_id},
        {"$set": {"id": target_id, "name": target_name, "added_by": message.from_user.id, "added_at": datetime.utcnow().isoformat()}},
        upsert=True
    )
    await message.reply_text(f"✅ ইউজার `{target_id}` সফলভাবে অ্যাডমিন হিসেবে যুক্ত হয়েছেন!")

@bot.on_message(filters.command(["deladmin", "demote"]) & filters.private)
async def del_admin_handler(client: Client, message: Message):
    if not message.from_user or not await check_is_admin(message.from_user.id):
        return

    parts = message.text.strip().split()
    if len(parts) < 2:
        await message.reply_text("📌 ব্যবহার: `/deladmin <user_id>`")
        return
    try:
        target_id = int(parts[1])
    except ValueError:
        return

    if target_id in ADMINS:
        await message.reply_text("⚠️ এটি রুট মেইন অ্যাডমিন আইডি, এটি ডিলিট করা যাবে না।")
        return

    await admins_col.delete_one({"id": target_id})
    await message.reply_text(f"✅ অ্যাডমিন `{target_id}` ডিলিট হয়েছে।")

@bot.on_message(filters.command("admins") & filters.private)
async def list_admins_handler(client: Client, message: Message):
    if not message.from_user or not await check_is_admin(message.from_user.id):
        return

    text = "👑 **অ্যাডমিনদের তালিকা:**\n\n"
    for a in ADMINS:
        text += f"• `{a}` (Root Admin)\n"

    cursor = admins_col.find({})
    dyn = await cursor.to_list(length=100)
    for d in dyn:
        text += f"• `{d['id']}` (বট দিয়ে যুক্ত)\n"

    await message.reply_text(text)

# ==============================================================================
# MEDIA HANDLER - Generates Exact Links & Buttons Like Your Screenshot
# ==============================================================================
@bot.on_message(filters.private & (filters.video | filters.document | filters.animation))
async def media_handler(client: Client, message: Message):
    user_id = message.from_user.id if message.from_user else 0
    if not await check_is_admin(user_id):
        await message.reply_text(
            f"🔒 **অনুমতি নেই!** শুধুমাত্র অনুমোদিত অ্যাডমিনরা ভিডিও লিঙ্ক তৈরি করতে পারেন।\nআপনার আইডি: `{user_id}`"
        )
        return

    media = message.video or message.document or message.animation
    if not media:
        return

    key = f"{message.chat.id}:{message.id}"
    if key in PROCESSED_KEYS:
        return
    PROCESSED_KEYS.add(key)

    status_msg = await message.reply_text("⏳ **Generating Stream Links...**")

    bin_msg_id = None
    try:
        if BIN_CHANNEL:
            fwd_msg = await message.copy(chat_id=BIN_CHANNEL)
            bin_msg_id = fwd_msg.id
    except FloodWait as f:
        await asyncio.sleep(f.value)
        fwd_msg = await message.copy(chat_id=BIN_CHANNEL)
        bin_msg_id = fwd_msg.id
    except Exception as copy_err:
        logger.warning(f"Copy to BIN_CHANNEL skipped: {copy_err}")

    stream_num = bin_msg_id or message.id
    stream_id = f"tg-{stream_num}"
    file_name = getattr(media, "file_name", None) or f"Video_{stream_num}.mp4"
    file_size = getattr(media, "file_size", 0) or 0
    mime_type = getattr(media, "mime_type", "video/mp4") or "video/mp4"
    duration = getattr(media, "duration", 120) or 120
    file_size_mb = round(file_size / (1024 * 1024), 2)

    doc = {
        "id": stream_id,
        "channel_msg_id": stream_num,
        "bin_msg_id": stream_num,
        "file_name": file_name,
        "file_size": file_size,
        "mime_type": mime_type,
        "duration": duration,
        "telegramFileId": getattr(media, "file_id", ""),
        "created_at": datetime.utcnow().isoformat()
    }
    await videos_col.update_one({"id": stream_id}, {"$set": doc}, upsert=True)
    await videos_col.update_one({"channel_msg_id": stream_num}, {"$set": doc}, upsert=True)

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
            InlineKeyboardButton("🎛️ Watch Quality Menu", callback_data=f"qw_{stream_id}"),
            InlineKeyboardButton("📥 Download Quality Menu", callback_data=f"qd_{stream_id}")
        ]
    ])

    await status_msg.edit_text(
        f"🎬 **{file_name}**\n"
        f"📦 **Size:** `{file_size_mb} MB`\n\n"
        f"▶️ **Watch Video Link:**\n{watch_url}\n\n"
        f"⬇️ **Direct Download Link:**\n{dl_url}\n\n"
        f"📥 **Direct Quality Download:**\n"
        f"• [Download 480p]({dl_url}?quality=480p) | [Download 720p]({dl_url}?quality=720p) | [Download 1080p]({dl_url}?quality=1080p)\n\n"
        f"👇 **নিচের বাটন থেকে কোয়ালিটি সিলেক্ট করে ভিডিও দেখুন বা ডাউনলোড করুন:**",
        reply_markup=buttons,
        disable_web_page_preview=True
    )

@bot.on_callback_query(filters.regex(r"^(qw_|qd_)"))
async def callback_quality_menu(client: Client, query: CallbackQuery):
    action, sid = query.data.split("_", 1)
    if action == "qw":
        text = (
            f"🎬 **Watch Quality Options for {sid}:**\n\n"
            f"• 480p: {DOMAIN}/player/{sid}?q=480p\n"
            f"• 720p: {DOMAIN}/player/{sid}?q=720p\n"
            f"• 1080p: {DOMAIN}/player/{sid}?q=1080p"
        )
    else:
        text = (
            f"📥 **Download Quality Options for {sid}:**\n\n"
            f"• 480p: {DOMAIN}/dl/{sid}?quality=480p\n"
            f"• 720p: {DOMAIN}/dl/{sid}?quality=720p\n"
            f"• 1080p: {DOMAIN}/dl/{sid}?quality=1080p"
        )
    await query.message.reply_text(text, disable_web_page_preview=True)
    await query.answer()

# ==============================================================================
# FASTAPI ENDPOINTS & STREAMING CHUNKER
# ==============================================================================
@web_app.api_route("/health", methods=["GET", "HEAD"])
async def health():
    return JSONResponse({"status": "healthy", "domain": DOMAIN, "admin": 8056243176})

def parse_range(range_header: str, file_size: int):
    if not range_header or "=" not in range_header:
        return 0, file_size - 1
    _, vals = range_header.split("=", 1)
    pts = vals.split("-")
    start = int(pts[0]) if pts[0] else 0
    end = int(pts[1]) if len(pts) > 1 and pts[1] else file_size - 1
    return min(start, file_size - 1), min(end, file_size - 1)

async def byte_streamer(client: Client, chat_id: int, message_id: int, start: int, end: int):
    part_size = 1024 * 1024
    start_part = start // part_size
    offset = start % part_size
    total = (end - start) + 1
    sent = 0

    msg = await client.get_messages(chat_id, message_ids=message_id)
    if not msg:
        return
    async for chunk in client.stream_media(msg, offset=start_part):
        if not chunk:
            break
        if offset > 0:
            if len(chunk) <= offset:
                offset -= len(chunk)
                continue
            chunk = chunk[offset:]
            offset = 0

        rem = total - sent
        if rem <= 0:
            break
        if len(chunk) > rem:
            chunk = chunk[:rem]
        sent += len(chunk)
        yield chunk
        if sent >= total:
            break

@web_app.get("/stream/{vid}")
@web_app.get("/download/{vid}")
@web_app.get("/dl/{vid}")
async def stream_download_route(vid: str, request: Request, quality: Optional[str] = None, q: Optional[str] = None):
    clean_id = vid.replace("tg-", "")
    num_id = int(clean_id) if clean_id.isdigit() else 0

    doc = await videos_col.find_one({
        "$or": [{"id": vid}, {"id": f"tg-{clean_id}"}, {"channel_msg_id": num_id}, {"bin_msg_id": num_id}]
    })

    file_size = doc.get("file_size", 0) if doc else 0
    file_name = doc.get("file_name", f"Video_{vid}.mp4") if doc else f"Video_{vid}.mp4"
    mime_type = doc.get("mime_type", "video/mp4") if doc else "video/mp4"

    if file_size == 0 and num_id > 0:
        try:
            m = await bot.get_messages(BIN_CHANNEL, message_ids=num_id)
            if m and (m.video or m.document):
                med = m.video or m.document
                file_size = getattr(med, "file_size", 0)
                file_name = getattr(med, "file_name", file_name)
        except Exception:
            pass

    if file_size == 0:
        raise HTTPException(status_code=404, detail="Video not found")

    range_h = request.headers.get("range")
    is_dl = "/download" in request.url.path or "/dl" in request.url.path

    if range_h:
        s, e = parse_range(range_h, file_size)
        headers = {
            "Content-Range": f"bytes {s}-{e}/{file_size}",
            "Accept-Ranges": "bytes",
            "Content-Length": str((e - s) + 1),
            "Content-Type": mime_type,
        }
        if is_dl:
            headers["Content-Disposition"] = f'attachment; filename="{file_name}"'
        return StreamingResponse(byte_streamer(bot, BIN_CHANNEL, num_id, s, e), status_code=206, headers=headers)
    else:
        headers = {"Accept-Ranges": "bytes", "Content-Length": str(file_size), "Content-Type": mime_type}
        if is_dl:
            headers["Content-Disposition"] = f'attachment; filename="{file_name}"'
        return StreamingResponse(byte_streamer(bot, BIN_CHANNEL, num_id, 0, file_size - 1), status_code=200, headers=headers)

# ==============================================================================
# PLAYER (With Screen Click Play/Pause & YouTube Double Tap 10s Seek)
# ==============================================================================
@web_app.get("/watch/{vid}", response_class=HTMLResponse)
@web_app.get("/player/{vid}", response_class=HTMLResponse)
async def watch_player_route(vid: str, q: Optional[str] = "720p"):
    clean_id = vid.replace("tg-", "")
    num_id = int(clean_id) if clean_id.isdigit() else 0
    doc = await videos_col.find_one({
        "$or": [{"id": vid}, {"id": f"tg-{clean_id}"}, {"channel_msg_id": num_id}, {"bin_msg_id": num_id}]
    })
    file_name = doc.get("file_name", f"Video_{vid}.mp4") if doc else f"Video_{vid}.mp4"
    duration = doc.get("duration", 120) if doc else 120
    stream_url = f"{DOMAIN}/stream/{clean_id}"
    download_url = f"{DOMAIN}/dl/{clean_id}"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
  <title>{file_name}</title>
  <style>
    * {{ box-sizing: border-box; margin: 0; padding: 0; -webkit-tap-highlight-color: transparent; }}
    html, body {{ width: 100%; height: 100%; overflow: hidden; background: #000; color: #f1f5f9; font-family: system-ui, sans-serif; }}
    .player-wrap {{ position: relative; width: 100vw; height: 100vh; display: flex; align-items: center; justify-content: center; background: #000; cursor: pointer; user-select: none; }}
    video {{ width: 100%; height: 100%; object-fit: contain; background: #000; }}
    
    .tap-zone {{
      position: absolute; top: 0; bottom: 70px; width: 45%; z-index: 25;
      display: flex; align-items: center; justify-content: center;
      pointer-events: none; opacity: 0; transition: opacity 0.25s ease;
    }}
    .tap-zone.left {{ left: 0; }}
    .tap-zone.right {{ right: 0; }}
    .tap-zone.active {{ opacity: 1; background: rgba(255, 255, 255, 0.08); }}
    .tap-pill {{
      background: rgba(15, 23, 42, 0.9); border: 1px solid rgba(255, 255, 255, 0.25);
      padding: 10px 18px; border-radius: 999px; font-size: 15px; font-weight: 700;
      color: #10b981; box-shadow: 0 8px 24px rgba(0, 0, 0, 0.7);
    }}

    .badge {{ position: absolute; top: 50%; left: 50%; transform: translate(-50%, -50%) scale(0.6); width: 76px; height: 76px; border-radius: 50%; background: rgba(15, 23, 42, 0.85); border: 2px solid rgba(255, 255, 255, 0.25); display: flex; align-items: center; justify-content: center; font-size: 34px; color: #fff; pointer-events: none; opacity: 0; transition: all 0.25s ease; z-index: 30; }}
    .badge.flash {{ opacity: 1; transform: translate(-50%, -50%) scale(1); }}
    .controls {{ position: absolute; left: 0; right: 0; bottom: 0; z-index: 40; padding: 24px 16px 14px; background: linear-gradient(to top, rgba(0,0,0,0.92) 0%, rgba(0,0,0,0.6) 65%, transparent 100%); display: flex; flex-direction: column; gap: 8px; cursor: default; }}
    .seek {{ width: 100%; accent-color: #10b981; cursor: pointer; height: 7px; border-radius: 999px; -webkit-appearance: none; background: rgba(255, 255, 255, 0.25); }}
    .seek::-webkit-slider-thumb {{ -webkit-appearance: none; width: 16px; height: 16px; border-radius: 50%; background: #10b981; cursor: pointer; }}
    .row {{ display: flex; align-items: center; justify-content: space-between; }}
    .left {{ display: flex; align-items: center; gap: 12px; }}
    .btn {{ background: rgba(255, 255, 255, 0.15); border: 1px solid rgba(255, 255, 255, 0.2); color: #fff; border-radius: 8px; padding: 6px 14px; font-size: 13px; font-weight: 600; cursor: pointer; }}
    .btn:hover {{ background: rgba(255, 255, 255, 0.25); }}
    .time {{ font-size: 13px; font-weight: 600; color: #94a3b8; }}
    .dl-btn {{ background: #10b981; color: #022c22; text-decoration: none; padding: 6px 14px; border-radius: 8px; font-size: 13px; font-weight: 700; }}
  </style>
</head>
<body>
  <div class="player-wrap" id="wrap">
    <div class="tap-zone left" id="zone-left"><div class="tap-pill">⏪ 10s</div></div>
    <div class="tap-zone right" id="zone-right"><div class="tap-pill">10s ⏩</div></div>

    <div class="badge" id="badge">▶️</div>
    <video id="v" playsinline webkit-playsinline preload="auto" src="{stream_url}"></video>
    
    <div class="controls" id="ctrls" onclick="event.stopPropagation()">
      <input type="range" class="seek" id="seek" min="0" max="{duration}" step="1" value="0">
      <div class="row">
        <div class="left">
          <button type="button" class="btn" id="playBtn" onclick="togglePlay()">▶️ Play</button>
          <span class="time" id="time">0:00 / 0:00</span>
        </div>
        <div class="left">
          <a href="{download_url}" class="dl-btn">⬇️ Download</a>
          <button type="button" class="btn" onclick="toggleFull()">⛶ Fullscreen</button>
        </div>
      </div>
    </div>
  </div>
  <script>
    const v = document.getElementById('v'), wrap = document.getElementById('wrap');
    const seek = document.getElementById('seek'), time = document.getElementById('time');
    const badge = document.getElementById('badge'), playBtn = document.getElementById('playBtn');
    const zoneLeft = document.getElementById('zone-left'), zoneRight = document.getElementById('zone-right');
    let totalDur = {duration}, isScrubbing = false, timer = null;
    let lastTapTime = 0, lastTapX = 0, tapTimeout = null;

    function fmt(s) {{ s = Math.max(0, Math.floor(s || 0)); const m = Math.floor(s / 60), r = s % 60; return m + ':' + (r < 10 ? '0' : '') + r; }}
    function flash(icon) {{ badge.textContent = icon; badge.classList.add('flash'); if (timer) clearTimeout(timer); timer = setTimeout(() => badge.classList.remove('flash'), 500); }}

    function togglePlay() {{
      if (v.paused) {{ v.play().catch(() => {{}}); flash('▶️'); playBtn.textContent = '⏸️ Pause'; }}
      else {{ v.pause(); flash('⏸️'); playBtn.textContent = '▶️ Play'; }}
    }}

    function showZoneEffect(side) {{
      const el = side === 'left' ? zoneLeft : zoneRight;
      el.classList.add('active');
      setTimeout(() => el.classList.remove('active'), 450);
    }}

    function seekBy(d) {{
      v.currentTime = Math.max(0, Math.min(totalDur, (v.currentTime || 0) + d));
    }}

    // SCREEN TOUCH: Double Tap Left = -10s, Double Tap Right = +10s, Single Tap = Play/Pause
    wrap.addEventListener('click', function(e) {{
      if (e.target.closest('#ctrls')) return;
      const rect = wrap.getBoundingClientRect();
      const clickX = e.clientX - rect.left;
      const width = rect.width;
      const now = Date.now();
      const diff = now - lastTapTime;

      if (diff < 320 && Math.abs(clickX - lastTapX) < width * 0.35) {{
        if (tapTimeout) {{ clearTimeout(tapTimeout); tapTimeout = null; }}
        if (clickX > width * 0.55) {{
          seekBy(10);
          showZoneEffect('right');
        }} else if (clickX < width * 0.45) {{
          seekBy(-10);
          showZoneEffect('left');
        }} else {{
          togglePlay();
        }}
        lastTapTime = 0;
      }} else {{
        lastTapTime = now;
        lastTapX = clickX;
        if (tapTimeout) clearTimeout(tapTimeout);
        tapTimeout = setTimeout(() => {{
          togglePlay();
          tapTimeout = null;
        }}, 280);
      }}
    }});

    function toggleFull() {{ if (!document.fullscreenElement) wrap.requestFullscreen().catch(() => {{}}); else document.exitFullscreen().catch(() => {{}}); }}

    v.addEventListener('loadedmetadata', function() {{ if (v.duration && isFinite(v.duration)) {{ totalDur = Math.floor(v.duration); seek.max = String(totalDur); }} }});
    v.addEventListener('timeupdate', function() {{ if (!isScrubbing) {{ const cur = v.currentTime || 0; seek.value = String(Math.floor(cur)); time.textContent = fmt(cur) + ' / ' + fmt(totalDur); }} }});
    seek.addEventListener('input', function() {{ isScrubbing = true; time.textContent = fmt(Number(seek.value)) + ' / ' + fmt(totalDur); }} {{
    seek.addEventListener('change', function() {{ isScrubbing = false; v.currentTime = Number(seek.value); }});
  </script>
</body>
</html>
"""
    return HTMLResponse(content=html)

# ==============================================================================
# SECURE START ENTRYPOINT (Guarantees Bot Starts on Render!)
# ==============================================================================
async def main():
    logger.info("Starting Web Streaming Server & Keep-Alive Task...")
    asyncio.create_task(keep_alive_ping())

    if BOT_TOKEN:
        logger.info("Connecting Telegram Bot client...")
        try:
            await bot.start()
            me = await bot.get_me()
            logger.info(f"✅ Bot Online as @{me.username} (ID: {me.id}) | Root Admin: 8056243176")
        except Exception as e:
            logger.error(f"❌ Bot start error: {e}")
    else:
        logger.warning("⚠️ No BOT_TOKEN found! Add BOT_TOKEN in Render Environment Variables.")

    config = uvicorn.Config(web_app, host="0.0.0.0", port=PORT, log_level="info")
    server = uvicorn.Server(config)
    await server.serve()

if __name__ == "__main__":
    asyncio.run(main())
