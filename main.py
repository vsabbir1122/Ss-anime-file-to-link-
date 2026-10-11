import os
import asyncio
from fastapi import FastAPI, Request, HTTPException, Response
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from hydrogram import Client

# ১. টেলিগ্রাম সেটিংস (Render Environment Variables থেকে আসবে)
API_ID = int(os.environ.get("API_ID", "0"))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
BIN_CHANNEL = int(os.environ.get("BIN_CHANNEL", "0"))
PORT = int(os.environ.get("PORT", 8080))

app = FastAPI(title="SSAnime Telegram Stream Server")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

bot = Client(
    "ssanime_streamer",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    in_memory=True
)

@app.on_event("startup")
async def on_startup():
    await bot.start()
    print("Telegram Stream Bot Started Successfully!")

@app.on_event("shutdown")
async def on_shutdown():
    await bot.stop()

@app.head("/")
def home():
    return {"status": "online", "message": "SSAnime Telegram Engine is Running!"}

# স্ট্রিমিং ও ডাউনলোডের মাস্টার ফাংশন (Zero Disk Usage)
@app.get("/stream/{msg_id}")
@app.get("/download/{msg_id}")
async def handle_stream_or_download(msg_id: int, request: Request):
    is_download = request.url.path.startswith("/download")
    try:
        msg = await bot.get_messages(BIN_CHANNEL, msg_id)
    except Exception as e:
        raise HTTPException(status_code=404, detail="Message not found")

    media = msg.video or msg.document or msg.animation
    if not media:
        raise HTTPException(status_code=404, detail="No video found in this message")

    file_size = getattr(media, "file_size", 0)
    file_name = getattr(media, "file_name", f"video_{msg_id}.mp4")
    mime_type = getattr(media, "mime_type", "video/mp4") or "video/mp4"

    # HTTP 206 Byte-Range ক্যালকুলেশন (ভিডিও ফাস্ট লোড ও সিঙ্ক করার জন্য)
    range_header = request.headers.get("Range", "")
    if range_header and range_header.startswith("bytes="):
        parts = range_header.replace("bytes=", "").split("-")
        from_bytes = int(parts[0]) if parts[0] else 0
        until_bytes = int(parts[1]) if len(parts) > 1 and parts[1] else file_size - 1
    else:
        from_bytes = 0
        until_bytes = file_size - 1

    until_bytes = min(until_bytes, file_size - 1)
    req_length = until_bytes - from_bytes + 1
    chunk_size = 1024 * 1024  # 1 MB chunk

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

    async def stream_generator():
        current_part = 1
        async for chunk in bot.stream_media(msg, offset=first_part, limit=part_count):
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
        stream_generator(),
        status_code=206 if range_header else 200,
        headers=headers
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=PORT)
