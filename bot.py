#!/usr/bin/env python3
"""
🤖 Mega Video Compressor Bot
- User Mega.nz link bhejta hai
- Bot download karta hai
- FFmpeg se H.265 CRF 18 compress karta hai
- Mega pe re-upload karta hai
- Compressed link wapas bhejta hai
"""

import os
import re
import sys
import logging
import asyncio
import subprocess
import tempfile
from pathlib import Path
from typing import Optional
from mega import Mega
from PIL import Image
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
)

# ─── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO,
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

# ─── Config ─────────────────────────────────────────────────────────────────
TOKEN = os.getenv("TELEGRAM_TOKEN", "")
MEGA_EMAIL = os.getenv("MEGA_EMAIL", "")
MEGA_PASSWORD = os.getenv("MEGA_PASSWORD", "")

VIDEO_EXTS = {".mp4", ".mkv", ".avi", ".mov", ".webm", ".flv", ".wmv", ".m4v", ".ts"}
IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tiff"}

MEGA_LINK_RE = re.compile(r"https?://(www\.)?mega\.(nz|co\.nz)/[^\s]+")
MAX_FFMPEG_SECONDS = 3 * 60 * 60

# Heavy processing ko single-file-at-a-time rakhne ke liye semaphore.
PROCESS_SEMAPHORE = asyncio.Semaphore(1)
mega_client_lock = asyncio.Lock()
mega_client: Optional[object] = None


# ─── Helpers ─────────────────────────────────────────────────────────────────

def human_size(num_bytes: int) -> str:
    """Bytes ko human-readable format mein convert karo."""
    for unit in ("B", "KB", "MB", "GB"):
        if num_bytes < 1024:
            return f"{num_bytes:.1f} {unit}"
        num_bytes /= 1024
    return f"{num_bytes:.1f} TB"


def compress_video(input_path: str, output_path: str) -> tuple[bool, str]:
    """H.265 CRF-18 se video compress karo."""
    cmd = [
        "ffmpeg", "-i", input_path,
        "-c:v", "libx265",
        "-crf", "18",
        "-preset", "slow",
        "-c:a", "copy",
        "-tag:v", "hvc1",   # Apple/iOS compatibility
        "-movflags", "+faststart",
        "-y",
        output_path,
    ]
    logger.info(f"FFmpeg command: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=MAX_FFMPEG_SECONDS)
    if result.returncode != 0:
        logger.error(f"FFmpeg stderr:\n{result.stderr[-2000:]}")
        return False, result.stderr[-500:]
    return True, ""


def compress_image(input_path: str, output_path: str) -> tuple[bool, str]:
    """Pillow se image optimize karo (lossless/near-lossless)."""
    try:
        img = Image.open(input_path)
        ext = Path(output_path).suffix.lower()
        if ext in (".jpg", ".jpeg"):
            img.save(output_path, "JPEG", quality=92, optimize=True)
        elif ext == ".png":
            img.save(output_path, "PNG", optimize=True, compress_level=9)
        elif ext == ".webp":
            img.save(output_path, "WEBP", quality=92, method=6)
        else:
            img.save(output_path, optimize=True)
        return True, ""
    except Exception as e:
        return False, str(e)


async def get_mega_client(force_relogin: bool = False):
    """Thread-safe Mega login with retry for stale sessions."""
    global mega_client

    async with mega_client_lock:
        if mega_client is not None and not force_relogin:
            return mega_client

        logger.info("🔐 Mega pe login ho raha hai...")

        def _login_once():
            session = Mega()
            return session.login(MEGA_EMAIL, MEGA_PASSWORD)

        for attempt in range(1, 4):
            try:
                mega_client = await asyncio.to_thread(_login_once)
                logger.info("✅ Mega login successful!")
                return mega_client
            except Exception as exc:
                logger.warning(f"Mega login failed (attempt {attempt}/3): {exc}")
                if attempt >= 3:
                    raise
                await asyncio.sleep(2 * attempt)


async def mega_download(url: str, dest_path: str) -> Path:
    """Download with one relogin retry when session expires."""
    for attempt in (1, 2):
        client = await get_mega_client(force_relogin=(attempt == 2))
        try:
            downloaded = await asyncio.to_thread(client.download_url, url, dest_path)
            if not downloaded:
                raise RuntimeError("Mega download returned empty path")
            if isinstance(downloaded, (list, tuple)):
                raise ValueError("Folder links abhi supported nahi hain, file link bhejo")
            return Path(downloaded)
        except Exception:
            if attempt == 2:
                raise


async def mega_upload(file_path: str) -> str:
    """Upload with one relogin retry when session expires."""
    for attempt in (1, 2):
        client = await get_mega_client(force_relogin=(attempt == 2))
        try:
            uploaded_file = await asyncio.to_thread(client.upload, file_path)
            return await asyncio.to_thread(client.get_upload_link, uploaded_file)
        except Exception:
            if attempt == 2:
                raise


# ─── Telegram Handlers ───────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 *Mega Video/Image Compressor Bot mein aapka swagat hai!*\n\n"
        "📌 *Kaise use karein:*\n"
        "Bas mujhe koi bhi *Mega.nz link* bhejdo\n\n"
        "🎬 *Video ke liye:*\n"
        "H.265 (CRF 18) codec se compress hoga\n"
        "_(Visually same quality, 50-70% chhoti file)_\n\n"
        "🖼️ *Image ke liye:*\n"
        "Lossless optimization hogi\n\n"
        "⚡ *Supported formats:*\n"
        "`MP4, MKV, AVI, MOV, WEBM` aur\n"
        "`JPG, PNG, WEBP, BMP`\n\n"
        "🔗 Link bhejo aur magic dekho! ✨",
        parse_mode="Markdown",
    )


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📖 *Help*\n\n"
        "1️⃣ Mega.nz pe apni file upload karo\n"
        "2️⃣ Share link copy karo\n"
        "3️⃣ Mujhe woh link bhejo\n"
        "4️⃣ Main compress karke new Mega link bhejunga\n\n"
        "⚠️ *Note:* Badi files (1GB+) mein zyada time lagta hai.",
        parse_mode="Markdown",
    )


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    if message is None:
        return

    text = message.text.strip() if message.text else ""

    # Mega link dhundho
    match = MEGA_LINK_RE.search(text)
    if not match:
        await message.reply_text(
            "⚠️ Koi valid *Mega.nz link* nahi mili!\n\n"
            "Format: `https://mega.nz/file/...` ya `https://mega.nz/folder/...`",
            parse_mode="Markdown",
        )
        return

    mega_url = match.group(0)
    user = message.from_user
    user_id = user.id if user else "unknown"
    logger.info(f"📥 New request | User: {user_id} | URL: {mega_url}")

    # Status message
    status = await message.reply_text(
        "⏳ *[1/4]* Mega se download shuru ho raha hai...\n"
        "_(Badi file hai toh thoda wait karo)_",
        parse_mode="Markdown",
    )

    async with PROCESS_SEMAPHORE:
        with tempfile.TemporaryDirectory(prefix="megabot_") as tmp_dir:
            try:
                # ── Step 1: Download ──────────────────────────────────────────
                logger.info(f"⬇️  Downloading: {mega_url}")
                input_path = await mega_download(mega_url, tmp_dir)
                ext = input_path.suffix.lower()

                orig_size = input_path.stat().st_size
                orig_str = human_size(orig_size)

                logger.info(f"✅ Download done: {input_path.name} ({orig_str})")

                # File type check
                if ext not in VIDEO_EXTS and ext not in IMAGE_EXTS:
                    await status.edit_text(
                        f"❌ Unsupported file type: `{ext}`\n\n"
                        "Sirf video (mp4, mkv, avi…) ya image (jpg, png…) files supported hain.",
                        parse_mode="Markdown",
                    )
                    return

                is_video = ext in VIDEO_EXTS
                file_type_emoji = "🎬" if is_video else "🖼️"

                # ── Step 2: Compress ──────────────────────────────────────────
                await status.edit_text(
                    f"✅ *[1/4]* Download complete! ({orig_str})\n\n"
                    f"🔄 *[2/4]* {file_type_emoji} Compress ho raha hai...\n"
                    f"_(H.265 CRF-18 | Isme waqt lagta hai, wait karo)_",
                    parse_mode="Markdown",
                )

                if is_video:
                    output_path = Path(tmp_dir) / f"compressed_{input_path.stem}.mp4"
                    success, err_msg = await asyncio.to_thread(compress_video, str(input_path), str(output_path))
                else:
                    output_path = Path(tmp_dir) / f"compressed_{input_path.name}"
                    success, err_msg = await asyncio.to_thread(compress_image, str(input_path), str(output_path))

                if not success:
                    await status.edit_text(
                        f"❌ Compression fail ho gayi!\n\n`{err_msg}`",
                        parse_mode="Markdown",
                    )
                    return

                comp_size = output_path.stat().st_size
                comp_str = human_size(comp_size)
                reduction_pct = ((orig_size - comp_size) / orig_size) * 100

                logger.info(f"✅ Compressed: {orig_str} → {comp_str} ({reduction_pct:.1f}% saved)")

                # ── Step 3: Upload ────────────────────────────────────────────
                await status.edit_text(
                    f"✅ *[1/4]* Download: {orig_str}\n"
                    f"✅ *[2/4]* Compress: {orig_str} → {comp_str} (*{reduction_pct:.1f}%* saved)\n\n"
                    f"📤 *[3/4]* Mega pe upload ho raha hai...",
                    parse_mode="Markdown",
                )

                logger.info(f"⬆️  Uploading to Mega: {output_path.name}")
                new_link = await mega_upload(str(output_path))

                logger.info(f"✅ Upload done: {new_link}")

                # ── Step 4: Done ──────────────────────────────────────────────
                savings_icon = "🔥" if reduction_pct > 40 else "✅"

                await status.edit_text(
                    f"{savings_icon} *Kaam ho gaya! Sab done!*\n\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"📊 *Compression Stats:*\n"
                    f"• Pehle:  `{orig_str}`\n"
                    f"• Baad:   `{comp_str}`\n"
                    f"• Saved:  `{reduction_pct:.1f}%` {savings_icon}\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"🔗 *Compressed File Download Link:*\n"
                    f"{new_link}",
                    parse_mode="Markdown",
                )

            except Exception as exc:
                logger.exception(f"❌ Unexpected error: {exc}")
                await status.edit_text(
                    f"Error aa gaya: {type(exc).__name__}: {exc}\n\n"
                    "Dobara try karo ya /help dekho.",
                )


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    missing = [
        name for name, value in (
            ("TELEGRAM_TOKEN", TOKEN),
            ("MEGA_EMAIL", MEGA_EMAIL),
            ("MEGA_PASSWORD", MEGA_PASSWORD),
        ) if not value
    ]
    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

    app = (
        Application.builder()
        .token(TOKEN)
        .build()
    )

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help",  cmd_help))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    logger.info("🤖 Bot polling shuru ho gaya!")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
