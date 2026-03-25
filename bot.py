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
import time
import json
import shutil
import uuid
from pathlib import Path
from typing import Awaitable, Callable, Optional
import requests
from mega import Mega
from PIL import Image
from telegram import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.error import BadRequest
from telegram.ext import (
    Application,
    CallbackQueryHandler,
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
MEGA_EMAIL = os.getenv("MEGA_EMAIL", "pakato5443@muncloud.com")
MEGA_PASSWORD = os.getenv("MEGA_PASSWORD", "Robot123/")
MEGA_EMAIL_2 = os.getenv("MEGA_EMAIL_2", "gojip31393@fabaos.com")
MEGA_PASSWORD_2 = os.getenv("MEGA_PASSWORD_2", "Robot123/")
MEGA_EMAIL_3 = os.getenv("MEGA_EMAIL_3", "vemorig951@fun4k.com")
MEGA_PASSWORD_3 = os.getenv("MEGA_PASSWORD_3", "Robot123/")

# Speed-oriented defaults; quality ko env vars se tune kiya ja sakta hai.
VIDEO_CRF = os.getenv("VIDEO_CRF", "18")
FFMPEG_PRESET = os.getenv("FFMPEG_PRESET", "medium")
FFMPEG_THREADS = os.getenv("FFMPEG_THREADS", "0")
PROGRESS_UPDATE_SECONDS = max(10, int(os.getenv("PROGRESS_UPDATE_SECONDS", "10")))
MEGA_MIN_ACTION_GAP_SECONDS = max(4, int(os.getenv("MEGA_MIN_ACTION_GAP_SECONDS", "6")))
MEGA_TEMP_COOLDOWN_SECONDS = max(60, int(os.getenv("MEGA_TEMP_COOLDOWN_SECONDS", "300")))
MEGA_BLOCK_COOLDOWN_SECONDS = max(300, int(os.getenv("MEGA_BLOCK_COOLDOWN_SECONDS", "1800")))
OUTPUT_SHARE_PROVIDER = os.getenv("OUTPUT_SHARE_PROVIDER", "transfersh").strip().lower()
OUTPUT_SHARE_PROVIDERS = os.getenv("OUTPUT_SHARE_PROVIDERS", "").strip()
TRANSFER_SH_BASE = os.getenv("TRANSFER_SH_BASE", "https://transfer.sh").rstrip("/")
FILEIO_BASE = os.getenv("FILEIO_BASE", "https://file.io").rstrip("/")
PREFER_ANON_MEGA_DOWNLOAD = os.getenv("PREFER_ANON_MEGA_DOWNLOAD", "1").strip() not in {"0", "false", "False"}
OUTPUT_CACHE_DIR = os.getenv("OUTPUT_CACHE_DIR", "_outputs").strip() or "_outputs"
MAX_STORED_OUTPUTS_PER_USER = max(1, int(os.getenv("MAX_STORED_OUTPUTS_PER_USER", "5")))

VIDEO_EXTS = {".mp4", ".mkv", ".avi", ".mov", ".webm", ".flv", ".wmv", ".m4v", ".ts"}
IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tiff"}

MEGA_LINK_RE = re.compile(r"https?://(www\.)?mega\.(nz|co\.nz)/[^\s]+")
MEGA_FILE_LINK_RE = re.compile(
    r"https?://(www\.)?mega\.(nz|co\.nz)/(file/[^\s#?]+#[^\s]+|#![^\s!]+![^\s]+)",
    re.IGNORECASE,
)
MAX_FFMPEG_SECONDS = 3 * 60 * 60

# Heavy processing ko single-file-at-a-time rakhne ke liye semaphore.
PROCESS_SEMAPHORE = asyncio.Semaphore(1)
mega_client_lock = asyncio.Lock()
mega_rate_lock = asyncio.Lock()
mega_client: Optional[object] = None
last_mega_action_at = 0.0
MEGA_ACCOUNTS: list[tuple[str, str]] = []
ACTIVE_MEGA_ACCOUNT_INDEX = 0
MEGA_ACCOUNT_COOLDOWN_UNTIL: dict[str, float] = {}
OUTPUT_REGISTRY: dict[str, dict] = {}
USER_OUTPUT_INDEX: dict[int, list[str]] = {}
output_registry_lock = asyncio.Lock()
PHASE_AVG_SECONDS = {
    "download": 180,
    "compress": 420,
    "upload": 180,
}

PROVIDER_LABELS = {
    "mega": "Mega",
    "fileio": "File.io",
    "transfersh": "Transfer.sh",
    "sendanywhere": "Transfer.sh",
    "wetransfer": "WeTransfer",
    "onionshare": "OnionShare",
}

AWAITING_MEGA_ACCOUNT_INPUT_KEY = "awaiting_mega_account_input"


# ─── Helpers ─────────────────────────────────────────────────────────────────

def human_size(num_bytes: int) -> str:
    """Bytes ko human-readable format mein convert karo."""
    for unit in ("B", "KB", "MB", "GB"):
        if num_bytes < 1024:
            return f"{num_bytes:.1f} {unit}"
        num_bytes /= 1024
    return f"{num_bytes:.1f} TB"


def human_duration(seconds: int) -> str:
    """Seconds ko human-readable duration mein convert karo."""
    seconds = max(0, int(seconds))
    mins, secs = divmod(seconds, 60)
    hours, mins = divmod(mins, 60)
    if hours > 0:
        return f"{hours}h {mins}m"
    if mins > 0:
        return f"{mins}m {secs}s"
    return f"{secs}s"


def mask_email(email: str) -> str:
    """Email ko logs/status ke liye partially mask karo."""
    if "@" not in email:
        return "unknown"
    local, domain = email.split("@", 1)
    safe_local = (local[:2] + "***") if local else "***"
    safe_domain = domain[:1] + "***"
    return f"{safe_local}@{safe_domain}"


def build_mega_accounts() -> list[tuple[str, str]]:
    """Primary + backup Mega accounts collect karo."""
    accounts: list[tuple[str, str]] = []

    for email, password in (
        (MEGA_EMAIL, MEGA_PASSWORD),
        (MEGA_EMAIL_2, MEGA_PASSWORD_2),
        (MEGA_EMAIL_3, MEGA_PASSWORD_3),
    ):
        if email and password:
            accounts.append((email.strip(), password))

    for idx in range(3, 7):
        email = os.getenv(f"MEGA_EMAIL_{idx}", "").strip()
        password = os.getenv(f"MEGA_PASSWORD_{idx}", "")
        if email and password:
            accounts.append((email, password))

    # Duplicate emails hata do.
    seen: set[str] = set()
    unique_accounts: list[tuple[str, str]] = []
    for email, password in accounts:
        key = email.lower()
        if key in seen:
            continue
        seen.add(key)
        unique_accounts.append((email, password))
    return unique_accounts


def apply_mega_runtime_patches() -> None:
    """Known mega.py runtime bugs ke liye safe monkey patches apply karo."""
    if getattr(Mega, "_megabot_download_patch_applied", False):
        return

    original_download = getattr(Mega, "_download_file", None)
    if original_download is None:
        return

    g = original_download.__globals__
    mega_base64_to_a32 = g.get("base64_to_a32")
    mega_base64_url_decode = g.get("base64_url_decode")
    mega_decrypt_attr = g.get("decrypt_attr")
    mega_a32_to_str = g.get("a32_to_str")
    mega_str_to_a32 = g.get("str_to_a32")
    mega_get_chunks = g.get("get_chunks")
    mega_request_error = g.get("RequestError")
    mega_counter = g.get("Counter")
    mega_aes = g.get("AES")

    required = [
        mega_base64_to_a32,
        mega_base64_url_decode,
        mega_decrypt_attr,
        mega_a32_to_str,
        mega_str_to_a32,
        mega_get_chunks,
        mega_request_error,
        mega_counter,
        mega_aes,
    ]
    if any(item is None for item in required):
        logger.warning("Skipping Mega runtime patch: dependency symbol missing")
        return

    def _download_file_workaround(self, file_handle, file_key, dest_path=None, dest_filename=None, is_public=False, file=None):
        if file is None:
            if is_public:
                file_key = mega_base64_to_a32(file_key)
                file_data = self._api_request({
                    "a": "g",
                    "g": 1,
                    "p": file_handle,
                })
            else:
                file_data = self._api_request({
                    "a": "g",
                    "g": 1,
                    "n": file_handle,
                })

            k = (file_key[0] ^ file_key[4], file_key[1] ^ file_key[5], file_key[2] ^ file_key[6], file_key[3] ^ file_key[7])
            iv = file_key[4:6] + (0, 0)
            meta_mac = file_key[6:8]
        else:
            file_data = self._api_request({"a": "g", "g": 1, "n": file["h"]})
            k = file["k"]
            iv = file["iv"]
            meta_mac = file["meta_mac"]

        if "g" not in file_data:
            raise mega_request_error("File not accessible anymore")

        file_url = file_data["g"]
        file_size = file_data["s"]
        attribs = mega_base64_url_decode(file_data["at"])
        attribs = mega_decrypt_attr(attribs, k)
        file_name = dest_filename if dest_filename is not None else attribs["n"]

        input_file = requests.get(file_url, stream=True).raw
        output_dir = "" if dest_path is None else f"{dest_path}/"

        with tempfile.NamedTemporaryFile(mode="w+b", prefix="megabot_", delete=False) as temp_output_file:
            k_str = mega_a32_to_str(k)
            counter = mega_counter.new(128, initial_value=((iv[0] << 32) + iv[1]) << 64)
            aes = mega_aes.new(k_str, mega_aes.MODE_CTR, counter=counter)

            mac_str = "\0" * 16
            mac_encryptor = mega_aes.new(k_str, mega_aes.MODE_CBC, mac_str.encode("utf8"))
            iv_str = mega_a32_to_str([iv[0], iv[1], iv[0], iv[1]])

            for chunk_start, chunk_size in mega_get_chunks(file_size):
                chunk = input_file.read(chunk_size)
                chunk = aes.decrypt(chunk)
                temp_output_file.write(chunk)

                encryptor = mega_aes.new(k_str, mega_aes.MODE_CBC, iv_str)
                last_idx = 0
                for idx in range(0, len(chunk) - 16, 16):
                    block = chunk[idx:idx + 16]
                    encryptor.encrypt(block)
                    last_idx = idx

                # mega.py bugfix: last chunk <16 bytes hone par idx undefined rehta tha.
                block_start = (last_idx + 16) if len(chunk) > 16 else 0
                block = chunk[block_start:block_start + 16]
                if len(block) % 16:
                    block += b"\0" * (16 - (len(block) % 16))
                mac_str = mac_encryptor.encrypt(encryptor.encrypt(block))

                file_info = os.stat(temp_output_file.name)
                logger.info("%s of %s downloaded", file_info.st_size, file_size)

            file_mac = mega_str_to_a32(mac_str)
            if (file_mac[0] ^ file_mac[1], file_mac[2] ^ file_mac[3]) != meta_mac:
                raise ValueError("Mismatched mac")

            output_path = Path(output_dir + file_name)
            shutil.move(temp_output_file.name, output_path)
            return output_path

    def _download_file_patched(self, file_handle, file_key, dest_path=None, dest_filename=None, is_public=False, file=None):
        try:
            return original_download(
                self,
                file_handle,
                file_key,
                dest_path=dest_path,
                dest_filename=dest_filename,
                is_public=is_public,
                file=file,
            )
        except UnboundLocalError as exc:
            if "local variable 'i' referenced before assignment" not in str(exc):
                raise
            logger.warning("Applying Mega download workaround for local variable 'i' bug")
            return _download_file_workaround(
                self,
                file_handle,
                file_key,
                dest_path=dest_path,
                dest_filename=dest_filename,
                is_public=is_public,
                file=file,
            )

    Mega._download_file = _download_file_patched
    Mega._megabot_download_patch_applied = True
    logger.info("Applied Mega runtime patch: download local variable 'i' workaround")


def parse_mega_account_input(text: str) -> Optional[tuple[str, str]]:
    """User input se Mega email/password parse karo."""
    cleaned = (text or "").strip()
    if not cleaned:
        return None

    if "\n" in cleaned:
        lines = [line.strip() for line in cleaned.splitlines() if line.strip()]
        if len(lines) >= 2:
            email, password = lines[0], lines[1]
            return (email, password)

    if "," in cleaned:
        parts = [part.strip() for part in cleaned.split(",", 1)]
        if len(parts) == 2 and parts[0] and parts[1]:
            return parts[0], parts[1]

    parts = cleaned.split(maxsplit=1)
    if len(parts) == 2 and parts[0] and parts[1]:
        return parts[0].strip(), parts[1].strip()

    return None


def is_valid_email_like(value: str) -> bool:
    """Basic email validation for UX (strict RFC validation ki zarurat nahi)."""
    if "@" not in value:
        return False
    local, domain = value.split("@", 1)
    return bool(local and domain and "." in domain)


async def add_runtime_mega_account(email: str, password: str) -> tuple[bool, str]:
    """Runtime account list mein naya Mega account add karo."""
    global MEGA_ACCOUNTS

    normalized_email = email.strip()
    if not normalized_email or not password:
        return False, "Email/password empty nahi hone chahiye."
    if not is_valid_email_like(normalized_email):
        return False, "Email format invalid lag raha hai."

    async with mega_client_lock:
        existing = {acc_email.lower() for acc_email, _ in MEGA_ACCOUNTS}
        if normalized_email.lower() in existing:
            return False, "Ye account pehle se configured hai."

        MEGA_ACCOUNTS.append((normalized_email, password))
        MEGA_ACCOUNT_COOLDOWN_UNTIL.pop(normalized_email.lower(), None)

    return True, mask_email(normalized_email)


def current_mega_account_email() -> str:
    """Current active Mega account email return karo."""
    if not MEGA_ACCOUNTS:
        return ""
    return MEGA_ACCOUNTS[ACTIVE_MEGA_ACCOUNT_INDEX][0]


def account_cooldown_remaining(email: str) -> int:
    """Account cooldown ka remaining time seconds mein do."""
    if not email:
        return 0
    until = MEGA_ACCOUNT_COOLDOWN_UNTIL.get(email.lower(), 0.0)
    return max(0, int(until - time.monotonic()))


def set_account_cooldown(email: str, seconds: int, reason: str) -> None:
    """Blocked/throttled account ko temporary rest do."""
    if not email or seconds <= 0:
        return
    until = time.monotonic() + seconds
    MEGA_ACCOUNT_COOLDOWN_UNTIL[email.lower()] = until
    logger.warning(
        f"⏸️ Cooling Mega account {mask_email(email)} for {seconds}s ({reason})"
    )


def find_available_account_index(start_index: int) -> Optional[int]:
    """Given index se next non-cooled Mega account dhundo."""
    if not MEGA_ACCOUNTS:
        return None
    total = len(MEGA_ACCOUNTS)
    for offset in range(total):
        idx = (start_index + offset) % total
        email, _ = MEGA_ACCOUNTS[idx]
        if account_cooldown_remaining(email) <= 0:
            return idx
    return None


def shortest_account_cooldown() -> int:
    """Sab accounts cooled hon to minimum wait duration return karo."""
    if not MEGA_ACCOUNTS:
        return 0
    waits = [
        account_cooldown_remaining(email)
        for email, _ in MEGA_ACCOUNTS
        if account_cooldown_remaining(email) > 0
    ]
    return min(waits) if waits else 0


async def throttle_mega_action(action: str, email: str = "") -> None:
    """Mega API actions ke beech minimum gap maintain karo to avoid rate spikes."""
    global last_mega_action_at
    async with mega_rate_lock:
        now = time.monotonic()
        wait_for = (last_mega_action_at + MEGA_MIN_ACTION_GAP_SECONDS) - now
        if wait_for > 0:
            logger.info(f"⏳ Pacing Mega {action}: waiting {wait_for:.1f}s")
            await asyncio.sleep(wait_for)
        last_mega_action_at = time.monotonic()


def estimate_eta_range(phase: str, elapsed_seconds: int) -> str:
    """Elapsed time aur phase averages se rough ETA range banao."""
    if elapsed_seconds < 20:
        return "estimating..."

    average = PHASE_AVG_SECONDS.get(phase, 180)
    remaining = max(average - elapsed_seconds, 8)
    if elapsed_seconds > average:
        remaining = max(int(elapsed_seconds * 0.35), 15)

    low = max(5, int(remaining * 0.7))
    high = max(low + 5, int(remaining * 1.35))
    return f"~{human_duration(low)} to {human_duration(high)}"


def update_phase_average(phase: str, elapsed_seconds: float) -> None:
    """Per phase moving-average update karo for better ETA estimates."""
    prev = PHASE_AVG_SECONDS.get(phase, int(elapsed_seconds))
    PHASE_AVG_SECONDS[phase] = max(10, int((prev * 0.7) + (elapsed_seconds * 0.3)))


def normalize_provider_name(name: str) -> str:
    """Provider aliases ko canonical provider names mein map karo."""
    key = name.strip().lower()
    alias_map = {
        "transfer": "transfersh",
        "transfer.sh": "transfersh",
        "transferit": "transfersh",
        "transfer.it": "transfersh",
        "send-anywhere": "sendanywhere",
        "send anywhere": "sendanywhere",
        "file.io": "fileio",
        "we-transfer": "wetransfer",
    }
    return alias_map.get(key, key)


def configured_upload_providers() -> list[str]:
    """Configured provider order return karo (resilient fallback chain ke saath)."""
    chain = OUTPUT_SHARE_PROVIDERS
    if chain:
        raw_items = [p.strip() for p in chain.split(",") if p.strip()]
    else:
        raw_items = [OUTPUT_SHARE_PROVIDER]

    providers: list[str] = []
    seen: set[str] = set()
    for item in raw_items:
        normalized = normalize_provider_name(item)
        if normalized not in seen:
            seen.add(normalized)
            providers.append(normalized)

    # Agar single public provider fail ho jaye (network blocked), backup providers auto add karo.
    if len(providers) <= 1:
        for fallback in ("fileio", "transfersh"):
            if fallback not in seen:
                seen.add(fallback)
                providers.append(fallback)

    # Mega account available ho to last-resort rescue path add karo.
    if MEGA_ACCOUNTS and "mega" not in seen:
        seen.add("mega")
        providers.append("mega")

    if not providers:
        providers = ["fileio", "transfersh"]

    return providers


def provider_display_name(name: str) -> str:
    """Provider ka user-friendly display name."""
    return PROVIDER_LABELS.get(name, name)


async def safe_edit_status(status_message, text: str, edit_lock: asyncio.Lock) -> None:
    """Status message edit with lock + harmless no-change handling."""
    async with edit_lock:
        await edit_text_with_fallback(status_message, text)


async def edit_text_with_fallback(message, text: str, reply_markup=None) -> None:
    """Markdown parse fail hone par plain text fallback ke saath edit karo."""
    try:
        await message.edit_text(text, parse_mode="Markdown", reply_markup=reply_markup)
        return
    except BadRequest as exc:
        lowered = str(exc).lower()
        if "message is not modified" in lowered:
            return
        if "parse entities" in lowered:
            # Dynamic URL/error text se Markdown parse fail ho to plain text fallback bhejo.
            plain_text = text.replace("`", "").replace("*", "").replace("_", "")
            await message.edit_text(plain_text, reply_markup=reply_markup)
            return
        raise


def build_progress_text(progress_state: dict) -> str:
    """Progress heartbeat message compose karo."""
    elapsed = int(time.monotonic() - progress_state["phase_started"])
    eta_text = estimate_eta_range(progress_state["phase"], elapsed)
    retry_line = ""
    if progress_state["max_attempts"] > 1:
        retry_line = f"\n🔁 Attempt: `{progress_state['attempt']}/{progress_state['max_attempts']}`"

    return (
        f"{progress_state['header']}\n"
        f"{progress_state['detail']}\n\n"
        f"⏱️ Elapsed: `{human_duration(elapsed)}`\n"
        f"⏳ ETA: `{eta_text}`\n"
        f"🔐 Account: `{progress_state['account']}`"
        f"{retry_line}"
    )


async def periodic_progress_notifier(
    status_message,
    progress_state: dict,
    stop_event: asyncio.Event,
    edit_lock: asyncio.Lock,
) -> None:
    """Har 10 second progress heartbeat bhejo jab tak stop signal na mile."""
    while not stop_event.is_set():
        await asyncio.sleep(PROGRESS_UPDATE_SECONDS)
        if stop_event.is_set():
            break
        await safe_edit_status(status_message, build_progress_text(progress_state), edit_lock)


def compress_video(input_path: str, output_path: str) -> tuple[bool, str]:
    """H.265 CRF-18 se video compress karo."""
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-i",
        input_path,
        "-c:v", "libx265",
        "-crf", VIDEO_CRF,
        "-preset", FFMPEG_PRESET,
        "-threads", FFMPEG_THREADS,
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


def is_valid_mega_file_link(url: str) -> bool:
    """Sirf valid single-file Mega links allow karo (folder ya key-less link reject)."""
    return bool(MEGA_FILE_LINK_RE.search(url.strip()))


def is_mega_transient_error(exc: Exception) -> bool:
    """Transient Mega/network errors identify karo taaki retry kiya ja sake."""
    text = str(exc).lower()
    retry_tokens = (
        "timed out",
        "timeout",
        "connection reset",
        "tempor",
        "eagain",
        "einternal",
        "etempunavail",
        "too many requests",
        "unboundlocalerror",
        "local variable 'i' referenced before assignment",
        "referenced before assignment",
    )
    return any(token in text for token in retry_tokens)


def is_mega_block_error_text(text: str) -> bool:
    """Mega block related errors ko consistently detect karo."""
    lowered = text.lower()
    return "eblocked" in lowered or "user blocked" in lowered


def is_mega_auth_error_text(text: str) -> bool:
    """Mega auth/login related errors identify karo."""
    lowered = text.lower()
    tokens = (
        "login",
        "auth",
        "credentials",
        "invalid email",
        "invalid password",
        "user blocked",
        "eblocked",
    )
    return any(token in lowered for token in tokens)


def mega_action_keyboard(output_id: Optional[str] = None) -> InlineKeyboardMarkup:
    """Bot actions ke liye inline keyboard build karo."""
    rows = [
        [InlineKeyboardButton("🔁 Change Mega Account", callback_data="mega_switch_account")],
        [InlineKeyboardButton("➕ Add New Mega Account", callback_data="mega_add_account_prompt")],
    ]
    if output_id:
        rows.append([InlineKeyboardButton("📤 Re-upload to Mega", callback_data=f"mega_reupload:{output_id}")])
    rows.append([InlineKeyboardButton("🧹 Delete Previous Outputs", callback_data="mega_delete_outputs")])
    return InlineKeyboardMarkup(rows)


def output_cache_root() -> Path:
    """Output cache folder ensure karo."""
    root = Path(OUTPUT_CACHE_DIR)
    root.mkdir(parents=True, exist_ok=True)
    return root


async def cache_output_for_user(user_id: int, file_path: Path) -> str:
    """Latest output ko cache karo taaki user re-upload kar sake."""
    output_id = uuid.uuid4().hex[:12]
    out_name = re.sub(r"[^A-Za-z0-9._-]", "_", file_path.name) or "output.bin"
    cached_path = output_cache_root() / f"{output_id}_{out_name}"
    await asyncio.to_thread(shutil.copy2, str(file_path), str(cached_path))

    async with output_registry_lock:
        OUTPUT_REGISTRY[output_id] = {
            "owner": user_id,
            "path": str(cached_path),
            "created_at": time.time(),
            "name": file_path.name,
        }
        user_outputs = USER_OUTPUT_INDEX.setdefault(user_id, [])
        user_outputs.append(output_id)

        # Purane outputs trim karo taaki disk grow na ho.
        while len(user_outputs) > MAX_STORED_OUTPUTS_PER_USER:
            stale_id = user_outputs.pop(0)
            stale = OUTPUT_REGISTRY.pop(stale_id, None)
            if stale and stale.get("path"):
                stale_path = Path(stale["path"])
                if stale_path.exists():
                    stale_path.unlink(missing_ok=True)

    return output_id


async def delete_cached_outputs_for_user(user_id: int) -> tuple[int, int]:
    """User ke sab cached output files delete karo."""
    async with output_registry_lock:
        output_ids = USER_OUTPUT_INDEX.pop(user_id, [])

    deleted_count = 0
    freed_bytes = 0
    for output_id in output_ids:
        meta = OUTPUT_REGISTRY.pop(output_id, None)
        if not meta:
            continue
        path_str = meta.get("path")
        if not path_str:
            continue
        fpath = Path(path_str)
        if fpath.exists():
            size = fpath.stat().st_size
            fpath.unlink(missing_ok=True)
            deleted_count += 1
            freed_bytes += size
    return deleted_count, freed_bytes


async def get_cached_output(output_id: str) -> Optional[dict]:
    """Cached output metadata safely read karo."""
    async with output_registry_lock:
        meta = OUTPUT_REGISTRY.get(output_id)
        if not meta:
            return None
        return dict(meta)


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
    global mega_client, ACTIVE_MEGA_ACCOUNT_INDEX

    async with mega_client_lock:
        if mega_client is not None and not force_relogin:
            return mega_client

        if not MEGA_ACCOUNTS:
            raise RuntimeError("No Mega accounts configured")

        logger.info("🔐 Mega pe login ho raha hai...")

        attempt = 1
        waited_cycles = 0
        while attempt <= 3:
            available_idx = find_available_account_index(ACTIVE_MEGA_ACCOUNT_INDEX)
            if available_idx is None:
                wait_seconds = shortest_account_cooldown()
                if wait_seconds > 0:
                    logger.warning(f"All Mega accounts cooling. Waiting {wait_seconds}s before retry login.")
                    await asyncio.sleep(min(wait_seconds, 60))
                    waited_cycles += 1
                    if waited_cycles >= 3:
                        raise RuntimeError(
                            "All Mega accounts are cooling/blocked. Wait a while and retry."
                        )
                    continue
            else:
                ACTIVE_MEGA_ACCOUNT_INDEX = available_idx

            email, password = MEGA_ACCOUNTS[ACTIVE_MEGA_ACCOUNT_INDEX]

            def _login_once():
                session = Mega()
                return session.login(email, password)

            try:
                await throttle_mega_action("login", email)
                mega_client = await asyncio.to_thread(_login_once)
                logger.info(f"✅ Mega login successful! ({mask_email(email)})")
                return mega_client
            except Exception as exc:
                logger.warning(f"Mega login failed (attempt {attempt}/3): {exc}")
                err = str(exc).lower()
                if is_mega_block_error_text(err):
                    set_account_cooldown(email, MEGA_BLOCK_COOLDOWN_SECONDS, "login blocked")
                elif "too many requests" in err or "tempor" in err:
                    set_account_cooldown(email, MEGA_TEMP_COOLDOWN_SECONDS, "login throttled")
                if len(MEGA_ACCOUNTS) > 1:
                    next_idx = find_available_account_index(ACTIVE_MEGA_ACCOUNT_INDEX + 1)
                    if next_idx is not None:
                        ACTIVE_MEGA_ACCOUNT_INDEX = next_idx
                    else:
                        ACTIVE_MEGA_ACCOUNT_INDEX = (ACTIVE_MEGA_ACCOUNT_INDEX + 1) % len(MEGA_ACCOUNTS)
                    mega_client = None
                    logger.warning(
                        f"🔁 Switching Mega account to {mask_email(current_mega_account_email())}"
                    )
                if attempt >= 3:
                    raise
                await asyncio.sleep(2 * attempt)
                attempt += 1

        raise RuntimeError("Mega login failed after retries.")


async def rotate_mega_account(reason: str, cooldown_seconds: int = 0) -> bool:
    """Active account rotate karo aur current session reset karo."""
    global mega_client, ACTIVE_MEGA_ACCOUNT_INDEX
    if len(MEGA_ACCOUNTS) <= 1:
        return False

    async with mega_client_lock:
        current_email = current_mega_account_email()
        if cooldown_seconds > 0 and current_email:
            set_account_cooldown(current_email, cooldown_seconds, reason)

        next_idx = find_available_account_index(ACTIVE_MEGA_ACCOUNT_INDEX + 1)
        if next_idx is not None:
            ACTIVE_MEGA_ACCOUNT_INDEX = next_idx
        else:
            ACTIVE_MEGA_ACCOUNT_INDEX = (ACTIVE_MEGA_ACCOUNT_INDEX + 1) % len(MEGA_ACCOUNTS)
        mega_client = None
        logger.warning(f"🔁 Mega account rotated ({reason}) -> {mask_email(current_mega_account_email())}")
        return True


async def mega_download(
    url: str,
    dest_path: str,
    on_attempt: Optional[Callable[[int, int, str], Awaitable[None]]] = None,
) -> Path:
    """Download with retries + relogin for stale session/network hiccups."""
    if PREFER_ANON_MEGA_DOWNLOAD:
        try:
            if on_attempt is not None:
                await on_attempt(1, 1, "download-anon")

            logger.info("⬇️ Trying anonymous Mega download first (no account login)...")

            def _anon_download():
                return Mega().download_url(url, dest_path)

            downloaded = await asyncio.to_thread(_anon_download)
            if not downloaded:
                raise RuntimeError("Anonymous Mega download returned empty path")
            if isinstance(downloaded, (list, tuple)):
                raise ValueError("Folder links abhi supported nahi hain, file link bhejo")
            logger.info("✅ Anonymous Mega download success")
            return Path(downloaded)
        except Exception as anon_exc:
            logger.warning(f"Anonymous Mega download failed, fallback to account mode: {anon_exc}")

    last_exc: Optional[Exception] = None
    for attempt in range(1, 5):
        if on_attempt is not None:
            await on_attempt(attempt, 4, "download")
        client = await get_mega_client(force_relogin=(attempt > 1))
        try:
            await throttle_mega_action("download", current_mega_account_email())
            downloaded = await asyncio.to_thread(client.download_url, url, dest_path)
            if not downloaded:
                raise RuntimeError("Mega download returned empty path")
            if isinstance(downloaded, (list, tuple)):
                raise ValueError("Folder links abhi supported nahi hain, file link bhejo")
            return Path(downloaded)
        except Exception as exc:
            last_exc = exc
            logger.warning(f"Mega download failed (attempt {attempt}/4): {exc}")
            err = str(exc).lower()
            if is_mega_block_error_text(err) and await rotate_mega_account("download blocked", MEGA_BLOCK_COOLDOWN_SECONDS):
                await asyncio.sleep(min(10 * attempt, 45))
                continue
            if "too many requests" in err:
                set_account_cooldown(current_mega_account_email(), MEGA_TEMP_COOLDOWN_SECONDS, "download throttled")
            if attempt >= 4 or not is_mega_transient_error(exc):
                raise
            await asyncio.sleep(min(4 * attempt, 20))
    if last_exc:
        raise last_exc
    raise RuntimeError("Mega download failed for unknown reason")


async def mega_upload(
    file_path: str,
    on_attempt: Optional[Callable[[int, int, str], Awaitable[None]]] = None,
) -> str:
    """Upload with retries; link generation failures bhi recover karne ki koshish karo."""
    last_exc: Optional[Exception] = None
    for attempt in range(1, 5):
        if on_attempt is not None:
            await on_attempt(attempt, 4, "upload")
        client = await get_mega_client(force_relogin=(attempt > 1))
        try:
            await throttle_mega_action("upload", current_mega_account_email())
            uploaded_file = await asyncio.to_thread(client.upload, file_path)
            await throttle_mega_action("upload_link", current_mega_account_email())
            link = await asyncio.to_thread(client.get_upload_link, uploaded_file)
            if not isinstance(link, str) or "mega." not in link.lower():
                raise RuntimeError("Mega upload link invalid/empty")
            return link
        except Exception as exc:
            last_exc = exc
            logger.warning(f"Mega upload failed (attempt {attempt}/4): {exc}")
            err = str(exc).lower()

            # EBLOCKED usually account/IP throttle hota hai; retries ke beech thoda wait helpful ho sakta hai.
            if is_mega_block_error_text(err) and attempt < 4:
                await rotate_mega_account("upload blocked", MEGA_BLOCK_COOLDOWN_SECONDS)
                await asyncio.sleep(min(15 * attempt, 60))
                continue

            if "too many requests" in err:
                set_account_cooldown(current_mega_account_email(), MEGA_TEMP_COOLDOWN_SECONDS, "upload throttled")

            if attempt >= 4 or not is_mega_transient_error(exc):
                raise
            await asyncio.sleep(min(4 * attempt, 20))
    if last_exc:
        raise last_exc
    raise RuntimeError("Mega upload failed for unknown reason")


def upload_to_transfersh(file_path: str) -> str:
    """No-account share upload via transfer.sh."""
    name = Path(file_path).name
    safe_name = re.sub(r"[^A-Za-z0-9._-]", "_", name) or "output.bin"
    url = f"{TRANSFER_SH_BASE}/{safe_name}"

    with open(file_path, "rb") as f:
        resp = requests.put(url, data=f, timeout=1800)

    if resp.status_code >= 400:
        raise RuntimeError(f"transfer.sh upload failed ({resp.status_code})")

    link = resp.text.strip()
    if not link.startswith("http"):
        raise RuntimeError(f"transfer.sh unexpected response: {link[:120]}")
    return link


def upload_to_fileio(file_path: str) -> str:
    """No-account share upload via file.io."""
    url = f"{FILEIO_BASE}/"
    with open(file_path, "rb") as f:
        resp = requests.post(
            url,
            files={"file": (Path(file_path).name, f)},
            timeout=1800,
        )

    if resp.status_code >= 400:
        raise RuntimeError(f"file.io upload failed ({resp.status_code})")

    try:
        data = resp.json()
    except Exception:
        raise RuntimeError(f"file.io invalid response: {resp.text[:200]}")

    if not data.get("success"):
        raise RuntimeError(f"file.io upload failed: {json.dumps(data)[:220]}")

    link = str(data.get("link", "")).strip()
    if not link.startswith("http"):
        raise RuntimeError(f"file.io missing link in response: {json.dumps(data)[:220]}")
    return link


def upload_to_wetransfer(file_path: str) -> str:
    """Anonymous WeTransfer upload; API changes ho sakte hain, isliye errors fallback trigger karte hain."""
    session = requests.Session()
    session.headers.update({"x-requested-with": "XMLHttpRequest"})

    name = Path(file_path).name
    size = Path(file_path).stat().st_size

    init_payload = {
        "files": [{"name": name, "size": size}],
        "message": "",
        "ui_language": "en",
        "from": "",
        "recipients": [],
    }
    create_resp = session.post(
        "https://wetransfer.com/api/v4/transfers/link",
        json=init_payload,
        timeout=120,
    )
    if create_resp.status_code >= 400:
        raise RuntimeError(f"wetransfer init failed ({create_resp.status_code})")

    try:
        create_data = create_resp.json()
    except Exception:
        raise RuntimeError(f"wetransfer invalid init response: {create_resp.text[:200]}")

    transfer_id = create_data.get("id")
    file_id = None
    upload_url = None

    files_data = create_data.get("files") or []
    if files_data:
        file_id = files_data[0].get("id")
        upload_url = files_data[0].get("upload_url") or files_data[0].get("url")

    if not transfer_id or not file_id or not upload_url:
        raise RuntimeError(f"wetransfer missing upload metadata: {json.dumps(create_data)[:260]}")

    with open(file_path, "rb") as f:
        put_resp = requests.put(upload_url, data=f, timeout=1800)
    if put_resp.status_code >= 400:
        raise RuntimeError(f"wetransfer upload failed ({put_resp.status_code})")

    finalize_resp = session.put(
        f"https://wetransfer.com/api/v4/transfers/{transfer_id}/files/{file_id}/finalize",
        timeout=120,
    )
    if finalize_resp.status_code >= 400:
        raise RuntimeError(f"wetransfer finalize failed ({finalize_resp.status_code})")

    link_resp = session.post(
        f"https://wetransfer.com/api/v4/transfers/{transfer_id}/finalize",
        timeout=120,
    )
    if link_resp.status_code >= 400:
        raise RuntimeError(f"wetransfer transfer finalize failed ({link_resp.status_code})")

    try:
        link_data = link_resp.json()
    except Exception:
        raise RuntimeError(f"wetransfer invalid finalize response: {link_resp.text[:200]}")

    # API variants: direct_link / shortened_url / url
    link = str(
        link_data.get("shortened_url")
        or link_data.get("direct_link")
        or link_data.get("url")
        or ""
    ).strip()
    if not link.startswith("http"):
        raise RuntimeError(f"wetransfer missing final link: {json.dumps(link_data)[:220]}")
    return link


def upload_to_onionshare(file_path: str) -> str:
    """OnionShare CLI based upload (requires local onionshare-cli + Tor setup)."""
    cmd = ["onionshare-cli", "--website", file_path]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if result.returncode != 0:
        raise RuntimeError(f"onionshare failed: {result.stderr[-300:]}")

    combined = f"{result.stdout}\n{result.stderr}"
    match = re.search(r"https?://[a-z2-7]{16,56}\.onion[^\s]*", combined)
    if not match:
        raise RuntimeError("onionshare link not found in CLI output")
    return match.group(0)


async def upload_with_provider(provider: str, file_path: str) -> str:
    """Single provider upload helper."""
    if provider == "mega":
        return await mega_upload(file_path)
    if provider == "fileio":
        return await asyncio.to_thread(upload_to_fileio, file_path)
    if provider in {"transfersh", "sendanywhere"}:
        return await asyncio.to_thread(upload_to_transfersh, file_path)
    if provider == "wetransfer":
        return await asyncio.to_thread(upload_to_wetransfer, file_path)
    if provider == "onionshare":
        return await asyncio.to_thread(upload_to_onionshare, file_path)
    raise RuntimeError(f"Unknown upload provider '{provider}'")


async def upload_output_file(
    file_path: str,
    on_attempt: Optional[Callable[[int, int, str], Awaitable[None]]] = None,
) -> tuple[str, str]:
    """Configured fallback chain ke saath upload karo; fail hone par next provider try hota hai."""
    providers = configured_upload_providers()
    failures: list[str] = []

    total = len(providers)
    for idx, provider in enumerate(providers, start=1):
        if on_attempt is not None:
            await on_attempt(idx, total, f"upload-{provider}")
        try:
            link = await upload_with_provider(provider, file_path)
            return link, provider
        except Exception as exc:
            failures.append(f"{provider}: {exc}")
            logger.warning(f"Upload provider failed ({provider}): {exc}")

    raise RuntimeError(
        "All upload providers failed: " + " | ".join(failures[-6:])
    )


# ─── Telegram Handlers ───────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 *Mega Video/Image Compressor Bot mein aapka swagat hai!*\n\n"
        "📌 *Kaise use karein:*\n"
        "Bas mujhe *single file* ka Mega link bhejdo\n\n"
        "🎬 *Video ke liye:*\n"
        "H.265 (CRF 18) codec se compress hoga\n"
        "_(Visually same quality, 50-70% chhoti file)_\n\n"
        "🖼️ *Image ke liye:*\n"
        "Lossless optimization hogi\n\n"
        "⚡ *Supported formats:*\n"
        "`MP4, MKV, AVI, MOV, WEBM` aur\n"
        "`JPG, PNG, WEBP, BMP`\n\n"
        "🔗 Link bhejo aur magic dekho! ✨\n\n"
        "Need ho toh niche button se account switch kar sakte ho.",
        parse_mode="Markdown",
        reply_markup=mega_action_keyboard(),
    )


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📖 *Help*\n\n"
        "1️⃣ Mega.nz pe apni *single file* upload karo\n"
        "2️⃣ Share link copy karo\n"
        "3️⃣ Mujhe woh link bhejo\n"
        "4️⃣ Main compress karke new Mega link bhejunga\n\n"
        "⚠️ *Note:* Badi files (1GB+) mein zyada time lagta hai.",
        parse_mode="Markdown",
        reply_markup=mega_action_keyboard(),
    )


async def on_switch_account_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Inline button callback se active Mega account switch karo."""
    query: Optional[CallbackQuery] = update.callback_query
    if query is None:
        return

    previous_email = current_mega_account_email()
    switched = await rotate_mega_account("manual switch")
    current_email = current_mega_account_email()

    if not current_email:
        await query.answer("No Mega account configured", show_alert=True)
        return

    if switched and previous_email and previous_email != current_email:
        await query.answer("Mega account switched")
        msg = (
            "✅ *Mega account changed*\n\n"
            f"`{mask_email(previous_email)}` ➜ `{mask_email(current_email)}`"
        )
    else:
        await query.answer("No alternate account available")
        msg = f"ℹ️ Active Mega account: `{mask_email(current_email)}`"

    if query.message is not None:
        await query.message.reply_text(
            msg,
            parse_mode="Markdown",
            reply_markup=mega_action_keyboard(),
        )


async def on_add_account_prompt_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """User se new Mega account credentials collect karne ka prompt bhejo."""
    query: Optional[CallbackQuery] = update.callback_query
    if query is None:
        return

    context.user_data[AWAITING_MEGA_ACCOUNT_INPUT_KEY] = True
    await query.answer("Send account credentials")

    if query.message is not None:
        await query.message.reply_text(
            "➕ *Add New Mega Account*\n\n"
            "Credentials bhejo in formats me se kisi ek mein:\n"
            "1) `email@example.com password123`\n"
            "2) `email@example.com,password123`\n"
            "3) Do lines me: pehle email, dusri line password\n\n"
            "Cancel karna ho to `cancel` bhej do.",
            parse_mode="Markdown",
            reply_markup=mega_action_keyboard(),
        )


async def on_reupload_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cached output ko dubara Mega pe upload karo."""
    query: Optional[CallbackQuery] = update.callback_query
    if query is None:
        return

    user = query.from_user
    user_id = user.id if user else 0
    callback_data = query.data or ""
    output_id = callback_data.split(":", 1)[1] if ":" in callback_data else ""

    if not output_id:
        await query.answer("Invalid output id", show_alert=True)
        return

    meta = await get_cached_output(output_id)
    if not meta:
        await query.answer("Output not found. Process file again.", show_alert=True)
        return

    owner = int(meta.get("owner", 0) or 0)
    if owner != user_id:
        await query.answer("Ye output aapka nahi hai.", show_alert=True)
        return

    output_path = Path(str(meta.get("path", "")))
    if not output_path.exists():
        await query.answer("Cached file missing. Process file again.", show_alert=True)
        return

    await query.answer("Re-upload started")
    if query.message is None:
        return

    status = await query.message.reply_text(
        "📤 Cached output ko Mega pe re-upload kar raha hoon...",
        parse_mode="Markdown",
    )

    try:
        new_link = await mega_upload(str(output_path))
        await edit_text_with_fallback(
            status,
            "✅ *Re-upload complete*\n\n"
            f"📄 File: `{meta.get('name', output_path.name)}`\n"
            f"🔗 Link: {new_link}",
            reply_markup=mega_action_keyboard(output_id=output_id),
        )
    except Exception as exc:
        err_text = str(exc)
        if is_mega_block_error_text(err_text):
            msg = (
                "⚠️ Mega ne upload block kiya (`EBLOCKED`).\n"
                "Account switch karke phir try karo."
            )
        else:
            msg = f"❌ Re-upload failed: `{type(exc).__name__}: {exc}`"
        await edit_text_with_fallback(
            status,
            msg,
            reply_markup=mega_action_keyboard(output_id=output_id),
        )


async def on_delete_outputs_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """User ke previous cached outputs delete karo."""
    query: Optional[CallbackQuery] = update.callback_query
    if query is None:
        return

    user = query.from_user
    user_id = user.id if user else 0

    await query.answer("Deleting outputs...")
    deleted_count, freed_bytes = await delete_cached_outputs_for_user(user_id)

    if query.message is not None:
        await query.message.reply_text(
            "🧹 *Previous outputs deleted*\n\n"
            f"• Files removed: `{deleted_count}`\n"
            f"• Space freed: `{human_size(freed_bytes)}`",
            parse_mode="Markdown",
            reply_markup=mega_action_keyboard(),
        )


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    if message is None:
        return

    text = message.text.strip() if message.text else ""

    if context.user_data.get(AWAITING_MEGA_ACCOUNT_INPUT_KEY):
        lowered = text.lower().strip()
        if lowered == "cancel":
            context.user_data.pop(AWAITING_MEGA_ACCOUNT_INPUT_KEY, None)
            await message.reply_text(
                "❎ Add account cancelled.",
                reply_markup=mega_action_keyboard(),
            )
            return

        parsed = parse_mega_account_input(text)
        if not parsed:
            await message.reply_text(
                "⚠️ Format samajh nahi aaya. Example bhejo:\n"
                "`email@example.com password123`\n"
                "Ya `email@example.com,password123`",
                parse_mode="Markdown",
                reply_markup=mega_action_keyboard(),
            )
            return

        email, password = parsed
        ok, detail = await add_runtime_mega_account(email, password)
        if not ok:
            await message.reply_text(
                f"⚠️ Account add nahi hua: {detail}",
                reply_markup=mega_action_keyboard(),
            )
            return

        context.user_data.pop(AWAITING_MEGA_ACCOUNT_INPUT_KEY, None)
        await message.reply_text(
            "✅ *New Mega account added*\n\n"
            f"Account: `{detail}`\n"
            f"Total accounts: `{len(MEGA_ACCOUNTS)}`\n\n"
            "Note: Ye runtime add hai; restart ke baad dubara set karna padega (ya env secrets me add karo).",
            parse_mode="Markdown",
            reply_markup=mega_action_keyboard(),
        )
        return

    # Mega link dhundho
    match = MEGA_LINK_RE.search(text)
    if not match:
        await message.reply_text(
            "⚠️ Koi valid *Mega.nz link* nahi mili!\n\n"
            "Format: `https://mega.nz/file/...`",
            parse_mode="Markdown",
        )
        return

    mega_url = match.group(0)
    lower_url = mega_url.lower()
    if "/folder/" in lower_url or "/#f!" in lower_url:
        await message.reply_text(
            "⚠️ *Folder link abhi supported nahi hai.*\n\n"
            "Please kisi *single file* ka Mega link bhejo:\n"
            "`https://mega.nz/file/...`",
            parse_mode="Markdown",
        )
        return

    if not is_valid_mega_file_link(mega_url):
        await message.reply_text(
            "⚠️ Ye Mega link incomplete lag raha hai (file key missing).\n\n"
            "Please full *single file* link bhejo:\n"
            "`https://mega.nz/file/...#...`",
            parse_mode="Markdown",
        )
        return

    user = message.from_user
    user_id = user.id if user else "unknown"
    logger.info(f"📥 New request | User: {user_id} | URL: {mega_url}")

    # Status message
    status = await message.reply_text(
        "⏳ *[1/4]* Mega se download shuru ho raha hai...\n"
        "_(Badi file hai toh thoda wait karo)_",
        parse_mode="Markdown",
    )

    edit_lock = asyncio.Lock()
    progress_stop_event = asyncio.Event()
    progress_state = {
        "phase": "download",
        "phase_started": time.monotonic(),
        "header": "⏳ *[1/4]* Mega se download shuru ho raha hai...",
        "detail": "_(Badi file hai toh thoda wait karo)_",
        "attempt": 1,
        "max_attempts": 4,
        "account": mask_email(current_mega_account_email()),
    }

    heartbeat_task = asyncio.create_task(
        periodic_progress_notifier(status, progress_state, progress_stop_event, edit_lock)
    )

    async with PROCESS_SEMAPHORE:
        with tempfile.TemporaryDirectory(prefix="megabot_") as tmp_dir:
            try:
                started_at = time.monotonic()

                async def download_attempt_update(attempt: int, max_attempts: int, _stage: str):
                    progress_state["phase"] = "download"
                    progress_state["attempt"] = attempt
                    progress_state["max_attempts"] = max_attempts
                    progress_state["account"] = mask_email(current_mega_account_email())

                async def upload_attempt_update(attempt: int, max_attempts: int, _stage: str):
                    progress_state["phase"] = "upload"
                    progress_state["attempt"] = attempt
                    progress_state["max_attempts"] = max_attempts
                    progress_state["account"] = mask_email(current_mega_account_email())

                # ── Step 1: Download ──────────────────────────────────────────
                logger.info(f"⬇️  Downloading: {mega_url}")
                progress_state["phase"] = "download"
                progress_state["phase_started"] = time.monotonic()
                progress_state["header"] = "⏳ *[1/4]* Mega se download ho raha hai..."
                progress_state["detail"] = "_(Har 10 sec update milega)_"

                input_path = await mega_download(mega_url, tmp_dir, on_attempt=download_attempt_update)
                update_phase_average("download", time.monotonic() - progress_state["phase_started"])
                ext = input_path.suffix.lower()

                orig_size = input_path.stat().st_size
                orig_str = human_size(orig_size)

                logger.info(f"✅ Download done: {input_path.name} ({orig_str})")

                # File type check
                if ext not in VIDEO_EXTS and ext not in IMAGE_EXTS:
                    await edit_text_with_fallback(
                        status,
                        f"❌ Unsupported file type: `{ext}`\n\n"
                        "Sirf video (mp4, mkv, avi…) ya image (jpg, png…) files supported hain.",
                    )
                    return

                is_video = ext in VIDEO_EXTS
                file_type_emoji = "🎬" if is_video else "🖼️"

                # ── Step 2: Compress ──────────────────────────────────────────
                progress_state["phase"] = "compress"
                progress_state["phase_started"] = time.monotonic()
                progress_state["attempt"] = 1
                progress_state["max_attempts"] = 1
                progress_state["header"] = f"✅ *[1/4]* Download complete! ({orig_str})"
                progress_state["detail"] = (
                    f"🔄 *[2/4]* {file_type_emoji} Compress ho raha hai...\n"
                    f"_(H.265 CRF-{VIDEO_CRF} | Fast preset: {FFMPEG_PRESET})_"
                )

                await safe_edit_status(
                    status,
                    f"✅ *[1/4]* Download complete! ({orig_str})\n\n"
                    f"🔄 *[2/4]* {file_type_emoji} Compress ho raha hai...\n"
                    f"_(H.265 CRF-{VIDEO_CRF} | Fast preset: {FFMPEG_PRESET})_\n\n"
                    f"⏱️ Elapsed: `0s`\n"
                    f"⏳ ETA: `estimating...`\n"
                    f"🔐 Account: `{mask_email(current_mega_account_email())}`",
                    edit_lock,
                )

                if is_video:
                    output_path = Path(tmp_dir) / f"compressed_{input_path.stem}.mp4"
                    success, err_msg = await asyncio.to_thread(compress_video, str(input_path), str(output_path))
                else:
                    output_path = Path(tmp_dir) / f"compressed_{input_path.name}"
                    success, err_msg = await asyncio.to_thread(compress_image, str(input_path), str(output_path))

                if not success:
                    await safe_edit_status(
                        status,
                        f"❌ Compression fail ho gayi!\n\n`{err_msg}`",
                        edit_lock,
                    )
                    return

                update_phase_average("compress", time.monotonic() - progress_state["phase_started"])

                comp_size = output_path.stat().st_size
                comp_str = human_size(comp_size)
                reduction_pct = ((orig_size - comp_size) / orig_size) * 100

                # Agar output noticeably better nahi hai, original upload karke time/save waste avoid karo.
                upload_path = output_path
                compression_note = ""
                if comp_size >= orig_size * 0.98:
                    upload_path = input_path
                    comp_size = orig_size
                    comp_str = orig_str
                    reduction_pct = 0.0
                    compression_note = "\nℹ️ Compression gain minimal tha, original file upload ki gayi."

                logger.info(f"✅ Compressed: {orig_str} → {comp_str} ({reduction_pct:.1f}% saved)")

                # ── Step 3: Upload ────────────────────────────────────────────
                progress_state["phase"] = "upload"
                progress_state["phase_started"] = time.monotonic()
                progress_state["attempt"] = 1
                progress_state["max_attempts"] = 4
                progress_state["header"] = (
                    f"✅ *[1/4]* Download: {orig_str}\n"
                    f"✅ *[2/4]* Compress: {orig_str} → {comp_str} (*{reduction_pct:.1f}%* saved)"
                )
                progress_state["detail"] = "📤 *[3/4]* Mega pe upload ho raha hai..."
                provider_chain = configured_upload_providers()
                target_label = " -> ".join(provider_display_name(p) for p in provider_chain)

                await safe_edit_status(
                    status,
                    f"✅ *[1/4]* Download: {orig_str}\n"
                    f"✅ *[2/4]* Compress: {orig_str} → {comp_str} (*{reduction_pct:.1f}%* saved)\n\n"
                    f"📤 *[3/4]* {target_label} upload ho raha hai...\n\n"
                    f"⏱️ Elapsed: `0s`\n"
                    f"⏳ ETA: `estimating...`\n"
                    f"🔐 Account: `{mask_email(current_mega_account_email())}`",
                    edit_lock,
                )

                logger.info(f"⬆️  Uploading output: {upload_path.name} | providers={provider_chain}")
                if provider_chain == ["mega"]:
                    new_link = await mega_upload(str(upload_path), on_attempt=upload_attempt_update)
                    used_provider = "mega"
                else:
                    progress_state["attempt"] = 1
                    progress_state["max_attempts"] = max(1, len(provider_chain))
                    new_link, used_provider = await upload_output_file(
                        str(upload_path),
                        on_attempt=upload_attempt_update,
                    )
                update_phase_average("upload", time.monotonic() - progress_state["phase_started"])

                logger.info(f"✅ Upload done via {used_provider}: {new_link}")

                # ── Step 4: Done ──────────────────────────────────────────────
                savings_icon = "🔥" if reduction_pct > 40 else "✅"
                total_secs = int(time.monotonic() - started_at)

                progress_stop_event.set()
                await heartbeat_task

                await safe_edit_status(
                    status,
                    f"{savings_icon} *Kaam ho gaya! Sab done!*\n\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"📤 Provider: `{provider_display_name(used_provider)}`\n"
                    f"📊 *Compression Stats:*\n"
                    f"• Pehle:  `{orig_str}`\n"
                    f"• Baad:   `{comp_str}`\n"
                    f"• Saved:  `{reduction_pct:.1f}%` {savings_icon}\n"
                    f"• Time:   `{total_secs} sec`\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"{compression_note}"
                    f"🔗 *Compressed File Download Link:*\n"
                    f"{new_link}",
                    edit_lock,
                )

                if user and user.id:
                    output_id = await cache_output_for_user(user.id, upload_path)
                    await message.reply_text(
                        "Quick actions use karo:",
                        reply_markup=mega_action_keyboard(output_id=output_id),
                    )

            except Exception as exc:
                logger.exception(f"❌ Unexpected error: {exc}")
                err_text = str(exc)
                progress_stop_event.set()
                await heartbeat_task
                if "Url key missing" in err_text:
                    await safe_edit_status(
                        status,
                        "⚠️ Ye link `folder` type lag raha hai ya invalid key hai.\n\n"
                        "Please *single file* ka Mega link bhejo:\n"
                        "`https://mega.nz/file/...`",
                        edit_lock,
                    )
                    return
                if is_mega_block_error_text(err_text):
                    await safe_edit_status(
                        status,
                        "⚠️ Mega ne request block kar di (`EBLOCKED`).\n\n"
                        "Possible reasons: bahut zyada requests ya account/IP throttle.\n"
                        "Bot auto backup account pe switch karne ki koshish karta hai.\n"
                        "Agar sab accounts blocked/cooling ho jayein toh kuch der baad retry karo.\n\n"
                        "Neeche button se account manually bhi switch kar sakte ho.",
                        edit_lock,
                    )
                    await message.reply_text(
                        "🔧 Manual switch chahiye toh yeh button use karo:",
                        reply_markup=mega_action_keyboard(),
                    )
                    return
                if is_mega_auth_error_text(err_text):
                    await safe_edit_status(
                        status,
                        "⚠️ Mega auth issue aa rahi hai.\n\n"
                        "Recommended: output provider `transfersh` use karo aur anonymous download enabled rakho.\n"
                        "Agar Mega accounts use karne hain toh cooldown ke baad retry karo.",
                        edit_lock,
                    )
                    return
                await safe_edit_status(
                    status,
                    f"Error aa gaya: {type(exc).__name__}: {exc}\n\n"
                    "Dobara try karo ya /help dekho.",
                    edit_lock,
                )
            finally:
                if not progress_stop_event.is_set():
                    progress_stop_event.set()
                if not heartbeat_task.done():
                    await heartbeat_task


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    global MEGA_ACCOUNTS
    apply_mega_runtime_patches()
    MEGA_ACCOUNTS = build_mega_accounts()

    missing = [
        name for name, value in (
            ("TELEGRAM_TOKEN", TOKEN),
        ) if not value
    ]
    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

    provider_chain = configured_upload_providers()
    need_mega_accounts = ("mega" in provider_chain) or not PREFER_ANON_MEGA_DOWNLOAD

    if need_mega_accounts and not MEGA_ACCOUNTS:
        raise RuntimeError(
            "No Mega account configured. Set MEGA_EMAIL/MEGA_PASSWORD or backup MEGA_EMAIL_2/MEGA_PASSWORD_2"
        )

    if MEGA_ACCOUNTS:
        logger.info(
            f"Mega accounts loaded: {len(MEGA_ACCOUNTS)} | Active: {mask_email(current_mega_account_email())}"
        )
    else:
        logger.info("Mega accounts not configured; anonymous Mega download mode only")

    logger.info(f"Output share providers: {provider_chain}")
    logger.info(f"Anonymous Mega download first: {PREFER_ANON_MEGA_DOWNLOAD}")

    app = (
        Application.builder()
        .token(TOKEN)
        .build()
    )

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help",  cmd_help))
    app.add_handler(CallbackQueryHandler(on_switch_account_callback, pattern="^mega_switch_account$"))
    app.add_handler(CallbackQueryHandler(on_add_account_prompt_callback, pattern="^mega_add_account_prompt$"))
    app.add_handler(CallbackQueryHandler(on_reupload_callback, pattern="^mega_reupload:"))
    app.add_handler(CallbackQueryHandler(on_delete_outputs_callback, pattern="^mega_delete_outputs$"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    logger.info("🤖 Bot polling shuru ho gaya!")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
