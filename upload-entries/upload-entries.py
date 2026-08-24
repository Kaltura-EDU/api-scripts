"""
Kaltura Bulk Uploader (upload-entries.py)

Uploads every media file you drop into the local "input" folder to Kaltura,
creating one new media entry per file. Metadata that should apply to the whole
batch — tags, owner, collaborators (co-editors / co-publishers), category
memberships, description — is configured once in .env.

Media type (video / audio / image) is detected automatically from each file's
extension, so you can mix file types in the same batch.

── Why this script uses "chunked" uploads ────────────────────────────────────
The Kaltura Python SDK sends an upload as a single streamed HTTP request with a
default 120-second timeout, and it silently retries the WHOLE file five times on
any timeout. Large videos therefore take longer than 120s to transfer, time out,
and retry forever — which is the classic reason naive upload scripts appear to
"hang" and never finish. To avoid that, this script splits each file into small
chunks (UPLOAD_CHUNK_MB) and uploads them one at a time using Kaltura's resumable
uploadToken flow, so every individual request finishes well inside the timeout.

── The upload flow per file ──────────────────────────────────────────────────
  1. uploadToken.add()        → reserve an upload token
  2. uploadToken.upload(...)  → send the bytes, chunk by chunk (the hard part)
  3. media.add()              → create the entry "shell" with the right mediaType
  4. media.addContent(...)    → bind the uploaded bytes to the entry

The admin secret is always prompted at runtime and never read from or written
to disk. Fill in the rest of your configuration in .env (copy .env.example).
"""

import csv
import getpass
import io
import mimetypes
import os
import shutil
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from os import getenv
from os.path import join as path_join

import requests
from dotenv import find_dotenv, load_dotenv
from KalturaClient import KalturaClient, KalturaConfiguration
from KalturaClient.Plugins.Core import (
    KalturaCategoryEntry,
    KalturaCategoryFilter,
    KalturaFilterPager,
    KalturaMediaEntry,
    KalturaMediaType,
    KalturaSessionType,
    KalturaUploadedFileTokenResource,
    KalturaUploadToken,
)
from KalturaClient.exceptions import KalturaClientException, KalturaException

load_dotenv(find_dotenv())


# ── Env helpers ───────────────────────────────────────────────────────
def require_env(key):
    val = getenv(key, "").strip()
    if not val:
        print(f"[ERROR] Missing or empty {key} in .env", file=sys.stderr)
        sys.exit(2)
    return val


def env_val(key):
    val = getenv(key, "").strip()
    return val or None


def env_list(key):
    """Parse a comma-separated env var into stripped, non-empty strings."""
    raw = getenv(key, "").strip()
    return [v.strip() for v in raw.split(",") if v.strip()] if raw else []


def env_bool(key, default=False):
    raw = getenv(key, "").strip().lower()
    if not raw:
        return default
    return raw in ("true", "1", "yes", "y", "on")


# ── Credentials ───────────────────────────────────────────────────────
PARTNER_ID = int(require_env("PARTNER_ID"))
ADMIN_SECRET = ""  # never read from .env — set in main() via getpass
USER_ID = env_val("USER_ID")  # session user; None → account default
SERVICE_URL = getenv("SERVICE_URL", "https://www.kaltura.com").rstrip("/")
PRIVILEGES = getenv("PRIVILEGES", "all:*,disableentitlement")


# ── Entry metadata applied to every uploaded file ─────────────────────
# Kaltura user ID that should OWN the new entries (the "owner"). If blank,
# Kaltura assigns the session user as owner.
OWNER_ID = env_val("OWNER_ID")
# Comma-separated tags applied to every entry.
TAGS = getenv("TAGS", "").strip()
# Optional description applied to every entry.
DESCRIPTION = getenv("DESCRIPTION", "").strip()
# Collaborators who may EDIT the entries (KMC "co-editors"). Comma = multiple.
COEDITORS = env_list("COEDITORS")
# Collaborators who may PUBLISH the entries (KMC "co-publishers").
COPUBLISHERS = env_list("COPUBLISHERS")
# Numeric Kaltura category IDs to place every entry in (KMC → Categories).
CATEGORY_IDS = env_list("CATEGORY_IDS")
# Category full paths/names to place every entry in; resolved to IDs at start.
CATEGORY_NAMES = env_list("CATEGORY_NAMES")
# Optional transcoding/conversion profile ID for the new entries (blank = the
# account default profile).
CONVERSION_PROFILE_ID = env_val("CONVERSION_PROFILE_ID")


# ── Local folders / file handling ─────────────────────────────────────
INPUT_DIR = getenv("INPUT_DIR", "input")
OUTPUT_DIR = getenv("OUTPUT_DIR", "output")
# After a file uploads successfully, move it out of the input folder so a
# re-run won't upload it again. Set False to leave files where they are.
MOVE_ON_SUCCESS = env_bool("MOVE_ON_SUCCESS", True)
PROCESSED_DIR = getenv("PROCESSED_DIR", "input/_uploaded")
FAILED_DIR = getenv("FAILED_DIR", "input/_failed")
# When True, only lists what WOULD be uploaded — creates nothing in Kaltura.
DRY_RUN = env_bool("DRY_RUN", False)


# ── Reliability / upload tuning ───────────────────────────────────────
# Size of each upload chunk, in megabytes. Smaller = more resilient on slow or
# flaky connections (each request finishes faster); larger = fewer round trips.
UPLOAD_CHUNK_MB = float(getenv("UPLOAD_CHUNK_MB", "5"))
UPLOAD_CHUNK_BYTES = int(UPLOAD_CHUNK_MB * 1024 * 1024)
# Per-request timeout (seconds). Generous because a single chunk on a slow
# uplink can still take a while; chunking keeps this from ever needing minutes.
REQUEST_TIMEOUT = int(getenv("REQUEST_TIMEOUT", "600"))
# Retries for transient network failures before giving up on a file.
MAX_NETWORK_RETRIES = int(getenv("MAX_NETWORK_RETRIES", "5"))
# Base seconds between retries (grows linearly: delay × attempt).
NETWORK_RETRY_DELAY = int(getenv("NETWORK_RETRY_DELAY", "5"))
# How many files to upload at once. Uploads are outbound-bandwidth bound, so
# more workers only help if your uplink isn't already saturated; on a slow or
# flaky connection, fewer (or 1) is more reliable. Each worker gets its own
# Kaltura session because the client is NOT thread-safe.
MAX_WORKERS = max(1, int(getenv("MAX_WORKERS", "4")))


# ── Media-type detection ──────────────────────────────────────────────
VIDEO_EXTS = {
    ".mp4", ".mov", ".avi", ".wmv", ".flv", ".mkv", ".webm", ".m4v",
    ".mpg", ".mpeg", ".m2ts", ".mts", ".ts", ".3gp", ".3g2", ".vob",
    ".ogv", ".mxf", ".asf", ".m2v", ".f4v",
}
AUDIO_EXTS = {
    ".mp3", ".wav", ".m4a", ".aac", ".flac", ".ogg", ".oga", ".wma",
    ".aiff", ".aif", ".aifc", ".opus", ".amr", ".ac3",
}
IMAGE_EXTS = {
    ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".tif", ".tiff", ".webp",
    ".heic", ".heif",
}

MEDIA_TYPE_LABEL = {
    KalturaMediaType.VIDEO: "Video",
    KalturaMediaType.AUDIO: "Audio",
    KalturaMediaType.IMAGE: "Image",
}


def detect_media_type(filename):
    """Map a filename's extension to a KalturaMediaType, or None if the
    extension isn't a media type we recognize (that file is skipped)."""
    ext = os.path.splitext(filename)[1].lower()
    if ext in VIDEO_EXTS:
        return KalturaMediaType.VIDEO
    if ext in AUDIO_EXTS:
        return KalturaMediaType.AUDIO
    if ext in IMAGE_EXTS:
        return KalturaMediaType.IMAGE
    return None


CSV_HEADERS = [
    "Filename", "Media Type", "Status", "Entry ID", "Name",
    "Owner", "Tags", "Categories", "Size", "Avg MB/s", "Detail",
]


# ── Reliability wrapper ───────────────────────────────────────────────
def call_with_retry(fn, *args, **kwargs):
    """Call fn(*args, **kwargs), retrying with linear backoff on transient
    network failures (timeouts, connection resets). Kaltura raises those as
    KalturaClientException, which is NOT a subclass of KalturaException, so it
    is caught here separately; requests' own errors are covered too. A real,
    well-formed API error (KalturaException) is never retried here — callers
    handle those."""
    for attempt in range(1, MAX_NETWORK_RETRIES + 1):
        try:
            return fn(*args, **kwargs)
        except (KalturaClientException, requests.exceptions.RequestException) as exc:
            if attempt == MAX_NETWORK_RETRIES:
                raise
            delay = NETWORK_RETRY_DELAY * attempt
            print(
                f"      [network error: {exc}; retry "
                f"{attempt}/{MAX_NETWORK_RETRIES} in {delay}s]"
            )
            time.sleep(delay)


class ThreadSafeCSVWriter:
    """Wraps csv.writer so concurrent worker threads can call writerow without
    interleaving rows or corrupting the file."""
    def __init__(self, writer, file):
        self._writer = writer
        self._file = file
        self._lock = threading.Lock()

    def writerow(self, row):
        with self._lock:
            self._writer.writerow(row)
            self._file.flush()


# Each worker thread builds and caches its own Kaltura client here, because the
# client is not thread-safe (its queued per-request state gets corrupted when
# shared across threads).
_thread_local = threading.local()


def thread_client():
    """Return this thread's own Kaltura client, creating (and logging in) once
    on first use. Relies on the module-level ADMIN_SECRET set in main()."""
    client = getattr(_thread_local, "client", None)
    if client is None:
        client = create_client()
        _thread_local.client = client
    return client


# ── Session ───────────────────────────────────────────────────────────
def create_client():
    """Start an admin Kaltura session and return the client. Relies on the
    module-level ADMIN_SECRET, which main() sets from a getpass prompt."""
    config = KalturaConfiguration()
    config.serviceUrl = SERVICE_URL
    config.requestTimeout = REQUEST_TIMEOUT
    c = KalturaClient(config)
    try:
        ks = call_with_retry(
            c.session.start,
            ADMIN_SECRET,
            USER_ID,
            KalturaSessionType.ADMIN,
            PARTNER_ID,
            86400,
            privileges=PRIVILEGES,
        )
    except Exception as e:
        if getattr(e, "code", "") == "START_SESSION_ERROR":
            print(
                "\n❌ Could not log in to Kaltura. Partner ID "
                f"[{PARTNER_ID}] and the Admin Secret were not accepted.\n"
                "   Double-check both values — the secret must be the "
                "Administrator secret (not the User secret),\n"
                "   copied exactly from KMC → Settings → Integration Settings.\n"
            )
        elif type(e).__name__ == "KalturaClientException":
            print(
                "\n❌ Could not reach Kaltura to start a session.\n"
                f"   {e}\n   Check your internet connection and try again.\n"
            )
        else:
            print(f"\n❌ Could not start Kaltura session: {e}\n")
        raise SystemExit(1)
    c.setKs(ks)
    return c


# ── Category resolution ───────────────────────────────────────────────
def resolve_category_names(client, names):
    """Resolve category full-paths/names to numeric IDs. Uses freeText search
    (same as KMC) to find candidates, then filters client-side for an exact
    name or fullName match. Prompts to choose when a name is ambiguous."""
    resolved = []
    for name in names:
        cat_filter = KalturaCategoryFilter()
        cat_filter.freeText = name
        pager = KalturaFilterPager()
        pager.pageSize = 500
        pager.pageIndex = 1

        candidates = []
        while True:
            result = call_with_retry(client.category.list, cat_filter, pager)
            if not result.objects:
                break
            candidates.extend(result.objects)
            if len(result.objects) < pager.pageSize:
                break
            pager.pageIndex += 1

        matches = [c for c in candidates if name in (c.name, c.fullName)]
        if not matches:
            print(f"❌ No category found matching '{name}'. Aborting.")
            sys.exit(1)
        if len(matches) == 1:
            cat = matches[0]
            print(f"   Resolved category '{name}' → {cat.fullName} (ID: {cat.id})")
            resolved.append(str(cat.id))
        else:
            print(f"\nMultiple categories match '{name}':")
            for i, cat in enumerate(matches, start=1):
                print(f"  [{i}] ID: {cat.id}  —  {cat.fullName}")
            while True:
                choice = input(f"Choose the category to use for '{name}': ").strip()
                if choice.isdigit() and 1 <= int(choice) <= len(matches):
                    cat = matches[int(choice) - 1]
                    print(f"   Using {cat.fullName} (ID: {cat.id})")
                    resolved.append(str(cat.id))
                    break
                print(f"   Please enter a number between 1 and {len(matches)}.")
    return resolved


# ── The chunked upload ────────────────────────────────────────────────
def upload_file_chunked(client, file_path, show_progress=True):
    """Reserve an upload token and stream the file to Kaltura in chunks using
    the resumable uploadToken flow. Returns the upload token ID.

    Each chunk is uploaded in its own HTTP request so that no single request
    approaches the SDK's timeout, which is what makes large-file uploads
    reliable. Sequential-append semantics:
      • first chunk : resume=False, resumeAt=-1
      • later chunks: resume=True,  resumeAt=<byte offset of this chunk>
      • the last chunk carries finalChunk=True
    """
    filename = os.path.basename(file_path)
    file_size = os.path.getsize(file_path)

    token = KalturaUploadToken()
    token.fileName = filename
    token.fileSize = file_size
    uploaded_token = call_with_retry(client.uploadToken.add, token)
    token_id = uploaded_token.id

    start = time.monotonic()

    # Tiny/empty files: one shot is simplest and still safe.
    if file_size <= UPLOAD_CHUNK_BYTES:
        with open(file_path, "rb") as fh:
            call_with_retry(
                client.uploadToken.upload,
                token_id, fh, False, True, -1,
            )
        return token_id, _mbps(file_size, time.monotonic() - start)

    offset = 0
    first = True
    with open(file_path, "rb") as fh:
        while True:
            chunk = fh.read(UPLOAD_CHUNK_BYTES)
            if not chunk:
                break
            is_last = (offset + len(chunk)) >= file_size

            # The SDK reads .name off the file-like object for the multipart
            # filename/mimetype, so preserve the original name on each chunk.
            buf = io.BytesIO(chunk)
            buf.name = filename

            resume = not first
            resume_at = offset if not first else -1
            call_with_retry(
                client.uploadToken.upload,
                token_id, buf, resume, is_last, resume_at,
            )

            offset += len(chunk)
            first = False
            if show_progress:
                pct = min(100, int(offset * 100 / file_size))
                speed = _mbps(offset, time.monotonic() - start)
                print(
                    f"      uploading… {pct:3d}%  "
                    f"({offset // (1024 * 1024)}/{file_size // (1024 * 1024)} MB)"
                    f"  {speed:.1f} MB/s   ",
                    end="\r",
                )
    if show_progress:
        print(" " * 70, end="\r")  # clear the progress line
    return token_id, _mbps(file_size, time.monotonic() - start)


def build_entry(filename, media_type):
    """Create a KalturaMediaEntry populated with the batch-wide metadata."""
    entry = KalturaMediaEntry()
    entry.name = os.path.splitext(filename)[0]
    entry.mediaType = media_type
    if TAGS:
        entry.tags = TAGS
    if DESCRIPTION:
        entry.description = DESCRIPTION
    if OWNER_ID:
        entry.userId = OWNER_ID
    if COEDITORS:
        entry.entitledUsersEdit = ",".join(COEDITORS)
    if COPUBLISHERS:
        entry.entitledUsersPublish = ",".join(COPUBLISHERS)
    if CONVERSION_PROFILE_ID:
        entry.conversionProfileId = int(CONVERSION_PROFILE_ID)
    return entry


def assign_categories(client, entry_id, category_ids):
    """Add the entry to each category via categoryEntry.add (the reliable way,
    independent of entitlement settings). Failures are non-fatal per category."""
    for cat_id in category_ids:
        cat_entry = KalturaCategoryEntry()
        cat_entry.categoryId = int(cat_id)
        cat_entry.entryId = entry_id
        try:
            call_with_retry(client.categoryEntry.add, cat_entry)
        except KalturaException as e:
            # Already-in-category is fine; anything else we surface but continue.
            if getattr(e, "code", "") == "CATEGORY_ENTRY_ALREADY_EXISTS":
                continue
            print(f"      ⚠️ Could not add to category {cat_id}: {e}")


def process_file(client, file_path, category_ids, show_progress=True):
    """Upload one file and create its entry.
    Returns (status, entry_id, detail, mbps); mbps is None when nothing was
    actually transferred (skipped or failed before/at the upload)."""
    filename = os.path.basename(file_path)
    media_type = detect_media_type(filename)
    if media_type is None:
        return "Skipped", "", "Unrecognized media file type", None

    # 1) create the entry shell
    entry = build_entry(filename, media_type)
    created = call_with_retry(client.media.add, entry)

    # 2 & 3) upload the bytes in chunks, then 4) bind them to the entry
    try:
        token_id, mbps = upload_file_chunked(client, file_path, show_progress)
        resource = KalturaUploadedFileTokenResource()
        resource.token = token_id
        call_with_retry(client.media.addContent, created.id, resource)
    except Exception as e:
        return "Failed", created.id, f"Upload failed: {e}", None

    # categories are best-effort and don't fail the upload
    if category_ids:
        assign_categories(client, created.id, category_ids)

    return "Uploaded", created.id, "", mbps


def collect_input_files():
    """Return sorted absolute paths of uploadable files sitting directly in the
    input folder (ignores subfolders like _uploaded/_failed and dotfiles)."""
    if not os.path.isdir(INPUT_DIR):
        return []
    files = []
    for name in sorted(os.listdir(INPUT_DIR)):
        if name.startswith("."):
            continue
        full = path_join(INPUT_DIR, name)
        if os.path.isfile(full):
            files.append(full)
    return files


def _move_into(file_path, dest_dir):
    os.makedirs(dest_dir, exist_ok=True)
    dest = path_join(dest_dir, os.path.basename(file_path))
    # Avoid clobbering a same-named file already parked in the dest folder.
    if os.path.exists(dest):
        base, ext = os.path.splitext(os.path.basename(file_path))
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        dest = path_join(dest_dir, f"{base}_{stamp}{ext}")
    shutil.move(file_path, dest)


def _human_size(num_bytes):
    mb = num_bytes / (1024 * 1024)
    if mb >= 1024:
        return f"{mb / 1024:,.2f} GB"
    return f"{mb:,.1f} MB"


def _mbps(num_bytes, seconds):
    """Throughput in MB/s (mebibytes), guarding against a zero interval."""
    if seconds <= 0:
        return 0.0
    return num_bytes / seconds / (1024 * 1024)


def upload_worker(file_path, idx, total, category_ids, writer, show_progress):
    """Run the full per-file job on a worker thread: log, upload, write the CSV
    row, and move the file. Returns the status string for tallying in main().

    Each thread uses its own Kaltura client (via thread_client) because the
    client is not thread-safe. Output is per-line (not a \\r progress bar) so it
    stays readable when several files upload concurrently."""
    filename = os.path.basename(file_path)
    media_type = detect_media_type(filename)
    mt_label = MEDIA_TYPE_LABEL.get(media_type, "")
    size = _human_size(os.path.getsize(file_path))
    label = f"[{idx}/{total}] {filename} ({mt_label or 'unrecognized'}, {size})"

    mbps = None
    if media_type is None:
        print(f"{label} — ⏭️ Skipped: unrecognized media file type")
        status, entry_id, detail = "Skipped", "", "Unrecognized media file type"
    else:
        print(f"{label} — starting")
        try:
            client = thread_client()
        except SystemExit:
            # create_client() exits on a failed session start; in a worker
            # thread, treat that as a failure for this file instead of killing
            # the whole run.
            status, entry_id, detail = (
                "Failed", "", "Could not start a Kaltura session"
            )
        else:
            try:
                status, entry_id, detail, mbps = process_file(
                    client, file_path, category_ids, show_progress
                )
            except Exception as e:
                status, entry_id, detail = "Failed", "", str(e)

        if status == "Uploaded":
            speed = f"  ({mbps:.1f} MB/s)" if mbps else ""
            print(f"{label} — ✅ Created entry {entry_id}{speed}")
        elif status == "Skipped":
            print(f"{label} — ⏭️ Skipped: {detail}")
        else:
            print(f"{label} — ❌ Failed: {detail}")

    writer.writerow([
        filename, mt_label, status, entry_id,
        os.path.splitext(filename)[0], OWNER_ID or "",
        TAGS, ",".join(category_ids), size,
        f"{mbps:.1f}" if mbps else "", detail,
    ])

    # Move the file so a re-run won't re-process it.
    if MOVE_ON_SUCCESS:
        try:
            if status == "Uploaded":
                _move_into(file_path, PROCESSED_DIR)
            elif status == "Failed":
                _move_into(file_path, FAILED_DIR)
        except OSError as e:
            print(f"      ⚠️ Could not move '{filename}' after upload: {e}")

    return status


def main():
    global ADMIN_SECRET
    mimetypes.init()

    files = collect_input_files()
    print(f"\nKaltura Bulk Uploader — scanning '{INPUT_DIR}/'")
    if not files:
        print(
            f"\nNo files found in '{INPUT_DIR}/'. Drop the media you want to "
            "upload into that folder and run again."
        )
        return

    # Categorize by detected media type up front so the user sees the plan.
    recognized, unrecognized = [], []
    for f in files:
        if detect_media_type(os.path.basename(f)) is None:
            unrecognized.append(f)
        else:
            recognized.append(f)

    print(f"Found {len(files)} file(s): {len(recognized)} uploadable, "
          f"{len(unrecognized)} unrecognized.")
    for f in recognized:
        mt = MEDIA_TYPE_LABEL[detect_media_type(os.path.basename(f))]
        print(f"   • {os.path.basename(f)}  [{mt}, {_human_size(os.path.getsize(f))}]")
    for f in unrecognized:
        print(f"   • {os.path.basename(f)}  [unrecognized — will skip]")

    if DRY_RUN:
        print("\nDRY_RUN is on — nothing was uploaded. Set DRY_RUN=false to run for real.")
        return
    if not recognized:
        print("\nNothing uploadable to do.")
        return

    ADMIN_SECRET = getpass.getpass("\nEnter your Kaltura Admin Secret: ").strip()
    client = create_client()

    category_ids = list(CATEGORY_IDS)
    if CATEGORY_NAMES:
        category_ids += resolve_category_names(client, CATEGORY_NAMES)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d-%H%M")
    report_path = path_join(OUTPUT_DIR, f"{timestamp}_upload_report.csv")

    all_files = recognized + unrecognized
    total = len(all_files)
    workers = min(MAX_WORKERS, len(recognized)) or 1
    # The inline % progress bar (which rewrites one line with \r) only makes
    # sense with a single uploader; with several it would garble, so parallel
    # runs use clean per-file lines instead.
    show_progress = workers == 1
    print(
        f"\nUploading {len(recognized)} file(s) with {workers} "
        f"worker{'s' if workers != 1 else ''}…\n"
    )
    counts = {"Uploaded": 0, "Failed": 0, "Skipped": 0}
    # Sizes captured now, because a file is moved out of input/ once uploaded.
    sizes = {f: os.path.getsize(f) for f in all_files}
    uploaded_bytes = 0
    run_start = time.monotonic()

    with open(report_path, "w", newline="", encoding="utf-8") as csv_file:
        writer = ThreadSafeCSVWriter(csv.writer(csv_file), csv_file)
        writer.writerow(CSV_HEADERS)

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(
                    upload_worker, file_path, idx, total,
                    category_ids, writer, show_progress,
                ): file_path
                for idx, file_path in enumerate(all_files, start=1)
            }
            for future in as_completed(futures):
                file_path = futures[future]
                try:
                    status = future.result()
                except Exception as e:
                    print(f"      ❌ Unexpected error on "
                          f"{os.path.basename(file_path)}: {e}")
                    status = "Failed"
                counts[status] = counts.get(status, 0) + 1
                if status == "Uploaded":
                    uploaded_bytes += sizes.get(file_path, 0)

    elapsed = time.monotonic() - run_start
    print(
        f"\nDone. {counts.get('Uploaded', 0)} uploaded, "
        f"{counts.get('Failed', 0)} failed, {counts.get('Skipped', 0)} skipped."
    )
    if uploaded_bytes:
        print(
            f"Transferred {_human_size(uploaded_bytes)} in {elapsed:.1f}s "
            f"— effective {_mbps(uploaded_bytes, elapsed):.1f} MB/s"
            + (f" across {workers} workers." if workers > 1 else ".")
        )
    if MOVE_ON_SUCCESS:
        print(f"Uploaded files moved to '{PROCESSED_DIR}/'; "
              f"any failures moved to '{FAILED_DIR}/'.")
    print(f"Report: {report_path}")


if __name__ == "__main__":
    main()
