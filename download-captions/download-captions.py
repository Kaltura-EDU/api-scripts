"""
download-captions.py
Downloads caption files for Kaltura entries and (optionally) writes TXT
transcripts.

Output format is controlled by OUTPUT_FORMAT in .env: 'srt' (default),
'txt', or 'both'. Configuration is managed through a .env file (see
README for details).

For security, the Kaltura admin secret is NOT read from .env; the script
prompts for it interactively at runtime so it is never stored on disk.
"""

import os
import re
import ssl
import time
import getpass
import urllib.request
import urllib.parse
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import List

import requests
from dotenv import load_dotenv
from KalturaClient import KalturaClient, KalturaConfiguration
from KalturaClient.Plugins.Core import (
    KalturaBaseEntryFilter,
    KalturaFilterPager,
    KalturaSessionType,
    KalturaCategoryEntryFilter,
    KalturaCategoryFilter,
)
from KalturaClient.Plugins.Caption import KalturaCaptionAssetFilter
from KalturaClient.exceptions import KalturaClientException
import pysrt

# Load .env alongside this script, not relying on current working directory
load_dotenv(dotenv_path=Path(__file__).with_name(".env"), override=False)


def _env_bool(key: str, default: str = "false") -> bool:
    return os.getenv(key, default).strip().lower() in ("1", "true", "yes", "y")


# ---------- Configuration from .env ----------
# NOTE: the admin secret is intentionally NOT read from .env. It is prompted
# for interactively at runtime (see main) so it is never stored on disk.
PARTNER_ID = os.getenv("PARTNER_ID", "").strip()
SERVICE_URL = os.getenv(
    "KALTURA_SERVICE_URL", "https://www.kaltura.com/"
).strip()

# Captions are written to a per-run subfolder named with the processing
# date and time, inside a fixed "output" folder next to this script — e.g.
# output/2026-07-28_142530/. This keeps filenames short and keeps each run
# (batch) neatly separated. The time component means repeated runs on the
# same day don't collide.
DOWNLOAD_FOLDER = "output"
RUN_TIMESTAMP = datetime.now().strftime("%Y-%m-%d_%H%M%S")
RUN_OUTPUT_DIR = os.path.join(DOWNLOAD_FOLDER, RUN_TIMESTAMP)

# ---------- Reliability knobs (from .env) ----------
# Seconds before an individual API request times out.
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "120"))
# Retries for transient network errors before giving up.
MAX_NETWORK_RETRIES = int(os.getenv("MAX_NETWORK_RETRIES", "5"))
# Base seconds between retries (grows linearly: delay × attempt).
NETWORK_RETRY_DELAY = int(os.getenv("NETWORK_RETRY_DELAY", "5"))

# OUTPUT_FORMAT controls what gets saved: 'srt' (default), 'txt', or 'both'.
# Falls back to legacy CONVERT_TO_TXT if OUTPUT_FORMAT is not set.
_output_format_raw = os.getenv("OUTPUT_FORMAT", "").strip().lower()
if not _output_format_raw:
    _output_format_raw = (
        "txt" if _env_bool("CONVERT_TO_TXT", "false") else "srt"
    )
if _output_format_raw not in ("srt", "txt", "both"):
    raise SystemExit(
        f"Error: invalid OUTPUT_FORMAT '{_output_format_raw}' in .env. "
        "Valid values are: srt, txt, both."
    )
OUTPUT_FORMAT = _output_format_raw
SAVE_SRT = OUTPUT_FORMAT in ("srt", "both")
SAVE_TXT = OUTPUT_FORMAT in ("txt", "both")
INCLUDE_CHILD_CATEGORIES = _env_bool("INCLUDE_CHILD_CATEGORIES", "true")
DEBUG = _env_bool("DEBUG", "false")

# ---------- Filename component toggles ----------
# The entry ID is ALWAYS included in the filename (it uniquely identifies the
# entry). Each toggle below adds one more component. At least one must be true
# so filenames carry something beyond the bare entry ID — enforced just below.
#
# INCLUDE_CREATION_DATE_IN_FILENAMES uses the ENTRY's (video's) creation date
# (entry.createdAt), NOT the caption track's own creation date. It is the same
# for every caption track on a given entry.
INCLUDE_CREATION_DATE_IN_FILENAMES = _env_bool(
    "INCLUDE_CREATION_DATE_IN_FILENAMES", "false"
)
# INCLUDE_CAPTION_NAME_IN_FILENAMES toggles the entry (video) title
# (entry.name), e.g. "XSE1_5B_Axial_Compression".
INCLUDE_CAPTION_NAME_IN_FILENAMES = _env_bool(
    "INCLUDE_CAPTION_NAME_IN_FILENAMES", "true"
)
# INCLUDE_CAPTION_LABEL_IN_FILENAMES toggles the caption track's label,
# e.g. "English" or "English (auto-generated)".
INCLUDE_CAPTION_LABEL_IN_FILENAMES = _env_bool(
    "INCLUDE_CAPTION_LABEL_IN_FILENAMES", "true"
)
if not (
    INCLUDE_CREATION_DATE_IN_FILENAMES
    or INCLUDE_CAPTION_NAME_IN_FILENAMES
    or INCLUDE_CAPTION_LABEL_IN_FILENAMES
):
    raise SystemExit(
        "Error: at least one of INCLUDE_CREATION_DATE_IN_FILENAMES, "
        "INCLUDE_CAPTION_NAME_IN_FILENAMES, or "
        "INCLUDE_CAPTION_LABEL_IN_FILENAMES must be true in .env, so caption "
        "filenames carry more than the bare entry ID."
    )

# Behavior toggles
SKIP_CHILD_ENTRIES = _env_bool("SKIP_CHILD_ENTRIES", "true")

# Skip machine (ASR) caption tracks. KMC optionally appends a suffix to the
# label of auto-generated captions — set in KMC under Settings > Reach >
# Service Parameters ("machine captions label suffix"). AUTO_GENERATED_LABEL
# is just that suffix (e.g. "(auto-generated)"), NOT a full track label, and
# it is matched as a case-insensitive substring — so it catches the suffix in
# every language (e.g. "English (auto-generated)", "Spanish (auto-generated)").
SKIP_AUTO_GENERATED = _env_bool("SKIP_AUTO_GENERATED", "false")
AUTO_GENERATED_LABEL = os.getenv(
    "AUTO_GENERATED_LABEL", "(auto-generated)"
).strip()
if SKIP_AUTO_GENERATED and not AUTO_GENERATED_LABEL:
    raise SystemExit(
        "Error: SKIP_AUTO_GENERATED is true but AUTO_GENERATED_LABEL is "
        "empty. Set AUTO_GENERATED_LABEL to the suffix KMC appends to "
        "machine-caption labels (e.g. \"(auto-generated)\"), or set "
        "SKIP_AUTO_GENERATED=false."
    )

# Query inputs (priority: ENTRY_IDS > CATEGORY_IDS > TAGS > OWNER)
CATEGORY_IDS = os.getenv("CATEGORY_IDS", "").strip()
TAGS = os.getenv("TAGS", "").strip()
ENTRY_IDS = os.getenv("ENTRY_IDS", "").strip()
# Prefer OWNER but tolerate a common typo "ONWER"
OWNER = os.getenv("OWNER", os.getenv("ONWER", "")).strip()

# User for session (optional; fallback to admin)
KALTURA_USER = os.getenv("KALTURA_USER", "admin").strip()


# ---------- Kaltura helpers ----------
def call_with_retry(fn, *args, **kwargs):
    """Call fn(*args, **kwargs), retrying with linear backoff on transient
    network failures (timeouts, connection resets). Kaltura raises those as
    KalturaClientException, which is NOT a subclass of KalturaException, so
    it is caught here separately; requests' own errors are covered too. A
    real, well-formed API error (KalturaException) is never retried here —
    callers handle those."""
    for attempt in range(1, MAX_NETWORK_RETRIES + 1):
        try:
            return fn(*args, **kwargs)
        except (
            KalturaClientException,
            requests.exceptions.RequestException,
        ) as exc:
            if attempt == MAX_NETWORK_RETRIES:
                raise
            delay = NETWORK_RETRY_DELAY * attempt
            print(
                f"    [network error: {exc}; retry "
                f"{attempt}/{MAX_NETWORK_RETRIES} in {delay}s]"
            )
            time.sleep(delay)


def get_kaltura_client(partner_id: str, admin_secret: str) -> KalturaClient:
    config = KalturaConfiguration(partner_id)
    config.serviceUrl = SERVICE_URL
    config.requestTimeout = REQUEST_TIMEOUT
    client = KalturaClient(config)
    ks = call_with_retry(
        client.session.start,
        admin_secret,
        KALTURA_USER,
        KalturaSessionType.ADMIN,
        partner_id,
        privileges="all:*,disableentitlement",
    )
    client.setKs(ks)
    return client


def sanitize_filename(name: str, max_length: int = 100) -> str:
    name = re.sub(r"[^a-zA-Z0-9_-]", "_", name)
    return name[:max_length]


def _is_child_entry(entry) -> bool:
    """
    Best-effort detection of child entries in multi-stream hierarchies.
    If an entry has a parent-like pointer, treat it as a child.
    We check multiple common fields to be safe across entry types.
    """
    for attr in ("parentId", "parentEntryId", "rootParentId", "rootEntryId"):
        try:
            val = getattr(entry, attr, None)
            if val and isinstance(val, str) and val != entry.id:
                return True
        except Exception:
            pass
    return False


def get_entry_ids_for_category(
    client: KalturaClient,
    category_ids: str,
    include_children: bool,
) -> List[str]:
    """
    Safe resolver for entry IDs in one or more categories.
    Strategy:
      - If include_children=True, expand each ancestor ID to include all
        descendant IDs via category.list(ancestorIdIn...).
      - Then, for each category ID (ancestor and any descendants), call
        categoryEntry.list with categoryIdEqual (one ID at a time) to
        gather entryIds. This avoids huge CSVs and plays nicely with the
        backend.
    """
    # --- 1) Build the full set of category IDs to scan ---
    cat_id_set = set()
    raw_ids = [c.strip() for c in category_ids.split(",") if c.strip()]
    for cid in raw_ids:
        # Always include the ancestor itself
        cat_id_set.add(cid)
        if include_children:
            cf = KalturaCategoryFilter()
            cf.ancestorIdIn = cid  # children only (not including the ancestor)
            pager_cat = KalturaFilterPager()
            pager_cat.pageSize = 500
            pager_cat.pageIndex = 1
            try:
                while True:
                    print(
                        f"Expanding subcategories for category {cid}"
                        f" (page {pager_cat.pageIndex})…"
                    )
                    cres = call_with_retry(
                        client.category.list, cf, pager_cat
                    )
                    if not getattr(cres, "objects", None):
                        break
                    for c in cres.objects:
                        cat_id_set.add(str(c.id))
                    if len(cres.objects) < pager_cat.pageSize:
                        break
                    pager_cat.pageIndex += 1
                print(
                    f"Finished expanding subcategories for {cid}:"
                    f" {len(cat_id_set)} categories collected so far."
                )
            except Exception as e:
                print(f"Error expanding subcategories for {cid}: {e}")

    if not cat_id_set:
        return []

    # --- 2) For each category ID, list members using categoryIdEqual ---
    all_entry_ids: List[str] = []
    cat_id_list = sorted(
        cat_id_set, key=lambda x: int(x) if x.isdigit() else x
    )
    for single_cat in cat_id_list:
        print(f"Scanning category {single_cat} for entries…")
        cef = KalturaCategoryEntryFilter()
        try:
            # categoryIdEqual is an integer; convert when possible
            try:
                cef.categoryIdEqual = int(single_cat)
            except ValueError:
                # Fallback to categoryIdIn if non-numeric
                cef.categoryIdIn = single_cat

            pager = KalturaFilterPager()
            pager.pageSize = 500
            pager.pageIndex = 1
            while True:
                res = call_with_retry(
                    client.categoryEntry.list, cef, pager
                )
                if not getattr(res, "objects", None):
                    break
                for ce in res.objects:
                    all_entry_ids.append(ce.entryId)
                print(
                    f"  Retrieved {len(res.objects)} entries from"
                    f" category {single_cat} (page {pager.pageIndex})"
                )
                if len(res.objects) < pager.pageSize:
                    break
                pager.pageIndex += 1
            print(
                f"Finished category {single_cat}:"
                f" {len(all_entry_ids)} entries collected so far."
            )
        except Exception as e:
            print(
                f"Error retrieving categoryEntry for"
                f" category {single_cat}: {e}"
            )

    # Deduplicate while preserving order
    seen = set()
    unique_entry_ids: List[str] = []
    for eid in all_entry_ids:
        if eid not in seen:
            seen.add(eid)
            unique_entry_ids.append(eid)

    return unique_entry_ids


def get_entries_by_ids(client: KalturaClient, entry_ids: List[str]):
    """Fetch entry objects by ID (for names/dates in filenames)."""
    entries = []
    for eid in entry_ids:
        try:
            entry = call_with_retry(client.baseEntry.get, eid)
            if entry:
                entries.append(entry)
        except Exception as e:
            print(f"Error retrieving entry {eid}: {e}")
    return entries


def get_entries(client: KalturaClient, method: str, identifier: str):
    entries = []
    base_filter = KalturaBaseEntryFilter()

    if method == "tag":
        base_filter.tagsLike = identifier
    elif method == "category":
        entry_ids = get_entry_ids_for_category(
            client, identifier, INCLUDE_CHILD_CATEGORIES
        )
        if not entry_ids:
            return []
        return get_entries_by_ids(client, entry_ids)
    elif method == "entry_ids":
        base_filter.idIn = identifier
    elif method == "owner":
        base_filter.userIdEqual = identifier
    else:
        print("Invalid method selection.")
        return []

    pager = KalturaFilterPager()
    pager.pageSize = 500
    pager.pageIndex = 1

    try:
        while True:
            result = call_with_retry(
                client.baseEntry.list, base_filter, pager
            )
            if not getattr(result, "objects", None):
                break
            entries.extend(result.objects)
            if len(entries) >= getattr(result, "totalCount", len(entries)):
                break
            pager.pageIndex += 1
    except Exception as e:
        print(f"Error retrieving entries: {e}")

    return entries


def get_captions(client: KalturaClient, entry_id: str):
    cap_filter = KalturaCaptionAssetFilter()
    cap_filter.entryIdEqual = entry_id
    # Filter to active/ready captions (statusEqual=2 indicates ACTIVE)
    try:
        cap_filter.statusEqual = 2
    except Exception:
        pass
    pager = KalturaFilterPager()
    try:
        res = call_with_retry(
            client.caption.captionAsset.list, cap_filter, pager
        )
        captions = res.objects if getattr(res, "objects", None) else []
    except Exception as e:
        print(f"Error retrieving captions for entry {entry_id}: {e}")
        return []

    if SKIP_AUTO_GENERATED and AUTO_GENERATED_LABEL:
        needle = AUTO_GENERATED_LABEL.lower()
        kept = []
        for cap in captions:
            if needle in (cap.label or "").lower():
                print(
                    f"  Skipping auto-generated caption "
                    f"'{cap.label}' for entry {entry_id}"
                )
            else:
                kept.append(cap)
        captions = kept

    return captions


def convert_caption_to_txt(caption_path: str, caption_ext: str) -> str:
    """
    Convert a caption file (.srt or .vtt) to a plain-text .txt transcript.
    Returns the txt path.
    """
    base, _ = os.path.splitext(caption_path)
    txt_path = base + ".txt"
    try:
        if caption_ext.lower() == ".srt":
            # Use pysrt for robust SRT parsing
            subs = pysrt.open(caption_path)
            with open(txt_path, "w", encoding="utf-8") as f:
                for sub in subs:
                    # Replace newlines within cues with spaces
                    f.write(sub.text.replace("\n", " ").strip() + "\n")
        elif caption_ext.lower() == ".vtt":
            # Lightweight VTT -> TXT: strip header, NOTE blocks, timestamps
            with open(
                caption_path, "r", encoding="utf-8", errors="ignore"
            ) as src, open(txt_path, "w", encoding="utf-8") as out:
                for line in src:
                    s = line.strip()
                    if not s:
                        continue
                    if s.upper().startswith("WEBVTT"):
                        continue
                    if s.startswith("NOTE"):
                        continue
                    # Timestamp lines like "00:00:10.500 --> 00:00:13.000"
                    if "-->" in s:
                        continue
                    # Keep only lines that look like dialog (contain letters)
                    if not re.search(r"[A-Za-z]", s):
                        continue
                    out.write(s + "\n")
        else:
            # Unknown type: best-effort extraction by dropping timing lines
            with open(
                caption_path, "r", encoding="utf-8", errors="ignore"
            ) as src, open(txt_path, "w", encoding="utf-8") as out:
                for line in src:
                    s = line.strip()
                    if not s:
                        continue
                    if s.upper().startswith("WEBVTT"):
                        continue
                    if "-->" in s:
                        continue
                    if re.fullmatch(r"\d{1,4}", s):
                        # SRT cue index
                        continue
                    out.write(s + "\n")
        return txt_path
    except Exception as e:
        print(f"Error converting {caption_path} to TXT: {e}")
        return ""


def _determine_caption_ext(cap, url: str) -> str:
    """
    Try to determine the caption file extension.
    Priority: cap.fileExt -> URL path suffix -> default '.srt'
    """
    ext = getattr(cap, "fileExt", None)
    if ext:
        ext = ext if ext.startswith(".") else f".{ext}"
        return ext.lower()
    try:
        path = urllib.parse.urlparse(url).path
        _, guessed_ext = os.path.splitext(path)
        if guessed_ext:
            return guessed_ext.lower()
    except Exception:
        pass
    return ".srt"


def download_captions(client: KalturaClient, captions, entry, counter):
    os.makedirs(RUN_OUTPUT_DIR, exist_ok=True)
    entry_id = entry.id
    entry_title = sanitize_filename(entry.name)
    # Entry (video) creation date, in UTC. Only used if the toggle is on.
    entry_date = datetime.fromtimestamp(
        entry.createdAt, tz=timezone.utc
    ).strftime("%Y-%m-%d")

    for cap in captions:
        try:
            raw_label = cap.label or ""
            label = sanitize_filename(raw_label)
            url = call_with_retry(
                client.caption.captionAsset.getUrl, cap.id, 0
            )
            ext = _determine_caption_ext(cap, url)  # .srt / .vtt / etc.

            # Assemble the filename. Entry ID is always present; the date,
            # entry title, and label are each optional (at least one of the
            # three is guaranteed true by the startup check).
            parts = []
            if INCLUDE_CREATION_DATE_IN_FILENAMES:
                parts.append(entry_date)
            parts.append(entry_id)
            if INCLUDE_CAPTION_NAME_IN_FILENAMES:
                parts.append(entry_title)
            if INCLUDE_CAPTION_LABEL_IN_FILENAMES and label:
                parts.append(label)
            base_name = "_".join(parts)

            out_path = os.path.join(RUN_OUTPUT_DIR, base_name + ext)
            # If several tracks on one entry would map to the same name
            # (e.g. the label is omitted), add a numeric suffix so none
            # overwrite each other.
            if os.path.exists(out_path):
                n = 2
                while os.path.exists(
                    os.path.join(RUN_OUTPUT_DIR, f"{base_name}_{n}{ext}")
                ):
                    n += 1
                base_name = f"{base_name}_{n}"
                out_path = os.path.join(RUN_OUTPUT_DIR, base_name + ext)

            try:
                with (
                    urllib.request.urlopen(url) as resp,
                    open(out_path, "wb") as fh,
                ):
                    fh.write(resp.read())

                # Always show the download line once (numbered). Pad the
                # counter to 4 digits so output stays vertically aligned up
                # to 9,999 downloaded caption assets.
                print(f"{counter[0]:>4}. Downloaded:\t{out_path}")

                if SAVE_TXT:
                    txt_path = convert_caption_to_txt(out_path, ext)
                    if txt_path:
                        print(f"      Converted to TXT:\t{txt_path}")
                    else:
                        print(
                            "      Warning:\tconversion failed"
                            f" for {out_path}"
                        )

                if not SAVE_SRT and os.path.exists(out_path):
                    try:
                        os.remove(out_path)
                        print(f"   Deleted:\t\t{out_path}")
                    except Exception as rm_err:
                        print(
                            f"Warning: could not delete"
                            f" {out_path}: {rm_err}"
                        )

                # Increment the main counter only once per caption asset
                counter[0] += 1
            except ssl.SSLError as ssl_err:
                print(
                    f"⚠️ SSL error downloading {cap.label}"
                    f" for entry {entry.id}: {ssl_err}"
                )
                print(
                    "If you're on macOS, try running"
                    " Install Certificates.command from your Python folder."
                )
        except Exception as e:
            print(f"Error downloading caption {getattr(cap, 'id', '?')}: {e}")


def main():
    print("▶ download-captions: starting…")
    if DEBUG:
        print("[DEBUG] Using .env from:", Path(__file__).with_name(".env"))
        print("[DEBUG] PARTNER_ID set:", bool(PARTNER_ID))
        print("[DEBUG] KALTURA_SERVICE_URL:", SERVICE_URL)
        print("[DEBUG] RUN_OUTPUT_DIR:", RUN_OUTPUT_DIR)
        print("[DEBUG] OUTPUT_FORMAT:", OUTPUT_FORMAT)
        print("[DEBUG] SKIP_AUTO_GENERATED:", SKIP_AUTO_GENERATED)
        print("[DEBUG] AUTO_GENERATED_LABEL:", AUTO_GENERATED_LABEL)
        print(
            "[DEBUG] INCLUDE_CREATION_DATE_IN_FILENAMES:",
            INCLUDE_CREATION_DATE_IN_FILENAMES,
        )
        print(
            "[DEBUG] INCLUDE_CAPTION_NAME_IN_FILENAMES:",
            INCLUDE_CAPTION_NAME_IN_FILENAMES,
        )
        print(
            "[DEBUG] INCLUDE_CAPTION_LABEL_IN_FILENAMES:",
            INCLUDE_CAPTION_LABEL_IN_FILENAMES,
        )
        print("[DEBUG] INCLUDE_CHILD_CATEGORIES:", INCLUDE_CHILD_CATEGORIES)
        print("[DEBUG] ENTRY_IDS:", ENTRY_IDS)
        print("[DEBUG] CATEGORY_IDS:", CATEGORY_IDS)
        print("[DEBUG] TAGS:", TAGS)
        print("[DEBUG] OWNER:", OWNER)

    if not PARTNER_ID:
        print("Error: PARTNER_ID not set in your .env file.")
        return

    admin_secret = getpass.getpass("Enter your Kaltura admin secret: ")
    if not admin_secret:
        print("Error: Admin secret cannot be empty.")
        return
    client = get_kaltura_client(PARTNER_ID, admin_secret)
    if DEBUG:
        print("[DEBUG] Connected as partner:", PARTNER_ID)

    # Decide method based on which .env variables are populated
    # (priority: ENTRY_IDS > CATEGORY_IDS > TAGS > OWNER)
    provided = {
        "entry_ids": bool(ENTRY_IDS),
        "category": bool(CATEGORY_IDS),
        "tag": bool(TAGS),
        "owner": bool(OWNER),
    }
    priority = ["entry_ids", "category", "tag", "owner"]
    method = next((m for m in priority if provided[m]), None)

    if not method:
        print(
            "Error: No query inputs set. Populate one of ENTRY_IDS,"
            " CATEGORY_IDS, TAGS, or OWNER in your .env file."
        )
        return

    extras = [m for m in provided if provided[m] and m != method]
    if extras:
        print(
            f"Note: Multiple query inputs found in .env."
            f" Using '{method}' and ignoring: {', '.join(extras)}"
        )

    identifier = {
        "entry_ids": ENTRY_IDS,
        "category": CATEGORY_IDS,
        "tag": TAGS,
        "owner": OWNER,
    }[method]

    if method == "category":
        scope = (
            "including subcategories"
            if INCLUDE_CHILD_CATEGORIES
            else "this category only"
        )
        print(
            f"Category search will target: {scope}"
            f" (INCLUDE_CHILD_CATEGORIES={INCLUDE_CHILD_CATEGORIES})"
        )

    entries = get_entries(client, method, identifier)
    print(f"{len(entries)} entries found.")
    if not entries:
        print("No entries found. Exiting.")
        return

    print(f"Saving captions to: {RUN_OUTPUT_DIR}/")

    counter = [1]
    for entry in entries:
        if SKIP_CHILD_ENTRIES and _is_child_entry(entry):
            print(f"Skipping child entry {entry.id} (parent-linked)")
            continue
        caps = get_captions(client, entry.id)
        if caps:
            download_captions(client, caps, entry, counter)
        else:
            print(f"No captions found for entry {entry.id}")

    print("Caption download complete.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("✖ Unhandled error:", e)
        traceback.print_exc()
