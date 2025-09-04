"""
download-captions.py
Downloads caption files for Kaltura entries and (optionally) writes TXT transcripts.

Configuration is managed through a .env file (see README for details).
"""

import os
import re
import ssl
import urllib.request
import urllib.parse
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import List

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
import pysrt

# Load .env alongside this script, not relying on current working directory
load_dotenv(dotenv_path=Path(__file__).with_name(".env"), override=False)


def _env_bool(key: str, default: str = "false") -> bool:
    return os.getenv(key, default).strip().lower() in ("1", "true", "yes", "y")


# ---------- Configuration from .env ----------
PARTNER_ID = os.getenv("PARTNER_ID", "").strip()
ADMIN_SECRET = os.getenv("ADMIN_SECRET", "").strip()
SERVICE_URL = os.getenv("KALTURA_SERVICE_URL", "https://www.kaltura.com/").strip()

DOWNLOAD_FOLDER = os.getenv("DOWNLOAD_FOLDER", "captions_download").strip()
CONVERT_TO_TXT = _env_bool("CONVERT_TO_TXT", "false")
INCLUDE_CHILD_CATEGORIES = _env_bool("INCLUDE_CHILD_CATEGORIES", "true")
DEBUG = _env_bool("DEBUG", "false")

# Behavior toggles
INCLUDE_CAPTION_LABEL_IN_FILENAMES = _env_bool("INCLUDE_CAPTION_LABEL_IN_FILENAMES", "true")
SKIP_CHILD_ENTRIES = _env_bool("SKIP_CHILD_ENTRIES", "true")

# Query inputs (priority: ENTRY_IDS > CATEGORY_IDS > TAGS > OWNER)
CATEGORY_IDS = os.getenv("CATEGORY_IDS", "").strip()
TAGS = os.getenv("TAGS", "").strip()
ENTRY_IDS = os.getenv("ENTRY_IDS", "").strip()
# Prefer OWNER but tolerate a common typo "ONWER"
OWNER = os.getenv("OWNER", os.getenv("ONWER", "")).strip()

# User for session (optional; fallback to admin)
USER = os.getenv("USER", "admin").strip()


# ---------- Kaltura helpers ----------
def get_kaltura_client(partner_id: str, admin_secret: str) -> KalturaClient:
    config = KalturaConfiguration(partner_id)
    config.serviceUrl = SERVICE_URL
    client = KalturaClient(config)
    ks = client.session.start(
        admin_secret,
        USER,
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


def get_entry_ids_for_category(client: KalturaClient, category_ids: str, include_children: bool) -> List[str]:
    """
    Safe resolver for entry IDs in one or more categories.
    Strategy:
      - If include_children=True, expand each ancestor ID to include all descendant IDs via category.list(ancestorIdIn...).
      - Then, for each category ID (ancestor and any descendants), call categoryEntry.list with categoryIdEqual
        (one ID at a time) to gather entryIds. This avoids huge CSVs and plays nicely with the backend.
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
                    print(f"Expanding subcategories for category {cid} (page {pager_cat.pageIndex})…")
                    cres = client.category.list(cf, pager_cat)
                    if not getattr(cres, "objects", None):
                        break
                    for c in cres.objects:
                        cat_id_set.add(str(c.id))  # keep as str for uniformity
                    if len(cres.objects) < pager_cat.pageSize:
                        break
                    pager_cat.pageIndex += 1
                print(f"Finished expanding subcategories for {cid}: {len(cat_id_set)} categories collected so far.")
            except Exception as e:
                print(f"Error expanding subcategories for {cid}: {e}")

    if not cat_id_set:
        return []

    # --- 2) For each category ID, list members using categoryIdEqual (one-by-one) ---
    all_entry_ids: List[str] = []
    cat_id_list = sorted(cat_id_set, key=lambda x: int(x) if x.isdigit() else x)
    for single_cat in cat_id_list:
        print(f"Scanning category {single_cat} for entries…")
        cef = KalturaCategoryEntryFilter()
        try:
            # categoryIdEqual is an integer; convert when possible
            try:
                cef.categoryIdEqual = int(single_cat)
            except ValueError:
                # Fallback to categoryIdIn if somehow non-numeric (shouldn't happen for real category IDs)
                cef.categoryIdIn = single_cat

            pager = KalturaFilterPager()
            pager.pageSize = 500
            pager.pageIndex = 1
            while True:
                res = client.categoryEntry.list(cef, pager)
                if not getattr(res, "objects", None):
                    break
                for ce in res.objects:
                    all_entry_ids.append(ce.entryId)
                print(f"  Retrieved {len(res.objects)} entries from category {single_cat} (page {pager.pageIndex})")
                if len(res.objects) < pager.pageSize:
                    break
                pager.pageIndex += 1
            print(f"Finished category {single_cat}: {len(all_entry_ids)} entries collected so far.")
        except Exception as e:
            print(f"Error retrieving categoryEntry for category {single_cat}: {e}")

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
            entry = client.baseEntry.get(eid)
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
        entry_ids = get_entry_ids_for_category(client, identifier, INCLUDE_CHILD_CATEGORIES)
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
            result = client.baseEntry.list(base_filter, pager)
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
    # Filter to active/ready captions if the enum is available (statusEqual=2 usually indicates ACTIVE)
    try:
        cap_filter.statusEqual = 2
    except Exception:
        pass
    pager = KalturaFilterPager()
    try:
        res = client.caption.captionAsset.list(cap_filter, pager)
        return res.objects if getattr(res, "objects", None) else []
    except Exception as e:
        print(f"Error retrieving captions for entry {entry_id}: {e}")
        return []


def convert_caption_to_txt(caption_path: str, caption_ext: str) -> str:
    """
    Convert a caption file (.srt or .vtt) to a plain-text .txt transcript.
    Returns the txt path. On success, deletes the original caption file if CONVERT_TO_TXT is True.
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
            # Lightweight VTT -> TXT: strip WEBVTT header, NOTE blocks, timestamps and cue settings
            with open(caption_path, "r", encoding="utf-8", errors="ignore") as src, open(txt_path, "w", encoding="utf-8") as out:
                for line in src:
                    s = line.strip()
                    if not s:
                        continue
                    if s.upper().startswith("WEBVTT"):
                        continue
                    if s.startswith("NOTE"):
                        continue
                    # Timestamp lines like "00:00:10.500 --> 00:00:13.000 align:start position:0%"
                    if "-->" in s:
                        continue
                    # Cue identifiers can be arbitrary; keep only lines that look like dialog (contain letters)
                    if not re.search(r"[A-Za-z]", s):
                        continue
                    out.write(s + "\n")
        else:
            # Unknown type: best-effort text extraction by dropping timing-like lines
            with open(caption_path, "r", encoding="utf-8", errors="ignore") as src, open(txt_path, "w", encoding="utf-8") as out:
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
        # If we were only asked for TXT output, remove the source caption file
        if CONVERT_TO_TXT and os.path.exists(caption_path):
            try:
                os.remove(caption_path)
            except Exception as rm_err:
                print(f"Warning: could not delete temporary caption file {caption_path}: {rm_err}")
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
    os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)
    entry_date = datetime.fromtimestamp(entry.createdAt, tz=timezone.utc).strftime("%Y-%m-%d")
    entry_id = entry.id
    entry_title = sanitize_filename(entry.name)

    for cap in captions:
        try:
            raw_label = cap.label or ""
            label = sanitize_filename(raw_label)
            url = client.caption.captionAsset.getUrl(cap.id, 0)
            ext = _determine_caption_ext(cap, url)  # .srt / .vtt / etc.
            if INCLUDE_CAPTION_LABEL_IN_FILENAMES and label:
                base_name = f"{entry_date}_{entry_id}_{entry_title}_{label}"
            else:
                base_name = f"{entry_date}_{entry_id}_{entry_title}"
            out_path = os.path.join(DOWNLOAD_FOLDER, base_name + ext)

            try:
                with urllib.request.urlopen(url) as resp, open(out_path, "wb") as fh:
                    fh.write(resp.read())

                # Always show the download line once (numbered)
                print(f"{counter[0]}. Downloaded:\t\t{out_path}")

                if CONVERT_TO_TXT:
                    txt_path = convert_caption_to_txt(out_path, ext)
                    if txt_path:
                        print(f"   Converted to TXT:\t{txt_path}")
                        # convert_caption_to_txt deletes the source when CONVERT_TO_TXT=True
                        print(f"   Deleted:\t\t{out_path}")
                    else:
                        print(f"   Warning:\t\tconversion failed for {out_path}")

                # Increment the main counter only once per caption asset
                counter[0] += 1
            except ssl.SSLError as ssl_err:
                print(f"⚠️ SSL error downloading {cap.label} for entry {entry.id}: {ssl_err}")
                print("If you're on macOS, try running Install Certificates.command from your Python folder.")
        except Exception as e:
            print(f"Error downloading caption {getattr(cap, 'id', '?')}: {e}")


def main():
    print("▶ download-captions: starting…")
    if DEBUG:
        print("[DEBUG] Using .env from:", Path(__file__).with_name(".env"))
        print("[DEBUG] PARTNER_ID set:", bool(PARTNER_ID))
        print("[DEBUG] ADMIN_SECRET set:", bool(ADMIN_SECRET))
        print("[DEBUG] KALTURA_SERVICE_URL:", SERVICE_URL)
        print("[DEBUG] DOWNLOAD_FOLDER:", DOWNLOAD_FOLDER)
        print("[DEBUG] CONVERT_TO_TXT:", CONVERT_TO_TXT)
        print("[DEBUG] INCLUDE_CHILD_CATEGORIES:", INCLUDE_CHILD_CATEGORIES)
        print("[DEBUG] ENTRY_IDS:", ENTRY_IDS)
        print("[DEBUG] CATEGORY_IDS:", CATEGORY_IDS)
        print("[DEBUG] TAGS:", TAGS)
        print("[DEBUG] OWNER:", OWNER)

    if not PARTNER_ID or not ADMIN_SECRET:
        print("Error: PARTNER_ID and/or ADMIN_SECRET not set in your .env file.")
        return

    client = get_kaltura_client(PARTNER_ID, ADMIN_SECRET)
    if DEBUG:
        print("[DEBUG] Connected as partner:", PARTNER_ID)

    # Decide method based on which .env variables are populated (priority: ENTRY_IDS > CATEGORY_IDS > TAGS > OWNER)
    provided = {
        "entry_ids": bool(ENTRY_IDS),
        "category": bool(CATEGORY_IDS),
        "tag": bool(TAGS),
        "owner": bool(OWNER),
    }
    priority = ["entry_ids", "category", "tag", "owner"]
    method = next((m for m in priority if provided[m]), None)

    if not method:
        print("Error: No query inputs set. Populate one of ENTRY_IDS, CATEGORY_IDS, TAGS, or OWNER in your .env file.")
        return

    extras = [m for m in provided if provided[m] and m != method]
    if extras:
        print(f"Note: Multiple query inputs found in .env. Using '{method}' and ignoring: {', '.join(extras)}")

    identifier = {
        "entry_ids": ENTRY_IDS,
        "category": CATEGORY_IDS,
        "tag": TAGS,
        "owner": OWNER,
    }[method]

    if method == "category":
        scope = "including subcategories" if INCLUDE_CHILD_CATEGORIES else "this category only"
        print(f"Category search will target: {scope} (INCLUDE_CHILD_CATEGORIES={INCLUDE_CHILD_CATEGORIES})")

    entries = get_entries(client, method, identifier)
    print(f"{len(entries)} entries found.")
    if not entries:
        print("No entries found. Exiting.")
        return

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
