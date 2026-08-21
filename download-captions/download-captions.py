"""
download-captions.py
Downloads caption assets for Kaltura entries and (optionally) writes TXT
transcripts. In Kaltura, both captions and audio descriptions are caption
assets; INCLUDE_ASR_CAPTIONS, INCLUDE_NON_ASR_CAPTIONS, and
INCLUDE_AUDIO_DESCRIPTIONS in .env control which types are downloaded.

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

# Audio descriptions are distinguished from captions by the caption asset's
# `usage` field (KalturaCaptionAssetUsage). That enum — and the `usage` field
# itself — were added to the Python SDK in KalturaApiClient 22.0.0. On older
# SDKs the field is silently absent, so we feature-detect it here and fail
# loudly later only if the run actually depends on telling the two apart.
try:
    from KalturaClient.Plugins.Caption import KalturaCaptionAssetUsage
    AUDIO_DESCRIPTION_USAGE = getattr(
        KalturaCaptionAssetUsage, "EXTENDED_AUDIO_DESCRIPTION", "1"
    )
    SDK_SUPPORTS_USAGE = True
except ImportError:
    AUDIO_DESCRIPTION_USAGE = "1"
    SDK_SUPPORTS_USAGE = False

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

# OUTPUT_FORMAT controls what gets saved FOR CAPTIONS: 'srt' (default —
# original file only), 'txt' (converted transcript only), or 'both'. It does
# NOT affect audio descriptions, which are always kept in their original
# format and never converted to TXT (stripping their timecodes would make the
# text meaningless). Falls back to legacy CONVERT_TO_TXT if OUTPUT_FORMAT is
# not set.
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

# When true, and the query uses multiple search terms (CATEGORY_IDS,
# CATEGORY_NAMES, TAGS, or OWNER — NOT ENTRY_IDS), each term's results go into
# its own subfolder of the run folder, named after the term as entered:
#   output/<timestamp>/<term>/...
# ENTRY_IDS are always kept flat in the run folder (there's no meaningful
# per-term grouping for a bare list of entries). When false (default), all
# results land flat in the run folder regardless of how many terms were used.
SUBFOLDER_PER_SEARCH_TERM = _env_bool("SUBFOLDER_PER_SEARCH_TERM", "false")

# ---------- Which caption-asset types to download ----------
# In Kaltura, captions AND audio descriptions are both "caption assets". We
# sort each asset into one of three buckets and download only the ones whose
# toggle is true:
#   1. Audio descriptions — identified by the asset's `usage` field (the
#      KalturaCaptionAssetUsage enum: 1 = audio description). This is the
#      reliable discriminator; label text is not.
#   2. ASR (machine) captions — a caption whose label contains
#      AUTO_GENERATED_LABEL (see below).
#   3. Non-ASR captions — every other caption. "Non-ASR" only means "not
#      labeled as machine-generated"; it does NOT assert the captions are
#      accurate or human-made.
# At least one of the three must be true (enforced below).
INCLUDE_ASR_CAPTIONS = _env_bool("INCLUDE_ASR_CAPTIONS", "true")
INCLUDE_NON_ASR_CAPTIONS = _env_bool("INCLUDE_NON_ASR_CAPTIONS", "true")
INCLUDE_AUDIO_DESCRIPTIONS = _env_bool("INCLUDE_AUDIO_DESCRIPTIONS", "false")

# The suffix KMC appends to the label of ASR (machine) captions — set in KMC
# under Settings > Reach > Service Parameters ("machine captions label
# suffix"). AUTO_GENERATED_LABEL is just that suffix (e.g. "(auto-generated)"),
# NOT a full track label, and it is matched as a case-insensitive substring —
# so it catches the suffix in every language (e.g. "English (auto-generated)",
# "Spanish (auto-generated)").
AUTO_GENERATED_LABEL = os.getenv(
    "AUTO_GENERATED_LABEL", "(auto-generated)"
).strip()

if not (
    INCLUDE_ASR_CAPTIONS
    or INCLUDE_NON_ASR_CAPTIONS
    or INCLUDE_AUDIO_DESCRIPTIONS
):
    raise SystemExit(
        "Error: at least one of INCLUDE_ASR_CAPTIONS, "
        "INCLUDE_NON_ASR_CAPTIONS, or INCLUDE_AUDIO_DESCRIPTIONS must be "
        "true in .env — otherwise there is nothing to download."
    )

# Telling audio descriptions apart from other captions requires the caption
# asset `usage` field, added to the SDK in KalturaApiClient 22.0.0. If the SDK
# is too old AND the run needs that distinction (audio descriptions treated
# differently from non-ASR captions), stop with a clear, actionable error
# rather than silently misclassifying audio descriptions as plain captions.
if not SDK_SUPPORTS_USAGE and (
    INCLUDE_AUDIO_DESCRIPTIONS != INCLUDE_NON_ASR_CAPTIONS
):
    raise SystemExit(
        "Error: your installed KalturaApiClient is too old to identify audio "
        "descriptions (it lacks the caption-asset 'usage' field, added in "
        "22.0.0), so the script cannot separate audio descriptions from other "
        "captions. Upgrade it with:\n"
        "    pip install -U KalturaApiClient\n"
        "(or re-run 'pip install -r requirements.txt'). Alternatively, set "
        "INCLUDE_AUDIO_DESCRIPTIONS and INCLUDE_NON_ASR_CAPTIONS to the same "
        "value so the distinction isn't needed."
    )

# AUTO_GENERATED_LABEL is only needed to tell ASR captions apart from non-ASR
# captions, i.e. when the two are set differently. If both are treated the
# same, the label is irrelevant.
if INCLUDE_ASR_CAPTIONS != INCLUDE_NON_ASR_CAPTIONS and not AUTO_GENERATED_LABEL:
    raise SystemExit(
        "Error: INCLUDE_ASR_CAPTIONS and INCLUDE_NON_ASR_CAPTIONS differ, so "
        "AUTO_GENERATED_LABEL is needed to tell the two apart, but it is "
        "empty. Set it to the suffix KMC appends to machine-caption labels "
        "(e.g. \"(auto-generated)\")."
    )

# Query inputs (priority: ENTRY_IDS > CATEGORY_IDS/CATEGORY_NAMES > TAGS > OWNER)
CATEGORY_IDS = os.getenv("CATEGORY_IDS", "").strip()
# CATEGORY_NAMES is a convenience alternative to CATEGORY_IDS: enter category
# names and the script resolves each to its ID (see resolve_category_names).
# Because category names are not guaranteed unique in Kaltura, an ambiguous
# name stops the script with the candidates listed. Resolved IDs are combined
# with any CATEGORY_IDS you also set.
CATEGORY_NAMES = os.getenv("CATEGORY_NAMES", "").strip()
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
    try:
        ks = call_with_retry(
            client.session.start,
            admin_secret,
            KALTURA_USER,
            KalturaSessionType.ADMIN,
            partner_id,
            privileges="all:*,disableentitlement",
        )
    except Exception as e:
        if getattr(e, "code", "") == "START_SESSION_ERROR":
            print(
                "\n❌ Could not log in to Kaltura. Partner ID "
                f"[{partner_id}] and the Admin Secret were not accepted.\n"
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
    client.setKs(ks)
    return client


def sanitize_filename(name: str, max_length: int = 100) -> str:
    name = re.sub(r"[^a-zA-Z0-9_-]", "_", name)
    return name[:max_length]


def resolve_category_names(client: KalturaClient, names_str: str) -> List[str]:
    """Resolve comma-delimited category NAMES to a list of category ID strings.

    Uses a freeText search (like KMC's search box) to find candidates, then
    requires an exact, case-sensitive name match. Category names are NOT
    guaranteed unique in Kaltura, so:
      - No exact match  → stop with an error (typo, or wrong capitalization).
      - Exactly one     → resolve to its ID.
      - More than one   → the name is ambiguous; stop and list the candidates
                          (ID + full path) so the user can put the specific
                          ID in CATEGORY_IDS instead.
    This script is non-interactive, so an ambiguous name halts rather than
    prompting.
    """
    names = [n.strip() for n in names_str.split(",") if n.strip()]
    resolved_ids: List[str] = []

    for name in names:
        cat_filter = KalturaCategoryFilter()
        cat_filter.freeText = name
        pager = KalturaFilterPager()
        pager.pageSize = 500
        pager.pageIndex = 1

        candidates = []
        while True:
            result = call_with_retry(client.category.list, cat_filter, pager)
            if not getattr(result, "objects", None):
                break
            candidates.extend(result.objects)
            if len(result.objects) < pager.pageSize:
                break
            pager.pageIndex += 1

        # freeText matches loosely (tokens/substrings); require an exact name.
        matches = [c for c in candidates if c.name == name]

        if not matches:
            raise SystemExit(
                f"Error: no category found with the exact name '{name}'. "
                "Check spelling and capitalization (matching is "
                "case-sensitive), or use CATEGORY_IDS instead."
            )

        if len(matches) > 1:
            print(
                f"Error: the category name '{name}' is not unique — "
                f"{len(matches)} categories share it:"
            )
            for c in matches:
                print(f"    ID {c.id}  —  {c.fullName}")
            raise SystemExit(
                "Category names must be unambiguous. Put the specific ID "
                "from the list above into CATEGORY_IDS (instead of using "
                f"CATEGORY_NAMES for '{name}')."
            )

        cat = matches[0]
        print(f"Resolved category name '{name}' → {cat.fullName} (ID {cat.id})")
        resolved_ids.append(str(cat.id))

    return resolved_ids


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


# Human-readable names for the three caption-asset buckets.
_KIND_LABELS = {
    "audio_description": "audio description",
    "asr": "auto-generated caption",
    "non_asr": "caption",
}

# The INCLUDE_* variable that would enable each bucket (for the end-of-run hint).
_KIND_INCLUDE_VAR = {
    "audio_description": "INCLUDE_AUDIO_DESCRIPTIONS",
    "asr": "INCLUDE_ASR_CAPTIONS",
    "non_asr": "INCLUDE_NON_ASR_CAPTIONS",
}

# Running tally of caption assets skipped because their type was excluded by the
# INCLUDE_* settings. Used to explain an empty run in the end-of-run summary.
SKIPPED_BY_KIND = {"audio_description": 0, "asr": 0, "non_asr": 0}


def _usage_value(cap) -> str:
    """Return a caption asset's `usage` as a plain string ("0"/"1"/…), or ""
    if the SDK doesn't expose it. In the SDK, `usage` is a
    KalturaCaptionAssetUsage enum object whose value is read via .getValue();
    older SDKs (< 22.0.0) omit the field entirely, so getattr returns None.
    """
    raw = getattr(cap, "usage", None)
    if raw is None:
        return ""
    if hasattr(raw, "getValue"):
        raw = raw.getValue()
    return str(raw).strip() if raw is not None else ""


def classify_caption_asset(cap) -> str:
    """Sort a caption asset into 'audio_description', 'asr', or 'non_asr'.

    Audio descriptions are identified by the `usage` field (the
    KalturaCaptionAssetUsage enum: "1" = audio description), which is more
    reliable than the label. Only exactly that usage value is treated as an
    audio description; everything else is a caption, so unknown/new usage
    values (and SDKs too old to report usage) fail safe as captions. A caption
    is then 'asr' if its label contains AUTO_GENERATED_LABEL, else 'non_asr'.
    """
    if _usage_value(cap) == AUDIO_DESCRIPTION_USAGE:
        return "audio_description"
    label = getattr(cap, "label", "") or ""
    if AUTO_GENERATED_LABEL and AUTO_GENERATED_LABEL.lower() in label.lower():
        return "asr"
    return "non_asr"


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

    # Keep only the asset types whose toggle is true.
    include = {
        "audio_description": INCLUDE_AUDIO_DESCRIPTIONS,
        "asr": INCLUDE_ASR_CAPTIONS,
        "non_asr": INCLUDE_NON_ASR_CAPTIONS,
    }
    kept = []
    for cap in captions:
        kind = classify_caption_asset(cap)
        if include[kind]:
            kept.append(cap)
        else:
            SKIPPED_BY_KIND[kind] += 1
            print(
                f"  Skipping {_KIND_LABELS[kind]} "
                f"'{cap.label}' for entry {entry_id}"
            )
    return kept


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


def download_captions(client: KalturaClient, captions, entry, counter, out_dir):
    os.makedirs(out_dir, exist_ok=True)
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
            is_audio_description = (
                classify_caption_asset(cap) == "audio_description"
            )

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

            out_path = os.path.join(out_dir, base_name + ext)
            # If several tracks on one entry would map to the same name
            # (e.g. the label is omitted), add a numeric suffix so none
            # overwrite each other.
            if os.path.exists(out_path):
                n = 2
                while os.path.exists(
                    os.path.join(out_dir, f"{base_name}_{n}{ext}")
                ):
                    n += 1
                base_name = f"{base_name}_{n}"
                out_path = os.path.join(out_dir, base_name + ext)

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

                # OUTPUT_FORMAT (srt/txt/both) governs CAPTIONS only. TXT
                # conversion strips timecodes to make a readable transcript —
                # meaningful for captions, but not for audio descriptions,
                # where the timing IS the content. So audio descriptions are
                # always kept in their original format and never converted.
                do_txt = SAVE_TXT and not is_audio_description
                keep_original = SAVE_SRT or is_audio_description

                if do_txt:
                    txt_path = convert_caption_to_txt(out_path, ext)
                    if txt_path:
                        print(f"      Converted to TXT:\t{txt_path}")
                    else:
                        print(
                            "      Warning:\tconversion failed"
                            f" for {out_path}"
                        )
                elif SAVE_TXT and is_audio_description:
                    print(
                        "      Kept audio description in original format"
                        " (not converted to TXT)"
                    )

                if not keep_original and os.path.exists(out_path):
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


def process_entries(client, entries, counter, out_dir):
    """Download captions for a list of entries into out_dir, honoring the
    child-entry skip. `counter` is a one-element list so numbering continues
    across multiple calls (e.g. one per search-term subfolder)."""
    for entry in entries:
        if SKIP_CHILD_ENTRIES and _is_child_entry(entry):
            print(f"Skipping child entry {entry.id} (parent-linked)")
            continue
        caps = get_captions(client, entry.id)
        if caps:
            download_captions(client, caps, entry, counter, out_dir)
        else:
            print(f"No captions found for entry {entry.id}")


def build_jobs(client, method):
    """Return a list of (out_dir, method, identifier) jobs to run.

    With SUBFOLDER_PER_SEARCH_TERM off (or for ENTRY_IDS, which is always
    flat), this is a single job writing to the run folder. With it on, each
    search term becomes its own job writing to output/<timestamp>/<term>/.
    For CATEGORY_NAMES, the subfolder is named after the name as entered (e.g.
    the Canvas course ID), while the identifier passed on is the resolved
    Kaltura category ID.
    """
    per_term = SUBFOLDER_PER_SEARCH_TERM and method != "entry_ids"

    def term_dir(term):
        return os.path.join(RUN_OUTPUT_DIR, sanitize_filename(term))

    if method == "category":
        id_terms = [c.strip() for c in CATEGORY_IDS.split(",") if c.strip()]
        name_terms = [n.strip() for n in CATEGORY_NAMES.split(",") if n.strip()]
        resolved = (
            resolve_category_names(client, CATEGORY_NAMES) if name_terms else []
        )
        if per_term:
            jobs = [(term_dir(cid), "category", cid) for cid in id_terms]
            jobs += [
                (term_dir(name), "category", rid)
                for name, rid in zip(name_terms, resolved)
            ]
            return jobs
        # Flat: combine explicit IDs + resolved IDs, deduped, one job.
        seen = set()
        combined = [
            c for c in (id_terms + resolved) if not (c in seen or seen.add(c))
        ]
        return [(RUN_OUTPUT_DIR, "category", ",".join(combined))]

    identifier = {"entry_ids": ENTRY_IDS, "tag": TAGS, "owner": OWNER}[method]
    if per_term:
        terms = [t.strip() for t in identifier.split(",") if t.strip()]
        return [(term_dir(t), method, t) for t in terms]
    return [(RUN_OUTPUT_DIR, method, identifier)]


def main():
    print("▶ download-captions: starting…")
    if DEBUG:
        print("[DEBUG] Using .env from:", Path(__file__).with_name(".env"))
        print("[DEBUG] PARTNER_ID set:", bool(PARTNER_ID))
        print("[DEBUG] KALTURA_SERVICE_URL:", SERVICE_URL)
        print("[DEBUG] RUN_OUTPUT_DIR:", RUN_OUTPUT_DIR)
        print("[DEBUG] OUTPUT_FORMAT:", OUTPUT_FORMAT)
        print("[DEBUG] INCLUDE_ASR_CAPTIONS:", INCLUDE_ASR_CAPTIONS)
        print("[DEBUG] INCLUDE_NON_ASR_CAPTIONS:", INCLUDE_NON_ASR_CAPTIONS)
        print("[DEBUG] INCLUDE_AUDIO_DESCRIPTIONS:", INCLUDE_AUDIO_DESCRIPTIONS)
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
        print("[DEBUG] SUBFOLDER_PER_SEARCH_TERM:", SUBFOLDER_PER_SEARCH_TERM)
        print("[DEBUG] ENTRY_IDS:", ENTRY_IDS)
        print("[DEBUG] CATEGORY_IDS:", CATEGORY_IDS)
        print("[DEBUG] CATEGORY_NAMES:", CATEGORY_NAMES)
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

    # Decide method based on which .env variables are populated. The category
    # method is chosen if CATEGORY_IDS and/or CATEGORY_NAMES is set.
    # (priority: ENTRY_IDS > CATEGORY_IDS/CATEGORY_NAMES > TAGS > OWNER)
    provided = {
        "entry_ids": bool(ENTRY_IDS),
        "category": bool(CATEGORY_IDS or CATEGORY_NAMES),
        "tag": bool(TAGS),
        "owner": bool(OWNER),
    }
    priority = ["entry_ids", "category", "tag", "owner"]
    method = next((m for m in priority if provided[m]), None)

    if not method:
        print(
            "Error: No query inputs set. Populate one of ENTRY_IDS,"
            " CATEGORY_IDS, CATEGORY_NAMES, TAGS, or OWNER in your .env file."
        )
        return

    extras = [m for m in provided if provided[m] and m != method]
    if extras:
        print(
            f"Note: Multiple query inputs found in .env."
            f" Using '{method}' and ignoring: {', '.join(extras)}"
        )

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

    # Build the download jobs (one per search-term subfolder, or a single flat
    # job) and process each. `resolve_category_names` inside build_jobs may
    # stop the script if a category name is ambiguous.
    jobs = build_jobs(client, method)
    per_term = len(jobs) > 1 or (
        SUBFOLDER_PER_SEARCH_TERM and method != "entry_ids"
    )
    print(f"Saving captions under: {RUN_OUTPUT_DIR}/")

    counter = [1]
    total_entries = 0
    for out_dir, job_method, identifier in jobs:
        if per_term:
            print(f"\n— Search term → {os.path.relpath(out_dir)}/")
        entries = get_entries(client, job_method, identifier)
        print(f"{len(entries)} entries found.")
        total_entries += len(entries)
        if not entries:
            continue
        process_entries(client, entries, counter, out_dir)

    if total_entries == 0:
        print("No entries found. Exiting.")
        return

    downloaded = counter[0] - 1
    print(
        f"\nCaption download complete. Scanned {total_entries} "
        f"entr{'y' if total_entries == 1 else 'ies'}; downloaded "
        f"{downloaded} caption asset{'' if downloaded == 1 else 's'}."
    )

    # If nothing downloaded but assets existed and were excluded by type, say
    # so and point at the toggle that would include them — an empty output
    # folder otherwise looks like a failure.
    excluded = sum(SKIPPED_BY_KIND.values())
    if downloaded == 0 and excluded:
        print(
            f"\nNote: {excluded} caption asset(s) were found but excluded by "
            "your caption-type settings in .env:"
        )
        for kind, n in SKIPPED_BY_KIND.items():
            if n:
                print(
                    f"  • {n} {_KIND_LABELS[kind]}(s) — "
                    f"set {_KIND_INCLUDE_VAR[kind]}=true to include them."
                )


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("✖ Unhandled error:", e)
        traceback.print_exc()
