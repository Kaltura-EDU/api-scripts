"""
Kaltura Search & Report

Queries Kaltura media entries using filters defined in .env and exports
results to CSV. Filter variables support comma-separated OR values; multiple
filter variables filled in are combined with AND.

Automatically re-chunks queries by progressively smaller time intervals if
Kaltura's 10,000-entry API cap is encountered, then fetches every page in
parallel across MAX_WORKERS threads (each with its own Kaltura session).

The admin secret is always prompted at runtime and never read from or
written to disk.
"""

import csv
import getpass
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, date, timedelta, time as dt_time
from os import getenv, makedirs
from os.path import join as path_join

import pytz
import requests
from dotenv import find_dotenv, load_dotenv
from KalturaClient import KalturaClient, KalturaConfiguration
from KalturaClient.Plugins.Core import (
    KalturaEntryModerationStatus,
    KalturaEntryStatus,
    KalturaFilterPager,
    KalturaMediaEntryFilter,
    KalturaMediaEntryOrderBy,
    KalturaMediaType,
    KalturaSessionType,
)
from KalturaClient.exceptions import (
    KalturaClientException,
    KalturaException,
)

load_dotenv(find_dotenv())


# ── Env helpers ───────────────────────────────────────────────────────
def require_env(key):
    val = getenv(key, "").strip()
    if not val:
        print(f"[ERROR] Missing or empty {key} in .env", file=sys.stderr)
        sys.exit(2)
    return val


def env_list(key):
    """Parse a comma-separated env var into stripped non-empty strings."""
    raw = getenv(key, "").strip()
    return [v.strip() for v in raw.split(",") if v.strip()] if raw else []


def env_val(key):
    val = getenv(key, "").strip()
    return val or None


# ── Credentials ───────────────────────────────────────────────────────
PARTNER_ID = int(require_env("PARTNER_ID"))
ADMIN_SECRET = ""  # never read from .env — set in main() via getpass
USER_ID = env_val("USER_ID")
SERVICE_URL = getenv("SERVICE_URL", "https://www.kaltura.com").rstrip("/")
PRIVILEGES = getenv("PRIVILEGES", "all:*,disableentitlement")


# ── Reliability ───────────────────────────────────────────────────────
# Seconds before an individual API request times out.
REQUEST_TIMEOUT = int(getenv("REQUEST_TIMEOUT", "120"))
# Retries for transient network errors before giving up.
MAX_NETWORK_RETRIES = int(getenv("MAX_NETWORK_RETRIES", "5"))
# Base seconds between retries (grows linearly: delay × attempt).
NETWORK_RETRY_DELAY = int(getenv("NETWORK_RETRY_DELAY", "5"))
# Pages fetched concurrently, each thread on its own Kaltura session.
# 1 = one page at a time. If Kaltura throttles or times out under load,
# lower this rather than raising MAX_NETWORK_RETRIES.
MAX_WORKERS = max(1, int(getenv("MAX_WORKERS", "10")))


# ── Output ────────────────────────────────────────────────────────────
EXPORT_CSV = getenv("EXPORT_CSV", "True").strip().lower() in (
    "true", "1", "yes",
)
# Subfolder the CSV is written into (created automatically if missing).
OUTPUT_DIR = getenv("OUTPUT_DIR", "output")
TIMEZONE = getenv("TIMEZONE", "US/Pacific")
# The earliest date your Kaltura repository contains entries (YYYY-MM-DD).
# Used as a fallback start date for auto-chunking when CREATED_AFTER is
# not set.
EARLIEST_START_DATE = getenv("EARLIEST_START_DATE", "2000-01-01")

local_tz = pytz.timezone(TIMEZONE)


# ── Search filter settings ────────────────────────────────────────────
# When True (default), name filters are pre-narrowed at the API with a
# nameMultiLikeOr query (fast, but relies on Kaltura's search index). Set
# False to skip that pre-filter and match names purely client-side — slower
# (scans the full date range) but exhaustive, useful if the index-based
# pre-filter misses matches on your account.
NAME_PREFILTER = getenv("NAME_PREFILTER", "True").strip().lower() in (
    "true", "1", "yes",
)
# One or more Kaltura entry IDs (e.g., 0_abc123). Comma = OR.
ENTRY_ID = env_list("ENTRY_ID")
# External/reference ID assigned to the entry outside of Kaltura (e.g.,
# from a CMS).
REFERENCE_ID = env_list("REFERENCE_ID")
# Full-text search across name, description, and tags
SEARCH_TEXT = env_list("SEARCH_TEXT")

ENTRY_NAME_EQUALS = env_list("ENTRY_NAME_EQUALS")
# Match entries whose name starts with any of these terms
ENTRY_NAME_BEGINS_WITH = env_list("ENTRY_NAME_BEGINS_WITH")
ENTRY_NAME_CONTAINS = env_list("ENTRY_NAME_CONTAINS")
ENTRY_NAME_ENDS_WITH = env_list("ENTRY_NAME_ENDS_WITH")
# Exclude entries whose name contains any of these terms
ENTRY_NAME_NOT_CONTAINS = env_list("ENTRY_NAME_NOT_CONTAINS")

# Kaltura user ID of the entry owner (the person who uploaded it)
OWNER_ID = env_list("OWNER_ID")
# Match entries whose owner ID starts / ends with any of these terms.
# Kaltura has no prefix/suffix filter on owner, so these are applied
# client-side only (case-insensitive) and don't reduce API calls.
OWNER_STARTS_WITH = env_list("OWNER_STARTS_WITH")
OWNER_ENDS_WITH = env_list("OWNER_ENDS_WITH")
TAG = env_list("TAG")
# Numeric Kaltura category IDs (KMC: Content > Categories)
CATEGORY_ID = env_list("CATEGORY_ID")
# Full category path in Kaltura (e.g., MediaSpace>site>channels>mychannel)
CATEGORY_NAME = env_list("CATEGORY_NAME")
# Options: VIDEO, AUDIO, IMAGE, LIVE_STREAM_FLASH,
#          LIVE_STREAM_WINDOWS_MEDIA, LIVE_STREAM_REAL_MEDIA,
#          LIVE_STREAM_QUICKTIME
MEDIA_TYPE_FILTER = env_list("MEDIA_TYPE")
# Entry lifecycle status. If blank, Kaltura returns only READY entries.
# Options: READY, PENDING, DELETED, BLOCKED, MODERATE, NO_CONTENT,
#          ERROR_CONVERTING, ERROR_IMPORTING, IMPORT, PRECONVERT
STATUS_FILTER = env_list("STATUS")
# Options: PENDING_MODERATION, APPROVED, REJECTED, FLAGGED_FOR_REVIEW,
#          AUTO_APPROVED, DELETED
MODERATION_STATUS_FILTER = env_list("MODERATION_STATUS")

# Match entries created on a single day (YYYY-MM-DD). Shorthand for setting
# CREATED_AFTER and CREATED_BEFORE to the same date; mutually exclusive with
# them.
CREATED_ON = env_val("CREATED_ON")
CREATED_AFTER = env_val("CREATED_AFTER")
CREATED_BEFORE = env_val("CREATED_BEFORE")
# Filter by when entries were last modified
UPDATED_AFTER = env_val("UPDATED_AFTER")
UPDATED_BEFORE = env_val("UPDATED_BEFORE")
# Duration bounds in seconds (e.g., 60 = 1 minute)
DURATION_MIN_SEC = env_val("DURATION_MIN_SEC")
DURATION_MAX_SEC = env_val("DURATION_MAX_SEC")


# ── Enum lookup tables ────────────────────────────────────────────────
def _enum_map(cls):
    return {k: v for k, v in vars(cls).items() if k.isupper()}


MEDIA_TYPE_MAP = _enum_map(KalturaMediaType)
STATUS_MAP = _enum_map(KalturaEntryStatus)
MOD_STATUS_MAP = _enum_map(KalturaEntryModerationStatus)

MEDIA_TYPE_LABEL = {v: k for k, v in MEDIA_TYPE_MAP.items()}
STATUS_LABEL = {v: k for k, v in STATUS_MAP.items()}
MOD_STATUS_LABEL = {v: k for k, v in MOD_STATUS_MAP.items()}


# ── Startup validation ────────────────────────────────────────────────
def _parse_date(s):
    return datetime.strptime(s, "%Y-%m-%d").date()


for _var, _val in [
    ("EARLIEST_START_DATE", EARLIEST_START_DATE),
    ("CREATED_ON", CREATED_ON),
    ("CREATED_AFTER", CREATED_AFTER),
    ("CREATED_BEFORE", CREATED_BEFORE),
    ("UPDATED_AFTER", UPDATED_AFTER),
    ("UPDATED_BEFORE", UPDATED_BEFORE),
]:
    if _val:
        try:
            _parse_date(_val)
        except ValueError:
            raise ValueError(
                f"{_var} must be YYYY-MM-DD, got: {_val!r}"
            )

if CREATED_ON and (CREATED_AFTER or CREATED_BEFORE):
    raise ValueError(
        "CREATED_ON is mutually exclusive with CREATED_AFTER/CREATED_BEFORE."
    )

if CREATED_AFTER and CREATED_BEFORE:
    if _parse_date(CREATED_BEFORE) < _parse_date(CREATED_AFTER):
        raise ValueError(
            "CREATED_BEFORE cannot be earlier than CREATED_AFTER."
        )

for _t in MEDIA_TYPE_FILTER:
    if _t.upper() not in MEDIA_TYPE_MAP:
        raise ValueError(
            f"Unknown MEDIA_TYPE: {_t!r}. "
            f"Valid options: {', '.join(MEDIA_TYPE_MAP)}"
        )
for _s in STATUS_FILTER:
    if _s.upper() not in STATUS_MAP:
        raise ValueError(
            f"Unknown STATUS: {_s!r}. "
            f"Valid options: {', '.join(STATUS_MAP)}"
        )
for _m in MODERATION_STATUS_FILTER:
    if _m.upper() not in MOD_STATUS_MAP:
        raise ValueError(
            f"Unknown MODERATION_STATUS: {_m!r}. "
            f"Valid options: {', '.join(MOD_STATUS_MAP)}"
        )


# ── Network retry ─────────────────────────────────────────────────────
def call_with_retry(fn, *args, **kwargs):
    """Call fn(*args, **kwargs), retrying with linear backoff on transient
    network failures (timeouts, connection resets). Kaltura raises those as
    KalturaClientException, which is NOT a subclass of KalturaException, so
    it is caught here separately; requests' own errors are covered too. A
    real, well-formed API error (KalturaException) is never retried here —
    callers handle those (e.g. the 10K-match cap)."""
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


# ── Kaltura session ───────────────────────────────────────────────────
_thread_local = threading.local()


def get_client():
    """Return this thread's Kaltura client, starting an admin session the
    first time each thread calls it. A single KalturaClient is not safe to
    share across concurrent calls, so every worker thread gets its own.
    Relies on the module-level ADMIN_SECRET, which main() sets from a
    getpass prompt."""
    if not hasattr(_thread_local, "client"):
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
                    "   copied exactly from KMC → Settings → Integration "
                    "Settings.\n"
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
        _thread_local.client = c
    return _thread_local.client


# ── Date / timestamp helpers ──────────────────────────────────────────
def to_start_ts(d):
    return int(datetime.combine(d, dt_time.min).timestamp())


def to_end_ts(d):
    return int(datetime.combine(d, dt_time.max).timestamp())


def fmt_ts(ts):
    if not ts:  # covers None and 0 (0 = never played / epoch sentinel)
        return ""
    return (
        datetime.fromtimestamp(ts, tz=pytz.utc)
        .astimezone(local_tz)
        .strftime("%Y-%m-%d %H:%M:%S")
    )


# ── Filter builder ────────────────────────────────────────────────────
def build_filter(created_start_ts=None, created_end_ts=None):
    f = KalturaMediaEntryFilter()
    # Oldest first keeps page contents stable while pages are fetched in
    # parallel: an entry added mid-run lands after the last page instead of
    # shifting every page down by one. Output is re-sorted newest first.
    f.orderBy = KalturaMediaEntryOrderBy.CREATED_AT_ASC

    if ENTRY_ID:
        f.idIn = ",".join(ENTRY_ID)
    if REFERENCE_ID:
        f.referenceIdIn = ",".join(REFERENCE_ID)
    if SEARCH_TEXT:
        # searchTextMatchOr treats each space-separated term as its own
        # OR condition.
        f.searchTextMatchOr = " ".join(SEARCH_TEXT)

    # nameMultiLikeOr is a word-token search on Kaltura's index, NOT a raw
    # substring match: the bare term "daw" only matches entries containing
    # the whole word "daw", not "Dawson". So BEGINS_WITH terms get a
    # trailing "*" wildcard ("daw*") to trigger prefix matching on the
    # index. EQUALS/CONTAINS terms are passed as-is (they target whole
    # words). This is only an approximation pre-filter — exact semantics are
    # enforced client-side in passes_client_filters(). Set NAME_PREFILTER
    # False to skip this entirely and rely on client-side matching alone.
    if NAME_PREFILTER:
        name_prefilter = (
            ENTRY_NAME_EQUALS
            + [t + "*" for t in ENTRY_NAME_BEGINS_WITH]
            + ENTRY_NAME_CONTAINS
        )
        if name_prefilter:
            f.nameMultiLikeOr = ",".join(name_prefilter)

    if OWNER_ID:
        f.userIdIn = ",".join(OWNER_ID)
    if TAG:
        f.tagsMultiLikeOr = ",".join(TAG)
    if CATEGORY_ID:
        f.categoriesIdsMatchOr = ",".join(CATEGORY_ID)
    if CATEGORY_NAME:
        # categoriesFullNameIn matches entries in any of these full paths
        f.categoriesFullNameIn = ",".join(CATEGORY_NAME)

    if MEDIA_TYPE_FILTER and len(MEDIA_TYPE_FILTER) == 1:
        f.mediaTypeEqual = KalturaMediaType(
            MEDIA_TYPE_MAP[MEDIA_TYPE_FILTER[0].upper()]
        )
    # Multiple MEDIA_TYPE values are handled client-side (no mediaTypeIn
    # on this filter).

    if STATUS_FILTER:
        f.statusIn = ",".join(
            STATUS_MAP[s.upper()] for s in STATUS_FILTER
        )
    if MODERATION_STATUS_FILTER:
        f.moderationStatusIn = ",".join(
            str(MOD_STATUS_MAP[m.upper()])
            for m in MODERATION_STATUS_FILTER
        )

    if UPDATED_AFTER:
        f.updatedAtGreaterThanOrEqual = to_start_ts(
            _parse_date(UPDATED_AFTER)
        )
    if UPDATED_BEFORE:
        f.updatedAtLessThanOrEqual = to_end_ts(
            _parse_date(UPDATED_BEFORE)
        )
    if DURATION_MIN_SEC:
        f.durationGreaterThanOrEqual = int(DURATION_MIN_SEC)
    if DURATION_MAX_SEC:
        f.durationLessThanOrEqual = int(DURATION_MAX_SEC)

    if created_start_ts is not None:
        f.createdAtGreaterThanOrEqual = created_start_ts
    if created_end_ts is not None:
        f.createdAtLessThanOrEqual = created_end_ts

    return f


# ── Client-side post-filters ──────────────────────────────────────────
def passes_client_filters(entry):
    """
    Apply filters that have no direct API equivalent, plus enforce exact
    semantics for name filters that were approximated at the API level.
    """
    name = (entry.name or "").lower()

    if ENTRY_NAME_EQUALS:
        if not any(name == v.lower() for v in ENTRY_NAME_EQUALS):
            return False

    if ENTRY_NAME_BEGINS_WITH:
        if not any(
            name.startswith(v.lower()) for v in ENTRY_NAME_BEGINS_WITH
        ):
            return False

    if ENTRY_NAME_CONTAINS:
        if not any(v.lower() in name for v in ENTRY_NAME_CONTAINS):
            return False

    if ENTRY_NAME_ENDS_WITH:
        if not any(name.endswith(v.lower()) for v in ENTRY_NAME_ENDS_WITH):
            return False

    if ENTRY_NAME_NOT_CONTAINS:
        if any(v.lower() in name for v in ENTRY_NAME_NOT_CONTAINS):
            return False

    owner = (entry.userId or "").lower()

    if OWNER_STARTS_WITH:
        if not any(owner.startswith(v.lower()) for v in OWNER_STARTS_WITH):
            return False

    if OWNER_ENDS_WITH:
        if not any(owner.endswith(v.lower()) for v in OWNER_ENDS_WITH):
            return False

    if MEDIA_TYPE_FILTER and len(MEDIA_TYPE_FILTER) > 1:
        entry_type = MEDIA_TYPE_LABEL.get(entry.mediaType.getValue(), "")
        if entry_type not in [m.upper() for m in MEDIA_TYPE_FILTER]:
            return False

    return True


# ── Planning: counts & auto-chunking ──────────────────────────────────
# Kaltura's maximum page size.
PAGE_SIZE = 500
# baseEntry.list can't page past ~10,000 matches. Ranges at or above this
# count are split into smaller date chunks before fetching; the margin
# below 10,000 leaves room for entries added between planning and fetching.
SPLIT_THRESHOLD = 9000


def count_entries(start, end):
    """Return how many entries in [start, end] match the filters, or None if
    the range is too large for a single query. One pageSize=1 request."""
    pager = KalturaFilterPager()
    pager.pageSize = 1
    pager.pageIndex = 1
    try:
        result = call_with_retry(
            get_client().media.list,
            build_filter(to_start_ts(start), to_end_ts(end)),
            pager,
        )
    except KalturaException as e:
        if e.code == "QUERY_EXCEEDED_MAX_MATCHES_ALLOWED":
            return None
        raise
    if result.totalCount >= SPLIT_THRESHOLD:
        return None
    return result.totalCount


def _date_chunks(start, end, interval):
    current = start
    while current <= end:
        if interval == "monthly":
            next_d = (
                current.replace(day=28) + timedelta(days=4)
            ).replace(day=1) - timedelta(days=1)
        elif interval == "weekly":
            next_d = current + timedelta(days=6)
        else:  # daily
            next_d = current
        yield current, min(next_d, end)
        current = next_d + timedelta(days=1)


_INTERVALS = ["monthly", "weekly", "daily"]


def plan_chunks(start, end, level=0):
    """Planning phase: return [(start, end, count), ...] covering every
    non-empty part of [start, end], each small enough for one query.
    Oversized ranges are split into monthly, then weekly, then daily chunks.
    Each probe is one tiny request, so this stays sequential; the expensive
    paging happens afterwards, in parallel."""
    count = count_entries(start, end)
    if count is not None:
        if not count:
            return []
        print(f"  {start} → {end}: {count:,} entries")
        return [(start, end, count)]

    if level >= len(_INTERVALS):
        raise RuntimeError(
            f"Even a single day has too many entries ({SPLIT_THRESHOLD:,}+) "
            f"on {start}. Apply more specific filters."
        )
    interval = _INTERVALS[level]
    print(
        f"  {start} → {end}: too many entries for one query, "
        f"splitting into {interval} chunks..."
    )
    chunks = []
    for s, e in _date_chunks(start, end, interval):
        chunks.extend(plan_chunks(s, e, level + 1))
    return chunks


# ── Parallel fetch ────────────────────────────────────────────────────
def fetch_page(start, end, page_index):
    """Fetch one page of matching entries in [start, end]. Runs in a worker
    thread, on that thread's own client."""
    pager = KalturaFilterPager()
    pager.pageSize = PAGE_SIZE
    pager.pageIndex = page_index
    result = call_with_retry(
        get_client().media.list,
        build_filter(to_start_ts(start), to_end_ts(end)),
        pager,
    )
    return result.objects or []


def fetch_all_entries(range_start, range_end):
    """Plan the date chunks, then fetch every page of every chunk across
    MAX_WORKERS threads. Workers only return their page; results are merged
    here on the main thread, so no locking is needed."""
    print(f"Searching: {range_start} to {range_end}")
    chunks = plan_chunks(range_start, range_end)
    pages = [
        (start, end, page_index)
        for start, end, count in chunks
        for page_index in range(1, (count + PAGE_SIZE - 1) // PAGE_SIZE + 1)
    ]
    if not pages:
        return []

    total = sum(count for _, _, count in chunks)
    print(
        f"\nFetching {total:,} entries: {len(pages)} page(s) from "
        f"{len(chunks)} date chunk(s), {min(MAX_WORKERS, len(pages))} "
        f"worker(s)..."
    )
    by_id = {}  # keyed by entry ID so a shifted page can't add duplicates
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(fetch_page, *p): p for p in pages}
        try:
            for done, future in enumerate(as_completed(futures), 1):
                start, end, page_index = futures[future]
                objects = future.result()
                print(
                    f"  [{done}/{len(pages)}] {start} → {end} "
                    f"page {page_index}: {len(objects)} entries"
                )
                for entry in objects:
                    by_id[entry.id] = entry
        except BaseException:
            # On an error or Ctrl+C, drop the queued pages instead of
            # waiting for the whole queue to finish first.
            pool.shutdown(cancel_futures=True)
            raise

    # Pages finish in any order; list newest first.
    return sorted(
        by_id.values(), key=lambda e: e.createdAt or 0, reverse=True
    )


# ── Entry → CSV row ───────────────────────────────────────────────────
def safe(entry, attr, default=""):
    val = getattr(entry, attr, default)
    return val if val is not None else default


def entry_to_row(entry):
    duration_sec = entry.duration or 0
    media_type_val = (
        entry.mediaType.getValue() if entry.mediaType else None
    )
    status_val = entry.status.getValue() if entry.status else None

    mod_raw = safe(entry, "moderationStatus")
    mod_val = (
        mod_raw.getValue() if hasattr(mod_raw, "getValue") else mod_raw
    )

    flavor_count = (
        len(entry.flavorParamsIds.split(","))
        if getattr(entry, "flavorParamsIds", None)
        else 0
    )

    return {
        "entry_id": entry.id,
        "name": entry.name or "",
        "description": safe(entry, "description"),
        "media_type": MEDIA_TYPE_LABEL.get(
            media_type_val, str(media_type_val)
        ),
        "status": STATUS_LABEL.get(status_val, str(status_val)),
        "moderation_status": (
            MOD_STATUS_LABEL.get(mod_val, str(mod_val))
            if mod_val != "" else ""
        ),
        "moderation_count": safe(entry, "moderationCount", 0),
        "duration_sec": duration_sec,
        "duration_min": round(duration_sec / 60, 4),
        "plays": safe(entry, "plays", 0),
        "views": safe(entry, "views", 0),
        "rank": safe(entry, "rank", ""),
        "total_rank": safe(entry, "totalRank", ""),
        "width": safe(entry, "width", ""),
        "height": safe(entry, "height", ""),
        "created_at": fmt_ts(entry.createdAt),
        "updated_at": fmt_ts(entry.updatedAt),
        "last_played_at": fmt_ts(safe(entry, "lastPlayedAt", None)),
        "owner_id": safe(entry, "userId"),
        "creator_id": safe(entry, "creatorId"),
        "categories": (
            safe(entry, "categories") or ""
        ).replace(",", ";"),
        "category_ids": safe(entry, "categoriesIds"),
        "tags": (safe(entry, "tags") or "").replace(",", ";"),
        "reference_id": safe(entry, "referenceId"),
        "access_control_id": safe(entry, "accessControlId"),
        "flavor_count": flavor_count,
        "partner_sort_value": safe(entry, "partnerSortValue"),
        "root_entry_id": safe(entry, "rootEntryId"),
        "parent_entry_id": safe(entry, "parentEntryId"),
        "display_in_search": safe(entry, "displayInSearch"),
        "thumbnail_url": safe(entry, "thumbnailUrl"),
    }


CSV_FIELDS = [
    "entry_id", "name", "description", "media_type", "status",
    "moderation_status", "moderation_count",
    "duration_sec", "duration_min",
    "plays", "views", "rank", "total_rank",
    "width", "height",
    "created_at", "updated_at", "last_played_at",
    "owner_id", "creator_id",
    "categories", "category_ids", "tags",
    "reference_id", "access_control_id",
    "flavor_count", "partner_sort_value",
    "root_entry_id", "parent_entry_id",
    "display_in_search", "thumbnail_url",
]


# ── Name-search probe (diagnostic) ────────────────────────────────────
def probe_name_search(term):
    """Try several Kaltura name-search strategies for `term` and report how
    many entries each returns, to reveal which one supports prefix matching
    on this account. Run:  python3 search-entries.py --probe-name daw"""
    strategies = [
        ("nameMultiLikeOr", term),
        ("nameMultiLikeOr", term + "*"),
        ("nameLike", term),
        ("nameLike", term + "*"),
        ("freeText", term),
        ("freeText", term + "*"),
    ]
    pager = KalturaFilterPager()
    pager.pageSize = 5
    pager.pageIndex = 1
    for attr, value in strategies:
        f = KalturaMediaEntryFilter()
        setattr(f, attr, value)
        try:
            result = call_with_retry(get_client().media.list, f, pager)
        except KalturaException as e:
            print(f"  {attr} = {value!r}: ERROR {e.code}")
            continue
        print(f"  {attr} = {value!r}: total={result.totalCount}")
        for obj in result.objects:
            print(f"        · {obj.name}")


# ── Main ──────────────────────────────────────────────────────────────
def main():
    global ADMIN_SECRET

    args = sys.argv[1:]

    ADMIN_SECRET = getpass.getpass("Enter your Kaltura admin secret: ")
    get_client()  # fails fast on bad credentials, before any workers start
    print("Session started OK.\n")

    if args and args[0] == "--probe-name":
        term = args[1] if len(args) > 1 else "daw"
        print(f"Probing name-search strategies for {term!r}:\n")
        probe_name_search(term)
        return

    if CREATED_ON:
        range_start = range_end = _parse_date(CREATED_ON)
    else:
        range_start = (
            _parse_date(CREATED_AFTER) if CREATED_AFTER
            else _parse_date(EARLIEST_START_DATE)
        )
        range_end = (
            _parse_date(CREATED_BEFORE) if CREATED_BEFORE else date.today()
        )

    raw_entries = fetch_all_entries(range_start, range_end)

    entries = [e for e in raw_entries if passes_client_filters(e)]
    if len(entries) < len(raw_entries):
        removed = len(raw_entries) - len(entries)
        print(f"Client-side filters removed {removed:,} entries.")

    rows = [entry_to_row(e) for e in entries]

    # ── Console summary ───────────────────────────────────────────────
    total_sec = sum(r["duration_sec"] for r in rows)
    total_min = total_sec / 60
    total_hours = total_min / 60

    print(f"\n{'─' * 40}")
    print(f"{'Entries:':<22}{len(rows):>15,}")
    print(f"{'Duration (min):':<22}{total_min:>15,.2f}")
    print(f"{'Duration (hours):':<22}{total_hours:>15,.2f}")

    # ── CSV export ────────────────────────────────────────────────────
    if EXPORT_CSV:
        makedirs(OUTPUT_DIR, exist_ok=True)
        timestamp = datetime.now().strftime("%Y-%m-%d-%H%M")
        filename = path_join(
            OUTPUT_DIR, f"{timestamp}_search_results.csv"
        )

        with open(filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            writer.writeheader()
            writer.writerows(rows)

        print(f"\nCSV written: {filename}")


if __name__ == "__main__":
    main()
