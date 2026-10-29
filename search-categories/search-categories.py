"""
Kaltura Category Search

Finds Kaltura categories (including MediaSpace channels and galleries) that
match the filters set in .env, prints them, and saves them to a CSV. It is a
friendlier front end for the category.list API call: no session.start, no
copying an admin secret into a web form.

Filter variables support comma-separated OR values; every filter variable you
fill in is combined with AND. Most filters are sent to Kaltura (fast); a few
that Kaltura cannot filter on are applied after the results come back.

The session uses the all:*,disableentitlement privileges, so MediaSpace
categories show up too. If a search matches more than Kaltura's 10,000-result
cap, it is split into smaller creation-date ranges automatically.

The admin secret is always prompted at runtime and never read from or
written to disk.
"""

import csv
import getpass
import sys
import time
import unicodedata
from datetime import date, datetime, time as dt_time
from itertools import product
from os import getenv, makedirs
from pathlib import Path

import pytz
import requests
from dotenv import load_dotenv
from KalturaClient import KalturaClient, KalturaConfiguration
from KalturaClient.Plugins.Core import (
    KalturaAppearInListType,
    KalturaCategoryFilter,
    KalturaCategoryOrderBy,
    KalturaCategoryStatus,
    KalturaCategoryUserPermissionLevel,
    KalturaContributionPolicyType,
    KalturaFilterPager,
    KalturaInheritanceType,
    KalturaNullableBoolean,
    KalturaPrivacyType,
    KalturaSessionType,
    KalturaUserJoinPolicyType,
)
from KalturaClient.exceptions import KalturaClientException

SCRIPT_DIR = Path(__file__).resolve().parent
load_dotenv(SCRIPT_DIR / ".env")


# ── Env helpers ───────────────────────────────────────────────────────
# Variables that had invisible characters removed (reported in main()).
CLEANED_VARS = []
# Settings that are filled in, as (name, value) pairs, for the recap in main().
ACTIVE_FILTERS = []
# Settings that shape the run but do not narrow the search.
NOT_FILTERS = {"PRINT_LIMIT", "EARLIEST_START_DATE", "MAX_RESULTS"}


def fail(message):
    print(f"[ERROR] {message}", file=sys.stderr)
    sys.exit(2)


def env_str(key):
    """Read an env var, dropping invisible format characters such as
    zero-width spaces (U+200B). They sneak in when IDs are copy-pasted from
    web pages or chat, and Python's strip() keeps them, so a line that
    looks blank (CATEGORY_ID=) would silently become a filter matching
    nothing."""
    raw = getenv(key, "")
    val = "".join(c for c in raw if unicodedata.category(c) != "Cf")
    if val != raw:
        CLEANED_VARS.append(key)
    return val.strip()


def _record(key, shown):
    ACTIVE_FILTERS.append((key, shown))


def env_list(key):
    """Comma-separated env var as a list of stripped, non-empty strings."""
    raw = env_str(key)
    items = [v.strip() for v in raw.split(",") if v.strip()] if raw else []
    if items:
        _record(key, ", ".join(items))
    return items


def env_val(key):
    val = env_str(key) or None
    if val:
        _record(key, val)
    return val


def env_bool(key, default=False):
    """True/False setting; blank means `default`."""
    raw = env_str(key).lower()
    if not raw:
        return default
    if raw in ("true", "1", "yes"):
        return True
    if raw in ("false", "0", "no"):
        return False
    fail(f"{key} must be True or False, got: {raw!r}")


def env_int(key):
    """Whole-number setting, or None when blank."""
    raw = env_str(key)
    if not raw:
        return None
    try:
        val = int(raw)
    except ValueError:
        fail(f"{key} must be a whole number, got: {raw!r}")
    _record(key, str(val))
    return val


def env_flag(key):
    """Optional True/False filter: None when blank (don't filter)."""
    raw = env_str(key).lower()
    if not raw:
        return None
    val = env_bool(key)
    _record(key, str(val))
    return val


def env_date(key):
    raw = env_str(key)
    if not raw:
        return None
    try:
        val = datetime.strptime(raw, "%Y-%m-%d").date()
    except ValueError:
        fail(f"{key} must be YYYY-MM-DD, got: {raw!r}")
    _record(key, raw)
    return val


# ── Enum lookup tables ────────────────────────────────────────────────
def _enum_map(cls):
    return {k: v for k, v in vars(cls).items() if k.isupper()}


def env_enum(key, enum_cls):
    """Comma-separated names from an SDK enum, returned as their values."""
    table = _enum_map(enum_cls)
    values = []
    for name in env_list(key):
        if name.upper() not in table:
            fail(
                f"Unknown {key}: {name!r}. "
                f"Valid options: {', '.join(table)}"
            )
        values.append(table[name.upper()])
    return values


def label_map(enum_cls):
    return {v: k for k, v in _enum_map(enum_cls).items()}


PRIVACY_LABEL = label_map(KalturaPrivacyType)
APPEAR_LABEL = label_map(KalturaAppearInListType)
INHERITANCE_LABEL = label_map(KalturaInheritanceType)
CONTRIBUTION_LABEL = label_map(KalturaContributionPolicyType)
JOIN_LABEL = label_map(KalturaUserJoinPolicyType)
PERMISSION_LABEL = label_map(KalturaCategoryUserPermissionLevel)
STATUS_LABEL = label_map(KalturaCategoryStatus)
NULLABLE_LABEL = {
    KalturaNullableBoolean.TRUE_VALUE: "True",
    KalturaNullableBoolean.FALSE_VALUE: "False",
}


# ── Credentials ───────────────────────────────────────────────────────
PARTNER_ID = env_str("PARTNER_ID")  # blank = ask when the script runs
ADMIN_SECRET = ""  # never read from .env — set in main() via getpass
USER_ID = env_str("USER_ID") or None
SERVICE_URL = (getenv("SERVICE_URL") or "https://www.kaltura.com").rstrip("/")
PRIVILEGES = getenv("PRIVILEGES") or "all:*,disableentitlement"


# ── Reliability ───────────────────────────────────────────────────────
# Seconds before an individual API request times out.
REQUEST_TIMEOUT = int(getenv("REQUEST_TIMEOUT", "120"))
# Retries for transient network errors before giving up.
MAX_NETWORK_RETRIES = int(getenv("MAX_NETWORK_RETRIES", "5"))
# Base seconds between retries (grows linearly: delay × attempt).
NETWORK_RETRY_DELAY = int(getenv("NETWORK_RETRY_DELAY", "5"))


# ── Output ────────────────────────────────────────────────────────────
EXPORT_CSV = env_bool("EXPORT_CSV", True)
# Subfolder the CSV is written into, next to this script (created if
# missing). An absolute path is used as is.
OUTPUT_DIR = env_str("OUTPUT_DIR") or "output"
# How many matches to list on screen; the CSV always has every match.
PRINT_LIMIT = env_int("PRINT_LIMIT")
if PRINT_LIMIT is None:
    PRINT_LIMIT = 50
TIMEZONE = env_str("TIMEZONE") or "US/Pacific"
try:
    local_tz = pytz.timezone(TIMEZONE)
except pytz.UnknownTimeZoneError:
    fail(f"Unknown TIMEZONE: {TIMEZONE!r} (try US/Pacific or Europe/Rome)")
# The earliest date your Kaltura account contains categories (YYYY-MM-DD).
# Only used as the starting point when a search has to be split by date.
EARLIEST_START_DATE = env_date("EARLIEST_START_DATE") or date(2000, 1, 1)


# ── Search settings: sent to Kaltura ──────────────────────────────────
# Numeric category IDs (KMC: Content > Categories). Comma = OR.
CATEGORY_ID = env_list("CATEGORY_ID")
EXCLUDE_CATEGORY_ID = env_list("EXCLUDE_CATEGORY_ID")
# Direct children of these category IDs.
PARENT_ID = env_list("PARENT_ID")
# Everything underneath these category IDs, at any depth.
ANCESTOR_ID = env_list("ANCESTOR_ID")
# Level in the tree: 0 = top level, 1 = its children, and so on. Comma = OR.
DEPTH = env_list("DEPTH")
# Full category path, e.g. MediaSpace>site>channels>my channel. Comma = OR.
FULL_NAME_EQUALS = env_list("FULL_NAME_EQUALS")
# Paths starting with, e.g. MediaSpace>site>channels. Comma = OR.
FULL_NAME_BEGINS_WITH = env_list("FULL_NAME_BEGINS_WITH")
# Word search across category name, description and tags.
SEARCH_TEXT = env_val("SEARCH_TEXT")
# External/reference ID assigned outside of Kaltura. Comma = OR.
REFERENCE_ID = env_list("REFERENCE_ID")
# True = only categories with no reference ID; False = only those with one.
HAS_NO_REFERENCE_ID = env_flag("HAS_NO_REFERENCE_ID")
# Categories that carry any / all of these tags.
TAG_ANY = env_list("TAG_ANY")
TAG_ALL = env_list("TAG_ALL")
# Users who manage / belong to the category. Comma = OR.
MANAGER_ID = env_list("MANAGER_ID")
MEMBER_ID = env_list("MEMBER_ID")
# Who can see the category's content: ALL, AUTHENTICATED_USERS, MEMBERS_ONLY
PRIVACY = env_enum("PRIVACY", KalturaPrivacyType)
# Who can see the category in lists: PARTNER_ONLY, CATEGORY_MEMBERS_ONLY
APPEAR_IN_LIST = env_enum("APPEAR_IN_LIST", KalturaAppearInListType)
# Where members come from: INHERIT (from the parent) or MANUAL
INHERITANCE_TYPE = env_enum("INHERITANCE_TYPE", KalturaInheritanceType)
# Who may add media: ALL or MEMBERS_WITH_CONTRIBUTION_PERMISSION
CONTRIBUTION_POLICY = env_enum(
    "CONTRIBUTION_POLICY", KalturaContributionPolicyType
)
# Context the category belongs to; MediaSpace channels use "MediaSpace".
PRIVACY_CONTEXT = env_list("PRIVACY_CONTEXT")
# UPDATING, ACTIVE, DELETED, PURGED. Blank = Kaltura's default.
STATUS = env_enum("STATUS", KalturaCategoryStatus)
# Member counts (inclusive bounds).
MEMBERS_MIN = env_int("MEMBERS_MIN")
MEMBERS_MAX = env_int("MEMBERS_MAX")
PENDING_MEMBERS_MIN = env_int("PENDING_MEMBERS_MIN")
PENDING_MEMBERS_MAX = env_int("PENDING_MEMBERS_MAX")
# Dates (YYYY-MM-DD, whole days in TIMEZONE, both ends included).
CREATED_AFTER = env_date("CREATED_AFTER")
CREATED_BEFORE = env_date("CREATED_BEFORE")
UPDATED_AFTER = env_date("UPDATED_AFTER")
UPDATED_BEFORE = env_date("UPDATED_BEFORE")
# Sort order, e.g. FULL_NAME_ASC. Blank = FULL_NAME_ASC.
ORDER_BY_NAME = (env_str("ORDER_BY") or "FULL_NAME_ASC").upper()
ORDER_BY_MAP = _enum_map(KalturaCategoryOrderBy)
if ORDER_BY_NAME not in ORDER_BY_MAP:
    fail(
        f"Unknown ORDER_BY: {ORDER_BY_NAME!r}. "
        f"Valid options: {', '.join(ORDER_BY_MAP)}"
    )
# Stop after this many matches (blank = all).
MAX_RESULTS = env_int("MAX_RESULTS")

# ── Search settings: applied after Kaltura answers ────────────────────
# Kaltura cannot filter on these, so every category the settings above
# match is checked here. Not case-sensitive; comma = OR.
CATEGORY_NAME_EQUALS = env_list("CATEGORY_NAME_EQUALS")
CATEGORY_NAME_BEGINS_WITH = env_list("CATEGORY_NAME_BEGINS_WITH")
CATEGORY_NAME_CONTAINS = env_list("CATEGORY_NAME_CONTAINS")
CATEGORY_NAME_ENDS_WITH = env_list("CATEGORY_NAME_ENDS_WITH")
# Drop categories whose name contains any of these.
CATEGORY_NAME_NOT_CONTAINS = env_list("CATEGORY_NAME_NOT_CONTAINS")
FULL_NAME_CONTAINS = env_list("FULL_NAME_CONTAINS")
FULL_NAME_NOT_CONTAINS = env_list("FULL_NAME_NOT_CONTAINS")
OWNER_ID = env_list("OWNER_ID")
# True (default): when OWNER_ID is set, Kaltura first narrows the search to
# categories that user manages (the owner is normally a manager too), so it
# does not have to scan every category. False: scan everything and check
# only the owner field; slower, but finds owners who are not managers.
OWNER_PREFILTER = env_bool("OWNER_PREFILTER", True)
ADMIN_TAG = env_list("ADMIN_TAG")
# True = only moderated categories; False = only unmoderated ones.
MODERATED = env_flag("MODERATED")
# Entry counts. ENTRIES counts media in the category and its subcategories;
# DIRECT_ENTRIES counts media in the category itself.
# ENTRIES_MAX=0 finds empty categories.
ENTRIES_MIN = env_int("ENTRIES_MIN")
ENTRIES_MAX = env_int("ENTRIES_MAX")
DIRECT_ENTRIES_MIN = env_int("DIRECT_ENTRIES_MIN")
DIRECT_ENTRIES_MAX = env_int("DIRECT_ENTRIES_MAX")
SUBCATEGORIES_MIN = env_int("SUBCATEGORIES_MIN")
SUBCATEGORIES_MAX = env_int("SUBCATEGORIES_MAX")


# ── Startup validation ────────────────────────────────────────────────
def _check_ints(key, values):
    for v in values:
        if not v.isdigit():
            fail(f"{key} must be whole numbers separated by commas, got: {v!r}")


_check_ints("CATEGORY_ID", CATEGORY_ID)
_check_ints("EXCLUDE_CATEGORY_ID", EXCLUDE_CATEGORY_ID)
_check_ints("PARENT_ID", PARENT_ID)
_check_ints("ANCESTOR_ID", ANCESTOR_ID)
_check_ints("DEPTH", DEPTH)

for _after, _before in [
    ("CREATED_AFTER", "CREATED_BEFORE"),
    ("UPDATED_AFTER", "UPDATED_BEFORE"),
]:
    if globals()[_after] and globals()[_before]:
        if globals()[_before] < globals()[_after]:
            fail(f"{_before} cannot be earlier than {_after}.")

for _lo, _hi in [
    ("MEMBERS_MIN", "MEMBERS_MAX"),
    ("PENDING_MEMBERS_MIN", "PENDING_MEMBERS_MAX"),
    ("ENTRIES_MIN", "ENTRIES_MAX"),
    ("DIRECT_ENTRIES_MIN", "DIRECT_ENTRIES_MAX"),
    ("SUBCATEGORIES_MIN", "SUBCATEGORIES_MAX"),
]:
    if globals()[_lo] is not None and globals()[_hi] is not None:
        if globals()[_hi] < globals()[_lo]:
            fail(f"{_hi} cannot be smaller than {_lo}.")


# ── Retry helper ──────────────────────────────────────────────────────
def call_with_retry(fn, *args, **kwargs):
    """Call fn(*args, **kwargs), retrying with linear backoff on transient
    network failures (timeouts, connection resets). Kaltura raises those as
    KalturaClientException, which is NOT a subclass of KalturaException, so
    it is caught here separately; requests' own errors are covered too. A
    real, well-formed API error (KalturaException) is never retried here."""
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
def start_client():
    """Start an admin session with the configured privileges."""
    config = KalturaConfiguration(int(PARTNER_ID))
    config.serviceUrl = SERVICE_URL
    config.requestTimeout = REQUEST_TIMEOUT
    client = KalturaClient(config)
    try:
        ks = call_with_retry(
            client.session.start,
            ADMIN_SECRET,
            USER_ID,
            KalturaSessionType.ADMIN,
            int(PARTNER_ID),
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
                f"   {e}\n"
                "   Check your internet connection and try again.\n"
            )
        else:
            print(f"\n❌ Could not start Kaltura session: {e}\n")
        raise SystemExit(1)
    client.setKs(ks)
    return client


# ── Date helpers ──────────────────────────────────────────────────────
def day_start_ts(d):
    return int(local_tz.localize(datetime.combine(d, dt_time.min)).timestamp())


def day_end_ts(d):
    return int(
        local_tz.localize(datetime.combine(d, dt_time(23, 59, 59))).timestamp()
    )


def fmt_ts(ts):
    try:
        return datetime.fromtimestamp(int(ts), local_tz).strftime(
            "%Y-%m-%d %H:%M"
        )
    except (TypeError, ValueError):
        return ""


# ── Building the Kaltura filter ───────────────────────────────────────
PAGE_SIZE = 500  # category.list maximum
RESULT_CAP = 10000  # Kaltura will not page past this many results

# Kaltura filters that take only one value. A comma-separated setting for
# one of these runs one search per value, and the results are merged (OR).
ONE_AT_A_TIME = {
    "fullNameStartsWith": FULL_NAME_BEGINS_WITH,
    "depthEqual": DEPTH,
    "referenceIdEqual": REFERENCE_ID,
    # An owner is normally a manager as well, so OWNER_ID can narrow the
    # search this way; the owner field is still checked afterward.
    "managerEqual": MANAGER_ID or (OWNER_ID if OWNER_PREFILTER else []),
    "memberEqual": MEMBER_ID,
    "privacyContextEqual": PRIVACY_CONTEXT,
    "appearInListEqual": APPEAR_IN_LIST,
    "contributionPolicyEqual": CONTRIBUTION_POLICY,
}
INT_FIELDS = {"depthEqual"}


def _csv(values):
    return ",".join(str(v) for v in values)


def fixed_settings():
    """Filter attributes that go into every search, as {name: value}."""
    s = {}
    if CATEGORY_ID:
        s["idIn"] = _csv(CATEGORY_ID)
    if EXCLUDE_CATEGORY_ID:
        s["idNotIn"] = _csv(EXCLUDE_CATEGORY_ID)
    if PARENT_ID:
        s["parentIdIn"] = _csv(PARENT_ID)
    if ANCESTOR_ID:
        s["ancestorIdIn"] = _csv(ANCESTOR_ID)
    if FULL_NAME_EQUALS:
        s["fullNameIn"] = _csv(FULL_NAME_EQUALS)
    if SEARCH_TEXT:
        s["freeText"] = SEARCH_TEXT
    if HAS_NO_REFERENCE_ID is not None:
        s["referenceIdEmpty"] = (
            KalturaNullableBoolean.TRUE_VALUE
            if HAS_NO_REFERENCE_ID
            else KalturaNullableBoolean.FALSE_VALUE
        )
    if TAG_ANY:
        s["tagsMultiLikeOr"] = _csv(TAG_ANY)
    if TAG_ALL:
        s["tagsMultiLikeAnd"] = _csv(TAG_ALL)
    if PRIVACY:
        s["privacyIn"] = _csv(PRIVACY)
    if INHERITANCE_TYPE:
        s["inheritanceTypeIn"] = _csv(INHERITANCE_TYPE)
    if STATUS:
        s["statusIn"] = _csv(STATUS)
    for name, val in [
        ("membersCountGreaterThanOrEqual", MEMBERS_MIN),
        ("membersCountLessThanOrEqual", MEMBERS_MAX),
        ("pendingMembersCountGreaterThanOrEqual", PENDING_MEMBERS_MIN),
        ("pendingMembersCountLessThanOrEqual", PENDING_MEMBERS_MAX),
    ]:
        if val is not None:
            s[name] = val
    if CREATED_AFTER:
        s["createdAtGreaterThanOrEqual"] = day_start_ts(CREATED_AFTER)
    if CREATED_BEFORE:
        s["createdAtLessThanOrEqual"] = day_end_ts(CREATED_BEFORE)
    if UPDATED_AFTER:
        s["updatedAtGreaterThanOrEqual"] = day_start_ts(UPDATED_AFTER)
    if UPDATED_BEFORE:
        s["updatedAtLessThanOrEqual"] = day_end_ts(UPDATED_BEFORE)
    s["orderBy"] = ORDER_BY_MAP[ORDER_BY_NAME]
    return s


def search_variants():
    """One settings dict per combination of single-value filters."""
    base = fixed_settings()
    names = [n for n, vals in ONE_AT_A_TIME.items() if vals]
    variants = []
    for combo in product(*(ONE_AT_A_TIME[n] for n in names)):
        v = dict(base)
        for name, value in zip(names, combo):
            v[name] = int(value) if name in INT_FIELDS else value
        variants.append(v)
    return variants


def make_filter(settings, created_from=None, created_to=None):
    flt = KalturaCategoryFilter()
    for name, value in settings.items():
        setattr(flt, name, value)
    # A date-split search narrows the creation window; keep the tighter
    # of that and any CREATED_AFTER/CREATED_BEFORE the user set.
    if created_from is not None:
        flt.createdAtGreaterThanOrEqual = max(
            created_from, settings.get("createdAtGreaterThanOrEqual", 0)
        )
    if created_to is not None:
        flt.createdAtLessThanOrEqual = min(
            created_to,
            settings.get("createdAtLessThanOrEqual", created_to),
        )
    return flt


# ── Fetching ──────────────────────────────────────────────────────────
def fetch_pages(client, flt, first_page, limit):
    """Collect every page of a filter whose total is within the cap."""
    found = list(first_page.objects or [])
    total = first_page.totalCount
    page_index = 1
    while len(found) < total and (limit is None or len(found) < limit):
        page_index += 1
        pager = KalturaFilterPager()
        pager.pageSize = PAGE_SIZE
        pager.pageIndex = page_index
        resp = call_with_retry(client.category.list, flt, pager)
        if not resp.objects:
            break
        found.extend(resp.objects)
    return found


def fetch_range(client, settings, created_from, created_to, limit):
    """All categories for one settings dict. If Kaltura reports more than it
    will page through, split the creation-date range in half and recurse."""
    flt = make_filter(settings, created_from, created_to)
    pager = KalturaFilterPager()
    pager.pageSize = PAGE_SIZE
    pager.pageIndex = 1
    first = call_with_retry(client.category.list, flt, pager)
    if first.totalCount <= RESULT_CAP:
        return fetch_pages(client, flt, first, limit)

    if created_from is None:
        created_from = max(
            day_start_ts(EARLIEST_START_DATE),
            settings.get("createdAtGreaterThanOrEqual", 0),
        )
    if created_to is None:
        created_to = min(
            int(time.time()),
            settings.get("createdAtLessThanOrEqual", int(time.time())),
        )
    if created_to - created_from < 1:
        print(
            f"    [warning: {first.totalCount} categories share one "
            "creation second; only the first 10,000 can be retrieved]"
        )
        return fetch_pages(client, flt, first, limit)
    mid = (created_from + created_to) // 2
    print(
        f"    [{first.totalCount} matches is over Kaltura's 10,000 cap; "
        "splitting by creation date]"
    )
    return fetch_range(
        client, settings, created_from, mid, limit
    ) + fetch_range(client, settings, mid + 1, created_to, limit)


def fetch_categories(client):
    """Run every search variant and merge the results by category ID."""
    variants = search_variants()
    merged = {}
    for i, settings in enumerate(variants, 1):
        if len(variants) > 1:
            print(f"  Search {i} of {len(variants)}…")
        for cat in fetch_range(client, settings, None, None, MAX_RESULTS):
            merged.setdefault(cat.id, cat)
    cats = list(merged.values())
    if len(variants) > 1:
        # Each search was sorted, but the merge was not.
        order = ORDER_BY_MAP[ORDER_BY_NAME]
        cats.sort(key=lambda c: sort_key(c, order[1:]), reverse=order[0] == "-")
    return cats


def sort_key(cat, field):
    value = val(cat, field)
    if value == "":
        return (0, 0)
    return (1, value.lower() if isinstance(value, str) else value)


# ── Reading a category ────────────────────────────────────────────────
def val(obj, attr):
    """Attribute value with SDK enums unwrapped; '' when unset."""
    v = getattr(obj, attr, None)
    if v is NotImplemented or v is None:
        return ""
    if hasattr(v, "getValue"):
        return v.getValue()
    return v


def number(obj, attr):
    v = val(obj, attr)
    return v if isinstance(v, (int, float)) else None


def text(obj, attr):
    v = val(obj, attr)
    return str(v) if v != "" else ""


def named(obj, attr, labels):
    v = val(obj, attr)
    return labels.get(v, v)


def lower_any(items):
    return [i.lower() for i in items]


# ── Filters Kaltura cannot do ─────────────────────────────────────────
def local_checks():
    """Predicates for the settings applied after the search."""
    checks = []

    def name_of(c):
        return text(c, "name").lower()

    def full_of(c):
        return text(c, "fullName").lower()

    if CATEGORY_NAME_EQUALS:
        terms = lower_any(CATEGORY_NAME_EQUALS)
        checks.append(lambda c: name_of(c) in terms)
    if CATEGORY_NAME_BEGINS_WITH:
        terms = tuple(lower_any(CATEGORY_NAME_BEGINS_WITH))
        checks.append(lambda c: name_of(c).startswith(terms))
    if CATEGORY_NAME_CONTAINS:
        terms = lower_any(CATEGORY_NAME_CONTAINS)
        checks.append(lambda c: any(t in name_of(c) for t in terms))
    if CATEGORY_NAME_ENDS_WITH:
        terms = tuple(lower_any(CATEGORY_NAME_ENDS_WITH))
        checks.append(lambda c: name_of(c).endswith(terms))
    if CATEGORY_NAME_NOT_CONTAINS:
        terms = lower_any(CATEGORY_NAME_NOT_CONTAINS)
        checks.append(lambda c: not any(t in name_of(c) for t in terms))
    if FULL_NAME_CONTAINS:
        terms = lower_any(FULL_NAME_CONTAINS)
        checks.append(lambda c: any(t in full_of(c) for t in terms))
    if FULL_NAME_NOT_CONTAINS:
        terms = lower_any(FULL_NAME_NOT_CONTAINS)
        checks.append(lambda c: not any(t in full_of(c) for t in terms))
    if OWNER_ID:
        terms = lower_any(OWNER_ID)
        checks.append(lambda c: text(c, "owner").lower() in terms)
    if ADMIN_TAG:
        terms = lower_any(ADMIN_TAG)

        def has_admin_tag(c):
            tags = [t.strip().lower() for t in text(c, "adminTags").split(",")]
            return any(t in tags for t in terms)

        checks.append(has_admin_tag)
    if MODERATED is not None:
        checks.append(
            lambda c: (val(c, "moderation") == KalturaNullableBoolean.TRUE_VALUE)
            == MODERATED
        )
    for attr, lo, hi in [
        ("entriesCount", ENTRIES_MIN, ENTRIES_MAX),
        ("directEntriesCount", DIRECT_ENTRIES_MIN, DIRECT_ENTRIES_MAX),
        ("directSubCategoriesCount", SUBCATEGORIES_MIN, SUBCATEGORIES_MAX),
    ]:
        if lo is not None:
            checks.append(lambda c, a=attr, n=lo: (number(c, a) or 0) >= n)
        if hi is not None:
            checks.append(lambda c, a=attr, n=hi: (number(c, a) or 0) <= n)
    return checks


# ── Output ────────────────────────────────────────────────────────────
CSV_COLUMNS = [
    "category_id", "full_name", "name", "parent_id", "depth", "status",
    "entries_count", "direct_entries_count", "direct_subcategories_count",
    "members_count", "pending_members_count", "pending_entries_count",
    "owner", "privacy", "privacy_context", "appear_in_list",
    "inheritance_type", "contribution_policy", "user_join_policy",
    "default_permission_level", "moderation", "reference_id", "tags",
    "admin_tags", "description", "created_at", "updated_at",
]


def category_to_row(c):
    return {
        "category_id": val(c, "id"),
        "full_name": text(c, "fullName"),
        "name": text(c, "name"),
        "parent_id": val(c, "parentId"),
        "depth": val(c, "depth"),
        "status": named(c, "status", STATUS_LABEL),
        "entries_count": val(c, "entriesCount"),
        "direct_entries_count": val(c, "directEntriesCount"),
        "direct_subcategories_count": val(c, "directSubCategoriesCount"),
        "members_count": val(c, "membersCount"),
        "pending_members_count": val(c, "pendingMembersCount"),
        "pending_entries_count": val(c, "pendingEntriesCount"),
        "owner": text(c, "owner"),
        "privacy": named(c, "privacy", PRIVACY_LABEL),
        # MediaSpace channels carry "MediaSpace" in privacyContexts (plural);
        # the singular privacyContext is blank on them.
        "privacy_context": text(c, "privacyContexts")
        or text(c, "privacyContext"),
        "appear_in_list": named(c, "appearInList", APPEAR_LABEL),
        "inheritance_type": named(c, "inheritanceType", INHERITANCE_LABEL),
        "contribution_policy": named(
            c, "contributionPolicy", CONTRIBUTION_LABEL
        ),
        "user_join_policy": named(c, "userJoinPolicy", JOIN_LABEL),
        "default_permission_level": named(
            c, "defaultPermissionLevel", PERMISSION_LABEL
        ),
        "moderation": named(c, "moderation", NULLABLE_LABEL),
        "reference_id": text(c, "referenceId"),
        "tags": text(c, "tags"),
        "admin_tags": text(c, "adminTags"),
        "description": text(c, "description"),
        "created_at": fmt_ts(val(c, "createdAt")),
        "updated_at": fmt_ts(val(c, "updatedAt")),
    }


def print_matches(rows):
    shown = rows if PRINT_LIMIT <= 0 else rows[:PRINT_LIMIT]
    print(f"\n{'ID':>10}  {'Entries':>8}  {'Members':>8}  Category")
    print(f"{'-' * 10}  {'-' * 8}  {'-' * 8}  {'-' * 40}")
    for r in shown:
        print(
            f"{r['category_id']!s:>10}  {r['entries_count']!s:>8}  "
            f"{r['members_count']!s:>8}  {r['full_name']}"
        )
    if len(shown) < len(rows):
        print(
            f"… and {len(rows) - len(shown)} more "
            "(raise PRINT_LIMIT to list more; the CSV has all of them)."
        )


def write_csv(rows):
    folder = Path(OUTPUT_DIR)
    if not folder.is_absolute():
        folder = SCRIPT_DIR / folder
    makedirs(folder, exist_ok=True)
    stamp = datetime.now().strftime("%Y-%m-%d-%H%M")
    path = folder / f"{stamp}_category-search.csv"
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    return path


def print_recap():
    if not ACTIVE_FILTERS:
        print("Settings filled in .env: none")
        return
    print("Settings filled in .env:")
    for key, shown in ACTIVE_FILTERS:
        print(f"  {key} = {shown}")


# ── Main ──────────────────────────────────────────────────────────────
def main():
    global PARTNER_ID, ADMIN_SECRET

    print("Kaltura Category Search\n")
    if CLEANED_VARS:
        print(
            "⚠️  Removed invisible characters (such as zero-width spaces) "
            f"from: {', '.join(CLEANED_VARS)}\n"
            "   They usually come from copy-pasting; check those lines in "
            ".env.\n"
        )
    if getenv("ADMIN_SECRET"):
        print(
            "⚠️  ADMIN_SECRET in .env is ignored — this script always asks "
            "for it. Delete that line from .env.\n"
        )

    print_recap()

    if not {k for k, _ in ACTIVE_FILTERS} - NOT_FILTERS:
        answer = input(
            "\nNo filters are set, so this will list EVERY category. "
            "Continue? [y/N] "
        )
        if answer.strip().lower() not in ("y", "yes"):
            print("Cancelled.")
            return

    if not PARTNER_ID:
        PARTNER_ID = input("\nKaltura Partner ID: ").strip()
    if not PARTNER_ID.isdigit():
        fail(f"Partner ID must be a number, got: {PARTNER_ID!r}")
    ADMIN_SECRET = getpass.getpass("Kaltura Admin Secret (hidden): ").strip()
    if not ADMIN_SECRET:
        fail("An Admin Secret is required.")

    print("\nLogging in…")
    client = start_client()

    print("Searching…")
    categories = fetch_categories(client)
    print(f"Kaltura returned {len(categories)} categories.")

    checks = local_checks()
    if checks:
        categories = [c for c in categories if all(f(c) for f in checks)]
        print(f"{len(categories)} remain after the name/owner/count filters.")
    if MAX_RESULTS is not None:
        categories = categories[:MAX_RESULTS]

    if not categories:
        print("\nNo categories matched.")
        return

    rows = [category_to_row(c) for c in categories]
    print_matches(rows)

    print(f"\n{len(rows)} categories matched.")
    if EXPORT_CSV:
        path = write_csv(rows)
        print(f"Saved: {path}")


if __name__ == "__main__":
    main()
