import csv
import json
import os
import time
import socket
from urllib.error import HTTPError, URLError
from collections import defaultdict
from datetime import datetime, timezone
from urllib.parse import urlencode
from urllib.request import Request, urlopen


# ============================================================
# CONFIGURATION
# ============================================================

KALTURA_BASE_URL = "https://www.kaltura.com/api_v3"

# ============================================================
# UPDATE THESE VALUES BEFORE RUNNING A NEW REPORT
# ============================================================

# 1) Partner ID
# Find this in your Kaltura Management Console (KMC) account information.
PARTNER_ID = 1234567  # <-- UPDATE THIS

# 2) Root / ancestor category ID
# In KMC, go to Content > Categories, find the root category you want
# to report on, and copy the number in the ID column.
CATEGORY_ANCESTOR_ID = "123456789"  # <-- UPDATE THIS

# 3) Report start and end dates as UNIX timestamps
# Replace these with timestamps for your desired reporting period.
REPORT_START_TIMESTAMP = 1758412800  # <-- UPDATE THIS
REPORT_END_TIMESTAMP = 1790121599    # <-- UPDATE THIS

# 4) Report label
# This text is used in the output filenames.
REPORT_LABEL = "YYYYMonDDtoYYYYMonDD"  # <-- UPDATE THIS

# ============================================================
# DO NOT PASTE YOUR KS OR ADMIN SECRET INTO THIS FILE
# ============================================================

# Before running the script, set your current KS in Terminal:
# export KALTURA_KS='PASTE_YOUR_CURRENT_KS_HERE'
KS = os.getenv("KALTURA_KS", "")

# Stay safely below Kaltura's 10,000-match limit.
MAX_RESULTS_PER_WINDOW = 9000

BASE_MEDIA_FILTER = {
    "objectType": "KalturaMediaEntryFilter",
    "categoryAncestorIdIn": CATEGORY_ANCESTOR_ID,
    "orderBy": "+createdAt",
}

MEDIA_PAGE_SIZE = 500
CAPTION_PAGE_SIZE = 500
CATEGORY_ENTRY_PAGE_SIZE = 500
CATEGORY_PAGE_SIZE = 500
ENTRY_ID_BATCH_SIZE = 50
CATEGORY_ID_BATCH_SIZE = 200
SLEEP_BETWEEN_CALLS = 1.0


RAW_MEDIA_CSV = f"KalturaMediaRaw_{REPORT_LABEL}.csv"
RAW_CAPTION_CSV = f"KalturaCaptionRaw_{REPORT_LABEL}.csv"
RAW_CATEGORY_ENTRY_CSV = f"KalturaCategoryEntryRaw_{REPORT_LABEL}.csv"
RAW_CATEGORY_CSV = f"KalturaCategoryRaw_{REPORT_LABEL}.csv"
FINAL_REPORT_CSV = f"KalturaMediaCaptionCategoryFinal_{REPORT_LABEL}.csv"
LEADERSHIP_REPORT_CSV = f"KalturaMediaCaptionCategoryLeadership_{REPORT_LABEL}.csv"
LOG_FILE = f"KalturaMediaCaptionCategoryRunLog_{REPORT_LABEL}.txt"


MEDIA_FIELDS = [
    "id",
    "name",
    "description",
    "partnerId",
    "userId",
    "creatorId",
    "tags",
    "adminTags",
    "status",
    "moderationStatus",
    "moderationCount",
    "type",
    "createdAt",
    "updatedAt",
    "rank",
    "totalRank",
    "votes",
    "downloadUrl",
    "searchText",
    "licenseType",
    "version",
    "thumbnailUrl",
    "accessControlId",
    "replacementStatus",
    "partnerSortValue",
    "conversionProfileId",
    "rootEntryId",
    "parentEntryId",
    "operationAttributes",
    "entitledUsersEdit",
    "entitledUsersPublish",
    "entitledUsersView",
    "capabilities",
    "displayInSearch",
    "blockAutoTranscript",
    "plays",
    "views",
    "lastPlayedAt",
    "width",
    "height",
    "duration",
    "msDuration",
    "mediaType",
    "conversionQuality",
    "sourceType",
    "dataUrl",
    "flavorParamsIds",
    "objectType",
    "referenceId",
    "application",
    "applicationVersion",
    "externalSourceType",
    "streams",
]

CAPTION_FIELDS = [
    "id",
    "entryId",
    "partnerId",
    "version",
    "size",
    "tags",
    "fileExt",
    "createdAt",
    "updatedAt",
    "description",
    "sizeInBytes",
    "captionParamsId",
    "language",
    "languageCode",
    "isDefault",
    "label",
    "format",
    "status",
    "accuracy",
    "displayOnPlayer",
    "usage",
    "objectType",
    "source",
    "associatedTranscriptIds",
]

CATEGORY_ENTRY_FIELDS = [
    "categoryFullIds",
    "categoryId",
    "createdAt",
    "creatorUserId",
    "entryId",
    "status",
    "objectType",
]

CATEGORY_FIELDS = [
    "id",
    "parentId",
    "depth",
    "partnerId",
    "name",
    "fullName",
    "fullIds",
    "entriesCount",
    "createdAt",
    "updatedAt",
    "referenceId",
    "directEntriesCount",
    "directSubCategoriesCount",
    "status",
    "objectType",
]

DATETIME_FIELDS = [
    "createdAtDateTimeUTC",
    "updatedAtDateTimeUTC",
    "lastPlayedAtDateTimeUTC",
]

CALCULATED_MEDIA_FIELDS = [
    "duration_minutes",
    "duration_hh_mm_ss",
]

CAPTION_SUMMARY_FIELDS = [
    "caption_asset_count",
    "has_captions",
    "has_default_caption",
    "caption_labels",
    "caption_languages",
    "default_caption_labels",
    "caption_formats",
    "caption_statuses",
    "caption_display_on_player",
    "caption_sources",
    "max_caption_accuracy",
    "min_caption_accuracy",
    "all_accuracy_values",
    "usage_values",
    "accessibility_asset_types",
    "has_extended_audio_description",
    "transcript_available",
    "associated_transcript_ids",
]

CATEGORY_SUMMARY_FIELDS = [
    "category_assignment_count",
    "category_ids",
    "category_full_ids",
    "category_names",
    "category_fullnames",
    "has_incontext_category",
    "incontext_category_paths",
]

LEADERSHIP_FIELDS = [
    "id",
    "name",
    "userId",
    "createdAtDateTimeUTC",
    "updatedAtDateTimeUTC",
    "lastPlayedAtDateTimeUTC",
    "plays",
    "views",
    "duration_hh_mm_ss",
    "mediaType",
    "sourceType",
    "caption_asset_count",
    "has_captions",
    "caption_languages",
    "default_caption_labels",
    "max_caption_accuracy",
    "min_caption_accuracy",
    "accessibility_asset_types",
    "transcript_available",
    "associated_transcript_ids",
    "category_assignment_count",
    "has_incontext_category",
    "category_fullnames",
    "incontext_category_paths",
]

# ============================================================
# GENERAL HELPERS
# ============================================================

def flatten(prefix, obj, out):
    if isinstance(obj, dict):
        for key, value in obj.items():
            new_key = f"{prefix}[{key}]" if prefix else key
            flatten(new_key, value, out)
    elif isinstance(obj, list):
        for index, value in enumerate(obj):
            new_key = f"{prefix}[{index}]"
            flatten(new_key, value, out)
    else:
        out[prefix] = obj


def kaltura_post(service: str, action: str, params: dict) -> dict:
    if not KS:
        raise RuntimeError(
            "KALTURA_KS is empty. Set it before running the script, for example:\n"
            "export KALTURA_KS='your_ks_here'"
        )

    url = f"{KALTURA_BASE_URL}/service/{service}/action/{action}"

    payload = {
        "ks": KS,
        "partnerId": PARTNER_ID,
        "format": 1,
    }

    flat_params = {}
    flatten("", params, flat_params)
    payload.update(flat_params)

    data = urlencode(payload, doseq=True).encode("utf-8")
    request = Request(url, data=data, method="POST")

    max_attempts = 5

    for attempt in range(1, max_attempts + 1):
        try:
            with urlopen(request, timeout=300) as response:
                return json.loads(response.read().decode("utf-8"))

        except (socket.timeout, TimeoutError, URLError, HTTPError) as error:
            if attempt == max_attempts:
                print(
                    f"Kaltura API request failed after {max_attempts} attempts: "
                    f"{service}.{action}"
                )
                raise

            wait_seconds = min(5 * (2 ** (attempt - 1)), 60)

            print(
                f"Kaltura API request failed for {service}.{action}: {error}"
            )
            print(
                f"Retrying in {wait_seconds} seconds "
                f"(attempt {attempt + 1} of {max_attempts})..."
            )

            time.sleep(wait_seconds)


def normalize_value(value):
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False)
    return value


def safe_int(value):
    try:
        if value in ("", None):
            return None
        return int(value)
    except (ValueError, TypeError):
        return None


def safe_float(value):
    try:
        if value in ("", None):
            return None
        return float(value)
    except (ValueError, TypeError):
        return None


def chunked(sequence, size):
    for index in range(0, len(sequence), size):
        yield sequence[index:index + size]


def check_for_api_error(result, label):
    if isinstance(result, dict) and result.get("objectType") == "KalturaAPIException":
        raise RuntimeError(f"{label} failed: {result}")


def unix_to_utc_string(value):
    timestamp = safe_int(value)
    if timestamp is None:
        return ""
    return datetime.fromtimestamp(
        timestamp,
        tz=timezone.utc
    ).strftime("%Y-%m-%d %H:%M:%S UTC")


def duration_minutes_from_row(row):
    ms_duration = safe_float(row.get("msDuration"))
    if ms_duration is not None and ms_duration > 0:
        return round(ms_duration / 60000, 2)

    duration_seconds = safe_float(row.get("duration"))
    if duration_seconds is not None and duration_seconds > 0:
        return round(duration_seconds / 60, 2)

    return ""


def duration_hh_mm_ss_from_row(row):
    ms_duration = safe_float(row.get("msDuration"))
    if ms_duration is not None and ms_duration > 0:
        total_seconds = int(round(ms_duration / 1000))
    else:
        duration_seconds = safe_float(row.get("duration"))
        if duration_seconds is None or duration_seconds <= 0:
            return ""
        total_seconds = int(round(duration_seconds))

    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def write_log(lines):
    with open(LOG_FILE, "w", encoding="utf-8") as file:
        for line in lines:
            file.write(str(line) + "\n")


# ============================================================
# MEDIA WINDOWING
# ============================================================

def build_media_filter(start_timestamp, end_timestamp):
    media_filter = dict(BASE_MEDIA_FILTER)
    media_filter["createdAtGreaterThanOrEqual"] = start_timestamp
    media_filter["createdAtLessThanOrEqual"] = end_timestamp
    return media_filter


def get_media_count(start_timestamp, end_timestamp):
    params = {
        "filter": build_media_filter(start_timestamp, end_timestamp),
        "pager": {
            "pageSize": 1,
            "pageIndex": 1,
        },
    }

    result = kaltura_post("media", "list", params)
    check_for_api_error(result, "media.list count check")

    return safe_int(result.get("totalCount")) or 0


def build_safe_media_windows(start_timestamp, end_timestamp):
    """
    Recursively split the reporting period until each window contains
    no more than MAX_RESULTS_PER_WINDOW matching media entries.
    """
    count = get_media_count(start_timestamp, end_timestamp)

    print(
        "Checking media window "
        f"{unix_to_utc_string(start_timestamp)} through "
        f"{unix_to_utc_string(end_timestamp)}: {count} entries"
    )

    if count <= MAX_RESULTS_PER_WINDOW:
        return [(start_timestamp, end_timestamp, count)]

    if start_timestamp >= end_timestamp:
        raise RuntimeError(
            "A one-second reporting window still contains more entries than "
            "the configured safe maximum. The window cannot be divided further."
        )

    midpoint = (start_timestamp + end_timestamp) // 2

    left_windows = build_safe_media_windows(
        start_timestamp,
        midpoint,
    )
    right_windows = build_safe_media_windows(
        midpoint + 1,
        end_timestamp,
    )

    return left_windows + right_windows


def fetch_media_window(start_timestamp, end_timestamp):
    all_rows = []
    page_index = 1

    while True:
        params = {
            "filter": build_media_filter(start_timestamp, end_timestamp),
            "pager": {
                "pageSize": MEDIA_PAGE_SIZE,
                "pageIndex": page_index,
            },
        }

        result = kaltura_post("media", "list", params)
        check_for_api_error(result, "media.list")

        rows = result.get("objects", [])
        if not rows:
            break

        all_rows.extend(rows)

        print(
            f"Fetched media window "
            f"{unix_to_utc_string(start_timestamp)} through "
            f"{unix_to_utc_string(end_timestamp)}, "
            f"page {page_index}: {len(rows)} rows"
        )

        if len(rows) < MEDIA_PAGE_SIZE:
            break

        page_index += 1
        time.sleep(SLEEP_BETWEEN_CALLS)

    return all_rows


def fetch_all_media():
    windows = build_safe_media_windows(
        REPORT_START_TIMESTAMP,
        REPORT_END_TIMESTAMP,
    )

    print(f"Reporting period divided into {len(windows)} safe media windows.")

    all_rows = []

    for window_number, (start_ts, end_ts, expected_count) in enumerate(
        windows,
        start=1,
    ):
        print(
            f"Starting media window {window_number} of {len(windows)} "
            f"(expected entries: {expected_count})"
        )

        all_rows.extend(fetch_media_window(start_ts, end_ts))
        time.sleep(SLEEP_BETWEEN_CALLS)

    unique_rows_by_id = {}

    for row in all_rows:
        entry_id = row.get("id")
        if entry_id and entry_id not in unique_rows_by_id:
            unique_rows_by_id[entry_id] = row

    unique_rows = list(unique_rows_by_id.values())
    unique_rows.sort(
        key=lambda row: (
            safe_int(row.get("createdAt")) or 0,
            str(row.get("id", "")),
        )
    )

    print(f"Total media rows before deduplication: {len(all_rows)}")
    print(f"Unique media rows collected: {len(unique_rows)}")

    return unique_rows, windows


def write_raw_media_csv(rows):
    with open(RAW_MEDIA_CSV, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=MEDIA_FIELDS,
            extrasaction="ignore",
        )
        writer.writeheader()

        for row in rows:
            writer.writerow({
                field: normalize_value(row.get(field, ""))
                for field in MEDIA_FIELDS
            })


# ============================================================
# CAPTION ASSETS
# ============================================================

def fetch_caption_assets_for_entry_ids(entry_ids):
    all_rows = []

    for batch_number, id_chunk in enumerate(
        chunked(entry_ids, ENTRY_ID_BATCH_SIZE),
        start=1,
    ):
        page_index = 1
        entry_id_in = ",".join(id_chunk)

        while True:
            params = {
                "filter": {
                    "objectType": "KalturaCaptionAssetFilter",
                    "entryIdIn": entry_id_in,
                    "orderBy": "-updatedAt",
                },
                "pager": {
                    "pageSize": CAPTION_PAGE_SIZE,
                    "pageIndex": page_index,
                },
            }

            result = kaltura_post(
                "caption_captionasset",
                "list",
                params,
            )
            check_for_api_error(result, "captionAsset.list")

            rows = result.get("objects", [])
            if not rows:
                break

            all_rows.extend(rows)

            print(
                f"Fetched caption batch {batch_number}, "
                f"page {page_index}: {len(rows)} rows"
            )

            if len(rows) < CAPTION_PAGE_SIZE:
                break

            page_index += 1
            time.sleep(SLEEP_BETWEEN_CALLS)

    return all_rows


def write_raw_caption_csv(rows):
    with open(RAW_CAPTION_CSV, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=CAPTION_FIELDS,
            extrasaction="ignore",
        )
        writer.writeheader()

        for row in rows:
            writer.writerow({
                field: normalize_value(row.get(field, ""))
                for field in CAPTION_FIELDS
            })


def summarize_caption_rows(caption_rows):
    grouped = defaultdict(list)

    for row in caption_rows:
        entry_id = row.get("entryId", "")
        if entry_id:
            grouped[entry_id].append(row)

    summary = {}

    for entry_id, rows in grouped.items():
        labels = sorted({
            str(row.get("label", ""))
            for row in rows
            if row.get("label", "") not in ("", None)
        })

        languages = sorted({
            str(row.get("language", ""))
            for row in rows
            if row.get("language", "") not in ("", None)
        })

        default_labels = sorted({
            str(row.get("label", ""))
            for row in rows
            if row.get("label", "") not in ("", None)
            and str(row.get("isDefault", "")).lower() in ("true", "1")
        })

        formats = sorted({
            str(row.get("format", ""))
            for row in rows
            if row.get("format", "") not in ("", None)
        })

        statuses = sorted({
            str(row.get("status", ""))
            for row in rows
            if row.get("status", "") not in ("", None)
        })

        display_values = sorted({
            "Yes"
            if str(row.get("displayOnPlayer", "")).lower() in ("true", "1")
            else "No"
            for row in rows
            if row.get("displayOnPlayer", "") not in ("", None)
        })

        sources = sorted({
            str(row.get("source", ""))
            for row in rows
            if row.get("source", "") not in ("", None)
        })

        has_default_caption = any(
            str(row.get("isDefault", "")).lower() in ("true", "1")
            for row in rows
        )

        accuracies = [
            safe_int(row.get("accuracy"))
            for row in rows
            if safe_int(row.get("accuracy")) is not None
        ]

        usage_values = sorted({
            safe_int(row.get("usage"))
            for row in rows
            if safe_int(row.get("usage")) is not None
        })

        usage_labels = []
        if 0 in usage_values:
            usage_labels.append("Captions")
        if 1 in usage_values:
            usage_labels.append("Extended Audio Description")

        transcript_ids = set()

        for row in rows:
            value = row.get("associatedTranscriptIds", "")

            if isinstance(value, list):
                for item in value:
                    if item not in ("", None):
                        transcript_ids.add(str(item))
            elif value not in ("", None):
                transcript_ids.add(str(value))

        summary[entry_id] = {
            "caption_asset_count": len(rows),
            "has_captions": "Yes" if 0 in usage_values or len(rows) > 0 else "No",
            "has_default_caption": "Yes" if has_default_caption else "No",
            "caption_labels": "; ".join(labels),
            "caption_languages": "; ".join(languages),
            "default_caption_labels": "; ".join(default_labels),
            "caption_formats": "; ".join(formats),
            "caption_statuses": "; ".join(statuses),
            "caption_display_on_player": "; ".join(display_values),
            "caption_sources": "; ".join(sources),
            "max_caption_accuracy": max(accuracies) if accuracies else "",
            "min_caption_accuracy": min(accuracies) if accuracies else "",
            "all_accuracy_values": (
                "; ".join(str(value) for value in sorted(accuracies))
                if accuracies
                else ""
            ),
            "usage_values": "; ".join(str(value) for value in usage_values),
            "accessibility_asset_types": "; ".join(usage_labels),
            "has_extended_audio_description": "Yes" if 1 in usage_values else "No",
            "transcript_available": "Yes" if transcript_ids else "No",
            "associated_transcript_ids": "; ".join(sorted(transcript_ids)),
        }

    return summary


# ============================================================
# CATEGORY ENTRY
# ============================================================

def fetch_category_entries_for_entry_ids(entry_ids):
    all_rows = []

    for batch_number, id_chunk in enumerate(
        chunked(entry_ids, ENTRY_ID_BATCH_SIZE),
        start=1,
    ):
        page_index = 1
        entry_id_in = ",".join(id_chunk)

        while True:
            params = {
                "filter": {
                    "entryIdIn": entry_id_in,
                    "orderBy": "+createdAt",
                },
                "pager": {
                    "pageSize": CATEGORY_ENTRY_PAGE_SIZE,
                    "pageIndex": page_index,
                },
            }

            result = kaltura_post("categoryentry", "list", params)
            check_for_api_error(result, "categoryEntry.list")

            rows = result.get("objects", [])
            if not rows:
                break

            all_rows.extend(rows)

            print(
                f"Fetched categoryEntry batch {batch_number}, "
                f"page {page_index}: {len(rows)} rows"
            )

            if len(rows) < CATEGORY_ENTRY_PAGE_SIZE:
                break

            page_index += 1
            time.sleep(SLEEP_BETWEEN_CALLS)

    return all_rows


def write_raw_category_entry_csv(rows):
    with open(
        RAW_CATEGORY_ENTRY_CSV,
        "w",
        newline="",
        encoding="utf-8",
    ) as file:
        writer = csv.DictWriter(
            file,
            fieldnames=CATEGORY_ENTRY_FIELDS,
            extrasaction="ignore",
        )
        writer.writeheader()

        for row in rows:
            writer.writerow({
                field: normalize_value(row.get(field, ""))
                for field in CATEGORY_ENTRY_FIELDS
            })


# ============================================================
# CATEGORY DETAILS
# ============================================================

def fetch_categories_by_ids(category_ids):
    all_rows = []

    unique_ids = list(dict.fromkeys(
        str(category_id)
        for category_id in category_ids
        if str(category_id).strip()
    ))

    if not unique_ids:
        return all_rows

    for batch_number, id_chunk in enumerate(
        chunked(unique_ids, CATEGORY_ID_BATCH_SIZE),
        start=1,
    ):
        page_index = 1
        id_in = ",".join(id_chunk)

        while True:
            params = {
                "filter": {
                    "idIn": id_in,
                    "orderBy": "+fullName",
                },
                "pager": {
                    "pageSize": CATEGORY_PAGE_SIZE,
                    "pageIndex": page_index,
                },
            }

            result = kaltura_post("category", "list", params)
            check_for_api_error(result, "category.list")

            rows = result.get("objects", [])
            if not rows:
                break

            all_rows.extend(rows)

            print(
                f"Fetched category batch {batch_number}, "
                f"page {page_index}: {len(rows)} rows"
            )

            if len(rows) < CATEGORY_PAGE_SIZE:
                break

            page_index += 1
            time.sleep(SLEEP_BETWEEN_CALLS)

    return all_rows


def write_raw_category_csv(rows):
    with open(RAW_CATEGORY_CSV, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=CATEGORY_FIELDS,
            extrasaction="ignore",
        )
        writer.writeheader()

        for row in rows:
            writer.writerow({
                field: normalize_value(row.get(field, ""))
                for field in CATEGORY_FIELDS
            })


def summarize_category_rows(category_entry_rows, category_rows):
    categories_by_id = {}

    for row in category_rows:
        category_id = str(row.get("id", ""))
        if category_id:
            categories_by_id[category_id] = row

    grouped = defaultdict(list)

    for row in category_entry_rows:
        entry_id = row.get("entryId", "")
        if entry_id:
            grouped[entry_id].append(row)

    summary = {}

    for entry_id, rows in grouped.items():
        category_ids = []
        category_full_ids = []
        category_names = []
        category_fullnames = []
        incontext_paths = []

        for row in rows:
            category_id = str(row.get("categoryId", ""))

            if category_id:
                category_ids.append(category_id)

            full_ids = row.get("categoryFullIds", "")
            if full_ids:
                category_full_ids.append(str(full_ids))

            category = categories_by_id.get(category_id)

            if category:
                name = category.get("name", "")
                full_name = category.get("fullName", "")

                if name:
                    category_names.append(str(name))

                if full_name:
                    full_name = str(full_name)
                    category_fullnames.append(full_name)

                    if full_name.endswith(">InContext") or ">InContext" in full_name:
                        incontext_paths.append(full_name)

        summary[entry_id] = {
            "category_assignment_count": len(rows),
            "category_ids": "; ".join(sorted(set(category_ids))),
            "category_full_ids": "; ".join(
                sorted(set(category_full_ids))
            ),
            "category_names": "; ".join(
                sorted(set(category_names))
            ),
            "category_fullnames": "; ".join(
                sorted(set(category_fullnames))
            ),
            "has_incontext_category": "Yes" if incontext_paths else "No",
            "incontext_category_paths": "; ".join(
                sorted(set(incontext_paths))
            ),
        }

    return summary


# ============================================================
# FINAL REPORTS
# ============================================================

def write_final_report(media_rows, caption_summary, category_summary):
    final_fields = (
        MEDIA_FIELDS
        + DATETIME_FIELDS
        + CALCULATED_MEDIA_FIELDS
        + CAPTION_SUMMARY_FIELDS
        + CATEGORY_SUMMARY_FIELDS
    )

    default_caption_summary = {
        "caption_asset_count": 0,
        "has_captions": "No",
        "has_default_caption": "No",
        "caption_labels": "",
        "caption_languages": "",
        "default_caption_labels": "",
        "caption_formats": "",
        "caption_statuses": "",
        "caption_display_on_player": "",
        "caption_sources": "",
        "max_caption_accuracy": "",
        "min_caption_accuracy": "",
        "all_accuracy_values": "",
        "usage_values": "",
        "accessibility_asset_types": "",
        "has_extended_audio_description": "No",
        "transcript_available": "No",
        "associated_transcript_ids": "",
    }

    default_category_summary = {
        "category_assignment_count": 0,
        "category_ids": "",
        "category_full_ids": "",
        "category_names": "",
        "category_fullnames": "",
        "has_incontext_category": "No",
        "incontext_category_paths": "",
    }

    with open(FINAL_REPORT_CSV, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=final_fields,
            extrasaction="ignore",
        )
        writer.writeheader()

        seen_ids = set()

        for row in media_rows:
            media_id = row.get("id", "")

            if not media_id or media_id in seen_ids:
                continue

            seen_ids.add(media_id)

            output = {
                field: normalize_value(row.get(field, ""))
                for field in MEDIA_FIELDS
            }

            output["createdAtDateTimeUTC"] = unix_to_utc_string(
                row.get("createdAt")
            )
            output["updatedAtDateTimeUTC"] = unix_to_utc_string(
                row.get("updatedAt")
            )
            output["lastPlayedAtDateTimeUTC"] = unix_to_utc_string(
                row.get("lastPlayedAt")
            )
            output["duration_minutes"] = duration_minutes_from_row(row)
            output["duration_hh_mm_ss"] = duration_hh_mm_ss_from_row(row)

            output.update(
                caption_summary.get(
                    media_id,
                    default_caption_summary,
                )
            )
            output.update(
                category_summary.get(
                    media_id,
                    default_category_summary,
                )
            )

            writer.writerow(output)


def write_leadership_report():
    with open(FINAL_REPORT_CSV, "r", encoding="utf-8") as input_file, \
         open(
             LEADERSHIP_REPORT_CSV,
             "w",
             newline="",
             encoding="utf-8",
         ) as output_file:

        reader = csv.DictReader(input_file)
        writer = csv.DictWriter(
            output_file,
            fieldnames=LEADERSHIP_FIELDS,
            extrasaction="ignore",
        )
        writer.writeheader()

        for row in reader:
            writer.writerow({
                field: row.get(field, "")
                for field in LEADERSHIP_FIELDS
            })


# ============================================================
# MAIN
# ============================================================

def main():
    log_lines = [
        "Kaltura media + caption + category report run started",
        f"KALTURA_BASE_URL={KALTURA_BASE_URL}",
        f"PARTNER_ID={PARTNER_ID}",
        f"REPORT_START_TIMESTAMP={REPORT_START_TIMESTAMP}",
        f"REPORT_END_TIMESTAMP={REPORT_END_TIMESTAMP}",
        f"CATEGORY_ANCESTOR_ID={CATEGORY_ANCESTOR_ID}",
        f"MAX_RESULTS_PER_WINDOW={MAX_RESULTS_PER_WINDOW}",
        f"MEDIA_PAGE_SIZE={MEDIA_PAGE_SIZE}",
        f"CAPTION_PAGE_SIZE={CAPTION_PAGE_SIZE}",
        f"CATEGORY_ENTRY_PAGE_SIZE={CATEGORY_ENTRY_PAGE_SIZE}",
        f"CATEGORY_PAGE_SIZE={CATEGORY_PAGE_SIZE}",
        f"ENTRY_ID_BATCH_SIZE={ENTRY_ID_BATCH_SIZE}",
        f"CATEGORY_ID_BATCH_SIZE={CATEGORY_ID_BATCH_SIZE}",
        f"SLEEP_BETWEEN_CALLS={SLEEP_BETWEEN_CALLS}",
    ]

    print("Starting media count checks and window creation...")
    media_rows, windows = fetch_all_media()

    log_lines.append(f"Safe media windows created: {len(windows)}")

    for number, (start_ts, end_ts, count) in enumerate(windows, start=1):
        log_lines.append(
            f"Window {number}: "
            f"{unix_to_utc_string(start_ts)} through "
            f"{unix_to_utc_string(end_ts)}; "
            f"expected media entries={count}"
        )

    log_lines.append(f"Total unique media rows fetched: {len(media_rows)}")
    print(f"Total unique media rows fetched: {len(media_rows)}")

    write_raw_media_csv(media_rows)

    media_ids = list(dict.fromkeys(
        row.get("id", "")
        for row in media_rows
        if row.get("id", "")
    ))

    log_lines.append(f"Unique media IDs collected: {len(media_ids)}")
    print(f"Unique media IDs collected: {len(media_ids)}")

    print("Starting caption pull...")
    caption_rows = fetch_caption_assets_for_entry_ids(media_ids)

    log_lines.append(f"Total caption rows fetched: {len(caption_rows)}")
    print(f"Total caption rows fetched: {len(caption_rows)}")

    write_raw_caption_csv(caption_rows)

    print("Summarizing caption data...")
    caption_summary = summarize_caption_rows(caption_rows)
    log_lines.append(f"Caption summary rows created: {len(caption_summary)}")

    print("Starting categoryEntry pull...")
    category_entry_rows = fetch_category_entries_for_entry_ids(media_ids)

    log_lines.append(
        f"Total categoryEntry rows fetched: {len(category_entry_rows)}"
    )
    print(f"Total categoryEntry rows fetched: {len(category_entry_rows)}")

    write_raw_category_entry_csv(category_entry_rows)

    category_ids = list(dict.fromkeys(
        row.get("categoryId", "")
        for row in category_entry_rows
        if row.get("categoryId", "")
    ))

    log_lines.append(f"Unique category IDs collected: {len(category_ids)}")
    print(f"Unique category IDs collected: {len(category_ids)}")

    print("Starting category pull...")
    category_rows = fetch_categories_by_ids(category_ids)

    log_lines.append(f"Total category rows fetched: {len(category_rows)}")
    print(f"Total category rows fetched: {len(category_rows)}")

    write_raw_category_csv(category_rows)

    print("Summarizing category data...")
    category_summary = summarize_category_rows(
        category_entry_rows,
        category_rows,
    )
    log_lines.append(f"Category summary rows created: {len(category_summary)}")

    print("Writing final merged report...")
    write_final_report(
        media_rows,
        caption_summary,
        category_summary,
    )

    print("Writing leadership-facing report...")
    write_leadership_report()

    log_lines.extend([
        f"Raw media CSV: {RAW_MEDIA_CSV}",
        f"Raw caption CSV: {RAW_CAPTION_CSV}",
        f"Raw categoryEntry CSV: {RAW_CATEGORY_ENTRY_CSV}",
        f"Raw category CSV: {RAW_CATEGORY_CSV}",
        f"Final report CSV: {FINAL_REPORT_CSV}",
        f"Leadership report CSV: {LEADERSHIP_REPORT_CSV}",
    ])

    write_log(log_lines)

    print("Done.")
    print(f"Raw media CSV: {RAW_MEDIA_CSV}")
    print(f"Raw caption CSV: {RAW_CAPTION_CSV}")
    print(f"Raw categoryEntry CSV: {RAW_CATEGORY_ENTRY_CSV}")
    print(f"Raw category CSV: {RAW_CATEGORY_CSV}")
    print(f"Final report CSV: {FINAL_REPORT_CSV}")
    print(f"Leadership report CSV: {LEADERSHIP_REPORT_CSV}")
    print(f"Log file: {LOG_FILE}")


if __name__ == "__main__":
    main()
