"""
Kaltura Report - Entry Count and Duration Summary

This script retrieves Kaltura video entries filtered by tag and/or category,
and generates a report showing entry counts and total durations, broken out
by time intervals (yearly, monthly, weekly, or daily). It's designed to
work around Kaltura's 10,000-entry API cap by chunking queries over the time
period specified by the user at runtime.

Outputs:
- Summary CSV: entry count and total duration per time chunk
- Detailed CSV: individual entries including ID, name, duration
  (in seconds and HH:MM:SS), creation date, update date, owner ID, and
  source filename
- Onscreen totals are displayed in minutes, hours, days, months, and years
- Console output confirms creation of both CSV files

Prompts the user at runtime for:
- Tag (optional)
- Category ID (optional)
- Start date and end date
- Chunking interval (1 = yearly, 2 = monthly, 3 = weekly, 4 = daily)

Timestamps in the detailed CSV are formatted in the time zone specified by the
global TIMEZONE variable (default is "US/Pacific"). You can change this at the
top of the script to match your region. Common options include:
    - "US/Eastern"
    - "US/Central"
    - "US/Mountain"
    - "US/Pacific"
    - "US/Alaska"
    - "US/Hawaii"

See the full list of supported values here:
https://en.wikipedia.org/wiki/List_of_tz_database_time_zones

If the result set for any chunk exceeds Kaltura's maximum match threshold,
the script will exit gracefully and suggest using a smaller interval.

Author: Galen Davis, UC San Diego
Last updated: 20 March 2025
"""

from KalturaClient import KalturaClient, KalturaConfiguration
from KalturaClient.Plugins.Core import (
    KalturaFilterPager, KalturaSessionType, KalturaMediaEntryFilter,
    KalturaMediaType, KalturaFlavorAssetFilter
)
from KalturaClient.exceptions import KalturaException
from datetime import datetime, date, timedelta, time
import csv
import pytz
import re


# ==== Global Variables ====
PARTNER_ID = ""
ADMIN_SECRET = ""
USER_ID = ""
EXPORT_CSV = True
# Set your desired timezone (e.g., US/Pacific, US/Eastern, US/Central)
TIMEZONE = "US/Pacific"

# Set the timezone object based on the configured string
local_tz = pytz.timezone(TIMEZONE)


# Helper function to clean up filenames for export
def clean_filename(filename):
    # Remove trailing " (Source)" with optional extra spaces before it
    cleaned = re.sub(r"\s*\(Source\)", "", filename)
    # Remove any trailing underscores before ".mp4"
    cleaned = re.sub(r"_*\.mp4$", ".mp4", cleaned)
    return cleaned.strip()


# Prompt the user for query parameters
TAG = input("Enter a tag (optional): ").strip()
CATEGORY_ID = input("Enter a category ID (optional): ").strip()
# Prompt user for date range
start_input = input("Enter a START DATE (YYYY-MM-DD): ").strip()
end_input = input(
    "Enter an END DATE (YYYY-MM-DD) [press Enter for today]: "
    ).strip()

# Parse dates
try:
    START_DATE = datetime.strptime(start_input, "%Y-%m-%d").date()
except ValueError:
    raise ValueError("START DATE must be in YYYY-MM-DD format.")

if end_input:
    try:
        END_DATE = datetime.strptime(end_input, "%Y-%m-%d").date()
    except ValueError:
        raise ValueError("END DATE must be in YYYY-MM-DD format.")
else:
    END_DATE = date.today()

# Validate date range early
if END_DATE < START_DATE:
    raise ValueError("END DATE cannot be earlier than START DATE.")

# Prompt for restriction interval
print("\nSelect a restriction interval to chunk the query:")
print("1 = Yearly, 2 = Monthly, 3 = Weekly, 4 = Daily")
interval_input = input(
    "Enter a number for RESTRICTION_INTERVAL [default is 2]: "
    ).strip()
RESTRICTION_INTERVAL = int(interval_input) if interval_input else 2

# ==== Initialize Kaltura Client ====
config = KalturaConfiguration()
config.serviceUrl = "https://www.kaltura.com"
client = KalturaClient(config)

privileges = "all:*,disableentitlement"
ks = client.session.start(
    ADMIN_SECRET, USER_ID, KalturaSessionType.ADMIN, PARTNER_ID, 86400,
    privileges="all:*,disableentitlement"
    )
client.setKs(ks)


# ==== Helper Functions ====
def parse_date(date_str):
    return datetime.strptime(date_str, "%Y-%m-%d")


def get_interval_ranges(start_date, end_date, interval_type):
    current = start_date

    while current <= end_date:
        if interval_type == 1:  # Yearly
            next_date = current.replace(month=12, day=31)
        elif interval_type == 2:  # Monthly
            next_date = (
                current.replace(day=28) + timedelta(days=4)
                ).replace(day=1) - timedelta(days=1)
        elif interval_type == 3:  # Weekly
            next_date = current + timedelta(days=6)
        elif interval_type == 4:  # Daily
            next_date = current
        else:
            raise ValueError(
                "Invalid RESTRICTION_INTERVAL value. Use 1=Yearly, 2=Monthly, "
                "3=Weekly, 4=Daily."
                )

        yield current, min(next_date, end_date)
        current = next_date + timedelta(days=1)


def fetch_entries_for_interval(start_ts, end_ts):
    total_duration = 0
    entry_count = 0
    pager = KalturaFilterPager()
    pager.pageSize = 500
    pager.pageIndex = 1

    filter = KalturaMediaEntryFilter()
    filter.mediaTypeEqual = KalturaMediaType.VIDEO
    if CATEGORY_ID:
        filter.categoriesIdsMatchOr = CATEGORY_ID
    if TAG:
        filter.tagsLike = TAG
    filter.createdAtGreaterThanOrEqual = int(start_ts.timestamp())
    filter.createdAtLessThanOrEqual = int(end_ts.timestamp())

    all_entries = []

    while True:
        try:
            result = client.media.list(filter, pager)
        except KalturaException as e:
            if e.code == "QUERY_EXCEEDED_MAX_MATCHES_ALLOWED":
                print(
                    "\nERROR: Kaltura refused to execute the query "
                    "because it exceeds the 10,000 match limit."
                    )
                print(
                    "Try increasing the RESTRICTION_INTERVAL value "
                    "(e.g., 3 = Weekly or 4 = Daily) to reduce the size "
                    "of each time chunk."
                )
                exit(1)
            else:
                raise

        if not result.objects:
            break

        for entry in result.objects:
            entry_count += 1
            total_duration += entry.duration or 0

            created_at_str = datetime.fromtimestamp(
                entry.createdAt, tz=pytz.utc
            ).astimezone(local_tz).strftime("%Y-%m-%d %H:%M:%S")

            updated_at_str = datetime.fromtimestamp(
                entry.updatedAt, tz=pytz.utc
            ).astimezone(local_tz).strftime("%Y-%m-%d %H:%M:%S")

            # Default to None in case there's an error
            original_filename = None

            try:
                # Get flavor assets for this entry
                flavor_filter = KalturaFlavorAssetFilter()
                flavor_filter.entryIdEqual = entry.id
                flavor_list = client.flavorAsset.list(flavor_filter)

                # Find the original flavor
                source_flavor = next(
                    (fa for fa in flavor_list.objects if fa.isOriginal), None
                )

                if source_flavor:
                    url = client.flavorAsset.getUrl(source_flavor.id)

                    # More flexible regex that matches anything after
                    # /fileName/ up to next /
                    match = re.search(r"/fileName/([^/]+)/", url)

                    if match:
                        raw_filename = match.group(1)
                        original_filename = clean_filename(raw_filename)

            except Exception as e:
                # Optional: log error for edge cases
                print(f"Error retrieving filename for entry {entry.id}: {e}")
                original_filename = None

            all_entries.append({
                "entryId": entry.id,
                "name": entry.name,
                "duration_sec": entry.duration,
                "duration": (
                    str(timedelta(seconds=entry.duration))
                    if entry.duration else "0:00:00"
                ),
                "created_at": created_at_str,
                "updated_at": updated_at_str,
                "owner_id": entry.userId,
                "original_filename": original_filename,
            })

        pager.pageIndex += 1

        if entry_count >= 10000:
            print("\nWARNING: Entry count reached Kaltura's 10,000 limit.")
            print("Results for this time range may be incomplete.")
            print(
                "Try increasing the value of the RESTRICTION_INTERVAL "
                "variable to reduce the size of each API query."
            )
            exit(1)

    return entry_count, total_duration, all_entries


# ==== Main Execution ====
summary = []
detailed_entries = []

start_date = START_DATE
end_date = END_DATE

for interval_start, interval_end in get_interval_ranges(
    start_date, end_date, RESTRICTION_INTERVAL
):
    print(
        f"Processing: {interval_start.strftime('%Y-%m-%d')} to "
        f"{interval_end.strftime('%Y-%m-%d')}"
    )
    count, duration, entries = fetch_entries_for_interval(
        datetime.combine(interval_start, time.min),
        datetime.combine(interval_end, time.max)
    )

    label = (
        f"{interval_start.strftime('%Y-%m-%d')} to "
        f"{interval_end.strftime('%Y-%m-%d')}"
    )

    summary.append({
        "range": label,
        "entry_count": count,
        "total_duration_minutes": round(duration / 60, 2)
    })
    detailed_entries.extend(entries)


# ==== Output Summary ====
print("\n--- Summary by Time Chunk ---")
for row in summary:
    print(
        f"{row['range']}: {row['entry_count']:,} entries, "
        f"{row['total_duration_minutes']:,.2f} minutes"
        )

# ==== Final Totals ====
total_entries = sum(row["entry_count"] for row in summary)
total_minutes = sum(row["total_duration_minutes"] for row in summary)
total_hours = total_minutes / 60
total_days = total_hours / 24
total_months = total_days / 30.4375  # Avg. Gregorian month
total_years = total_days / 365.25   # Accounting for leap years

print("\nTotals")
print("-" * 35)
print(f"{'Entries:':<20}{total_entries:>15,}")
print(f"{'Duration (mins):':<20}{total_minutes:>15,.2f}")
print(f"{'Duration (hours):':<20}{total_hours:>15,.2f}")
print(f"{'Duration (days):':<20}{total_days:>15,.2f}")
print(f"{'Duration (months):':<20}{total_months:>15,.2f}")
print(f"{'Duration (years):':<20}{total_years:>15,.2f}")


# ==== CSV Export ====
if EXPORT_CSV:
    interval_label = {
        1: "year",
        2: "month",
        3: "week",
        4: "day"
    }.get(RESTRICTION_INTERVAL, "custom")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Sanitize tag/category text for filenames
    tag_label = TAG.replace(" ", "_") if TAG else "noTag"
    cat_label = CATEGORY_ID if CATEGORY_ID else "noCategory"

    summary_filename = (
        "video_summary_"
        f"{tag_label}_{cat_label}_{interval_label}_{timestamp}.csv"
    )
    details_filename = (
        "video_details_"
        f"{tag_label}_{cat_label}_{interval_label}_{timestamp}.csv"
    )

    with open(summary_filename, "w", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=["range", "entry_count", "total_duration_minutes"]
            )
        writer.writeheader()
        writer.writerows(summary)

    with open(details_filename, "w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "entryId", "name", "duration_sec", "duration",
                "created_at", "updated_at", "owner_id", "original_filename"
                ]
        )

        writer.writeheader()
        writer.writerows(detailed_entries)

print("\nCSV files created:")
print(f"  - {summary_filename}")
print(f"  - {details_filename}")
