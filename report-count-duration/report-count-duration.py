"""
Kaltura Report: Entry Count and Duration Summary

Generates a report of Kaltura video entries filtered by owner ID, tag,
and/or category ID, summarized by time intervals (yearly, monthly, weekly,
or daily). Helps avoid Kaltura's 10,000-entry API cap by chunking queries
over time.

Outputs:
- Summary CSV (per interval): entry count and total duration
- Detailed CSV: each entry with ID, name, duration, timestamps, owner ID,
  and original filename

Prompts user for:
- Owner user ID (optional)
- Tag (optional)
- Category ID (optional)
- Start and end dates (optional — defaults to full repository range
  if left blank)
- Restriction interval for chunking (1 = yearly, 2 = monthly, etc.)

Timestamps are formatted in the configured TIMEZONE (default: "US/Pacific").
Set EARLIEST_START_DATE at the top of the script to define the beginning of
your repository timeline.

See README.md for usage instructions and configuration options.
"""

from os import getenv

from KalturaClient import KalturaClient, KalturaConfiguration
from KalturaClient.Plugins.Core import (
    KalturaFilterPager,
    KalturaSessionType,
    KalturaMediaEntryFilter,
    KalturaMediaType,
    KalturaFlavorAssetFilter,
)
from KalturaClient.exceptions import KalturaException
from datetime import datetime, date, timedelta, time
import csv
import pytz
import re
from dotenv import load_dotenv, find_dotenv


# ==== Global Variables ====
# find the .env file and load it
load_dotenv(find_dotenv())

PARTNER_ID = int(getenv("PARTNER_ID"))
ADMIN_SECRET = getenv("ADMIN_SECRET")
USER_ID = getenv("USER_ID")
EXPORT_CSV = bool(getenv("EXPORT_CSV"))
TIMEZONE = getenv("TIMEZONE")
EARLIEST_START_DATE = getenv("EARLIEST_START_DATE")

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
OWNER_ID = input("Enter an owner user ID (optional): ").strip()
TAG = input("Enter a tag (optional): ").strip()
CATEGORY_ID = input("Enter a category ID (optional): ").strip()
# Prompt the user for flavor size
FLAVOR_SIZE = input("Do you want to calculate flavor size? (slower): ").strip()
if FLAVOR_SIZE:
    FLAVOR_SOURCE_NAME = input("Do you want to know the name of the source file? (slower): ").strip()
# Prompt user for date range
start_input = input(
    "Enter a START DATE (YYYY-MM-DD) [press Enter " "to search from the beginning]: "
).strip()
end_input = input("Enter an END DATE (YYYY-MM-DD) [press Enter for today]: ").strip()

# Parse START_DATE
if start_input:
    try:
        START_DATE = datetime.strptime(start_input, "%Y-%m-%d").date()
    except ValueError:
        raise ValueError("START DATE must be in YYYY-MM-DD format.")
else:
    START_DATE = datetime.strptime(EARLIEST_START_DATE, "%Y-%m-%d").date()

# Parse END_DATE
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

# Prompt for a restriction interval
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
    ADMIN_SECRET,
    USER_ID,
    KalturaSessionType.ADMIN,
    PARTNER_ID,
    86400,
    privileges=privileges,
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
            next_date = (current.replace(day=28) + timedelta(days=4)).replace(
                day=1
            ) - timedelta(days=1)
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
    total_flavor_size = 0
    pager = KalturaFilterPager()
    pager.pageSize = 500 # it's the maximum allowed
    pager.pageIndex = 1

    filter = KalturaMediaEntryFilter()
    # filter.mediaTypeEqual = KalturaMediaType(KalturaMediaType.VIDEO) # commented for allowing also other media types
    if OWNER_ID:
        filter.userIdEqual = OWNER_ID
    if CATEGORY_ID:
        filter.categoriesIdsMatchOr = CATEGORY_ID
    if TAG:
        filter.tagsLike = TAG
    filter.createdAtGreaterThanOrEqual = int(start_ts.timestamp())
    filter.createdAtLessThanOrEqual = int(end_ts.timestamp())

    all_entries = []

    # Python does not have a reflection class, so we have to build a reverse-dictionary with the Enum constants
    media_types = {
        value: name for name, value in vars(KalturaMediaType).items() if name.isupper()
    }

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

        print(
            f"Processing page index {pager.pageIndex} that contains {len(result.objects)} entries..."
        )

        for entry in result.objects:
            # Default to None in case there's an error
            original_filename = None
            flavor_count = 0
            flavor_size_sum = 0

            if FLAVOR_SIZE:
                try:
                    # Get flavor assets for this entry
                    flavor_filter = KalturaFlavorAssetFilter()
                    flavor_filter.entryIdEqual = entry.id
                    flavor_list = client.flavorAsset.list(flavor_filter)

                    flavor_count = len(flavor_list.objects)
                    flavor_size_sum = sum(fa.size for fa in flavor_list.objects)

                    # Find the original flavor
                    source_flavor = next(
                        (fa for fa in flavor_list.objects if fa.isOriginal), None
                    )

                    if FLAVOR_SOURCE_NAME and source_flavor:
                        url = client.flavorAsset.getUrl(source_flavor.id)

                        # More flexible regex that matches anything after
                        # /fileName/ up to next /
                        match = re.search(r"/fileName/([^/]+)/", url)

                        if match:
                            raw_filename = match.group(1)
                            original_filename = clean_filename(raw_filename)

                except Exception as e:
                    print(f"Error retrieving filename for entry {entry.id}: {e}")

            else:
                flavor_count = len(entry.flavorParamsIds.split(','))

            all_entries.append(
                {
                    "entryId": entry.id,
                    "name": entry.name,
                    "media_type": media_types[entry.mediaType.getValue()],
                    "duration_sec": entry.duration,
                    "duration": (
                        str(timedelta(seconds=entry.duration))
                        if entry.duration
                        else "0:00:00"
                    ),
                    "created_at": datetime.fromtimestamp(entry.createdAt, tz=pytz.utc)
                    .astimezone(local_tz)
                    .strftime("%Y-%m-%d %H:%M:%S"),
                    "updated_at": datetime.fromtimestamp(entry.updatedAt, tz=pytz.utc)
                    .astimezone(local_tz)
                    .strftime("%Y-%m-%d %H:%M:%S"),
                    "lastplayed_at": (
                        datetime.fromtimestamp(entry.lastPlayedAt, tz=pytz.utc)
                        .astimezone(local_tz)
                        .strftime("%Y-%m-%d %H:%M:%S")
                        if entry.lastPlayedAt is not None
                        else None
                    ),
                    "plays": entry.plays,
                    "categories": entry.categories.replace(",", ";"),
                    "tags": entry.tags.replace(",", ";"),
                    "owner_id": entry.userId,
                    "original_filename": original_filename,
                    "flavor_count": str(flavor_count),
                    "flavor_size_sum": str(
                        round(flavor_size_sum / 1024, 2)
                    ),  # MegaBytes (Kaltura returns KBytes)
                }
            )

            # calculate outputs of the method
            entry_count += 1
            total_duration += entry.duration or 0
            total_flavor_size += flavor_size_sum

        pager.pageIndex += 1

        if entry_count >= 10000:
            print("\nWARNING: Entry count reached Kaltura's 10,000 limit.")
            print("Results for this time range may be incomplete.")
            print(
                "Try increasing the value of the RESTRICTION_INTERVAL "
                "variable to reduce the size of each API query."
            )
            exit(1)

    return entry_count, total_duration, all_entries, total_flavor_size


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
    count, duration, entries, flavor_size_sum = fetch_entries_for_interval(
        datetime.combine(interval_start, time.min),
        datetime.combine(interval_end, time.max),
    )

    label = (
        f"{interval_start.strftime('%Y-%m-%d')} to "
        f"{interval_end.strftime('%Y-%m-%d')}"
    )

    summary.append(
        {
            "range": label,
            "entry_count": count,
            "total_duration_minutes": round(duration / 60, 2),
            "flavor_size_sum": round(
                flavor_size_sum / 1024, 2
            ),  # MegaBytes (Kaltura returns KBytes)
        }
    )
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
total_years = total_days / 365.25  # Accounting for leap years
total_flavor_size_sum = (
    sum(row["flavor_size_sum"] for row in summary) / 1024
)  # GigaBytes since row["flavor_size_sum"] is expressed in MegaBytes

print("\nTotals")
print("-" * 35)
print(f"{'Entries:':<20}{total_entries:>15,}")
print(f"{'Duration (mins):':<20}{total_minutes:>15,.2f}")
print(f"{'Duration (hours):':<20}{total_hours:>15,.2f}")
print(f"{'Duration (days):':<20}{total_days:>15,.2f}")
print(f"{'Duration (months):':<20}{total_months:>15,.2f}")
print(f"{'Duration (years):':<20}{total_years:>15,.2f}")
print(f"{'Flavor Size (GB):':<20}{total_flavor_size_sum:>15,.2f}")


# ==== CSV Export ====
if EXPORT_CSV:
    interval_label = {1: "year", 2: "month", 3: "week", 4: "day"}.get(
        RESTRICTION_INTERVAL, "custom"
    )

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Sanitize tag/category text for filenames
    owner_label = OWNER_ID if OWNER_ID else "noOwner"
    tag_label = TAG.replace(" ", "_") if TAG else "noTag"
    cat_label = CATEGORY_ID if CATEGORY_ID else "noCategory"

    summary_filename = (
        "video_summary_"
        f"{PARTNER_ID}_{tag_label}_{cat_label}_{owner_label}_"
        f"{interval_label}_{timestamp}.csv"
    )
    details_filename = (
        "video_details_"
        f"{PARTNER_ID}_{tag_label}_{cat_label}_{owner_label}_"
        f"{interval_label}_{timestamp}.csv"
    )

    with open(summary_filename, "w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "range",
                "entry_count",
                "total_duration_minutes",
                "flavor_size_sum",
            ],
        )
        writer.writeheader()
        writer.writerows(summary)

    with open(details_filename, "w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "entryId",
                "name",
                "duration_sec",
                "duration",
                "media_type",
                "created_at",
                "updated_at",
                "lastplayed_at",
                "plays",
                "categories",
                "tags",
                "owner_id",
                "original_filename",
                "flavor_count",
                "flavor_size_sum",
            ],
        )

        writer.writeheader()
        writer.writerows(detailed_entries)

    print("\nCSV files created:")
    print(f"  - {summary_filename}")
    print(f"  - {details_filename}")
