from KalturaClient import KalturaClient, KalturaConfiguration
from KalturaClient.Plugins.Core import (
    KalturaFilterPager, KalturaSessionType, KalturaMediaEntryFilter,
    KalturaMediaType
)
import datetime
import csv

# ==== Global Variables ====
PARTNER_ID = ""
ADMIN_SECRET = ""
USER_ID = "admin"
CATEGORY_ID = ""  # Optional; leave blank for no category filter
TAG = "uc san diego podcast"  # Optional; leave blank for no tag filter
START_DATE = "2024-01-01"  # YYYY-MM-DD
END_DATE = "2025-12-31"  # YYYY-MM-DD
EXPORT_CSV = True
# Restriction period for chunking the query to avoid 10K limit:
# 1 = Yearly, 2 = Monthly, 3 = Weekly, 4 = Daily
RESTRICTION_INTERVAL = 2


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
    return datetime.datetime.strptime(date_str, "%Y-%m-%d")


def get_interval_ranges(start_date, end_date, interval_type):
    current = start_date

    while current <= end_date:
        if interval_type == 1:  # Yearly
            next_date = current.replace(month=12, day=31)
        elif interval_type == 2:  # Monthly
            next_date = (
                current.replace(day=28) + datetime.timedelta(days=4)
                ).replace(day=1) - datetime.timedelta(days=1)
        elif interval_type == 3:  # Weekly
            next_date = current + datetime.timedelta(days=6)
        elif interval_type == 4:  # Daily
            next_date = current
        else:
            raise ValueError(
                "Invalid RESTRICTION_INTERVAL value. Use 1=Yearly, 2=Monthly, "
                "3=Weekly, 4=Daily."
                )

        yield current, min(next_date, end_date)
        current = next_date + datetime.timedelta(days=1)


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
        result = client.media.list(filter, pager)
        if not result.objects:
            break
        for entry in result.objects:
            entry_count += 1
            total_duration += entry.duration or 0
            all_entries.append({
                "entryId": entry.id,
                "name": entry.name,
                "duration_sec": entry.duration,
                "created_at": datetime.datetime.fromtimestamp(
                    entry.createdAt
                    ).strftime("%Y-%m-%d")
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

if not START_DATE or not END_DATE:
    print(
        "ERROR: You must set values for START_DATE and END_DATE in the script "
        "(YYYY-MM-DD format)."
        )
    exit(1)

start_date = parse_date(START_DATE).date()
end_date = parse_date(END_DATE).date()

for interval_start, interval_end in get_interval_ranges(
    start_date, end_date, RESTRICTION_INTERVAL
):
    print(
        f"Processing: {interval_start.strftime('%Y-%m-%d')} to "
        f"{interval_end.strftime('%Y-%m-%d')}"
        )
    count, duration, entries = fetch_entries_for_interval(
        datetime.datetime.combine(interval_start, datetime.time.min),
        datetime.datetime.combine(interval_end, datetime.time.max)
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
        4: "day",
    }.get(RESTRICTION_INTERVAL, "custom")

    with open(f"video_summary_by_{interval_label}.csv", "w", newline='') as f:
        writer = csv.DictWriter(
            f, fieldnames=["range", "entry_count", "total_duration_minutes"]
            )
        writer.writeheader()
        writer.writerows(summary)

    with open(f"video_details_by_{interval_label}.csv", "w", newline='') as f:
        writer = csv.DictWriter(
            f, fieldnames=["entryId", "name", "duration_sec", "created_at"]
            )
        writer.writeheader()
        writer.writerows(detailed_entries)
