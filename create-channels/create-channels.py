"""
Bulk Kaltura Channel Creation

Create Kaltura MediaSpace channels in bulk from a CSV. Configure credentials
and environment-specific settings via a .env file. Column header names are
configurable by constants in the script.

Author: Galen Davis
"""


import csv
from urllib.parse import quote_plus
from datetime import datetime
from KalturaClient import KalturaClient, KalturaConfiguration
from KalturaClient.Plugins.Core import (
    KalturaSessionType, KalturaCategory,
    KalturaCategoryUser, KalturaCategoryUserPermissionLevel,
    KalturaCategoryFilter, KalturaFilterPager
)


#
# SESSION & CONFIG (loaded from .env) ---------------------------------------
import os
from dotenv import load_dotenv

# load variables from .env (user must create .env from .env.example)
load_dotenv()

# Credentials / session
PARTNER_ID = os.getenv("PARTNER_ID")
ADMIN_SECRET = os.getenv("ADMIN_SECRET")
USER_ID = os.getenv("USER_ID")

# Channel configuration (from .env); PARENT_ID may be numeric string in env
PARENT_ID = os.getenv("PARENT_ID")
FULL_NAME_PREFIX = os.getenv("FULL_NAME_PREFIX", "MediaSpace>site>channels>")
MEDIA_SPACE_BASE_URL = os.getenv(
    "MEDIA_SPACE_BASE_URL", "https://mediaspace.ucsd.edu/channel/"
    )
PRIVACY_CONTEXT = os.getenv("PRIVACY_CONTEXT", "MediaSpace")
USER_JOIN_POLICY = int(os.getenv("USER_JOIN_POLICY", "3"))
APPEAR_IN_LIST = int(os.getenv("APPEAR_IN_LIST", "3"))
INHERITANCE_TYPE = int(os.getenv("INHERITANCE_TYPE", "2"))
DEFAULT_PERMISSION_LEVEL = int(os.getenv("DEFAULT_PERMISSION_LEVEL", "3"))
CONTRIBUTION_POLICY = int(os.getenv("CONTRIBUTION_POLICY", "2"))
MODERATION = int(os.getenv("MODERATION", "0"))

# CSV header names (customize if your CSV uses different headers)
CHANNEL_NAME_HEADER = os.getenv("CHANNEL_NAME_HEADER", "channelName")
OWNER_ID_HEADER = os.getenv("OWNER_ID_HEADER", "owner")
CHANNEL_MEMBERS_HEADER = os.getenv("CHANNEL_MEMBERS_HEADER", "members")
PRIVACY_SETTING_HEADER = os.getenv("PRIVACY_SETTING_HEADER", "privacy")

# Basic sanity checks for required env variables
if not PARTNER_ID or not ADMIN_SECRET:
    raise ValueError(
        "PARTNER_ID and ADMIN_SECRET must be set in your .env file before "
        "running."
    )

# Convert PARENT_ID to int where used later; keep string now to allow empty
# check. Allow input/output CSV configuration from .env, with sensible defaults
INPUT_CSV = os.getenv("INPUT_CSV_FILENAME", "channelDetails.csv")

# Check if the file actually exists
if not os.path.exists(INPUT_CSV):
    raise FileNotFoundError(
        f"🚨 File '{INPUT_CSV}' not found in directory: {os.getcwd()}"
        )

REPORTS_DIR = "Reports"
os.makedirs(REPORTS_DIR, exist_ok=True)
timestamp = datetime.now().strftime("%Y-%m-%d-T%H%M")
OUTPUT_CSV = os.path.join(
    REPORTS_DIR, f"{timestamp}_report_create-channels.csv"
    )

# INITIALIZE CLIENT -----------------------------------------------------------
config = KalturaConfiguration(PARTNER_ID)
config.serviceUrl = "https://www.kaltura.com/"
client = KalturaClient(config)

ks = client.session.start(
    ADMIN_SECRET,
    USER_ID,
    KalturaSessionType.ADMIN,
    PARTNER_ID,
    privileges="all:*,disableentitlement"
)
client.setKs(ks)


def get_existing_channel_names():
    filter = KalturaCategoryFilter()
    filter.fullNameStartsWith = FULL_NAME_PREFIX
    pager = KalturaFilterPager()
    pager.pageSize = 500
    pager.pageIndex = 1

    existing_names = set()

    while True:
        response = client.category.list(filter, pager)
        for category in response.objects:
            full_path = category.fullName.strip()
            if full_path.startswith(FULL_NAME_PREFIX):
                last_segment = full_path.split(">")[-1].strip()
                existing_names.add(last_segment)
        if len(response.objects) < pager.pageSize:
            break
        pager.pageIndex += 1

    return existing_names


# CHECK FOR DUPLICATE CHANNEL NAMES BEFORE PROCESSING CSV ---------------------
existing_channel_names = get_existing_channel_names()

with open(INPUT_CSV, newline='', encoding='utf-8-sig') as csvfile:
    reader = list(csv.DictReader(csvfile))  # Convert to list to reuse
    required_headers = {
        CHANNEL_NAME_HEADER, OWNER_ID_HEADER, PRIVACY_SETTING_HEADER
        }
    csv_headers = set(reader[0].keys()) if reader else set()
    missing_headers = required_headers - csv_headers
    if missing_headers:
        raise ValueError(
            f"Missing expected column headers in input "
            f"CSV: {', '.join(missing_headers)}"
        )
    duplicate_names = [
        row[CHANNEL_NAME_HEADER].strip()
        for row in reader
        if (
            CHANNEL_NAME_HEADER in row
            and row[CHANNEL_NAME_HEADER].strip() in existing_channel_names
        )
    ]

    if duplicate_names:
        print(
            "🚫 The following channel names already exist and cannot be reused:"
            )
        for name in duplicate_names:
            print(f"  - {name}")
        print(
            "\nNo channels were created. Please update your CSV file to "
            "remove or rename the duplicates and try again."
        )
        exit(1)

    print(f"📄 Using input file: {INPUT_CSV}")

    # Validate all rows before making any changes

    for i, row in enumerate(reader, start=2):
        missing_fields = [
            field_name for field_name, header_key in [
                ("channelName", CHANNEL_NAME_HEADER),
                ("owner", OWNER_ID_HEADER),
                ("privacy", PRIVACY_SETTING_HEADER)
            ]
            if not row.get(header_key, '').strip()
        ]

        if missing_fields:
            channel_preview = row.get(
                CHANNEL_NAME_HEADER, ''
                ).strip() or "<unnamed>"
            raise ValueError(
                f"Row {i}: Missing field(s): {', '.join(missing_fields)} "
                f"(channelName: '{channel_preview}')"
            )

        privacy_raw = row[PRIVACY_SETTING_HEADER].strip()
        if privacy_raw not in ('1', '2', '3'):
            raise ValueError(
                f"Row {i}: Invalid privacy value '{privacy_raw}'. "
                f"Must be 1, 2, or 3."
            )

        members_raw = row.get(CHANNEL_MEMBERS_HEADER, '').strip()
        if not members_raw:
            print(
                f"⚠️  Row {i}: No members specified for channel "
                f"'{row[CHANNEL_NAME_HEADER].strip()}'."
                )

    results = []
    for row in reader:
        channel_name = row[CHANNEL_NAME_HEADER].strip()
        owner = row[OWNER_ID_HEADER].strip()
        privacy_raw = row[PRIVACY_SETTING_HEADER].strip()
        if not privacy_raw:
            raise ValueError(
                f"Missing 'privacy' value for channel "
                f"'{row[CHANNEL_NAME_HEADER]}'. "
                "Please ensure all rows in your CSV include a valid "
                "privacy level (1, 2, or 3)."
            )
        privacy = int(privacy_raw)
        members = [
            m.strip() for m in row[CHANNEL_MEMBERS_HEADER].split(',')
            if m.strip()
            ]

        category = KalturaCategory()
        category.name = channel_name
        category.owner = owner
        category.privacy = privacy
        category.userJoinPolicy = USER_JOIN_POLICY
        category.appearInList = APPEAR_IN_LIST
        category.inheritanceType = INHERITANCE_TYPE
        category.defaultPermissionLevel = DEFAULT_PERMISSION_LEVEL
        category.contributionPolicy = CONTRIBUTION_POLICY
        category.moderation = MODERATION
        category.parentId = int(PARENT_ID)
        category.privacyContext = PRIVACY_CONTEXT

        created_category = client.category.add(category)
        print(
            f"Created channel: {created_category.id} "
            f"({channel_name}) [Owner: {owner}]"
            )

        # Add members
        for member in members:
            category_user = KalturaCategoryUser()
            category_user.categoryId = created_category.id
            category_user.userId = member
            category_user.permissionLevel = (
                KalturaCategoryUserPermissionLevel.MEMBER
            )
            client.categoryUser.add(category_user)
            print(f"  Added member: {member}")

        results.append({
            'channelName': channel_name,
            'categoryId': created_category.id,
            'channelLink': (
                f"{MEDIA_SPACE_BASE_URL}"
                f"{quote_plus(quote_plus(channel_name))}/"
                f"{created_category.id}"
            ),
            'membersAdded': ', '.join(members),
            'owner': owner,
        })


# WRITE RESULTS CSV -----------------------------------------------------------
with open(OUTPUT_CSV, mode='w', newline='', encoding='utf-8') as csvfile:
    fieldnames = [
        'channelName', 'categoryId', 'channelLink', 'membersAdded', 'owner'
        ]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(results)

print(f"\nAll channels created. Results saved to {OUTPUT_CSV}.")
