"""
Bulk Kaltura Channel Creation Script

This script reads a CSV file ('channelDetails.csv') containing channel
metadata and creates channels in bulk using the Kaltura API. Each channel is
added as a child of a specified parent category (PARENT_ID), and members are
granted member-level access (can view content but cannot contribute or
moderate). The script checks for duplicate channel names and validates all
input rows before attempting creation.

Required global variables to set before running:
- PARTNER_ID: (int) Your Kaltura partner ID.
- ADMIN_SECRET: (str) The admin secret for your Kaltura account.
- USER_ID: (str) Optional, used to generate the KS.
- PARENT_ID: (int) The category ID under which new channels will be created.
  This must point to a category you have permission to write to.

CSV input format (channelDetails.csv):
- Required headers: channelName, owner, members, privacy
  - members: Comma-separated user IDs (wrap in quotes if multiple)
  - privacy:
    1 = Public (Anyone can view)
    2 = Authenticated Users (Only logged-in users can view)
    3 = Private (Only members can view)

The script generates a results CSV file with a timestamped filename,
containing:
  - channelName
  - categoryId (Kaltura's internal ID for the created category)
  - channelLink (direct MediaSpace link to the channel)
  - membersAdded (comma-separated list)
  - owner

By default, the script sets:
  - userJoinPolicy = Not Allowed
  - appearInList = Category Members Only
  - privacyContext = MediaSpace
  - Validates all rows and blocks execution if required fields are missing or
    invalid

These are configurable via global variables.

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


# SESSION CONFIGURATION -------------------------------------------------------
PARTNER_ID = ""  # Set your partner ID
ADMIN_SECRET = ""  # Set your admin secret
USER_ID = ""  # Optional; used for session

# CHANNEL CONFIGURATION VARIABLES ---------------------------------------------
PARENT_ID = ""  # category ID of the parent category for created channels
FULL_NAME_PREFIX = ""  # e.g. "MediaSpace>site>channels>"
MEDIA_SPACE_BASE_URL = ""  # e.g. "https://mediaspace.ucsd.edu/channel/"
PRIVACY_CONTEXT = "MediaSpace"
USER_JOIN_POLICY = 3
APPEAR_IN_LIST = 3
INHERITANCE_TYPE = 2
DEFAULT_PERMISSION_LEVEL = 3
CONTRIBUTION_POLICY = 2
MODERATION = 0

if PARENT_ID is None:
    raise ValueError(
        "PARENT_ID is not set. Please provide the numeric ID of the parent "
        "category under which new channels will be created. This is usually "
        "the 'channels' category in your MediaSpace/Kaltura instance, but it "
        "may vary depending on your configuration. You can find this ID by "
        "browsing categories in the KMC or contacting your Kaltura admin."
    )


INPUT_CSV = "channelDetails.csv"
timestamp = datetime.now().strftime("%Y-%m-%d-T%H%M")
OUTPUT_CSV = f"channelCreationResults_{timestamp}.csv"

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

with open(INPUT_CSV, newline='', encoding='utf-8') as csvfile:
    reader = list(csv.DictReader(csvfile))  # Convert to list to reuse
    duplicate_names = [
        row['channelName'].strip()
        for row in reader
        if row['channelName'].strip() in existing_channel_names
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

    print("✅ No duplicate channel names found. Proceeding...\n")

    # Validate all rows before making any changes
    print("🔎 Validating CSV data...")

    for i, row in enumerate(reader, start=2):
        missing_fields = [
            field for field in ('channelName', 'owner', 'privacy')
            if not row.get(field, '').strip()
        ]

        if missing_fields:
            channel_preview = row.get('channelName', '').strip() or "<unnamed>"
            raise ValueError(
                f"Row {i}: Missing field(s): {', '.join(missing_fields)} "
                f"(channelName: '{channel_preview}')"
            )

        privacy_raw = row['privacy'].strip()
        if privacy_raw not in ('1', '2', '3'):
            raise ValueError(
                f"Row {i}: Invalid privacy value '{privacy_raw}'. "
                f"Must be 1, 2, or 3."
            )

        members_raw = row.get('members', '').strip()
        if not members_raw:
            print(
                f"⚠️  Row {i}: No members specified for channel "
                f"'{row['channelName'].strip()}'."
                )

    print("✅ All CSV rows validated. Proceeding with channel creation...\n")

    results = []
    for row in reader:
        channel_name = row['channelName'].strip()
        owner = row['owner'].strip()
        privacy_raw = row['privacy'].strip()
        if not privacy_raw:
            raise ValueError(
                f"Missing 'privacy' value for channel '{row['channelName']}'. "
                "Please ensure all rows in your CSV include a valid "
                "privacy level (1, 2, or 3)."
            )
        privacy = int(privacy_raw)
        members = [m.strip() for m in row['members'].split(',') if m.strip()]

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
