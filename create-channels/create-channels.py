"""
Bulk Kaltura Channel Creation

Create Kaltura MediaSpace channels in bulk from a CSV. Configure credentials
and environment-specific settings via a .env file. Column header names are
configurable via environment variables.

Author: Galen Davis
"""

import csv
import getpass
import os
import traceback
from datetime import datetime
from pathlib import Path
from urllib.parse import quote_plus

from dotenv import load_dotenv
from KalturaClient import KalturaClient, KalturaConfiguration
from KalturaClient.Plugins.Core import (
    KalturaCategory,
    KalturaCategoryFilter,
    KalturaCategoryUser,
    KalturaCategoryUserPermissionLevel,
    KalturaFilterPager,
    KalturaSessionType,
)

load_dotenv(dotenv_path=Path(__file__).with_name(".env"))

# ---------- Configuration from .env ----------
PARTNER_ID = os.getenv("PARTNER_ID", "")
USER_ID = os.getenv("USER_ID", "")
SERVICE_URL = os.getenv("SERVICE_URL", "https://www.kaltura.com/")

PARENT_ID = os.getenv("PARENT_ID", "")
FULL_NAME_PREFIX = os.getenv(
    "FULL_NAME_PREFIX", "MediaSpace>site>channels>"
)
MEDIA_SPACE_BASE_URL = os.getenv("MEDIA_SPACE_BASE_URL", "")
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
INPUT_CSV = os.getenv("INPUT_CSV_FILENAME", "channelDetails.csv")


def get_existing_channel_names(client):
    cat_filter = KalturaCategoryFilter()
    cat_filter.fullNameStartsWith = FULL_NAME_PREFIX
    pager = KalturaFilterPager()
    pager.pageSize = 500
    pager.pageIndex = 1

    existing_names = set()

    while True:
        response = client.category.list(cat_filter, pager)
        for category in response.objects:
            full_path = category.fullName.strip()
            if full_path.startswith(FULL_NAME_PREFIX):
                last_segment = full_path.split(">")[-1].strip()
                existing_names.add(last_segment)
        if len(response.objects) < pager.pageSize:
            break
        pager.pageIndex += 1

    return existing_names


def main():
    if not PARTNER_ID:
        print("Error: PARTNER_ID not set in your .env file.")
        return
    if not PARENT_ID:
        print("Error: PARENT_ID not set in your .env file.")
        return
    if not MEDIA_SPACE_BASE_URL:
        print("Error: MEDIA_SPACE_BASE_URL not set in your .env file.")
        return
    if not os.path.exists(INPUT_CSV):
        print(
            f"Error: input file '{INPUT_CSV}' not found"
            f" in {os.getcwd()}."
        )
        return

    admin_secret = getpass.getpass("Enter your Kaltura admin secret: ")
    if not admin_secret:
        print("Error: Admin secret cannot be empty.")
        return

    timestamp = datetime.now().strftime("%Y-%m-%d-%H%M")
    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)
    output_csv = os.path.join(
        output_dir, f"{timestamp}_report_create-channels.csv"
    )

    config = KalturaConfiguration(PARTNER_ID)
    config.serviceUrl = SERVICE_URL
    client = KalturaClient(config)
    ks = client.session.start(
        admin_secret,
        USER_ID,
        KalturaSessionType.ADMIN,
        PARTNER_ID,
        privileges="all:*,disableentitlement",
    )
    client.setKs(ks)

    existing_channel_names = get_existing_channel_names(client)

    with open(INPUT_CSV, newline='', encoding='utf-8-sig') as csvfile:
        reader = list(csv.DictReader(csvfile))
        required_headers = {
            CHANNEL_NAME_HEADER, OWNER_ID_HEADER, PRIVACY_SETTING_HEADER
        }
        csv_headers = set(reader[0].keys()) if reader else set()
        missing_headers = required_headers - csv_headers
        if missing_headers:
            print(
                "Error: missing column headers in input CSV:"
                f" {', '.join(missing_headers)}"
            )
            return

        duplicate_names = [
            row[CHANNEL_NAME_HEADER].strip()
            for row in reader
            if (
                CHANNEL_NAME_HEADER in row
                and row[CHANNEL_NAME_HEADER].strip()
                in existing_channel_names
            )
        ]

        if duplicate_names:
            print(
                "🚫 The following channel names already exist and cannot"
                " be reused:"
            )
            for name in duplicate_names:
                print(f"  - {name}")
            print(
                "\nNo channels were created. Please update your CSV file"
                " to remove or rename the duplicates and try again."
            )
            return

        print(f"📄 Using input file: {INPUT_CSV}")

        # Validate all rows before making any changes
        for i, row in enumerate(reader, start=2):
            missing_fields = [
                field_name for field_name, header_key in [
                    ("channelName", CHANNEL_NAME_HEADER),
                    ("owner", OWNER_ID_HEADER),
                    ("privacy", PRIVACY_SETTING_HEADER),
                ]
                if not row.get(header_key, '').strip()
            ]

            if missing_fields:
                channel_preview = (
                    row.get(CHANNEL_NAME_HEADER, '').strip()
                    or "<unnamed>"
                )
                print(
                    f"Error: row {i}: missing field(s):"
                    f" {', '.join(missing_fields)}"
                    f" (channelName: '{channel_preview}')"
                )
                return

            privacy_raw = row[PRIVACY_SETTING_HEADER].strip()
            if privacy_raw not in ('1', '2', '3'):
                print(
                    f"Error: row {i}: invalid privacy value"
                    f" '{privacy_raw}'. Must be 1, 2, or 3."
                )
                return

            members_raw = row.get(CHANNEL_MEMBERS_HEADER, '').strip()
            if not members_raw:
                print(
                    f"⚠️  Row {i}: no members specified for channel"
                    f" '{row[CHANNEL_NAME_HEADER].strip()}'."
                )

        if '/channel/' in MEDIA_SPACE_BASE_URL:
            link_base_url = MEDIA_SPACE_BASE_URL
        else:
            link_base_url = (
                MEDIA_SPACE_BASE_URL.rstrip('/') + '/channel/'
            )

        results = []
        try:
            for row in reader:
                channel_name = row[CHANNEL_NAME_HEADER].strip()
                owner = row[OWNER_ID_HEADER].strip()
                privacy_raw = row[PRIVACY_SETTING_HEADER].strip()
                if not privacy_raw:
                    print(
                        f"Error: missing 'privacy' value for channel"
                        f" '{row[CHANNEL_NAME_HEADER]}'."
                        " Ensure all rows include a valid privacy"
                        " level (1, 2, or 3)."
                    )
                    return
                privacy = int(privacy_raw)
                members = [
                    m.strip()
                    for m in row.get(CHANNEL_MEMBERS_HEADER, '').split(',')
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
                    f"Created channel: {created_category.id}"
                    f" ({channel_name}) [Owner: {owner}]"
                )

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
                        f"{link_base_url}"
                        f"{quote_plus(quote_plus(channel_name))}/"
                        f"{created_category.id}"
                    ),
                    'membersAdded': ', '.join(members),
                    'owner': owner,
                })
        finally:
            # Always write whatever was completed, even if an error
            # interrupted the run.
            if results:
                with open(
                    output_csv, mode='w', newline='', encoding='utf-8'
                ) as out:
                    fieldnames = [
                        'channelName', 'categoryId', 'channelLink',
                        'membersAdded', 'owner'
                    ]
                    writer = csv.DictWriter(out, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(results)
                print(f"\nResults saved to {output_csv}.")

    print("\nAll channels created successfully.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("✖ Unhandled error:", e)
        traceback.print_exc()
