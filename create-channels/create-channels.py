"""
Bulk Kaltura Channel Creation Script

This script reads a CSV file ('channelDetails.csv') containing channel
metadata and creates channels in bulk using the Kaltura API. Each channel
is added as a child of a specified parent category (PARENT_ID), and members
are granted member-level access (can view content but cannot contribute or
moderate).

Required global variables to set before running:
- PARTNER_ID: (int) Your Kaltura partner ID.
- ADMIN_SECRET: (str) The admin secret for your Kaltura account.
- USER_ID: (str) Optional, used to generate the KS.
- PARENT_ID: (int) The category ID under which new channels will be created.
  This must point to a category you have permission to write to.
- `MEDIA_SPACE_BASE_URL`: The base URL of your MediaSpace instance, ending in `/channel/`.
  Example: `https://mediaspace.ucsd.edu/channel/`

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
)

# Session configuration
PARTNER_ID = ""  # Set your partner ID
ADMIN_SECRET = ""  # Set your admin secret
USER_ID = ""  # Optional; used for session


# Channel configuration constants; change as desired
PARENT_ID = None  # Category ID for parent folder (no quotes required)
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
MEDIA_SPACE_BASE_URL = "https://mediaspace.ucsd.edu/channel/"

# Initialize client
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

# Read input CSV
with open(INPUT_CSV, newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    results = []
    for row in reader:
        channel_name = row['channelName'].strip()
        owner = row['owner'].strip()
        privacy = int(row['privacy'].strip())
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
        category.parentId = PARENT_ID
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


# Write results CSV
with open(OUTPUT_CSV, mode='w', newline='', encoding='utf-8') as csvfile:
    fieldnames = [
        'channelName', 'categoryId', 'channelLink', 'membersAdded', 'owner'
        ]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(results)

print(f"\nAll channels created. Results saved to {OUTPUT_CSV}.")
