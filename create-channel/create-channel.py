"""
Creates a new MediaSpace channel (Kaltura category) with specified properties.
Optionally assigns members, moderators, and contributors by user ID.
"""

from KalturaClient import KalturaClient, KalturaConfiguration
from KalturaClient.Plugins.Core import (
    KalturaSessionType, KalturaCategory,
    KalturaCategoryUser, KalturaCategoryUserPermissionLevel
)
import urllib

# Session Variables - Set these before running the script
PARTNER_ID = ""
ADMIN_SECRET = ""
USER_ID = ""

# Channel variables
MEDIA_SPACE_URL = ""  #  E.g "https://mediaspace.ucsd.edu"
PARENT_ID = None  # Default parent category ID for MediaSpace channels
PRIVACY_CONTEXT = "MediaSpace"  # Default privacy context for MediaSpace
CHANNEL_DESCRIPTION = ""
CHANNEL_NAME = ""
OWNER = ""
MEMBERS = ""  # Comma-separated list of user IDs (e.g., "user1,user2,user3")
CHANNEL_PRIVACY = 3
USER_JOIN_POLICY = 3  # Not Allowed
APPEAR_IN_LIST = 3    # Category Members Only
INHERITANCE_TYPE = 2  # Manual
DEFAULT_PERMISSION_LEVEL = 3  # Member
CONTRIBUTION_POLICY = 2  # Members with Contribution Permission
MODERATION = 0  # No moderation

# Initialize Kaltura client
config = KalturaConfiguration(PARTNER_ID)
config.serviceUrl = "https://www.kaltura.com/"
client = KalturaClient(config)

# Start a session with full permissions
ks = client.session.start(
    ADMIN_SECRET,
    USER_ID,
    KalturaSessionType.ADMIN,
    PARTNER_ID,
    privileges="all:*,disableentitlement"
)
client.setKs(ks)

# Step 1: Create the private channel
category = KalturaCategory()
category.name = CHANNEL_NAME
category.description = CHANNEL_DESCRIPTION
category.owner = OWNER
category.privacy = CHANNEL_PRIVACY
category.userJoinPolicy = USER_JOIN_POLICY
category.appearInList = APPEAR_IN_LIST
category.inheritanceType = INHERITANCE_TYPE
category.defaultPermissionLevel = DEFAULT_PERMISSION_LEVEL
category.contributionPolicy = CONTRIBUTION_POLICY
category.moderation = MODERATION
category.parentId = PARENT_ID
category.privacyContext = PRIVACY_CONTEXT  # Ensure appearInList works

created_category = client.category.add(category)
# Encode channel name for URL
encoded_name = urllib.parse.quote(created_category.name).replace(" ", "+")
double_encoded_name = urllib.parse.quote(encoded_name)
channel_url = (
    f"{MEDIA_SPACE_URL}/channel/{double_encoded_name}/"
    f"{created_category.id}"
)


# Step 2: Prepare member list and add members if provided
member_list = (
    [m.strip() for m in MEMBERS.split(",") if m.strip()]
    if MEMBERS.strip() else []
)

for member in member_list:
    category_user = KalturaCategoryUser()
    category_user.categoryId = created_category.id
    category_user.userId = member
    category_user.permissionLevel = KalturaCategoryUserPermissionLevel.MEMBER
    client.categoryUser.add(category_user)
    print(f"Added member: {member} to channel {created_category.id}")

# Format and print output
print("Channel created.\n-----------------")
print(f"{'Channel Name:':20} {created_category.name}")
print(f"{'Category ID:':20} {created_category.id}")
print(f"{'Channel Owner:':20} {created_category.owner}")
print(
    f"{'Channel Members:':20} "
    f"{', '.join(member_list) if member_list else 'None'}"
)
print(f"{'Channel URL:':20} {channel_url}")

print("Script execution complete.")
