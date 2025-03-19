from KalturaClient import KalturaClient, KalturaConfiguration
from KalturaClient.Plugins.Core import (
    KalturaSessionType, KalturaCategory,
    KalturaCategoryUser, KalturaCategoryUserPermissionLevel,
)

# Session variables - Set these before running the script
PARTNER_ID = ""
ADMIN_SECRET = ""
USER_ID = ""  # Optional; used for the KS (not channel creation)

# Channel variables
PARENT_ID =  # Category ID for the parent of the channel created
PRIVACY_CONTEXT = "MediaSpace"  # Default for MediaSpace at UCSD
CHANNEL_DESCRIPTION = ""
OWNER = ""
CHANNEL_NAME = ""
MEMBERS = ""  # Comma-separated list
MODERATORS = ""   # Comma-separated list
CONTRIBUTORS = ""  # Comma-separated list
USER_JOIN_POLICY = 3  # Not Allowed
APPEAR_IN_LIST = 3    # Category Members Only
INHERITANCE_TYPE = 2  # Manual
DEFAULT_PERMISSION_LEVEL = 3  # Member
CONTRIBUTION_POLICY = 2  # Members with Contribution Permission
MODERATION = 0  # No moderation
CHANNEL_PRIVACY = 3  # Private

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
category.privacy = CHANNEL_PRIVACY  # Set privacy level from global variable
category.userJoinPolicy = USER_JOIN_POLICY
category.appearInList = APPEAR_IN_LIST
category.inheritanceType = INHERITANCE_TYPE
category.defaultPermissionLevel = DEFAULT_PERMISSION_LEVEL
category.contributionPolicy = CONTRIBUTION_POLICY
category.moderation = MODERATION
category.parentId = PARENT_ID
category.privacyContext = PRIVACY_CONTEXT

created_category = client.category.add(category)
print(f"Created channel: {created_category.id} ({created_category.name})")

# Step 2: Add members if provided
if MEMBERS.strip():
    member_list = [m.strip() for m in MEMBERS.split(",") if m.strip()]
    for member in member_list:
        category_user = KalturaCategoryUser()
        category_user.categoryId = created_category.id
        category_user.userId = member
        category_user.permissionLevel = (
            KalturaCategoryUserPermissionLevel.MEMBER
        )
        client.categoryUser.add(category_user)
        print(f"Added member: {member} to channel {created_category.id}")

# Step 3: Add moderators if provided
moderator_list = [
    m.strip() for m in MODERATORS.split(",") if m.strip()
] if MODERATORS.strip() else []

if moderator_list:
    for moderator in moderator_list:
        category_user = KalturaCategoryUser()
        category_user.categoryId = created_category.id
        category_user.userId = moderator
        category_user.permissionLevel = (
            KalturaCategoryUserPermissionLevel.MODERATOR
        )
        client.categoryUser.add(category_user)
        print(
            f"Added moderator: {moderator} to channel {created_category.id}"
        )

# Step 4: Add contributors if provided
contributor_list = [
    m.strip() for m in CONTRIBUTORS.split(",") if m.strip()
] if CONTRIBUTORS.strip() else []

if contributor_list:
    for contributor in contributor_list:
        category_user = KalturaCategoryUser()
        category_user.categoryId = created_category.id
        category_user.userId = contributor
        category_user.permissionLevel = (
            KalturaCategoryUserPermissionLevel.CONTRIBUTOR
        )
        client.categoryUser.add(category_user)
        print(
            f"Added contributor: {contributor} "
            f"to channel {created_category.id}"
        )


print("Script execution complete.")
