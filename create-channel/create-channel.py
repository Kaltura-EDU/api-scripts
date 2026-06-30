"""
Creates a new MediaSpace channel (Kaltura category) with specified
properties. Optionally assigns members at time of creation.
"""

import getpass
import os
import traceback
import urllib.parse
from pathlib import Path

from dotenv import load_dotenv
from KalturaClient import KalturaClient, KalturaConfiguration
from KalturaClient.Plugins.Core import (
    KalturaCategory,
    KalturaCategoryUser,
    KalturaCategoryUserPermissionLevel,
    KalturaSessionType,
)

load_dotenv(dotenv_path=Path(__file__).with_name(".env"))

# ---------- Configuration from .env ----------
PARTNER_ID = os.getenv("PARTNER_ID", "")
USER_ID = os.getenv("USER_ID", "")

MEDIA_SPACE_URL = os.getenv("MEDIA_SPACE_URL", "")
PARENT_ID_RAW = os.getenv("PARENT_ID")
PARENT_ID = int(PARENT_ID_RAW) if PARENT_ID_RAW else None

PRIVACY_CONTEXT = os.getenv("PRIVACY_CONTEXT", "MediaSpace")
CHANNEL_DESCRIPTION = os.getenv("CHANNEL_DESCRIPTION", "")
CHANNEL_NAME = os.getenv("CHANNEL_NAME", "")
OWNER = os.getenv("OWNER", "")
MEMBERS = os.getenv("MEMBERS", "")

CHANNEL_PRIVACY = int(os.getenv("CHANNEL_PRIVACY", 3))
USER_JOIN_POLICY = int(os.getenv("USER_JOIN_POLICY", 3))
APPEAR_IN_LIST = int(os.getenv("APPEAR_IN_LIST", 3))
INHERITANCE_TYPE = int(os.getenv("INHERITANCE_TYPE", 2))
DEFAULT_PERMISSION_LEVEL = int(os.getenv("DEFAULT_PERMISSION_LEVEL", 3))
CONTRIBUTION_POLICY = int(os.getenv("CONTRIBUTION_POLICY", 2))
MODERATION = int(os.getenv("MODERATION", 0))


def main():
    if not PARTNER_ID:
        print("Error: PARTNER_ID not set in your .env file.")
        return
    if not CHANNEL_NAME:
        print("Error: CHANNEL_NAME not set in your .env file.")
        return
    if not MEDIA_SPACE_URL:
        print("Error: MEDIA_SPACE_URL not set in your .env file.")
        return

    admin_secret = getpass.getpass("Enter your Kaltura admin secret: ")
    if not admin_secret:
        print("Error: Admin secret cannot be empty.")
        return

    # Initialize Kaltura client and start session
    config = KalturaConfiguration(PARTNER_ID)
    config.serviceUrl = "https://www.kaltura.com/"
    client = KalturaClient(config)
    ks = client.session.start(
        admin_secret,
        USER_ID,
        KalturaSessionType.ADMIN,
        PARTNER_ID,
        privileges="all:*,disableentitlement",
    )
    client.setKs(ks)

    # Step 1: Create the channel
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
    encoded_name = urllib.parse.quote(
        created_category.name
    ).replace(" ", "+")
    double_encoded_name = urllib.parse.quote(encoded_name)
    channel_url = (
        f"{MEDIA_SPACE_URL}/channel/{double_encoded_name}/"
        f"{created_category.id}"
    )

    # Step 2: Add members if provided
    member_list = (
        [m.strip() for m in MEMBERS.split(",") if m.strip()]
        if MEMBERS.strip() else []
    )

    for member in member_list:
        category_user = KalturaCategoryUser()
        category_user.categoryId = created_category.id
        category_user.userId = member
        category_user.permissionLevel = (
            KalturaCategoryUserPermissionLevel.MEMBER
        )
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


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("✖ Unhandled error:", e)
        traceback.print_exc()
