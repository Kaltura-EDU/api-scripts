"""
unpublish-republish-entry.py

This script is meant to address a recurring Kaltura issue where an entry
appears to be published to a Canvas Media Gallery but returns an "Access
Denied" error when viewed by users. The fix is to unpublish the entry from the
category and republish it—this script automates that process.

Features:
- Accepts either a category ID or a Canvas course ID as input.
- If using a Canvas course ID, the script uses a known 'fullName' path prefix
  (e.g., "Canvas_Prod>site>channels>") to determine the correct category ID.
- Verifies whether the entry is currently in the category and removes it
  only if fully active.
- Re-adds the entry to the category and confirms success.

Configuration:
- Set 'USE_CATEGORY_NAME = True' to use Canvas course ID input.
- Set 'USE_CATEGORY_NAME = False' to input category ID directly.
- Set 'FULL_NAME_PREFIX' to match your Canvas production category structure.

Note:
Every session started by this script uses the privileges string
"all:*,disableentitlement", which is required to override Kaltura entitlement
rules.

Author: Galen Davis
"""


from KalturaClient import KalturaClient, KalturaConfiguration
from KalturaClient.Plugins.Core import (
    KalturaSessionType, KalturaCategoryFilter,
    KalturaCategoryEntry,
    KalturaCategoryEntryFilter
)

# SESSION CONFIGURATION =======================================================
PARTNER_ID = ""  # Replace with your partner ID
ADMIN_SECRET = ""  # Replace with yours
USER_ID = ""  # Optional; only used for the session
PRIVILEGES = "all:*,disableentitlement"
# ADDITIONAL GLOBAL VARIABLES =================================================
USE_CATEGORY_NAME = False  # Set to True to prompt for category name instead
FULL_NAME_PREFIX = ""  # E.g. Canvas_Prod>site>channels>


# START SESSION ===============================================================
config = KalturaConfiguration()
config.serviceUrl = "https://www.kaltura.com"
config.partnerId = int(PARTNER_ID)
client = KalturaClient(config)

ks = client.session.start(
    ADMIN_SECRET,
    USER_ID,
    KalturaSessionType.ADMIN,
    int(PARTNER_ID),
    privileges=PRIVILEGES
)
client.setKs(ks)

# GET INPUTS ==================================================================
entry_id = input("Entry ID: ").strip()

if USE_CATEGORY_NAME:
    category_name = input("Canvas course ID: ").strip()
    cat_filter = KalturaCategoryFilter()
    cat_filter.fullNameEqual = FULL_NAME_PREFIX + category_name
    cat_result = client.category.list(cat_filter).objects

    if not cat_result:
        print(
            f"❌ No category found with full name "
            f"'{FULL_NAME_PREFIX + category_name}'. Exiting."
            )
        exit(1)

    category_id = str(cat_result[0].id)
    print(
        f"✅ Found category ID: {category_id} for full name "
        f"'{FULL_NAME_PREFIX + category_name}'"
        )
else:
    category_id = input("Category ID: ").strip()
    print(f"✅ Using category ID: {category_id}")


# CHECK AND REMOVE ENTRY IF PRESENT ===========================================
entry_check_filter = KalturaCategoryEntryFilter()
entry_check_filter.categoryIdEqual = category_id
entry_check_filter.entryIdEqual = entry_id
existing_associations = client.categoryEntry.list(
    entry_check_filter
    ).totalCount

if existing_associations == 0:
    print(f"ℹ️ Entry ID {entry_id} does not appear to be assigned to "
          f"category {category_id}. Will proceed with re-adding.")
else:
    print("🔄 Removing entry from category...")

# Re-check category entry status before attempting deletion
entry_status_check = client.categoryEntry.list(entry_check_filter)
entry_active = (
    entry_status_check.totalCount > 0 and
    entry_status_check.objects[0].status.value == 2
)

if entry_active:
    try:
        client.categoryEntry.delete(
            entryId=entry_id,
            categoryId=category_id
        )
        print("✅ Removal successful.")
    except Exception as e:
        msg = str(e).replace(
            "Entry doesn't assigned",
            "Entry isn't assigned"
        )
        print(f"⚠️ Could not remove entry: {msg}")
        exit(1)
else:
    print(
        f"⚠️ Entry is not in an active state for category {category_id}. "
        "Skipping removal and proceeding with re-add."
    )

    # === VERIFY REMOVAL ===
    removed_check = client.categoryEntry.list(entry_check_filter).totalCount
    if removed_check == 0:
        print(
            f"✅ Confirmed that entry ID {entry_id} is no "
            f"longer in category {category_id}"
            )
    else:
        print(
            "❌ Failed to confirm removal. Entry still "
            "appears in category. Exiting."
            )
        exit(1)

# ADD ENTRY BACK TO CATEGORY ==================================================
print("🔄 Adding entry to category...")
assoc = KalturaCategoryEntry()
assoc.categoryId = category_id
assoc.entryId = entry_id

try:
    client.categoryEntry.add(assoc)
except Exception as e:
    print(f"⚠️ Could not re-add entry: {e}")
    exit(1)

# VERIFY RE-ADDITION ==========================================================
added_check = client.categoryEntry.list(entry_check_filter).totalCount

if added_check > 0:
    print(
        f"✅ Confirmed that entry ID {entry_id} is "
        f"now in category {category_id}"
        )
else:
    print(
        "❌ Failed to confirm addition. Entry still not appearing in category."
        )
