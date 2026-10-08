from KalturaClient import KalturaClient, KalturaConfiguration
from KalturaClient.Plugins.Core import (
    KalturaCategoryUserFilter, KalturaFilterPager, KalturaSessionType
)
from collections import Counter
import csv
import getpass

from dotenv import load_dotenv

# Optional .env: only the reliability settings below are read from it.
load_dotenv()

# ── Network retry ──────────────────────────────────────────────────────
# KalturaClientException (timeouts, resets) is NOT a KalturaException, so
# plain `except KalturaException` misses it. Knobs come from .env:
# REQUEST_TIMEOUT, MAX_NETWORK_RETRIES, NETWORK_RETRY_DELAY.
import os as _os
import time as _time

import requests as _requests
from KalturaClient.exceptions import (
    KalturaClientException as _KalturaClientException,
)


def _retry_env_int(name, default):
    try:
        return int((_os.getenv(name) or "").strip() or default)
    except ValueError:
        return default


def call_with_retry(fn, *args, **kwargs):
    """Call fn(*args, **kwargs), retrying with linear backoff on transient
    network failures. Real API errors (KalturaException) are re-raised
    untouched so callers can handle them."""
    retries = _retry_env_int("MAX_NETWORK_RETRIES", 5)
    delay = _retry_env_int("NETWORK_RETRY_DELAY", 5)
    for attempt in range(1, retries + 1):
        try:
            return fn(*args, **kwargs)
        except (
            _KalturaClientException,
            _requests.exceptions.RequestException,
        ) as exc:
            if attempt == retries:
                raise
            wait = delay * attempt
            print(
                f"    [network error: {exc}; retry "
                f"{attempt}/{retries} in {wait}s]"
            )
            _time.sleep(wait)


# CONFIGURABLE VARIABLES
CREATE_CSV_OUTPUT = True  # Set to False if you just want on-screen results
AGGREGATE_CSV_OUTPUT = True  # Set to False if you want separate CSVs per user

# --- SETUP CLIENT ---
config = KalturaConfiguration()
config.requestTimeout = _retry_env_int("REQUEST_TIMEOUT", 120)
config.serviceUrl = "https://www.kaltura.com"
client = KalturaClient(config)

# Start a session (admin ks with full privileges)
# The admin secret is never stored in this file -- it is prompted.
admin_secret = getpass.getpass(
    "Enter your Kaltura admin secret (input hidden): "
).strip()
if not admin_secret:
    print("❌ No admin secret entered. Exiting.")
    raise SystemExit(1)
partner_id = ""
user_id = ""
try:
    ks = call_with_retry(client.session.start,
        admin_secret, user_id, KalturaSessionType.ADMIN, partner_id,
        privileges="all:*,disableentitlement"
        )
except Exception as e:
    if getattr(e, "code", "") == "START_SESSION_ERROR":
        print(
            "\n❌ Could not log in to Kaltura. Partner ID "
            f"[{partner_id}] and the Admin Secret were not accepted.\n"
            "   Double-check both values — the secret must be the "
            "Administrator secret (not the User secret),\n"
            "   copied exactly from KMC → Settings → Integration Settings.\n"
        )
    elif type(e).__name__ == "KalturaClientException":
        print(
            "\n❌ Could not reach Kaltura to start a session.\n"
            f"   {e}\n   Check your internet connection and try again.\n"
        )
    else:
        print(f"\n❌ Could not start Kaltura session: {e}\n")
    raise SystemExit(1)
client.setKs(ks)

# --- GET USER INPUT ---
user_input = input("Enter one or more Kaltura user IDs (comma-delimited): ")
user_ids = [uid.strip() for uid in user_input.split(",") if uid.strip()]

# --- STORAGE FOR AGGREGATE RESULTS ---
all_memberships = []


# --- FUNCTION TO LIST CATEGORY MEMBERSHIPS ---
def list_user_category_roles(target_user_id):
    filter_ = KalturaCategoryUserFilter()
    filter_.userIdEqual = target_user_id

    pager = KalturaFilterPager()
    results = []

    while True:
        response = call_with_retry(client.categoryUser.list, filter_, pager)
        for cu in response.objects:
            category = call_with_retry(client.category.get, cu.categoryId)
            category_name = category.name

            if category.owner == target_user_id:
                role = "Owner"
            else:
                permission_value = cu.permissionLevel.value if hasattr(
                    cu.permissionLevel, "value") else cu.permissionLevel
                role = {
                    0: "Manager",
                    1: "Moderator",
                    2: "Contributor",
                    3: "Member",
                    4: "None"
                }.get(permission_value, f"Unknown ({permission_value})")

            results.append({
                "Category ID": cu.categoryId,
                "Category Name": category_name,
                "Role": role
            })

        if response.totalCount <= len(results):
            break

        pager.pageIndex += 1

    return results


# --- PROCESS EACH USER ---
for target_username in user_ids:
    memberships = list_user_category_roles(target_username)

    # Add username and hierarchy to each row
    for row in memberships:
        category = call_with_retry(client.category.get, row["Category ID"])
        row["Username"] = target_username
        row["Hierarchy"] = category.fullName

    # Always print summary
    if memberships:
        role_counts = Counter(m["Role"] for m in memberships)
        print(
            f"\n{len(memberships)} category affiliations found for user: "
            f"{target_username}\n"
              )
        for role in sorted(role_counts.keys()):
            print(f"  - {role_counts[role]} as {role}")
        print("\nCategory affiliations:")
        for m in memberships:
            print(f"- {m['Category ID']}: {m['Category Name']} – {m['Role']}")
    else:
        print(f"\n0 category affiliations found for user: {target_username}")

    if CREATE_CSV_OUTPUT:
        if AGGREGATE_CSV_OUTPUT:
            all_memberships.extend(memberships)
        else:
            if memberships:  # Optional: skip empty CSVs
                csv_filename = f"categoryAffiliations_{target_username}.csv"
                fieldnames = [
                    "Username",
                    "Category ID",
                    "Category Name",
                    "Role",
                    "Hierarchy"
                    ]
                with open(
                    csv_filename, mode="w", newline="", encoding="utf-8"
                     ) as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(memberships)
                print(f"\nCSV file created: {csv_filename}\n" + "-" * 50)


# --- EXPORT AGGREGATE CSV IF ENABLED ---
if CREATE_CSV_OUTPUT and AGGREGATE_CSV_OUTPUT:
    csv_filename = "categoryAffiliations_multipleUsers.csv"
    fieldnames = [
        "Username",
        "Category ID",
        "Category Name",
        "Role",
        "Hierarchy"
        ]
    with open(csv_filename, mode="w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_memberships)
    print(f"\nCSV file created: {csv_filename}")
