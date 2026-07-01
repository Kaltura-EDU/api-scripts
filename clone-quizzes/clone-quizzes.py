"""
Clones Kaltura quiz entries along with their quiz question cue points.

Reads entry IDs from .env (ENTRY_IDS, comma-separated), clones each entry,
copies all quiz.QUIZ_QUESTION cue points to the clone, and optionally adds a
tag. Results are written to a timestamped CSV in the output/ subfolder.
"""

import csv
import getpass
import os
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from KalturaClient import KalturaClient, KalturaConfiguration
from KalturaClient.Plugins.Core import (
    KalturaBaseEntry,
    KalturaFilterPager,
    KalturaSessionType,
)
from KalturaClient.Plugins.CuePoint import KalturaCuePointFilter
from KalturaClient.exceptions import KalturaException

load_dotenv(dotenv_path=Path(__file__).with_name(".env"))

PARTNER_ID = os.getenv("PARTNER_ID", "").strip()
USER_ID = os.getenv("USER_ID", "").strip()
SERVICE_URL = os.getenv("SERVICE_URL", "https://www.kaltura.com/")
PRIVILEGES = os.getenv("PRIVILEGES", "all:*,disableentitlement")
ENTRY_IDS = [
    e.strip()
    for e in os.getenv("ENTRY_IDS", "").split(",")
    if e.strip()
]
TAG = os.getenv("TAG", "").strip()


def build_client(admin_secret):
    config = KalturaConfiguration(int(PARTNER_ID))
    config.serviceUrl = SERVICE_URL
    client = KalturaClient(config)
    ks = client.session.start(
        admin_secret,
        USER_ID,
        KalturaSessionType.ADMIN,
        int(PARTNER_ID),
        privileges=PRIVILEGES,
    )
    client.setKs(ks)
    return client


def clone_entry_with_quizzes(client, original_entry_id, tag=None):
    cue_filter = KalturaCuePointFilter()
    cue_filter.entryIdEqual = original_entry_id
    pager = KalturaFilterPager()
    pager.pageSize = 500

    response = client.cuePoint.cuePoint.list(cue_filter, pager)
    cue_points = response.objects or []

    question_cue_points = [
        cp for cp in cue_points
        if (
            hasattr(cp.cuePointType, 'value')
            and cp.cuePointType.value == "quiz.QUIZ_QUESTION"
        )
    ]
    question_ids = [cp.id for cp in question_cue_points]

    print(
        f"Found {len(question_ids)} quiz questions in entry"
        f" {original_entry_id}."
    )

    new_entry_object = client.baseEntry.clone(original_entry_id)
    new_entry_id = new_entry_object.id
    print(
        f"Cloned entry {original_entry_id} to new entry {new_entry_id}."
    )

    if tag:
        cloned_entry = client.baseEntry.get(new_entry_id)
        current_tags = cloned_entry.tags.strip() if cloned_entry.tags else ""
        updated_tags = f"{current_tags},{tag}" if current_tags else tag
        entry_update = KalturaBaseEntry()
        entry_update.tags = updated_tags
        client.baseEntry.update(new_entry_id, entry_update)
        print(f"Tag '{tag}' added to {new_entry_id}.")

    for qid in question_ids:
        cloned_cue = client.cuePoint.cuePoint.clone(qid, new_entry_id)
        print(
            f"Cloned quiz question {qid} to {new_entry_id}"
            f" as {cloned_cue.id}."
        )

    final_entry = client.baseEntry.get(new_entry_id)

    print("------------------------------------------------------")
    print("SUMMARY:")
    print(f"  Title:                 {final_entry.name}")
    print(f"  Original Entry ID:     {original_entry_id}")
    print(f"  New Entry ID:          {new_entry_id}")
    print(f"  Quiz Questions Cloned: {len(question_ids)}")
    if tag:
        print(f"  Tag Added:             {tag}")
    print("------------------------------------------------------\n")

    return final_entry.name, original_entry_id, new_entry_id, len(question_ids)


def main():
    if not PARTNER_ID:
        print("Error: PARTNER_ID not set in your .env file.")
        return
    if not PARTNER_ID.isdigit():
        print("Error: PARTNER_ID in .env must be a number.")
        return
    if not ENTRY_IDS:
        print("Error: ENTRY_IDS not set in your .env file.")
        return

    admin_secret = getpass.getpass("Enter your Kaltura admin secret: ")
    if not admin_secret:
        print("Error: Admin secret cannot be empty.")
        return

    client = build_client(admin_secret)

    ts = datetime.now().strftime("%Y-%m-%d-%H%M")
    output_dir = Path(__file__).with_name("output")
    output_dir.mkdir(exist_ok=True)
    output_csv = output_dir / f"{ts}_clone-quizzes.csv"

    with open(output_csv, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            "title", "original_entry_id", "new_entry_id",
            "questions_cloned",
        ])
        for eid in ENTRY_IDS:
            try:
                result = clone_entry_with_quizzes(
                    client, eid, tag=TAG or None
                )
                writer.writerow(result)
            except KalturaException as e:
                print(f"Error processing entry {eid}: {e}")
            except Exception as e:
                print(f"Unexpected error with entry {eid}: {e}")

    print(f"Results saved to {output_csv}.")


if __name__ == "__main__":
    main()
