'''
This script duplicates quizzes based on a comma-delimited list entered on the
command line. It clones the original quiz entry, identifying all
the cuePoints of only of question type quiz.QUIZ_QUESTION, and then clones 
those cuePoints into the new entry. You're also given an option to add a tag to
the new entries. Finally, the script ouputs a .csv that lists all the old and
new entries.
'''

import csv
from datetime import datetime
from KalturaClient import KalturaClient
from KalturaClient.Base import KalturaConfiguration
from KalturaClient.Plugins.Core import (
    KalturaSessionType, KalturaFilterPager, KalturaBaseEntry
)
from KalturaClient.Plugins.CuePoint import KalturaCuePointFilter
from KalturaClient.exceptions import KalturaException


def get_kaltura_client(partner_id, admin_secret):
    config = KalturaConfiguration(partner_id)
    config.serviceUrl = "https://www.kaltura.com/"
    client = KalturaClient(config)
    ks = client.session.start(
        admin_secret, "admin", KalturaSessionType.ADMIN, partner_id,
        privileges="all:*,disableentitlement"
    )
    client.setKs(ks)
    return client


def clone_entry_with_quizzes(client, original_entry_id, user_tag=None):
    cue_filter = KalturaCuePointFilter()
    cue_filter.entryIdEqual = original_entry_id
    pager = KalturaFilterPager()

    # List cue points associated with the entry
    response = client.cuePoint.cuePoint.list(cue_filter, pager)
    cue_points = response.objects or []

    for cp in cue_points:
        cptype_str = cp.cuePointType.value if hasattr(
            cp.cuePointType, 'value') else str(cp.cuePointType)
        print(f"CuePoint ID: {cp.id}, Type: {cptype_str}")

    # Filter to quiz questions
    question_cue_points = [
        cp for cp in cue_points if hasattr(cp.cuePointType, 'value')
        and cp.cuePointType.value == "quiz.QUIZ_QUESTION"
        ]
    question_ids = [cp.id for cp in question_cue_points]

    print(
        f"Found {len(question_ids)} quiz questions in entry "
        f"{original_entry_id}."
        )

    # Clone the entry
    new_entry_object = client.baseEntry.clone(original_entry_id)
    new_entry_id = new_entry_object.id
    print(f"Cloned entry {original_entry_id} to new entry {new_entry_id}.")

    # If the user wants to add a tag, do it now
    if user_tag:
        # Get the new entry
        cloned_entry = client.baseEntry.get(new_entry_id)
        current_tags = cloned_entry.tags.strip() if cloned_entry.tags else ""

        # If current_tags is empty, just set it to the user_tag
        # Otherwise, append with a comma
        if current_tags:
            updated_tags = current_tags + "," + user_tag
        else:
            updated_tags = user_tag

        entry_update = KalturaBaseEntry()
        entry_update.tags = updated_tags
        client.baseEntry.update(new_entry_id, entry_update)
        print(f"Tag '{user_tag}' added to {new_entry_id}")

    # Clone each question cue point to the new entry
    for qid in question_ids:
        cloned_cue = client.cuePoint.cuePoint.clone(qid, new_entry_id)
        print(
            f"Cloned quiz question cue point {qid} to {new_entry_id} "
            f"as {cloned_cue.id}."
        )

    # Retrieve entry details to report
    final_entry = client.baseEntry.get(new_entry_id)

    # Print final summary
    print("------------------------------------------------------")
    print("SUMMARY:")
    print(f"Title: {final_entry.name}")
    print(f"Original Entry ID: {original_entry_id}")
    print(f"New Entry ID: {new_entry_id}")
    print(f"Quiz Questions Cloned: {len(question_ids)}")
    if user_tag:
        print(f"Tag Added: {user_tag}")
    print("------------------------------------------------------\n")

    # Return the info needed for CSV
    return (
        final_entry.name, original_entry_id, new_entry_id, len(question_ids)
    )


def main():
    partner_id = input("Enter your Partner ID: ").strip()
    admin_secret = input("Enter your Admin Secret: ").strip()

    client = get_kaltura_client(partner_id,admin_secret)

    entry_ids_input = input("Enter comma-delimited list of entry IDs: ")
    entry_ids = [e.strip() for e in entry_ids_input.split(",") if e.strip()]

    add_tag_response = input(
        "Do you want to add a tag to the new entries? (Y/N): "
    ).strip().upper()
    user_tag = None
    if add_tag_response == "Y":
        user_tag = input("Enter the tag you want to add: ").strip()

    # Create the CSV filename with current date and time
    now = datetime.now()
    # Format: YYYY-MM-DD-HHMM
    timestamp_str = now.strftime("%Y-%m-%d-%H%M")
    csv_filename = f"QuizzesCloned_{timestamp_str}.csv"

    # Open CSV file and write header
    with open(csv_filename, mode='w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([
            "Title", "Original Entry ID", "New Entry ID", "Number of Questions"
            ])

        for eid in entry_ids:
            try:
                title, orig_id, new_id, num_questions = (
                    clone_entry_with_quizzes(client, eid, user_tag=user_tag)
                )
                # Write the row to CSV
                writer.writerow([title, orig_id, new_id, num_questions])
            except KalturaException as e:
                print(f"Error processing entry {eid}: {e}")
            except Exception as ex:
                print(f"Unexpected error with entry {eid}: {ex}")

    print(f"All done! Results saved to {csv_filename}.")

if __name__ == "__main__":
    main()