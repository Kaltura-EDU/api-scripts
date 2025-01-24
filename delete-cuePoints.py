"""
Deletes 1 of 3 cuePoint types from Kaltura entries.

This script allows a user to input a comma-delimited list of Kaltura entry IDs
and select a specific type of cue point (e.g., Chapters, Quiz Questions, Quiz
Answers) to delete. It confirms the number of cue points found for each entry
before deleting them and provides detailed feedback on the deletions.

Steps:
1. Prompts the user for entry IDs.
2. Prompts the user to select a cue point type to delete.
3. Lists the cue points found for each entry and asks for confirmation before
deletion.
4. Displays a summary of the deleted cue points for each entry.

Requires valid Kaltura credentials and the appropriate privileges.
"""

from KalturaClient import KalturaClient
from KalturaClient.Base import KalturaConfiguration
from KalturaClient.Plugins.CuePoint import KalturaCuePointFilter
from KalturaClient.Plugins.Core import KalturaSessionType
from KalturaClient.exceptions import KalturaException


def get_kaltura_client():
    partner_id = ""  # Replace with your partner ID
    admin_secret = ""  # Replace with your admin secret
    service_url = "https://www.kaltura.com/"

    config = KalturaConfiguration(partner_id)
    config.serviceUrl = service_url
    client = KalturaClient(config)

    ks = client.session.start(
        admin_secret,
        userId="admin",
        type=KalturaSessionType.ADMIN,
        partnerId=partner_id,
        expiry=None,
        privileges="all:*,disableentitlement"
    )
    client.setKs(ks)
    return client


def list_and_delete_cue_points(client, entry_ids, cue_point_type):
    total_deleted = 0
    for entry_id in entry_ids:
        print(f"Processing entry: {entry_id}")
        print("-" * 20)
        
        try:
            cue_filter = KalturaCuePointFilter()
            cue_filter.entryIdEqual = entry_id
            cue_filter.cuePointTypeEqual = cue_point_type

            response = client.cuePoint.cuePoint.list(cue_filter)
            cue_points = response.objects or []
            
            print(f"Debug: Response received for entry {entry_id} - {len(cue_points)} cue points found.")

            if cue_points:
                confirm = input(f"{len(cue_points)} cue points of type {cue_point_type} found. Delete them? (Y/N): ").strip().lower()
                if confirm != 'y':
                    print("Skipping deletion for this entry.")
                    continue

            for cue_point in cue_points:
                client.cuePoint.cuePoint.delete(cue_point.id)
                print(f"Deleted cue point ID: {cue_point.id}")
                total_deleted += 1

        except KalturaException as e:
            print(f"Error processing entry {entry_id}: {e}")
        except Exception as ex:
            print(f"Unexpected error with entry {entry_id}: {ex}")

        print(f"Finished processing entry: {entry_id}")
        print("-" * 20)

    return total_deleted

def main():
    client = get_kaltura_client()

    # Step 1: Prompt for entry IDs
    entry_ids_input = input("Please enter a comma-delimited list of entry IDs: ")
    entry_ids = [eid.strip() for eid in entry_ids_input.split(',') if eid.strip()]

    if not entry_ids:
        print("No valid entry IDs provided. Exiting.")
        return

    # Step 2: Prompt for cue point type
    print("What cue points do you want to delete?")
    cue_point_types = {
        1: ("Chapters", "thumbCuePoint.Thumb"),
        2: ("Quiz Questions", "quiz.QUIZ_QUESTION"),
        3: ("Quiz Answers", "quiz.QUIZ_ANSWER")
    }

    for key, (name, _) in cue_point_types.items():
        print(f"[{key}] {name}")

    try:
        choice = int(input("Enter the number corresponding to the cue point type: "))
        if choice not in cue_point_types:
            print("Invalid choice. Exiting.")
            return

        selected_type = cue_point_types[choice][1]
    except ValueError:
        print("Invalid input. Please enter a number. Exiting.")
        return

    # Step 3: Delete cue points
    print(f"Deleting {cue_point_types[choice][0]} from specified entries...")
    deleted_count = list_and_delete_cue_points(client, entry_ids, selected_type)

    # Step 4: Display results
    print(f"Total {cue_point_types[choice][0]} deleted: {deleted_count}")


if __name__ == "__main__":
    main()
