"""
Allows users to delete specific types of cue points (Chapters,
Quiz Questions, and Quiz Answers) and associated user entries from Kaltura
entries.

This script allows a user to input a comma-delimited list of Kaltura entry IDs
and select a specific type of cue point (e.g., Chapters, Quiz Questions, Quiz
Answers) to delete. It confirms the number of cue points found for each entry
before deleting them and provides feedback on the deletions. Lastly, it
generates CSV reports based on the deleted cue points or associated user
entries.

Steps:
1. Prompts the user for entry IDs.
2. Prompts the user to select a cue point type to delete.
3. Lists the cue points found for each entry and asks for confirmation before
   deletion.
4. When deleting quiz answers, collects the associated user IDs and deletes
   the corresponding user entries.
5. Generates a CSV report summarizing the deleted items.
6. Displays a summary of the deleted cue points and user entries for each
   entry.

Requires valid Kaltura credentials and appropriate privileges.

Be sure to enter your partner ID (PID) and admin secret in the appropriate
place below.
"""

import csv
from datetime import datetime
from KalturaClient import KalturaClient
from KalturaClient.Base import KalturaConfiguration
from KalturaClient.Plugins.CuePoint import KalturaCuePointFilter
from KalturaClient.Plugins.Core import KalturaSessionType
from KalturaClient.Plugins.Quiz import KalturaUserEntryFilter
from KalturaClient.exceptions import KalturaException

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


QUESTION_TYPES = {
    1: "Multiple Choice",
    2: "True/False",
    3: "Reflection Point",
    8: "Open Question"
}


def get_kaltura_client(partner_id, admin_secret):
    config = KalturaConfiguration(partner_id)
    config.requestTimeout = _retry_env_int("REQUEST_TIMEOUT", 120)
    config.serviceUrl = "https://www.kaltura.com/"
    client = KalturaClient(config)
    try:
        ks = call_with_retry(client.session.start,
            admin_secret, "admin", KalturaSessionType.ADMIN, partner_id,
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
    return client


def generate_csv(filename, headers, rows):
    with open(filename, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        writer.writerows(rows)
    print(f"CSV report generated: {filename}")


def list_and_delete_cue_points(client, entry_ids, cue_point_type):
    total_deleted = 0
    user_ids_to_delete = set()
    rows = []

    for entry_id in entry_ids:
        print(f"Processing entry: {entry_id}")
        print("-" * 20)

        try:
            entry = call_with_retry(client.baseEntry.get, entry_id)
            entry_title = entry.name

            cue_filter = KalturaCuePointFilter()
            cue_filter.entryIdEqual = entry_id
            cue_filter.cuePointTypeEqual = cue_point_type

            response = call_with_retry(client.cuePoint.cuePoint.list, cue_filter)
            cue_points = response.objects or []
            
            print(f"entry {entry_id} has {len(cue_points)}.")

            if cue_points:
                confirm = input(
                    f"{len(cue_points)} cue points of type {cue_point_type} "
                    f"found. Delete them? (Y/N): "
                    ).strip().lower()
                if confirm != 'y':
                    print("Skipping deletion for this entry.")
                    continue

            for cue_point in cue_points:
                if cue_point_type == "quiz.QUIZ_ANSWER":
                    user_ids_to_delete.add(cue_point.userId)
                    rows.append([
                        entry_id,
                        entry_title,
                        cue_point.userId,
                        datetime.utcfromtimestamp(
                            cue_point.createdAt
                            ).strftime('%Y-%m-%d %H:%M:%S'),
                        cue_point.question,
                        cue_point.answer,
                        "Yes" if cue_point.isCorrect else "No"
                    ])
                elif cue_point_type == "quiz.QUIZ_QUESTION":
                    question_type = QUESTION_TYPES.get(
                        cue_point.questionType, "Unknown"
                        )
                    optional_answers = cue_point.optionalAnswers or []
                    row = [
                        entry_id,
                        entry_title,
                        question_type,
                        cue_point.question,
                        *(answer.text for answer in optional_answers[:4]),
                        next(
                            (answer.text for answer in optional_answers
                             if answer.isCorrect), ""
                            )
                    ]
                    rows.append(row)
                elif cue_point_type == "thumbCuePoint.Thumb":
                    rows.append([
                        entry_id,
                        entry_title,
                        cue_point.title,
                        cue_point.description,
                        cue_point.startTime / 1000  # Convert ms to seconds
                    ])

                call_with_retry(client.cuePoint.cuePoint.delete, cue_point.id)
                print(f"Deleted cue point ID: {cue_point.id}")
                total_deleted += 1

        except KalturaException as e:
            print(f"Error processing entry {entry_id}: {e}")
        except Exception as ex:
            print(f"Unexpected error with entry {entry_id}: {ex}")

        print(f"Finished processing entry: {entry_id}")
        print("-" * 20)

    # Sort rows by start time for chapters
    if cue_point_type == "thumbCuePoint.Thumb":
        rows.sort(key=lambda x: x[4])

    # Determine CSV filename
    now = datetime.now().strftime("%Y-%m-%d-%H%M")
    if cue_point_type == "quiz.QUIZ_QUESTION":
        filename = f"{now}_quiz-questions-deleted.csv"
        headers = [
            "Entry ID", "Entry Title", "Question Type", "Question",
            "Option 1", "Option 2", "Option 3", "Option 4", "Correct Answer"
            ]
    elif cue_point_type == "quiz.QUIZ_ANSWER":
        filename = f"{now}_quiz-answers-deleted.csv"
        headers = [
            "Entry ID", "Entry Title", "User ID", "Date Submitted",
            "Question", "Answer", "Correct"
            ]
    elif cue_point_type == "thumbCuePoint.Thumb":
        filename = f"{now}_chapters-deleted.csv"
        headers = [
            "Entry ID", "Entry Title", "Chapter Title", "Chapter Description",
            "Start Time (Seconds)"
            ]
    else:
        return total_deleted, user_ids_to_delete

    generate_csv(filename, headers, rows)

    return total_deleted, user_ids_to_delete


def list_and_delete_user_entries(client, entry_ids, user_ids):
    total_deleted = 0

    for entry_id in entry_ids:
        print(f"Processing user entries for entry: {entry_id}")
        print("-" * 20)

        try:
            user_entry_filter = KalturaUserEntryFilter()
            user_entry_filter.entryIdEqual = entry_id
            user_entry_filter.userIdIn = ','.join(user_ids)

            response = call_with_retry(client.userEntry.list, user_entry_filter)
            user_entries = response.objects or []

            print(
                f"entry {entry_id} has {len(user_entries)} user entries "
                f"(records of quiz submissions)."
                )

            if user_entries:
                confirm = input(
                    f"{len(user_entries)} user entries found for entry "
                    f"{entry_id}. Delete them? (Y/N): "
                    ).strip().lower()
                if confirm != 'y':
                    print("Skipping user entry deletion for this entry.")
                    continue

            for user_entry in user_entries:
                call_with_retry(client.userEntry.delete, user_entry.id)
                print(f"Deleted user entry ID: {user_entry.id}")
                total_deleted += 1

        except KalturaException as e:
            print(f"Error processing user entries for entry {entry_id}: {e}")
        except Exception as ex:
            print(f"Unexpected error with user entries for entry {entry_id}: {ex}")

        print(f"Finished processing user entries for entry: {entry_id}")
        print("-" * 20)

    return total_deleted


def main():
    partner_id = input("Enter your Partner ID: ").strip()
    admin_secret = input("Enter your Admin Secret: ").strip()

    client = get_kaltura_client(partner_id,admin_secret)

    # Step 1: Prompt for entry IDs
    entry_ids_input = input(
        "Please enter a comma-delimited list of entry IDs: "
        )
    entry_ids = [
        eid.strip() for eid in entry_ids_input.split(',') if eid.strip()
        ]

    if not entry_ids:
        print("No valid entry IDs provided. Exiting.")
        return

    # Step 2: Prompt for cue point type
    print("What kinds of cue points do you want to delete?")
    cue_point_types = {
        1: ("Chapters", "thumbCuePoint.Thumb"),
        2: ("Quiz Questions", "quiz.QUIZ_QUESTION"),
        3: ("Quiz Submissions", "quiz.QUIZ_ANSWER")
    }

    for key, (name, _) in cue_point_types.items():
        print(f"[{key}] {name}")

    try:
        choice = int(
            input("Enter the number corresponding to the cue point type: ")
            )
        if choice not in cue_point_types:
            print("Invalid choice. Exiting.")
            return

        selected_type = cue_point_types[choice][1]
    except ValueError:
        print("Invalid input. Please enter a number. Exiting.")
        return

    # Step 3: Delete cue points
    print(f"Deleting {cue_point_types[choice][0]} from specified entries...")
    deleted_cue_points, user_ids_to_delete = list_and_delete_cue_points(
        client, entry_ids, selected_type
        )

    # Step 4: Delete user entries if quiz answers were deleted
    if selected_type == "quiz.QUIZ_ANSWER" and user_ids_to_delete:
        print("Deleting associated user entries...")
        deleted_user_entries = list_and_delete_user_entries(
            client, entry_ids, user_ids_to_delete
            )
        print(f"Total user entries deleted: {deleted_user_entries}")

    # Step 5: Display results
    print(f"Total {cue_point_types[choice][0]} deleted: {deleted_cue_points}")


if __name__ == "__main__":
    main()
