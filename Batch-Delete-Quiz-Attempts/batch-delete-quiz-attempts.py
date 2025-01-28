'''
This script prompts for an auth token, user ID file, entry ID file, and output file.
The script will collect video quiz attempt IDs and add those to an array and write to an output file.
The script will then prompt the user if they want to delete all collected quiz attempts.
The input files can contain 1 or more users or entry IDs.
Since I'm not specifying JSON to be returned, I'm parsing XML. You can alternatively specify JSON.
The output file logs the results from collecting quiz attempt IDs for each user from each entry ID. 
The output file also logs the confirmed deleted attempt IDs.
'''

import requests
import xml.etree.ElementTree as ET
from tqdm import tqdm
# I like colors
from colorama import Fore, Style, init

SERVICE_URL = "https://www.kaltura.com/api_v3/"

# Get quiz attempt IDs and save them to an array
def get_quiz_attempt_ids(ks, entry_id, user_id):

    payload = {
        "service": "userEntry",
        "action": "list",
        "ks": ks,
        "filter:objectType": "KalturaQuizUserEntryFilter",
        "filter:entryIdEqual": entry_id,
        "filter:userIdEqual": user_id,
    }
    
    response = requests.post(SERVICE_URL, data=payload)

    if response.status_code == 200:
        try:
            root = ET.fromstring(response.text)
            error = root.find(".//error")
            if error is not None:
                raise Exception(f"Error: {error.find('message').text}")

            attempts = root.findall(".//item")
            attempt_ids = [attempt.find("id").text for attempt in attempts]
            return attempt_ids
        except ET.ParseError:
            raise Exception("Failed to parse XML response.")
    else:
        raise Exception(f"Request failed with status code {response.status_code}: {response.text}")

# Delete quiz attempts using the IDs in the array
def delete_quiz_attempt(ks, attempt_id):
 
    payload = {
        "service": "userEntry",
        "action": "delete",
        "ks": ks,
        "id": attempt_id,
    }
    
    response = requests.post(SERVICE_URL, data=payload)

    if response.status_code == 200:
        try:
            root = ET.fromstring(response.text)
            error = root.find(".//error")
            if error is not None:
                return False
            return True
        except ET.ParseError:
            return False
    else:
        return False

def main():
    ks = input(f"{Fore.YELLOW}Enter your Kaltura session token (KS): {Style.RESET_ALL}").strip()
    user_file = input(f"{Fore.YELLOW}Enter the name of the text file containing user IDs: {Style.RESET_ALL}").strip()
    entry_file = input(f"{Fore.YELLOW}Enter the name of the text file containing entry IDs: {Style.RESET_ALL}").strip()
    output_file = input(f"{Fore.YELLOW}Enter the name of the output text file to save the detailed report: {Style.RESET_ALL}").strip()

    all_attempt_ids = []

    try:
        with open(user_file, 'r') as file:
            user_ids = [line.strip() for line in file if line.strip()]
    except FileNotFoundError:
        print(f"Error: File '{user_file}' not found.")
        return

    try:
        with open(entry_file, 'r') as file:
            entry_ids = [line.strip() for line in file if line.strip()]
    except FileNotFoundError:
        print(f"Error: File '{entry_file}' not found.")
        return

    with open(output_file, 'w') as outfile:
        for entry_id in tqdm(entry_ids, desc="Processing Entry IDs", unit=" Entry IDs"):
            outfile.write(f"\nProcessing Entry ID: {entry_id}\n")
            for user_id in tqdm(user_ids, desc=f"{Fore.CYAN}Processing User IDs for Entry ID {Fore.YELLOW}{entry_id}{Style.RESET_ALL}", unit=" User IDs", leave=False):
                try:
                    attempt_ids = get_quiz_attempt_ids(ks, entry_id, user_id)
                    if attempt_ids:
                        outfile.write(f"  User ID: {user_id}\n")
                        for attempt_id in attempt_ids:
                            outfile.write(f"    Quiz Attempt ID: {attempt_id}\n")
                            all_attempt_ids.append(attempt_id)
                    else:
                        outfile.write(f"  User ID: {user_id} - No quiz attempts found.\n")
                except Exception as e:
                    outfile.write(f"  Error processing user ID '{user_id}' for Entry ID '{entry_id}': {str(e)}\n")

    print(f"{Fore.GREEN}\nCollected {Fore.YELLOW}{len(all_attempt_ids)} {Fore.GREEN}quiz attempt IDs.{Style.RESET_ALL}")
    proceed = input(f"{Fore.YELLOW}Do you want to delete these quiz attempts? {Fore.RED}(yes/no): {Style.RESET_ALL}").strip().lower()

    if proceed == 'yes':
        deleted_count = 0
        with open(output_file, 'a') as outfile:
            outfile.write("\nDeleting Quiz Attempts:\n")
            for attempt_id in tqdm(all_attempt_ids, desc="Deleting Quiz Attempts", unit=" Attempt IDs"):
                if delete_quiz_attempt(ks, attempt_id):
                    outfile.write(f"  Successfully deleted Quiz Attempt ID: {attempt_id}\n")
                    deleted_count += 1
                else:
                    outfile.write(f"  Failed to delete Quiz Attempt ID: {attempt_id}\n")

        print(f"{Fore.GREEN}\nDeleted {Fore.CYAN}{deleted_count} {Fore.GREEN}quiz attempts. Results logged in '{Fore.YELLOW}{output_file}{Fore.GREEN}'.{Style.RESET_ALL}")
    else:
        print(f"{Fore.YELLOW}No quiz attempts were deleted.{Style.RESET_ALL}")

if __name__ == "__main__":
    main()
