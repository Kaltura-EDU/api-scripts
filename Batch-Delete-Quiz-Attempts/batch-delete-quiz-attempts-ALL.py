'''
This script prompts for an auth token, entry ID file, and output file.
The script will collect video quiz attempt IDs for ALL users and add those to an array and write to an output file.
The script will then prompt the user if they want to delete all collected quiz attempts.
The input file can contain 1 or more entry IDs.
XML is parsed.
The output file logs the results from collecting quiz attempt IDs for each entry ID. 
The output file also logs the confirmed deleted attempt IDs.
'''

import requests
import xml.etree.ElementTree as ET
from tqdm import tqdm
# I like colors
from colorama import Fore, Style, init

SERVICE_URL = "https://www.kaltura.com/api_v3/"

# Get quiz attempt IDs and save them to an array
def get_quiz_attempt_ids(ks, entry_id):
    attempt_ids = []
    page_size = 1000  # Default is 30 items. Kaltura allows up to 10,000 per page/request.
    page_index = 1

    while True:
        payload = {
            "service": "userEntry",
            "action": "list",
            "ks": ks,
            "filter:objectType": "KalturaQuizUserEntryFilter",
            "filter:entryIdEqual": entry_id,
            "pager:pageSize": page_size,
            "pager:pageIndex": page_index,
        }

        response = requests.post(SERVICE_URL, data=payload)

        if response.status_code == 200:
            try:
                root = ET.fromstring(response.text)
                error = root.find(".//error")
                if error is not None:
                    raise Exception(f"Error: {error.find('message').text}")

                attempts = root.findall(".//item")
                if not attempts:
                    break  # Exit the loop if no more attempts are found.

                for attempt in attempts:
                    attempt_id = attempt.find("id").text
                    attempt_ids.append(attempt_id)

                page_index += 1  # Increment the page index for the next request.
            except ET.ParseError:
                raise Exception("Failed to parse XML response.")
        else:
            raise Exception(f"Request failed with status code {response.status_code}: {response.text}")

    return attempt_ids


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
    entry_file = input(f"{Fore.YELLOW}Enter the name of the text file containing entry IDs: {Style.RESET_ALL}").strip()
    output_file = input(f"{Fore.YELLOW}Enter the name of the output text file to save the detailed report: {Style.RESET_ALL}").strip()

    all_attempt_ids = []

    try:
        with open(entry_file, 'r') as file:
            entry_ids = [line.strip() for line in file if line.strip()]
    except FileNotFoundError:
        print(f"Error: File '{entry_file}' not found.")
        return

    with open(output_file, 'w') as outfile:
        for entry_id in tqdm(entry_ids, desc="Collecting Quiz Attempt IDs", unit=" Quiz IDs"):
            outfile.write(f"\nProcessing Entry ID: {entry_id}\n")
            try:
                attempt_ids = get_quiz_attempt_ids(ks, entry_id)
                if attempt_ids:
                    for attempt_id in attempt_ids:
                        outfile.write(f"    Quiz Attempt ID: {attempt_id}\n")
                        all_attempt_ids.append(attempt_id)
                else:
                    outfile.write(f"  No quiz attempts found for Entry ID: {entry_id}.\n")
            except Exception as e:
                outfile.write(f"  Error processing Entry ID '{entry_id}': {str(e)}\n")

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
