"""
PROCESS: Kaltura report - User Export to Email, KR-uxe.py
Last Updated: 2026-06-15 15:18:34
SUMMARY: 
Generates an ADMIN session using credentials pulled from a local .env file. If 
an .env file doesn't exist, it checks the directory for any *.env file and 
copies it to .env. The user is prompted to select a user status to export 
(BLOCKED, ACTIVE, DELETED, or ALL). The script then triggers Kaltura's 
built-in background job using the 'user' service and 'exportToCsv' action. 
Kaltura processes the list server-side and automatically emails the resulting 
CSV download link to the user associated with the session (ADMIN_ID in .env).
"""

import os
import sys
import glob
import shutil
from dotenv import load_dotenv

# Kaltura API Client Imports
from KalturaClient import KalturaClient, KalturaConfiguration
from KalturaClient.Plugins.Core import (
    KalturaSessionType, 
    KalturaUserFilter, 
    KalturaUserStatus,
    KalturaUserType
)

# --- Configuration Constants ---
SERVICE_URL = "https://www.kaltura.com/"
SESSION_TYPE = KalturaSessionType.ADMIN
SESSION_EXPIRY = 86400 # 24 hours in seconds

# --- ANSI Color Codes ---
CYAN = '\033[96m'
GREEN = '\033[92m'
RED = '\033[91m'
RESET = '\033[0m'

def setup_environment():
    """
    Checks for a .env file. If it doesn't exist, searches for any *.env file 
    in the current directory and copies the first one found to '.env'.
    """
    if not os.path.exists('.env'):
        env_files = glob.glob('*.env')
        if env_files:
            source_env = env_files[0]
            try:
                shutil.copy(source_env, '.env')
                print(f"{CYAN}Auto-copied {source_env} to .env{RESET}")
            except Exception as e:
                print(f"{RED}Failed to copy {source_env} to .env: {e}{RESET}")
                sys.exit(1)
        else:
            print(f"{RED}Error: No .env or *.env file found in the current directory.{RESET}")
            sys.exit(1)
            
    load_dotenv()

def get_user_status_choice():
    """
    Prompts the user to select which status to filter the export by.
    Returns the corresponding KalturaUserStatus Enum, or None if "ALL".
    """
    print(f"\n{CYAN}Select user status to search for:")
    print("  0: BLOCKED")
    print("  1: ACTIVE")
    print("  2: DELETED")
    print(f"  3: ALL{RESET}")
    
    while True:
        choice = input(f"\n{GREEN}Enter choice (0-3): {RESET}").strip()
        
        if choice == "0":
            return KalturaUserStatus.BLOCKED
        elif choice == "1":
            return KalturaUserStatus.ACTIVE
        elif choice == "2":
            return KalturaUserStatus.DELETED
        elif choice == "3":
            return None # None means we omit the statusEqual filter
        else:
            print(f"{RED}Invalid selection. Please enter 0, 1, 2, or 3.{RESET}")

def main():
    # 1. Setup Environment and Load Variables
    setup_environment()
    
    partner_id = os.getenv("PARTNER_ID")
    admin_secret = os.getenv("ADMIN_SECRET")
    admin_id = os.getenv("ADMIN_ID") # Admin_User_ID Email
    
    if not partner_id or not admin_secret or not admin_id:
        print(f"{RED}Error: PARTNER_ID, ADMIN_SECRET, and ADMIN_ID must be set in the .env file.{RESET}")
        sys.exit(1)

    # 2. Setup Base Kaltura Client & Authentication
    config = KalturaConfiguration()
    config.serviceUrl = SERVICE_URL
    client = KalturaClient(config)
    
    try:
        ks = client.session.start(
            admin_secret,
            admin_id,
            SESSION_TYPE,
            int(partner_id),
            SESSION_EXPIRY
        )
        client.setKs(ks)
    except Exception as e:
        print(f"{RED}Failed to authenticate with Kaltura API: {e}{RESET}")
        sys.exit(1)

    # 3. Prompt user for status selection
    status_selection = get_user_status_choice()

    # 4. Build the User Filter
    user_filter = KalturaUserFilter()
    user_filter.typeEqual = KalturaUserType.USER
    
    # Apply the status filter unless 'ALL' (3) was selected
    if status_selection is not None:
        user_filter.statusEqual = status_selection

    # 5. Execute Export
    print(f"\n{CYAN}Submitting export request to Kaltura servers...{RESET}")
    
    try:
        # By passing only the filter, we rely on the API's default arguments 
        # for metadataProfileId, additionalFields, mappedFields, and options, 
        # avoiding any format schema errors.
        response = client.user.exportToCsv(user_filter)
        
        print(f"{CYAN}Success! Export job initiated.{RESET}")
        print(f"{CYAN}Kaltura will process this request in the background and email the download link to: {admin_id}{RESET}")
        print(f"{CYAN}Job ID/Response: {response}{RESET}")
    except Exception as e:
        print(f"{RED}Failed to initiate CSV export: {e}{RESET}")
        sys.exit(1)

if __name__ == "__main__":
    main()