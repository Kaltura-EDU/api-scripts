# Kaltura API Scripts

This is a collection of scripts written to provide reporting and bulk actions for the University of Alaska's Kaltura account, mostly using Python and the Kaltura API Client.

## Requirements and Credentials
These scripts use a common requirements.txt for loading all the dependencies needed. Note that not all  
dependencies will be used by each script, but this keeps setup uniform and simple.

They use a common .env for passing the following fields, to help each script be portable:  
- Partner ID  
- Admin Secret  
- Admin User ID  

## Commonalities
If a .env is not found, all scripts look for a {anyfilename}.env, and copy it to .env

For scripts that have batch processing using an input CSV, typing the .csv extension is optional when submitting at the prompt.

All scripts save their output/report CSV files to a folder "exports", and delete any intermediate "_processing.csv" files upon completion, to keep the scripts folder clean.

Most scripts have a Session Key expiration of 12 hours, and the script ends the session after completing successfully. 

All scripts that perform updates/changes (KU) have a DryRun mode (default), to be able to check for and resolve errors before doing the actual changes.

Scripts are designed with entire processes in mind to support both User Management and Media Retention policies. Actionable csv reports are generated where they can be used by other processes to streamline and simplify, while not over-complicating a singular script beyond its intended purpose. e.g.: User Management script outputs can be used as Media Retention script inputs.

My current monthly process is:
1. use KR-uxe.py to generate a list of all users in our account
2. use KR-uss.py to update that user list with their current status in Kaltura.
3. use KR-ADP.ps1 to check users' Active Directory Status (Active/Deactivated), and process an actionable report of needed changes in Kaltura
4. use KU-usu.py to apply those changes
6. use KU-cuf.py to cleanup entries on users who were blocked, and have media.

## Kaltura report - User Export to Email, KR-uxe.py
Last Updated: 2026-06-15 15:18:34
SUMMARY: 
Generates an ADMIN session using credentials pulled from a local .env file. If 
an .env file doesn't exist, it checks the directory for any *.env file and 
copies it to .env. The user is prompted to select a user status to export 
(BLOCKED, ACTIVE, DELETED, or ALL). The script then triggers Kaltura's 
built-in background job using the 'user' service and 'exportToCsv' action. 
Kaltura processes the list server-side and automatically emails the resulting 
CSV download link to the user associated with the session (ADMIN_ID in .env).

## Kaltura Report - AD Processing for User Status Updates, KR-ADP.ps1
Last Updated: 2026-07-22 15:40:00
SUMMARY:
This PowerShell script prompts for a CSV file of users, checks their existence and object class (inetOrgPerson) in Active Directory using a multi-threaded Runspace Pool. It compares the Kaltura_Status from the input against the calculated AD_Status to determine if an account needs to be Deactivated, Reactivated, or Transferred.

Generates three reports: 
  1. A full report with processing summary, listing all usernames processed.
  2. A report of UserIds (kadpUpdates) for those users who require a Change (Deactivate, Reactivate, or Transfer).
  3. A report of Transfer accounts mapping the original user ID to the new parent transfer ID (only created if transfers exist).

## Kaltura Report - User Media Detail, KR-umd.py
LAST UPDATED: 2026-07-13 08:35:42
SUMMARY:
This script accepts a User ID via command-line arguments or interactive input to generate an itemized CSV report of all owned Kaltura media entries. It uses highly optimized bulk-fetching (entryIdIn) to retrieve related assets, and explicitly fetches categoryEntry data to map private/entitled categories hidden by default. To bypass Kaltura's 10,000 API pagination limit, it utilizes a time-windowed infinite scrolling mechanism. Data is streamed directly to the CSV per-page to maintain a near-zero memory footprint and ensure data recovery in the event of an interruption.

## Kaltura Report - User Media Summary, KR-ums.py
LAST UPDATED: 2026-07-21 09:08:42
SUMMARY:
This script aggregates comprehensive media profiles for Kaltura users via single ID lookup or batch CSV input. For each user, it calculates totals for media quantity, distinct media types, and playback duration. It analyzes storage footprint by querying flavor sizes (including child entry flavors) along with captions, thumbnails, and attachments. Crucially, the script leverages robust time-based pagination and Kaltura MultiRequests to safely and accurately process deep-dive asset queries without reaching API caps or dropping data for power users. It checks for standard categories and Media Retention Policy categories from "MRP-Categories.csv". Based on configurable media-driven criteria, it conditionally triggers a secondary multi-threaded validation script (KR-umd.py) for detailed reporting.

## Kaltura Report - User Status Summary, KR-uss.py
LAST UPDATED: 2026-08-11 08:35:15
SUMMARY:
This script retrieves user status, media quantity, and timestamps via single ID lookup 
or batch CSV input using Batched API Polling. Includes a timed prompt to toggle the 
ignore list, programmatic ID filtering (>30 chars), local temporary file processing, 
and generates a final execution summary report alongside the standard data export.

## Kaltura Update - User Status Updater, KU-usu.py
LAST UPDATED: 2026-07-28 15:35:00
SUMMARY:
This script efficiently updates Kaltura user statuses (Block, Activate/Reactivate, or Delete) via single ID lookup or batch CSV input. 
In Batch Mode, it reads Column B ('Change') to process 'Deactivate' (Block), 'Reactivate' (Active), and 'Delete' users simultaneously.
It introduces a DryRun safety mode as the default. Includes a robust retry scheme with jitter, and strictly isolated, thread-safe API clients.

## Kaltura Update - Cleanup User Extra Flavors, KU-cuf.py
LAST UPDATED: 2026-08-11 12:01:34
SUMMARY:
This script cleans up extra transcoded media flavors for specific entries or users to reclaim storage space. It preserves the original source file or the largest remaining flavor. The script processes main media entries along with their child entries, supporting single entry, single user, bulk entry CSV, or bulk user CSV inputs. It evaluates flavor sizes in KB, prioritizing the size property over sizeInBytes. It also applies a toggleable Media Retention Policy (MRP) based on creation and view dates to protect recent media, utilizing a 10-second auto-default countdown timer prompt.
Notes: If there are a lot of entries to process, this can take a while.
Additional: This script applies a Retention-related Category ID, to entries that have been cleaned, for easier reporting later. You will want to replace the "CLEANUP_CATEGORY_ID" in the script with a defined category Id from your Kaltura instance, so that the correct category is mapped to processed entries!

## Kaltura Update - media entry Extra Flavor recreate Request, KU-efr.py
Last Updated: 2026-06-23 14:58:42
Summary:
Connects to the Kaltura API using administrative credentials to verify and trigger the recreation of flavor ID 487041 "Basic/Small - WEB/MBL (H264/400)" for targeted media entries. Supports single manual entries or batch CSV processing.

## Created and Managed by:
Sofia Fronzuto, safronzuto@alaska.edu  
Collaboration and Learning Spaces Specialist  
University of Alaska Fairbanks

_LastUpdated:_ 2026-09-03 12:40:50 AKDT
