# Kaltura API Scripts

This is a collection of scripts written to provide reporting and bulk actions for the University of Alaska's Kaltura account, using Python and the Kaltura API Client.

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

Scripts are designed with entire processes in mind to support both User Management and Media Retention policies. Actionable csv reports are generated where they can be used by other processes to streamline and simplify, while not over-complicating a singular script beyond its intended purpose.
e.g.: User Management script outputs can be directly used as Media Retention script inputs.

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

## Kaltura Report - User Media Detail, KR-umd.py
LAST UPDATED: 2026-06-23 15:40:00
SUMMARY:
This script accepts a User ID via command-line arguments or interactive input to generate an itemized CSV report of all owned Kaltura media entries. It aggregates storage sizes in Kilobytes (KB) from primary video flavors, child entry flavors, and supplemental assets like attachments, captions, and thumbnails. The final export calculates cumulative data usage in Gigabytes (GB), lists child flavor asset IDs, tracks real-time generation metrics, and appends a dynamic flavor definition legend mapped directly from the API.

## Kaltura Report - User Media Summary, KR-ums.py
LAST UPDATED: 2026-06-23 15:26:15
SUMMARY:
This script aggregates comprehensive media profiles for Kaltura users via single ID lookup or batch CSV input. For each user, it calculates totals for media quantity, distinct media types, and playback duration. It analyzes storage footprint by querying flavor sizes (including child entry flavors) along with captions, thumbnails, and attachments. Crucially, the script checks for collaborator configurations, standard categories, and Media Retention Policy categories from "MRP-Categories.csv". Based on configurable media-driven criteria, it conditionally triggers a secondary multi-threaded validation script (KR-umd.py) for deep-dive reporting directly in the terminal interface.

## Kaltura Update - Cleanup User Extra Flavors, KU-cuf.py
LAST UPDATED: 2026-06-23 15:35:24
SUMMARY:
This script cleans up extra transcoded media flavors for specific entries or users to reclaim storage space. It preserves the original source file or the largest remaining flavor. The script processes main media entries along with their child entries, supporting single entry, single user, bulk entry CSV, or bulk user CSV inputs. It evaluates flavor sizes in KB, prioritizing the size property over sizeInBytes. It also applies a toggleable Media Retention Policy (MRP) based on creation and view dates to protect recent media, utilizing a 10-second auto-default countdown timer prompt.
Notes: If there are a lot of entries to process, this can take a while.

## Kaltura Update - media entry Extra Flavor recreate Request, KU-efr.py
Last Updated: 2026-06-23 14:58:42
Summary:
Connects to the Kaltura API using administrative credentials to verify and trigger the recreation of flavor ID 487041 "Basic/Small - WEB/MBL (H264/400)" for targeted media entries. Supports single manual entries or batch CSV processing.

## Created and Managed by:
Sofia Fronzuto, safronzuto@alaska.edu  
Collaboration and Learning Spaces Specialist  
University of Alaska Fairbanks

_LastUpdated:_ 2026-06-23 14:16:54 AKDT
