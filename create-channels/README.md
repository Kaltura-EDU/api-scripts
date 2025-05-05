# Bulk Kaltura Channel Creation

This script allows you to create multiple Kaltura MediaSpace channels in bulk by reading from a CSV input file. Each channel will be created under a specified parent category with designated owners and members.

## What It Does

* Creates Kaltura channels using the Kaltura API
* Assigns an owner and adds members to each channel
* Checks for channel name duplicates before processing
* Validates CSV input data before processing
* Outputs a CSV summary of all created channels, including direct MediaSpace links

## CSV Input File

Your input file should be named `channelDetails.csv` and placed in the same
directory as the script.

### Required Columns in CSV (channelDetails.csv)

| Column        | Description                                                              |
| ------------- | ------------------------------------------------------------------------ |
| `channelName` | The name of the channel to be created                                    |
| `owner`       | The Kaltura user ID of the channel owner                                 |
| `members`     | Comma-separated list of member user IDs. Use quotes if listing multiple. |
| `privacy`     | Channel privacy level: `1` = Public, `2` = Authenticated, `3` = Private  |

You can use `channelDetails.csv` as a template. Be sure not to change the filename, and keep it in the same directory as the script.

## Required Configuration

Before running the script, you **must** edit the following variables at the top of the file:

* `PARTNER_ID`: Your Kaltura partner ID (integer)
* `ADMIN_SECRET`: Your admin secret key (string)
* `USER_ID`: Your Kaltura user ID (optional; actions will be associated with this user)
* `PARENT_ID`: The category ID under which new channels will be created. Usually this is the "channels" category in your MediaSpace instance.
* `MEDIA_SPACE_BASE_URL`: The base URL of your MediaSpace instance, usually ending in `/channel/`. Example: `https://mediaspace.ucsd.edu/channel/`. This is so the output CSV has an accurate channel URL.
* `FULL_NAME_PREFIX`: The category path prefix used to identify existing channels (e.g. `MediaSpace>site>channels>`)

## Features

* Validates all rows in the CSV before making any changes
* Exits the script gracefully if a duplicate channel name is found
* Accepts empty `members` fields (a warning is shown but processing continues)
* Displays error messages if required fields are missing or invalid
* Outputs a timestamped CSV with the results of the channel creation process

## 🦖 Getting Started

### 1. Install dependencies

Before running the script, make sure you have Python 3 installed and install the required packages:

```bash
pip install -r requirements.txt
```

### 2. Download the script and CSV template

Clone or download the repo, then edit the provided `channelDetails_template.csv` with your own data. Save it as `channelDetails.csv` in the same directory as the script.

### 3. Edit global variables

Open the script file and fill in the required values at the top:

* `PARTNER_ID`
* `ADMIN_SECRET`
* `USER_ID` *(optional but recommended)*
* `PARENT_ID`
* `MEDIA_SPACE_BASE_URL`
* `FULL_NAME_PREFIX`

### 4. Run the script

Once everything is filled in and saved, run:

```bash
python3 create-channels.py
```

After the script runs, a timestamped results file will be created in the same directory. It lists all channels created, along with their IDs, members, and direct MediaSpace links.

## Author

Galen Davis  
Senior Education Technology Specialist, UC San Diego  
May 2025
