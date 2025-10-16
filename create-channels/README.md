# Bulk Kaltura Channel Creation

This script allows you to create multiple Kaltura MediaSpace channels in bulk by reading from a CSV input file. Each channel will be created under a specified parent category with designated owners and members.

## What It Does

* Creates Kaltura channels using the Kaltura API
* Assigns an owner and adds members to each channel
* Checks for channel name duplicates before processing
* Validates CSV input data before processing
* Outputs a CSV summary of all created channels, including direct MediaSpace links

## CSV Input File

The input CSV filename is configurable via the `.env` file, allowing you to specify the filename and location as needed.

### Configurable CSV Column Headers

| .env Variable             | Default CSV Column Header |
|---------------------------|---------------------------|
| CHANNEL_NAME_HEADER       | channelName               |
| OWNER_ID_HEADER           | owner                     |
| CHANNEL_MEMBERS_HEADER    | members                   |
| PRIVACY_SETTING_HEADER    | privacy                   |

## Required Configuration

Before running the script, you **must** configure the following environment variables in your `.env` file:

* `PARTNER_ID`: Your Kaltura partner ID (integer)
* `ADMIN_SECRET`: Your admin secret key (string)
* `USER_ID`: Your Kaltura user ID (optional; actions will be associated with this user)
* `PARENT_ID`: The category ID under which new channels will be created. Usually this is the "channels" category in your MediaSpace instance.
* `MEDIA_SPACE_BASE_URL`: The base URL of your MediaSpace instance, usually ending in `/channel/`. Example: `https://mediaspace.ucsd.edu/channel/`. This is so the output CSV has an accurate channel URL.
* `FULL_NAME_PREFIX`: The category path prefix used to identify existing channels (e.g. `MediaSpace>site>channels>`)

All of these are set as environment variables in your `.env` file.

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

### 2. Setup configuration

Copy the `.env.example` file to `.env` and customize the values to match your environment and preferences:

```bash
cp .env.example .env
```

### 3. Edit the `.env` file

Open the `.env` file and fill in the required values for your Kaltura partner, admin secret, user ID, parent category ID, MediaSpace base URL, full name prefix, CSV filename, and CSV column headers as needed.

### 4. Run the script

Once everything is configured and saved, run:

```bash
python3 create-channels.py
```

After the script runs, a timestamped results file will be created in the `reports/` subfolder. It lists all channels created, along with their IDs, members, and direct MediaSpace links.

## Author

Galen Davis  
Senior Education Technology Specialist, UC San Diego  
May 2025
