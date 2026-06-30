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

Before running the script, configure the following in your `.env` file:

* `PARTNER_ID`: Your Kaltura partner ID (integer)
* `USER_ID`: Your Kaltura user ID (optional; actions will be associated with this user)
* `PARENT_ID`: The category ID under which new channels will be created. Usually this is the "channels" category in your MediaSpace instance.
* `MEDIA_SPACE_BASE_URL`: The base URL of your MediaSpace instance, usually ending in `/channel/`. Example: `https://mediaspace.ucsd.edu/channel/`. Used to generate accurate channel URLs in the output CSV.
* `FULL_NAME_PREFIX`: The category path prefix used to identify existing channels for duplicate detection (e.g. `MediaSpace>site>channels>`)

Your admin secret is **not** stored in `.env` — you will be prompted for it securely at runtime.

## Channel Settings Reference

| Variable | Value | Meaning |
| :--- | :--- | :--- |
| **USER_JOIN_POLICY** | `1` | Auto Join |
| | `2` | Request to Join |
| | `3` | Not Allowed |
| **APPEAR_IN_LIST** | `1` | Partner Only (Visible to everyone) |
| | `2` | KMC Only |
| | `3` | Category Members Only (Hidden from non-members) |
| **INHERITANCE_TYPE** | `1` | Inherit from parent |
| | `2` | Manual (Override parent settings) |
| **DEFAULT_PERMISSION_LEVEL** | `1` | Contributor |
| | `2` | Moderator |
| | `3` | Member |
| | `4` | Manager |
| **CONTRIBUTION_POLICY** | `1` | All Members |
| | `2` | Members with Contribution Permission |
| **MODERATION** | `0` | No Moderation |
| | `1` | Moderation Required |

## Getting Started

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Set up configuration

Copy `.env.example` to `.env` and fill in your values:

```bash
cp .env.example .env
```

### 3. Run the script

```bash
python3 create-channels.py
```

When prompted, enter your Kaltura admin secret. The input will not be visible as you type. The script validates all rows in the CSV before making any API calls, and writes a timestamped results CSV to the `output/` subfolder when complete.

## Author

Galen Davis  
Senior Education Technology Specialist, UC San Diego
