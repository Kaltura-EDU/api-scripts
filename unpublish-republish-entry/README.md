# unpublish-republish-entry.py

This script addresses a common issue in Kaltura where a video entry appears in a Canvas Media Gallery but gives users an "Access Denied" error when clicked. The fix is to unpublish the entry from the category and re-add it. This script automates that process using the Kaltura API, as long as you know the entry ID and the category name OR ID. A global variable determines whether you want to use a category ID or the category name. 

## Features

- Supports both category ID or Canvas course ID as input.
- Uses a known Canvas category path (via `fullNameEqual`) for reliable course matching.
- Checks if the entry is actually published before attempting to remove it.
- Skips removal for inactive or ghosted entries.
- Adds the entry back and confirms it's assigned to the category.

## Configuration

Edit the script to set the following global variables:

```python
USE_CATEGORY_NAME = True  # Set to False to enter a category ID directly
FULL_NAME_PREFIX = "Canvas_Prod>site>channels>"  # Update to match your environment
```

Update the session credentials:

```python
PARTNER_ID = ""       # Your partner ID
ADMIN_SECRET = ""     # Your admin secret
USER_ID = ""   # Optional; only used the session creation
```

All sessions use: `privileges = "all:*,disableentitlement"`

## Requirements

Install dependencies with:

```
pip install -r requirements.txt
```

Dependencies:

- `KalturaApiClient`
- `lxml`

## Usage

Run the script and follow the prompts:

```bash
python3 unpublish-republish-entry.py
```

It will ask for:
- Entry ID
- Canvas course ID (if `USE_CATEGORY_NAME = True`)
- Or category ID (if `USE_CATEGORY_NAME = False`)

## Notes

This script only handles one entry at a time. It's designed to be a fast fix for support tickets where an entry needs a metadata reset in the Media Gallery.

