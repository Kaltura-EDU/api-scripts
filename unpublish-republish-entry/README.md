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

### Important Note on FULL_NAME_PREFIX

We initially attempted to use the category nameEqual filter to look up categories by Canvas course ID (e.g., `15712`), but encountered unreliable results due to Kaltura allowing duplicate category names across different parts of the hierarchy. This made it difficult to consistently find the correct Media Gallery category. 

Since this script is primarily designed to fix "Access Denied" errors in Canvas Media Galleries, we instead rely on the  `fullNameEqual` field. This matches the full path of the category in the hierarchy, like:

```shell
`Canvas_Prod>site>channels>15712`
```
To make this work in your own environment, you’ll need to set the FULL_NAME_PREFIX global variable near the top of the script. For us (UC San Diego), all Media Gallery categories live under:

```python
FULL_NAME_PREFIX = "Canvas_Prod>site>channels>"
```

If your institution uses a different naming or folder structure, be sure to update this variable accordingly so the script can correctly locate the category.

Author: Galen Davis — Kaltura survivor and automation enthusiast
