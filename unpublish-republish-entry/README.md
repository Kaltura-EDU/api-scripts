# unpublish-republish-entry.py

This script addresses a common issue in Kaltura where a video entry appears in a Canvas Media Gallery but gives users an "Access Denied" error when clicked. The fix is to unpublish the entry from the category and re-add it. This script automates that process using the Kaltura API, as long as you know the entry ID and the category name OR ID. A global variable determines whether you want to use a category ID or the category name. 

## Features

- Supports both category ID or Canvas course ID as input.
- Uses a known Canvas category path (via `fullNameEqual`) for reliable course matching.
- Checks if the entry is actually published before attempting to remove it.
- Skips removal for inactive or ghosted entries.
- Adds the entry back and confirms it's assigned to the category.

## Configuration

Create a `.env` file in the same directory as the script with the following keys:

```env
PARTNER_ID=your_partner_id
ADMIN_SECRET=your_admin_secret
USER_ID=your_user_id
USE_CATEGORY_NAME=True
CATEGORY_PATH_PREFIX=Canvas_Prod>site>channels>
ENTRY_IDS=1_abcd1234,1_efgh5678
```

- Set `USE_CATEGORY_NAME` to `True` to use course IDs (e.g., `15712`) or `False` to use the full category ID directly.
- `ENTRY_IDS` is a comma-delimited list of one or more Kaltura entry IDs to unpublish and republish.

## Requirements

Install dependencies with:

```
pip install -r requirements.txt
```

Dependencies:

- `KalturaApiClient`
- `lxml`
- `python-dotenv`

## Usage

Run the script and follow the prompts:

```bash
python3 unpublish-republish-entry.py
```

It will use the `.env` file for:
- Entry ID(s)
- Canvas course ID (if `USE_CATEGORY_NAME = True`)
- Or category ID (if `USE_CATEGORY_NAME = False`)

## Notes

This script only handles one entry at a time. It's designed to be a fast fix for support tickets where an entry needs a metadata reset in the Media Gallery.

### Important Note on CATEGORY_PATH_PREFIX

We initially attempted to use the category nameEqual filter to look up categories by Canvas course ID (e.g., `15712`), but encountered unreliable results due to Kaltura allowing duplicate category names across different parts of the hierarchy. This made it difficult to consistently find the correct Media Gallery category. 

Since this script is primarily designed to fix "Access Denied" errors in Canvas Media Galleries, we instead rely on the `fullNameEqual` field. This matches the full path of the category in the hierarchy, like:

```shell
`Canvas_Prod>site>channels>15712`
```

The `CATEGORY_PATH_PREFIX` variable represents the full category path prefix used with `fullNameEqual`. Depending on your use case, this may refer to Canvas course folders, MediaSpace channels, or other hierarchical structures.

To make this work in your own environment, you’ll need to set the `CATEGORY_PATH_PREFIX` global variable near the top of the script. For us (UC San Diego), all Media Gallery categories live under:

```python
CATEGORY_PATH_PREFIX = "Canvas_Prod>site>channels>"
```

If your institution uses a different naming or folder structure, be sure to update this variable accordingly so the script can correctly locate the category.

Author: Galen Davis
Senior Education Technology Specialist, UC San Diego
Updated 11/4/2025
