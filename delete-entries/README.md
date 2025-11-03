# delete-entries.py

This script permanently deletes or recycles Kaltura media entries by entry ID using the Kaltura API. It is designed for use by administrators who need to remove a large number of entries quickly, particularly in cases where batch deletion or recycling through the KMC is not feasible.

## ⚠️ WARNING

This script **permanently deletes entries** and cannot be undone. They are put in a "recycled" status if required. Use with caution. Entries listed as parents will automatically remove associated child entries as well.

---

## Features

- Accepts a comma-separated list of Kaltura entry IDs or CSV file
- Uses `baseEntry.get()` to collect entry metadata:
  - Entry ID
  - Entry name
  - Owner user ID
  - Duration (in seconds)
- Confirms intent to delete
- Uses `baseEntry.delete()` to permanently remove entries or `baseEntry.recycle()` to recycle them
- Gracefully handles:
  - Already deleted entries
  - Missing or invalid entry IDs
- Outputs a timestamped CSV report with:
  - `entry_id`
  - `entry_name`
  - `owner_user_id`
  - `duration_seconds`
  - `status` (`FOUND`, `NOT FOUND`, `DELETED/RECYCLED`, `ALREADY DELETED/RECYCLED`)

## Instructions

1. Download all files in this repository or clone the repo.
2. Rename `.env.example` to `.env`.
3. Create a virtual environment (`python3 -m venv venv`).
4. Activate the virtual environment (`source venv/bin/activate` on macOS/Linux, `venv\Scripts\activate` on Windows).
5. Install dependencies (`pip install -r requirements.txt`).
6. Assign values in the `.env` file for your environment.
7. Run the script (`python3 delete-entries.py`). 
8. Review the entries listed in the terminal. A preview report CSV will be created with a name like `20250516_1040_deleted_entries_PREVIEW.csv`.
9. Type `DELETE` to confirm and proceed with deletion or `RECYCLE` for recycling. Running it will permanently delete or recycle entries and cannot be undone.
10. A result report will be created with a name like `20250516_1040_deleted_entries_RESULT.csv`. The final status column indicates whether each entry was successfully deleted, recycled, not found, or skipped.

## Configuration

The script requires a `.env` file with the following variables:

- `PARTNER_ID`: Your Kaltura partner ID.
- `ADMIN_SECRET`: Your Kaltura admin secret key.
- `SERVICE_URL`: The Kaltura service URL.
- `ENTRY_IDS`: Comma-delimited list of media entry IDs to process.
- `CSV_FILE`: Path to a CSV file containing entry IDs to process. If provided, this will be used instead of `ENTRY_IDS`.
- `ENTRY_ID_COLUMN_HEADER`: The column header name in the CSV file that contains the entry IDs. Headers with quotation marks in them (in the CSV) are handled correctly. Don't use quotation marks in .env.