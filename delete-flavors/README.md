# delete-flavors.py

This script helps administrators efficiently manage Kaltura media storage by permanently removing all redundant flavors (transcoded versions) of media entries, keeping only the source flavor. Additionally, it generates a CSV report detailing the entries processed, the number of flavors deleted, and the total space saved. This tool is designed for administrators who need to remove a large number of flavors quickly, as batch deletion through the KMC is not feasible.

## ⚠️ WARNING

This script **permanently deletes flavors** and cannot be undone. They are **not** put in a "recycled" status. Use with caution. If you need to regenerate flavors, you can use media.convert or flavorAsset.add. 

---

## How it works

- Keeps a flavor if `isOriginal == True`.
- Fallback: keeps a flavor if its tags include "source".
- Fallback: keeps the largest flavor by `sizeInBytes`.
- Skips entries that have only one flavor (the source).

## Instructions

1. Download all files in this repository or clone the repo.
2. Rename `.env.example` to `.env`.
3. Create a virtual environment (`python3 -m venv venv`).
4. Activate the virtual environment (`source venv/bin/activate` on macOS/Linux, `venv\Scripts\activate` on Windows).
5. Install dependencies (`pip install -r requirements.txt`).
6. Assign values in the `.env` file for your environment.
7. Run the script (`python3 delete-flavors.py`).

**Note:** The script generates reports in the `reports` subdirectory. Running it will permanently delete flavors and cannot be undone.

If `CSV_FILE` and `ENTRY_ID_COLUMN_HEADER` environment variables are provided, the script will use the CSV file to determine which entries to process instead of using `ENTRY_IDS`, `CATEGORY_IDS`, or `TAGS`.

## Configuration

The script requires a `.env` file with the following variables:

- `PARTNER_ID`: Your Kaltura partner ID.
- `ADMIN_SECRET`: Your Kaltura admin secret key.
- `SERVICE_URL`: The Kaltura service URL.
- `ADDITIONAL_FLAVORS_TO_KEEP`: Comma-delimited list of Flavor IDs to preserve in addition to the source one.
- `ENTRY_IDS`: Comma-delimited list of media entry IDs to process.
- `CATEGORY_IDS`: Comma-delimited list of category IDs to filter entries.
- `TAGS`: Comma-delimited list of tags to filter entries.
- `CSV_FILE`: Path to a CSV file containing entry IDs to process. If provided, this will be used instead of `ENTRY_IDS`, `CATEGORY_IDS`, or `TAGS`.
- `ENTRY_ID_COLUMN_HEADER`: The column header name in the CSV file that contains the entry IDs. Headers with quotation marks in them (in the CSV) are handled correctly. Don't use quotation marks in .env.

## Output

The script creates a CSV file containing the following columns:

- `entry_id`: The ID of the media entry.
- `entry_name`: The name of the media entry.
- `flavors_deleted`: Number of flavors deleted for the entry.
- `kilobytes_saved`: Total KiloBytes of storage space saved by deleting flavors.