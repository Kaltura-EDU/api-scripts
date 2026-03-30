# generate-flavors-from-transcoding-profile.py

This script generates missing flavor assets for Kaltura media entries based on a specified transcoding profile. For each entry, it checks which flavor params defined in the profile are absent or in a failed state, and queues only those for conversion — skipping any flavors that are already ready or in progress.

---

## How it works

1. Fetches the flavor params defined in the specified transcoding profile.
2. Selects entries based on `ENTRY_IDS`, `TAGS`, `CATEGORY_IDS`, or a CSV file.
3. For each entry, compares existing flavor assets against the profile's flavor params.
4. Flavor params already in an active state (READY, QUEUED, CONVERTING, WAIT_FOR_CONVERT, IMPORTING, VALIDATING, EXPORTING) are skipped.
5. Flavor params with no existing asset, or in ERROR, NOT_APPLICABLE, or DELETED state, are queued for conversion via `flavorAsset.convert`.
6. Writes a preview CSV before converting, and a results CSV after.

## Instructions

1. Download all files in this repository or clone the repo.
2. Rename `.env.example` to `.env`.
3. Create a virtual environment (`python3 -m venv venv`).
4. Activate the virtual environment (`source venv/bin/activate` on macOS/Linux, `venv\Scripts\activate` on Windows).
5. Install dependencies (`pip install -r requirements.txt`).
6. Assign values in the `.env` file for your environment.
7. Run the script (`python3 generate-flavors-from-transcoding-profile.py`).

The script writes preview and result reports to the `output` subdirectory alongside the script.

If `CSV_FILENAME` and `ENTRY_ID_COLUMN_HEADER` are provided, the script uses the CSV file to determine which entries to process, overriding `ENTRY_IDS`, `TAGS`, and `CATEGORY_IDS`.

## Configuration

The script requires a `.env` file with the following variables:

**Session**
- `PARTNER_ID`: Your Kaltura partner ID.
- `ADMIN_SECRET`: Your Kaltura admin secret key.
- `USER_ID`: The Kaltura user ID to associate with the session.
- `PRIVILEGES`: Session privileges (default: `all:*,disableentitlement`).

**Required**
- `TRANSCODING_PROFILE_ID`: The ID of the transcoding profile whose flavor params will be generated.

**Entry selection** (use one approach)
- `ENTRY_IDS`: Comma-delimited list of media entry IDs to process.
- `TAGS`: Comma-delimited list of tags to filter entries by (matches entries with any of the listed tags).
- `CATEGORY_IDS`: Comma-delimited list of category IDs to filter entries by.
- `CSV_FILENAME`: Filename of a CSV file (located alongside the script) containing entry IDs to process.
- `ENTRY_ID_COLUMN_HEADER`: The column header in the CSV file that contains the entry IDs. Headers with quotation marks in them are handled correctly — do not include quotation marks in `.env`.

**Performance**
- `MAX_WORKERS`: Number of parallel workers for API calls (default: `5`; recommended maximum: `10`).

## Output

Two CSV files are written to the `output` subdirectory:

**Preview CSV** (`YYYY-MM-DD-HHMM_generate_flavors_PREVIEW.csv`)

| Column | Description |
|---|---|
| `entry_id` | Media entry ID |
| `entry_name` | Media entry name |
| `owner_user_id` | Entry owner |
| `existing_conversion_profile_id` | The conversion profile currently assigned to the entry |
| `transcoding_profile_id` | The transcoding profile ID specified in `.env` |
| `transcoding_profile_name` | The name of that transcoding profile |
| `profile_flavor_params_ids` | All flavor params IDs defined in the profile |
| `flavors_to_generate` | Flavor params IDs that will be queued for conversion |
| `flavors_to_generate_count` | Number of flavor params to generate |
| `flavors_skipped` | Flavor params IDs already in an active state (with status label) |
| `flavors_skipped_count` | Number of flavor params skipped |
| `status` | `READY`, `SKIPPED_ALL_PRESENT`, or `ERROR` |

**Result CSV** (`YYYY-MM-DD-HHMM_generate_flavors_RESULT.csv`)

Same columns as the preview, plus:

| Column | Description |
|---|---|
| `flavors_generated_count` | Number of flavor assets successfully queued |
| `status` | `CONVERTED`, `PARTIAL`, `FAILED`, `SKIPPED_ALL_PRESENT`, or `ERROR` |
| `error` | Error detail for any failed conversion calls |
