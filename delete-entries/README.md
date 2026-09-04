# delete-entries.py

This script permanently deletes or recycles Kaltura media entries by entry ID using the Kaltura API. It is designed for use by administrators who need to remove a large number of entries quickly, particularly in cases where batch deletion or recycling through the KMC is not feasible.

## ⚠️ A note on speed

Kaltura throttles deletes. Pushing calls at it faster than the account allows does not delete entries faster — the excess simply comes back as `ACTION_BLOCKED`. On one account, running at ~21 calls/sec landed only 14.5% of deletes, holding steady at ~3.1–3.6 successful deletes per second; the other 85% had to be re-run. The script therefore paces itself at `DELETE_RATE_PER_SEC` (default `2.5`) and retries anything that is throttled. Raising `MAX_WORKERS` will not make it go faster than this ceiling.

## ⚠️ WARNING

This script **permanently deletes entries** and cannot be undone. They are put in a "recycled" status if required. Use with caution. Entries listed as parents will automatically remove associated child entries as well.

---

## Features

- Accepts a comma-separated list of Kaltura entry IDs or a CSV file
- Uses `baseEntry.get()` to collect entry metadata:
  - Entry ID
  - Entry name
  - Owner user ID
  - Duration (in seconds)
  - Play count
- Confirms intent before deleting or recycling
- Uses `baseEntry.delete()` to permanently remove entries or `baseEntry.recycle()` to recycle them
- Gracefully handles already deleted entries and missing or invalid entry IDs
- Retries transient network errors (timeouts, dropped connections) automatically
- Paces requests under Kaltura's delete throttle and retries throttled entries
- Supports concurrent API calls via `MAX_WORKERS` for faster processing of large batches
- `DRY_RUN=true` writes a preview CSV without making any API calls
- `LOOKUP_BEFORE_ACTION=false` skips metadata lookup and goes straight to deletion (faster, but result CSV columns will be blank)
- Numbers each line of the deletion phase (`[ 42/162]`) so progress is visible at a glance
- Outputs a timestamped CSV report with entry ID, name, owner, duration, and status
- Ends with a summary grouping failures by error code, with guidance for common ones

## Requirements

Python 3.10 or newer.

## Instructions

1. Download all files in this folder or clone the repo.
2. Rename `.env.example` to `.env`.
3. Create a virtual environment:
   ```bash
   python3 -m venv venv
   ```
4. Activate the virtual environment:
   ```bash
   source venv/bin/activate   # macOS/Linux
   venv\Scripts\activate      # Windows
   ```
5. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
6. Set values in the `.env` file for your environment.
7. Run the script:
   ```bash
   python3 delete-entries.py
   ```
8. Enter your Kaltura admin secret when prompted. The input will not be visible as you type. The script verifies it immediately, so a wrong secret or partner ID fails right away instead of part-way through the run.
9. Review the entries listed in the terminal. A preview CSV will be written to the `output/` subfolder.
10. Type `DELETE` to permanently delete entries or `RECYCLE` to recycle them. This cannot be undone.
11. A result CSV will be written to the `output/` subfolder. The `status` column shows whether each entry was successfully deleted, recycled, not found, or skipped.

## Configuration

| Variable | Required | Description |
| :--- | :--- | :--- |
| `PARTNER_ID` | Yes | Your Kaltura partner ID |
| `USER_ID` | No | Kaltura user ID for the session (recommended for audit logs) |
| `SERVICE_URL` | No | Kaltura API URL (default: `https://www.kaltura.com`) |
| `PRIVILEGES` | No | Session privileges (default: `all:*,disableentitlement`) |
| `ENTRY_IDS` | One of these | Comma-separated list of entry IDs to process |
| `CSV_FILENAME` | One of these | Filename of a CSV containing entry IDs (relative to script directory) |
| `ENTRY_ID_COLUMN_HEADER` | If using CSV | Column header name for entry IDs in the CSV |
| `DRY_RUN` | No | Set to `true` to preview without deleting (default: `true`) |
| `LOOKUP_BEFORE_ACTION` | No | Set to `false` to skip metadata lookup (default: `true`) |
| `MAX_WORKERS` | No | Concurrent API calls (default: `1`; `10` recommended). Not the delete-speed dial — see `DELETE_RATE_PER_SEC`. It governs the unpaced lookup phase, and provides the headroom the paced delete phase needs (roughly `DELETE_RATE_PER_SEC` × call latency) |
| `DELETE_RATE_PER_SEC` | No | Calls per second, to stay under Kaltura's delete throttle (default: `2.5`; `0` disables pacing) |
| `BLOCKED_RETRIES` | No | Extra attempts for an entry rejected with `ACTION_BLOCKED` (default: `4`) |
| `BLOCKED_RETRY_DELAY` | No | Base seconds between those retries; grows linearly (default: `10`) |
| `REQUEST_TIMEOUT_SEC` | No | API response timeout in seconds (default: `30`) |
| `REQUEST_CONNECT_TIMEOUT_SEC` | No | API connection timeout in seconds (default: `10`) |
| `MAX_NETWORK_RETRIES` | No | Attempts per API call before a network error gives up (default: `5`) |
| `NETWORK_RETRY_DELAY` | No | Base seconds between retries; grows linearly (default: `5`) |

Your admin secret is **not** stored in `.env` — you will be prompted for it securely at runtime.

## Author

Galen Davis  
Senior Education Technology Specialist, UC San Diego
