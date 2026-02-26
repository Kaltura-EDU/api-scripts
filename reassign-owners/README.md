

# reassign-owners.py

Reassigns ownership of Kaltura entries using a CSV mapping of old user IDs to new user IDs.

The script uses `baseEntry.list` to retrieve all entries owned by each specified user and `baseEntry.update` to change the owner (`userId`) to the new user.

---

## What This Script Does

For each row in the input CSV:

1. Validates that both the old and new user IDs exist in Kaltura.
2. Retrieves all entries owned by the old user (with pagination).
3. Updates each entry’s owner to the new user.
4. Logs each ownership change to a timestamped CSV file.
5. Writes a summary file and an error log.

---

## Input CSV Requirements

The input file must contain two columns:

- Old/current owner user ID
- New owner user ID

The column header names are configurable in `.env`:

```
COLUMN_HEADER_OLD=old_username
COLUMN_HEADER_NEW=new_username
```

Example CSV:

```
old_username,new_username
jsmith,jdoe
adoe,bdoe
```

Blank usernames are not allowed and will cause the script to stop.

If the same old user appears multiple times with conflicting new users, the script will stop and report the conflict.

---

## .env Configuration

Required Kaltura credentials:

```
PARTNER_ID=
ADMIN_SECRET=
USER_ID=
SERVICE_URL=https://www.kaltura.com/
PRIVILEGES=all:*,disableentitlement
```

Script configuration:

```
INPUT_FILENAME=input.csv
COLUMN_HEADER_OLD=old_username
COLUMN_HEADER_NEW=new_username

DRY_RUN=true
MAX_WORKERS=10
PAGE_SIZE=100

MAX_RETRIES=3
BACKOFF_BASE_SEC=0.5
REQUEST_DELAY_SEC=0

TIMEZONE=America/Los_Angeles
```

### Important Settings

**DRY_RUN**  
When set to `true`, the script will simulate changes without updating ownership.  
Always run in DRY_RUN mode first.

**MAX_WORKERS**  
Number of concurrent update workers. Default is 10.  
Avoid increasing this significantly to prevent API throttling.

**PAGE_SIZE**  
Number of entries retrieved per page (1–500).

---

## Output Files

All output files are timestamped:

```
reassignOwners_YYYY-MM-DD-HHMM.csv
reassignOwners_YYYY-MM-DD-HHMM_summary.txt
reassignOwners_YYYY-MM-DD-HHMM_errors.txt
```

### Main Output CSV

Headers:

```
entry_id,entry_name,owner_old,owner_new
```

Each successful update (or simulated update in DRY_RUN mode) is logged here.

### Summary File

Includes:

- Timestamp
- Input file used
- DRY_RUN status
- Worker count
- Per-user counts of affected entries
- Total successes and failures

### Error Log

Lists any entries that failed to update, including the error message returned by the API.

---

## Pagination and API Limits

The script implements pagination using `KalturaFilterPager`.

If a user owns 10,000 or more entries, Kaltura may cap results. The script will issue a warning if this threshold is reached.

---

## Concurrency and Retries

Ownership updates are performed concurrently using a thread pool.

Each update includes:

- Optional per-request delay
- Retry attempts (`MAX_RETRIES`)
- Exponential backoff with jitter

If all retries fail, the error is logged and processing continues.

---

## Recommended Workflow

1. Prepare your CSV mapping file.
2. Set `DRY_RUN=true` in `.env`.
3. Run the script and review:
   - Output CSV
   - Summary file
   - Error log
4. If everything looks correct, set `DRY_RUN=false`.
5. Run again to perform actual ownership updates.

---

## Run the Script

```
python3 reassign-owners.py
```

---

## Safety Notes

- Always test with a small sample first.
- Always run in DRY_RUN mode before making real changes.
- Avoid increasing worker count beyond recommended limits.
- Keep secure credentials out of version control.

This script assumes that all entries owned by each specified old user should be transferred.