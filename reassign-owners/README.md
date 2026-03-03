# reassign-owners.py

Reassigns ownership of Kaltura entries using two supported modes:

1. **User mode** – reassign all entries owned by one user to another user.
2. **Entry mode** – reassign specific entries using a CSV mapping of entry IDs to new owner user IDs.

---

## What This Script Does

1. Reads a CSV mapping file.
2. Determines which mode the script is running in.
   - **User mode:** each row maps an existing owner to a new owner.
   - **Entry mode:** each row maps a specific `entry_id` to a new owner.
3. Retrieves the relevant entries from Kaltura using `baseEntry.list` or `baseEntry.get`.
4. Updates each entry’s owner (`userId`) to the specified new user.
5. Logs the result of every attempted update to a timestamped CSV file.
6. Writes a summary file and an error log.

---

## Input CSV Requirements

The required CSV format depends on the mode used.

### Entry Mode (entry_map)

Used when reassigning ownership of specific entries.

Required columns:

- `entry_id` — The Kaltura entry ID
- `owner_new` — The new owner user ID

Example:

```
entry_id,owner_new
1_abcd1234,multimedia@ucsd.edu
1_efgh5678,multimedia@ucsd.edu
```

Each row represents a single entry whose ownership should be reassigned.


### User Mode (user_map)

Used when reassigning **all entries owned by one user to another user**.

Required columns:

- `owner_old` — Current owner user ID
- `owner_new` — New owner user ID

Example:

```
owner_old,owner_new
jsmith,multimedia@ucsd.edu
adoe,multimedia@ucsd.edu
```

All entries owned by `owner_old` will be reassigned to `owner_new`.


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
MODE=entry_map

# Used only for user_map mode
COLUMN_HEADER_OLD=owner_old
COLUMN_HEADER_NEW=owner_new

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
entry_id,entry_name,owner_old,owner_new,success,error
```

Every attempted update is logged here.

- `success` will contain `success` or `fail`
- `error` will contain the exception message if the operation failed

This makes it easy to audit exactly which entries were processed and which ones require follow‑up.

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

When running in **user_map mode**, if a user owns 10,000 or more entries, Kaltura may cap results. The script will issue a warning if this threshold is reached.

---

## Concurrency and Retries

Ownership updates are performed concurrently using a thread pool.

Each update includes:

- Optional per-request delay
- Retry attempts (`MAX_RETRIES`)
- Exponential backoff with jitter

If all retries fail, the error is logged and processing continues.

Because the Kaltura Python client is not guaranteed to be thread‑safe, the script protects API calls with an internal lock to prevent concurrent client access. This avoids intermittent errors when using multiple workers.

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

In `user_map` mode the script assumes that **all entries owned by each specified old user should be transferred**. In `entry_map` mode only the explicitly listed entry IDs are updated.


Galen Davis  
Senior Education Technology Specialist  
UC San Diego  
3 March 2026