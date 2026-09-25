# recycle-entries

Bulk-recycles Kaltura media entries from a CSV list of entry IDs, moving them to the owner's recycle bin. Entries in the recycle bin are recoverable by their owner for 30 days before permanent deletion.

## Files

| File | Purpose |
|------|---------|
| `recycle-entries.py` | Main script |
| `.env.example` | Template for environment configuration |

Input CSVs go in `input/`. Output CSVs are written to `output/` (both gitignored).

## Setup

```bash
pip install python-dotenv requests KalturaApiClient wakepy
cp .env.example .env
# Edit .env with your credentials and settings
```

## Configuration

All configuration is via `.env`. Copy `.env.example` and fill in values.

### Credentials

| Variable | Description |
|----------|-------------|
| `PARTNER_ID` | Kaltura partner ID |
| `USER_ID` | User ID for the API session (recommended for audit trail) |
| `SERVICE_URL` | Kaltura API base URL (default: `https://www.kaltura.com`) |
| `PRIVILEGES` | Session privileges (default: `all:*,disableentitlement`) |

The admin secret is **not** stored in `.env` — the script asks for it each time it runs (typing is hidden). Find it in KMC → Settings → Integration Settings (use the Administrator secret, not the User secret).

### Input

| Variable | Description |
|----------|-------------|
| `INPUT_FILENAME` | Path to input CSV, relative to the script directory |
| `COLUMN_HEADER_ENTRY_ID` | Column header name containing entry IDs |

Additional columns in the input CSV are preserved in the output.

### Execution controls

| Variable | Default | Description |
|----------|---------|-------------|
| `DRY_RUN` | `true` | When true, reports what would happen without recycling anything |
| `VALIDATE_ENTRY_EXISTS` | `false` | Calls `baseEntry.get` before recycling to detect missing or already-recycled entries |
| `VALIDATE_ENTRY_EXISTS_IN_DRY_RUN` | `false` | Whether to perform live existence checks during dry runs |

### Concurrency and rate limiting

Kaltura throttles recycles. Sending calls faster than the account allows doesn't recycle entries faster: the extra calls just come back as `ACTION_BLOCKED`. On UCSD's account, running at ~22 calls/sec recycled only ~9% of entries, holding steady at ~2 successful recycles per second. The other 91% had to be re-run. The script therefore paces itself at `RECYCLE_RATE_PER_SEC` and retries anything that is throttled. Plan on about 11 minutes per 1,000 entries at the default rate.

| Variable | Default | Description |
|----------|---------|-------------|
| `RECYCLE_RATE_PER_SEC` | `1.5` | Recycle calls per second, shared across all workers, to stay under Kaltura's recycle throttle. `0` disables pacing |
| `BLOCKED_RETRIES` | `4` | Extra attempts for an entry that comes back `ACTION_BLOCKED` |
| `BLOCKED_RETRY_DELAY` | `10` | Base seconds between `ACTION_BLOCKED` retries (grows: 10, 20, 30, 40) |
| `MAX_WORKERS` | `3` | Number of concurrent workers. Not the speed dial — `RECYCLE_RATE_PER_SEC` is. Beyond about 3, extra workers just wait their turn |
| `REQUEST_DELAY_SEC` | `0` | Extra pause after each request, per worker. Not needed for throttling any more |
| `MAX_RETRIES` | `3` | Retry attempts on network errors |
| `BACKOFF_BASE_SEC` | `1` | Base for exponential backoff between retries |
| `REQUEST_TIMEOUT_SEC` | `10` | Read timeout per API request |
| `REQUEST_CONNECT_TIMEOUT_SEC` | `10` | Connect timeout per API request |
| `PROGRESS_EVERY` | `1` | Print progress every N completed rows |
| `TIMEZONE` | `America/Los_Angeles` | Timezone for output filename timestamps |

## Usage

```bash
# Dry run first (DRY_RUN=true in .env)
python3 recycle-entries.py

# Then set DRY_RUN=false and run live
python3 recycle-entries.py
```

The script keeps the computer from sleeping while it runs (macOS, Windows, and Linux), so long runs aren't interrupted. The display can still turn off.

## Output

The output CSV is written to `output/` with a timestamp in the filename. It contains all original input columns plus:

| Column | Description |
|--------|-------------|
| `entry_id_normalized` | Cleaned entry ID extracted from input |
| `entry_status` | Set to `validated_by_raw_get` if existence check was performed |
| `recycled_success` | `true` or `false` |
| `failure_reason` | Stable enum: `invalid_entry_id`, `entry_does_not_exist`, `permission_denied`, `rate_limited`, `kaltura_error`, `connection_error`, `already_recycled_or_invalid_status`, `child_entry`, `action_blocked`, `stopped`, `unknown_error` |
| `failure_detail` | Human-readable error message from the API |
| `kaltura_error_code` | Raw Kaltura error code if available |
| `attempt_number` | Which retry attempt succeeded or last failed |
| `recycled_at` | ISO timestamp of the recycle attempt |

Output is written incrementally — if the script is interrupted, results processed so far are preserved.

## Known limitations

- **`0_` prefix entries** (legacy imports): `baseEntry.recycle` returns ACTION_BLOCKED via admin API, and a silent no-op when called as the owner. These entries cannot be reliably recycled. Consider deleting them outright. Because the script can't tell this apart from throttling, each one uses up all its `ACTION_BLOCKED` retries (about 100 seconds) before ending up as `failure_reason=action_blocked`. If your list has many of them, lower `BLOCKED_RETRIES` or remove them first.
- **Some Zoom-ingested entries** (`sourceType: 5`, empty `entitledUsersEdit/Publish/View`): Same behavior as above — recycle appears to succeed but the entry remains in My Media. Cause is under investigation with Kaltura Support.
- **Child entries** (where `rootEntryId != entryId`): Automatically detected and skipped with `failure_reason=child_entry`. Recycle the parent entry instead.
- **ACTION_BLOCKED under load**: Going over Kaltura's recycle throttle causes generic ACTION_BLOCKED errors. The script paces and retries automatically. If many entries still end up as `action_blocked`, lower `RECYCLE_RATE_PER_SEC`.
