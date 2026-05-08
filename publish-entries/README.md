# publish-entries

Bulk-publishes Kaltura entries to categories (MediaSpace channels) from a CSV. Supports configurable concurrency, automatic retry with exponential backoff, and optional row filtering — useful for retrying failed publications from the [canvas-to-mediaspace](../canvas-to-mediaspace/) script or any other bulk workflow.

## Prerequisites

- Python 3.8+
- A Kaltura admin API secret
- The following Python packages:

```
pip install KalturaApiClient python-dotenv
```

## Setup

1. Copy `.env.example` to `.env` and fill in your credentials and settings.
2. Create an `input/` directory and place your CSV there.
3. Run the script.

## Input CSV

The CSV must contain at minimum two columns (names are configurable):

| Column | Default name | Description |
|---|---|---|
| Entry ID | `entry_id` | Kaltura entry ID |
| Category ID | `category_id` | Kaltura category ID to publish the entry into |

If your CSV uses different column names, set `ENTRY_ID_COLUMN` and
`CATEGORY_ID_COLUMN` in `.env`.

### Retrying failures from canvas-to-mediaspace

The `published_entries.csv` output from canvas-to-mediaspace can be passed
directly. Set the following in `.env` to process only the failed rows:

```
ENTRY_ID_COLUMN=entry_id
CATEGORY_ID_COLUMN=ms_channel_id
STATUS_COLUMN=publish_status
STATUS_FILTER=error
```

## Running

```bash
# Uses INPUT_CSV_FILENAME from .env
python3 publish-entries.py

# Or pass the file as an argument (overrides .env)
python3 publish-entries.py input/entries_to_publish.csv
```

## Output

A timestamped report CSV is written to `output/`:

| Column | Description |
|---|---|
| `entry_id` | The entry that was processed |
| `category_id` | The target category |
| `status` | `ok` or `error` |
| `error` | Error message if status is `error`, otherwise empty |

## Configuration

| Variable | Default | Description |
|---|---|---|
| `PARTNER_ID` | — | Kaltura partner ID |
| `ADMIN_SECRET` | — | Kaltura admin secret |
| `USER_ID` | — | API user ID |
| `SERVICE_URL` | `https://www.kaltura.com` | Kaltura API endpoint |
| `SESSION_EXPIRY` | `86400` | Session TTL in seconds |
| `INPUT_CSV_FILENAME` | — | Path to input CSV |
| `ENTRY_ID_COLUMN` | `entry_id` | CSV column containing entry IDs |
| `CATEGORY_ID_COLUMN` | `category_id` | CSV column containing category IDs |
| `STATUS_COLUMN` | _(empty)_ | If set, only rows where this column equals `STATUS_FILTER` are processed |
| `STATUS_FILTER` | `error` | Value to match in `STATUS_COLUMN` |
| `THREAD_COUNT` | `10` | Parallel publish requests (5–15 recommended) |
| `MAX_RETRIES` | `4` | Total attempts per entry (1 = no retry) |
