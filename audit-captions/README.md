# audit-captions

Audits Kaltura media entries to determine whether each has a ready (fully processed, usable) caption asset, and optionally whether each has a ready Extended Audio Description (EAD) asset. Does not download or modify anything — read-only.

"Ready" means `KalturaCaptionAssetStatus.READY` — the file has finished processing, not just uploaded or queued.

## Files

| File | Purpose |
|------|---------|
| `audit-captions.py` | Main script |

Output CSVs are written to `output/` (gitignored).

## Setup

```bash
pip install -r requirements.txt
```

Create a `.env` file in this folder (see Configuration below for the variables it can contain — none are required; the script prompts for anything missing).

## Configuration

All configuration is optional and read from `.env`. Anything left unset is prompted for at runtime instead.

| Variable | Description |
|----------|-------------|
| `PARTNER_ID` | Kaltura partner ID. Not secret; prompted if blank |
| `CHECK_EAD` | `true`/`false` (default `false`). Whether to also check for a ready Extended Audio Description asset |
| `RETRY_ATTEMPTS` | Retry attempts per API call on failure (default `3`) |
| `MAX_WORKERS` | Number of entries to audit concurrently (default `3`) |
| `AUDIT_RATE_PER_SEC` | Maximum API calls per second, shared across all workers combined (default `5`). Not a number measured against a real account's throttle — a starting point. `0` disables pacing |
| `INPUT_FILENAME` | Path to input CSV, relative to the script directory. Only used if you choose the CSV input method |
| `COLUMN_HEADER_ENTRY_ID` | Column header name containing entry IDs in that CSV. Only used if you choose the CSV input method |

The Admin Secret is **not** stored in `.env` — the script asks for it each time it runs (typing is hidden). Find it in KMC → Settings → Integration Settings (use the Administrator secret, not the User secret).

## Usage

```bash
python3 audit-captions.py
```

At the prompt, choose one input method for the run:

1. **CSV of entry IDs** — reads `INPUT_FILENAME` / `COLUMN_HEADER_ENTRY_ID` from `.env`, or prompts for both if unset.
2. **Tag(s)** — comma-delimited, OR logic (any entry matching any tag is included).
3. **Entry ID(s)** — typed directly, comma-delimited.

The script keeps the computer from sleeping while it runs (macOS, Windows, and Linux), so long runs aren't interrupted. The display can still turn off.

## Output

A timestamped CSV is written to `output/`, one row per audited entry:

| Column | Description |
|--------|-------------|
| `entryId` | The entry's ID |
| `title` | The entry's name, or `[entry not found]` if the ID didn't resolve |
| `captions` | `Y` or `N` — has at least one ready caption asset that is **not** an EAD |
| `EAD` | `Y` or `N` if `CHECK_EAD=true`; left **blank** if `CHECK_EAD` is off (blank means "not checked," not "no") |

An entry can be `Y` on both columns — a regular caption and an EAD are separate assets, checked independently.

## Known limitations

- **No throttle-specific retry**: this script paces calls in advance via `AUDIT_RATE_PER_SEC`, but doesn't detect or specifically retry a Kaltura throttle response (`ACTION_BLOCKED`). It only makes read-only `get`/`list` calls, which may not be throttled the same way mutating actions (like recycling or deleting an entry) are.
- **Title lookup cost varies by input method**: tag-search results already include the title (no extra API call); CSV and typed-entry-ID input only have the ID, so each of those costs one extra `baseEntry.get` call per entry.