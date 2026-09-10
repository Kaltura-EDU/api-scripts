# Search Entries

Query Kaltura media entries using a set of filters defined in `.env` and
export the matching entries to a CSV. Filters cover entry IDs, name, owner,
tags, category, media type, status, moderation status, created/updated date
ranges, and duration. The script automatically works around Kaltura's
10,000-entry API result cap by re-chunking the query into progressively
smaller time intervals.

## Usage

```
pip install -r requirements.txt
cp .env.example .env
```

Edit `.env` with your Kaltura Partner ID and user ID, then fill in whichever
search filters you want. `PARTNER_ID` is required; everything else falls back
to the defaults in `.env.example`. The admin secret is never stored in
`.env` — it's always prompted at runtime.

```
python3 search-entries.py
```

You'll be prompted for your Kaltura admin secret. The session starts
immediately (failing fast on a bad secret), then the search runs using the
filters from `.env`. Results are written to
`output/YYYY-MM-DD-HHMM_search_results.csv` (the `output` folder,
configurable via `OUTPUT_DIR`, is created automatically). Set
`EXPORT_CSV=False` to print the summary only, without writing a file.

## How filters combine

- **Comma-separated values within a single variable are OR'd.** For example
  `MEDIA_TYPE=VIDEO,AUDIO` matches video *or* audio.
- **Different variables are AND'd.** Setting both `TAG=lecture` and
  `OWNER_ID=jdoe` matches entries that are tagged `lecture` *and* owned by
  `jdoe`.
- Leaving a variable blank means "don't filter on this."

## Filter reference (`.env`)

| Variable | Matches |
| --- | --- |
| `ENTRY_ID` | Specific Kaltura entry IDs (e.g. `0_abc123`) |
| `REFERENCE_ID` | External/reference IDs assigned outside Kaltura |
| `SEARCH_TEXT` | Full-text search across name, description, and tags |
| `ENTRY_NAME_EQUALS` | Name exactly equals a term |
| `ENTRY_NAME_BEGINS_WITH` | Name starts with a term |
| `ENTRY_NAME_CONTAINS` | Name contains a term |
| `ENTRY_NAME_ENDS_WITH` | Name ends with a term |
| `ENTRY_NAME_NOT_CONTAINS` | Exclude entries whose name contains a term |
| `OWNER_ID` | Kaltura user ID of the entry owner |
| `OWNER_STARTS_WITH` | Owner's user ID starts with a term (client-side) |
| `OWNER_ENDS_WITH` | Owner's user ID ends with a term, e.g. `@ucsd.edu` (client-side) |
| `TAG` | Entry tags |
| `CATEGORY_ID` | Numeric category IDs (KMC: Content > Categories) |
| `CATEGORY_NAME` | Full category path (e.g. `MediaSpace>site>channels>x`) |
| `MEDIA_TYPE` | `VIDEO`, `AUDIO`, `IMAGE`, `LIVE_STREAM_*` |
| `STATUS` | Entry lifecycle status (blank = `READY` only) |
| `MODERATION_STATUS` | `APPROVED`, `PENDING_MODERATION`, etc. |
| `CREATED_ON` | Created on a single day (`YYYY-MM-DD`); excludes AFTER/BEFORE |
| `CREATED_AFTER` / `CREATED_BEFORE` | Creation date range (`YYYY-MM-DD`) |
| `UPDATED_AFTER` / `UPDATED_BEFORE` | Last-modified date range (`YYYY-MM-DD`) |
| `DURATION_MIN_SEC` / `DURATION_MAX_SEC` | Duration bounds, in seconds |

### How name matching works (important)

Kaltura's `nameMultiLikeOr` search is a **word-token** match on the search
index, not a raw substring match. Searching `daw` matches only entries that
contain the whole word `daw` — it will *not* match `Dawson`, because
`dawson` is a single token that isn't equal to `daw`. To make
`ENTRY_NAME_BEGINS_WITH` work, the script appends a `*` wildcard to each
term (`daw` → `daw*`) so the index does prefix matching, then enforces the
exact "name starts with" check client-side. `EQUALS` and `CONTAINS` terms
are passed as-is (they target whole words); `ENDS_WITH` and `NOT_CONTAINS`
are applied purely client-side after the API query, so they narrow the CSV
but don't reduce API calls.

If a name search still misses entries you can see in the KMC, your account's
search index may not honor the wildcard. Set `NAME_PREFILTER=False` in
`.env`: the script then skips the API name pre-filter entirely and matches
names purely client-side. This is exhaustive and always correct, but it
scans the whole date range, so it's slower — pair it with a `CREATED_AFTER`/
`CREATED_BEFORE` window or another filter when you can.

### Diagnosing name search

If you're unsure which strategy your account supports, probe it directly:

```
python3 search-entries.py --probe-name daw
```

This prompts for the admin secret, then reports how many entries each of
`nameMultiLikeOr`, `nameLike`, and `freeText` returns for the term both
with and without a trailing `*`, plus a few sample names for each. Whichever
row returns your expected `Dawson…`-style entries tells you the matching
behavior to rely on.

## Auto-chunking around the 10,000-entry cap

`baseEntry.list` refuses any query matching more than ~10,000 entries
(`QUERY_EXCEEDED_MAX_MATCHES_ALLOWED`). Before fetching anything, the script
runs a quick count on the whole date range. If it's too large (9,000 or more,
leaving some headroom), the script splits the created-date range into monthly
chunks, then weekly, then daily, counting again and narrowing only the chunks
that are still too large. The date range searched runs from `CREATED_AFTER`
(or `EARLIEST_START_DATE` if unset) to `CREATED_BEFORE` (or today). If even a
single day exceeds the cap, the script stops and asks you to apply more
specific filters.

## Parallel fetching

Once every chunk's count is known, the script fetches all of their pages (500
entries each) at the same time across `MAX_WORKERS` threads (default `10`),
each with its own Kaltura session. This matters most for searches that scan a
lot of entries — especially `OWNER_STARTS_WITH` / `OWNER_ENDS_WITH`, which have
to check every entry in the date range. Narrow searches that return a page or
two run the same as before.

Higher isn't always faster: if you see repeated timeouts or errors under load,
lower `MAX_WORKERS` (`1` fetches one page at a time) rather than raising
`MAX_NETWORK_RETRIES`. Pages finish in any order, but the CSV is always sorted
by created date, newest first.

## Network reliability

Every Kaltura API call (the session start and each page of `media.list`) is
wrapped in a retry-with-backoff helper. Transient network failures — read
timeouts, connection resets, and the like, which Kaltura surfaces as
`KalturaClientException` (not a subclass of `KalturaException`) — are retried
in place up to `MAX_NETWORK_RETRIES` times, with a linearly growing delay
(`NETWORK_RETRY_DELAY × attempt`). Real, well-formed API errors such as the
10,000-match cap are *not* retried here — they're handled by the auto-chunking
logic above. Tune `REQUEST_TIMEOUT`, `MAX_NETWORK_RETRIES`, and
`NETWORK_RETRY_DELAY` in `.env`.

## Output columns

The CSV includes: `entry_id`, `name`, `description`, `media_type`, `status`,
`moderation_status`, `moderation_count`, `duration_sec`, `duration_min`,
`plays`, `views`, `rank`, `total_rank`, `width`, `height`, `created_at`,
`updated_at`, `last_played_at`, `owner_id`, `creator_id`, `categories`,
`category_ids`, `tags`, `reference_id`, `access_control_id`, `flavor_count`,
`partner_sort_value`, `root_entry_id`, `parent_entry_id`, `display_in_search`,
and `thumbnail_url`. Timestamps are formatted in the `TIMEZONE` from `.env`
(default `US/Pacific`); commas inside `categories` and `tags` are replaced
with semicolons so they stay in a single CSV field.

## Caveats

- If you hit repeated connection errors or timeouts during large queries,
  connecting via VPN can help — an ISP may throttle a burst of API calls to
  the same host.
- Kaltura's API can only match an owner's user ID exactly, so
  `OWNER_STARTS_WITH` and `OWNER_ENDS_WITH` are applied client-side
  (case-insensitive) after the query. They narrow the CSV but don't reduce
  API calls — used alone, the script fetches every entry in the date range.
  Narrow it with `CREATED_AFTER`/`CREATED_BEFORE` or another filter.
- Category filtering by `CATEGORY_ID` uses `categoriesIdsMatchOr`, so entries
  in matching subcategories are included too.
- When no `STATUS` is set, Kaltura returns only `READY` entries by default.
