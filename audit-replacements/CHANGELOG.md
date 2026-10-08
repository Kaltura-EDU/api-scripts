# Changelog

## v2.1.1 – 2026-10-08
### Changed
- Output now goes to the `output/` folder next to the script (created automatically), not the launch folder: the Excel report.

## v2.1.0 – 2026-10-08
### Changed
- Every Kaltura API call now goes through a `call_with_retry` helper: transient network errors (timeouts, connection resets) are retried with a growing delay instead of ending the run, while real API errors still surface normally. Tune with `REQUEST_TIMEOUT` (default 120), `MAX_NETWORK_RETRIES` (default 5) and `NETWORK_RETRY_DELAY` (default 5) in `.env`; see `.env.example`.

## v2.0.2 – 2026-10-08

### Changed
- The Admin Secret is now prompted at runtime with a hidden `getpass` prompt instead of being read from `.env`, so it no longer sits in the script folder. If `ADMIN_SECRET` is still set in `.env` the script warns and ignores it; delete that line. `.env.example` and the README no longer list it.

## v2.0.1 – 2026-08-20

### Changed

- Login failures now show a readable message instead of a raw Python traceback: a wrong Partner ID or Admin Secret (`START_SESSION_ERROR`) prints a clear "could not log in — double-check both values, and use the Administrator (not User) secret" message and exits cleanly, and a network error reaching Kaltura prints a separate "could not reach Kaltura" message.

## v2.0 – 2025-07-17

### Enhancements

- Switched to environment-based configuration using `.env` and `.env.example`
- Added support for the following search parameters:
  - `CREATOR_ID`
  - `DATE_START` and `DATE_END` (YYYY-MM-DD format)
- `TAGS` now supports comma-delimited OR logic
- `CATEGORY_IDS` now supports comma-delimited OR logic
- Added timezone support via the `TIMEZONE` variable (defaults to `America/Los_Angeles`)
- Introduced `MIN_REPLACEMENT_DELAY_MINUTES` variable to filter out API-initiated creation-time replacements
- Added `MAX_REPLACEMENTS` to limit the number of replacement events shown per entry
- Each entry now appears as a **single row** in the spreadsheet, making it easier to see how many entries among your search results were replaced
- Each replacement timestamp and responsible user ID appears in separate columns: `replacement01`, `replacement01_user`, etc.
- Added support for paginated API responses to handle large result sets
- Script now prints progress to the console (`Processing: <entry ID>`)
- Added a second Excel sheet named `Search_Terms` showing search criteria used (excluding sensitive fields)


### Requirements Update

- Now uses: `python-dotenv`
- Updated `requirements.txt` accordingly
