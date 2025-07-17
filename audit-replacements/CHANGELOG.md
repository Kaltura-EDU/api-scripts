# Changelog

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
