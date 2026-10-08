# Changelog

All notable changes to this project will be documented in this file.

## v1.6.2 – 8 October 2026
- The Admin Secret is now prompted at runtime with a hidden `getpass` prompt instead of being read from `.env`, so it no longer sits in the script folder. If `ADMIN_SECRET` is still set in `.env` the script warns and ignores it; delete that line. `.env.example` and the README no longer list it.

## v1.6.1 – 20 August 2026
- Login failures now show a readable message instead of a raw Python traceback: a wrong Partner ID or Admin Secret (`START_SESSION_ERROR`) prints a clear "could not log in — double-check both values, and use the Administrator (not User) secret" message and exits cleanly, and a network error reaching Kaltura prints a separate "could not reach Kaltura" message.

## v1.6 – 30 April 2026
- Output filename format standardized: timestamp moved to the beginning of each filename and format updated to `YYYY-MM-DD-HHMM` for consistent chronological sorting across all scripts in the repository.
- Updated README to reflect new filename format.

## v1.5 – 25 June 2025
- Added other fields to CSV exports: `media_type, lastplayed_at, plays, categories, tags, flavor_count, flavor_size_sum`
- Added `python-dotenv` for better managing the Kaltura secrets

## v1.4 – 21 March 2025
- Added optional runtime filter for `owner_id`, allowing searches by media entry owner
- Introduced `EARLIEST_START_DATE` global variable to support full-repository searches
- If both start and end dates are left blank, the script now defaults to search from this earliest known date through today
- Improved date prompt logic for better error handling and user flexibility
- Updated docstring and README to reflect new filtering and fallback behavior

## v1.3 – 20 March 2025
- Added `original_filename` field to the detailed CSV export
- Filenames are extracted from the source flavor's download URL, retrieved via flavorAsset.getUrl
- Cleanup logic removes Kaltura-appended " (Source)" at the end of filenames and also removes any trailing underscores
- Shortened the docstring

## v1.2 – 15 March 2025
- Added `created_at` and `updated_at` fields to the detailed CSV output, now showing full timestamp (YYYY-MM-DD HH:MM:SS)
- Introduced `TIMEZONE` global variable to control output time zone (default is `"US/Pacific"`); documentation updated to include options for `"US/Eastern"`, `"US/Central"`, `"US/Mountain"`, `"US/Alaska"`, and `"US/Hawaii"`
- Re-added confirmation message printed to console when CSV files are successfully created

## v1.1 – 14 March 2025
- Added `owner_id` field to the detailed CSV output
- Added top-level script docstring describing functionality, inputs, and outputs
- Improved Flake8 compliance throughout key blocks
- Enhanced error handling for Kaltura's 10,000 match limit (QUERY_EXCEEDED_MAX_MATCHES_ALLOWED)

## v1.0 – 12 March 2025
- Initial release of report-count-duration script
- Supports filtering by tag and/or category ID (AND logic)
- Auto-chunking of queries using RESTRICTION_INTERVAL (yearly, monthly, weekly, daily)
- Totals displayed onscreen in multiple time units (minutes, hours, days, months, years)
- Summary and detailed CSVs saved with timestamped filenames to avoid overwrites
