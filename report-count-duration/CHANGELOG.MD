# Changelog

All notable changes to this project will be documented in this file.

## v1.3 – 20 March 2025
- Added `original_filename` field to the detailed CSV export
- Filenames are extracted from the source flavorAsset URL using Kaltura's delivery path
- Cleanup logic removes Kaltura-appended " (Source)" text and trailing underscores before ".mp4"

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
