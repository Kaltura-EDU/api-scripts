# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [1.0.0] - 2025-06-27

### Added
- Initial release of the `delete-flavors` script, published on GitHub.
- Deletes all non-source flavors for specified entries (basic `isOriginal == True` check).
- Supports identifying entries by tag, category ID, or list of entry IDs.

## [1.1.0] - 2025-08-28

### Changed
- Reports moved to a `reports` subfolder; folder is created if missing.
- Filenames updated to use date-time format as prefix (YYYY-MM-DD-HHMM).
- Improved source flavor detection logic (`isOriginal`, then tags, then largest size).
- Added safeguards against deleting the only flavor of an entry.
- Multi-stream child entry processing with onscreen notification.
- Expanded CSV report with explicit columns: entry ID, name, deleted flavors count, and space saved.
- Onscreen feedback improvements including progress updates and multistream notifications.

## [1.2.0] - 2025-09-18

### Changed
- Support CSV input: can now supply a `CSV_FILE` and `ENTRY_ID_COLUMN_HEADER` in the `.env` to load entry IDs from a CSV.
- Improved header parsing: `ENTRY_ID_COLUMN_HEADER` normalizes column names (strip quotes / whitespace) so CSV headers with quotation marks work.
- Updated instructions in README: added setup steps (virtualenv, dependencies, etc.).

## [1.3.0] - 2025-10-23

### Changed
- Added a new `.env` configuration parameter `ADDITIONAL_FLAVORS_TO_KEEP` that allows to preserve multiple flavors in addition to the source one.