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