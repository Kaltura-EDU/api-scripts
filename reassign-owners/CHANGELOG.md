# Changelog

All notable changes to this project will be documented in this file.

The format is loosely based on Keep a Changelog.

---

## [1.2.0] - 2026-04-30

### Changed

- Output filenames updated: timestamp moved to the beginning of each filename for consistent chronological sorting. New format: `YYYY-MM-DD-HHMM_reassignOwners_[dryRun|live].csv`, `..._summary.txt`, `...errors.txt`.

---

## [1.1.0] - 2026-03-03

### Added

- New **entry_map mode** allowing reassignment of ownership for specific Kaltura entries using a CSV list of `entry_id -> owner_new` mappings.
- Additional onscreen progress feedback during processing to make long runs easier to monitor.

### Changed

- Improved error handling and more user-friendly error messages when CSV input or API calls fail.
- Output CSV now records results for every attempted entry, including success/failure status and error details.

---

## [1.0.0] - 2026-02-24

### Added

- Initial release of `reassign-owners.py`.
- Uses `baseEntry.list` to retrieve entries by owner.
- Uses `baseEntry.update` to reassign ownership (`userId`).
- Full pagination support via `KalturaFilterPager`.
- CSV-driven ownership mapping (old_user -> new_user).
- Validation of user IDs via `user.get` before processing.
- Detection of duplicate/conflicting mappings.
- DRY_RUN mode (configurable via `.env`).
- Configurable concurrency (`MAX_WORKERS`).
- Retry logic with exponential backoff and jitter.
- Optional per-request delay (`REQUEST_DELAY_SEC`).
- Timestamped output files (CSV, summary, error log).
- Configurable timezone for timestamps.
- Structured summary and error reporting.
- README documentation.
- Secure `.env` handling with `.gitignore` protection.

---