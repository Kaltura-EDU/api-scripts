# Changelog

All notable changes to this project will be documented in this file.

The format is loosely based on Keep a Changelog.

---

## [1.4.0] - 2026-10-05
### Added
- **Co-editors, co-publishers and co-viewers.** Reassigned entries can now keep access for other people:
  - `KEEP_OLD_OWNER_AS` (any of `editor`, `publisher`, `viewer`) keeps the previous owner on as a co-user after the transfer.
  - `COEDITORS`, `COPUBLISHERS` and `COVIEWERS` add the same user or group IDs to every reassigned entry, in any mode.
  - Optional CSV columns `new_co_editors`, `new_co_publishers` and `new_co_viewers` (header names configurable) add co-users per row in `owner_map` and `entry_map`. Separate several IDs in one cell with semicolons.
- Co-users are appended to the entry's existing ones, never replacing them. The entry is re-read with `baseEntry.get` before its co-users are changed, and the owner and co-user changes go in a single `baseEntry.update`.
- Results CSV has three new columns, `co_editors_added`, `co_publishers_added` and `co_viewers_added`. The summary counts entries that got new co-users.
- An `entry_map` row whose entry already belongs to `owner_new` still gets its co-users added.

### Fixed
- **Runs no longer stall silently.** All workers used to share one Kaltura connection behind a lock, so one slow request held up every other entry, and the SDK's own hidden retries (up to about 75 seconds per request) showed nothing on screen. Each worker now has its own connection, so `MAX_WORKERS` actually runs entries in parallel. SDK retries are printed as they happen.
- In `entry_map` mode, one entry is now checked on its own before the workers start, so a connection problem shows up immediately.

 - 2026-10-05
### Added
- **Optional `owner_old` column in `entry_map` mode** (header set by `COLUMN_HEADER_OWNER_OLD`). It isn't required: the script still looks up each entry's real owner. When the column is there, the results CSV shows it in a new `owner_expected` column and flags any entry whose actual owner is different. New `SKIP_OWNER_MISMATCH` setting (default `false`): when `true`, mismatched entries are left unchanged and logged as skipped instead of reassigned.
- Results CSV has two new columns, `owner_expected` and `note`. The note flags owner mismatches and entries that already belonged to the new owner.
- If `MODE`, `INPUT_FILENAME`, `TAG` or `TAG_NEW_OWNER` is blank in `.env`, the script now asks for it at the terminal. `MODE` used to default silently to `owner_map`.
- Network timeouts and dropped connections are retried automatically on every Kaltura call (`REQUEST_TIMEOUT=120`, `MAX_NETWORK_RETRIES=5`, `NETWORK_RETRY_DELAY=5`).
- Invisible characters (such as zero-width spaces from copy-pasting) are stripped from `.env` values and CSV cells, with a warning naming any `.env` lines that had them.

### Changed
- **The admin secret is now asked for each time the script runs** (typing is hidden) and is no longer read from `.env`. If `ADMIN_SECRET` is still in `.env`, the script ignores it and reminds you to delete it.
- **Input CSVs are read from the `input/` folder** next to the script; set `INPUT_FILENAME` to just the file name (`input/my-list.csv` also works). Results always go to `output/` next to the script, whatever folder you run it from, and `.env` is loaded from the script's folder.
- The "missing headers" error now shows the right example for the mode you're in.
- README rewritten: it now matches the script's actual mode and column names (`owner_map` with `old_username,new_username`; it previously said `user_map` with `owner_old,owner_new`) and adds a getting-started section.

## [1.2.1] - 2026-08-20
### Changed
- Login failures now show a readable message instead of a raw Python traceback: a wrong Partner ID or Admin Secret (`START_SESSION_ERROR`) prints a clear "could not log in — double-check both values, and use the Administrator (not User) secret" message and exits cleanly, and a network error reaching Kaltura prints a separate "could not reach Kaltura" message.

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