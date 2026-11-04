# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [2.0.1] - 2026-11-04
### Changed
* The Kaltura session is now closed (`session.end`) when the script exits, whether it finishes, hits an error, or you stop it with Ctrl+C. Before, the session stayed valid until it expired on its own, so a copied or leaked session key kept working. You'll see "Kaltura session closed." at the end.

## [2.0.0] - 2026-09-24

### Changed
- **Merged the multithreaded version.** Adds `MAX_WORKERS` (parallel lookups and deletions, default 10), `PROGRESS_EVERY`, `SKIP_ENTRY_METADATA`, and `SKIP_CONFIRMATION`, with preview and result CSVs written incrementally. Input CSV columns are carried into the reports.
- **The admin secret is no longer read from `.env`.** The script asks for it at runtime (hidden input). Delete `ADMIN_SECRET` from your `.env`; the script warns if it's still there.
- Settings use the `KALTURA_` prefix: `KALTURA_PARTNER_ID`, `KALTURA_USER`, `KALTURA_SERVICE_URL`. The old names (`PARTNER_ID`, `USER_ID`, `SERVICE_URL`) still work. A blank partner ID is asked for at runtime.
- Input CSVs live in the `input/` folder and reports in `output/`. `INPUT_FILENAME` and `COLUMN_HEADER_ENTRY_ID` replace `CSV_FILENAME` and `ENTRY_ID_COLUMN_HEADER` (old names still work). Header matching ignores capitalization, and a one-column CSV works whatever its header.
- With no entries selected in `.env`, the script asks which entries to process instead of running.
- All API calls retry automatically on network timeouts and dropped connections (`REQUEST_TIMEOUT`, `MAX_NETWORK_RETRIES`, `NETWORK_RETRY_DELAY`). `REQUEST_TIMEOUT_SEC` is still honored.
- Login failures show a readable message instead of a Python traceback.
- Sessions always use `all:*,disableentitlement`; the `PRIVILEGES` setting was removed.
- Removed the unused `pandas` requirement.

### Fixed
- **An empty selection no longer matches every entry in the partner.** Previously, with no `ENTRY_IDS`, `TAGS`, `CATEGORY_IDS`, or CSV set, the search had no filter.
- A flavor lookup that fails is now reported as an `ERROR`. Previously it could be recorded as "single flavor, skipped", and a lookup that failed partway could plan deletions from an incomplete flavor list.
- A flavor whose `isOriginal` value is missing is no longer treated as the source.
- `REQUEST_TIMEOUT_SEC` never took effect (it set a nonexistent client option); the timeout is now applied.
- `.env` is read from the script's folder, not searched for upward from wherever the script is run.
- An entry listed in the input and also found as a child of a listed parent is planned and deleted only once; duplicate IDs in the input CSV are ignored.
- A failed child-entry lookup is noted on the parent row instead of silently skipped.

## [1.4.0] - 2026-03-30

### Changed
- Renamed script and directory from `delete-flavors` to `delete-nonsource-flavors` to better reflect its function.
- Fixed tag filtering: `tagsLikeAny` is not a valid filter field and was silently ignored by the Kaltura client, causing tag-based entry selection to return all entries. Replaced with `tagsMultiLikeOr`.
- Added `.env.example` and `.gitignore` to the repository.

## [1.3.0] - 2025-10-23

### Changed
- Added a new `.env` configuration parameter `ADDITIONAL_FLAVORS_TO_KEEP` that allows to preserve multiple flavors in addition to the source one.

## [1.2.0] - 2025-09-18

### Changed
- Support CSV input: can now supply a `CSV_FILE` and `ENTRY_ID_COLUMN_HEADER` in the `.env` to load entry IDs from a CSV.
- Improved header parsing: `ENTRY_ID_COLUMN_HEADER` normalizes column names (strip quotes / whitespace) so CSV headers with quotation marks work.
- Updated instructions in README: added setup steps (virtualenv, dependencies, etc.).

## [1.1.0] - 2025-08-28

### Changed
- Reports moved to a `reports` subfolder; folder is created if missing.
- Filenames updated to use date-time format as prefix (YYYY-MM-DD-HHMM).
- Improved source flavor detection logic (`isOriginal`, then tags, then largest size).
- Added safeguards against deleting the only flavor of an entry.
- Multi-stream child entry processing with onscreen notification.
- Expanded CSV report with explicit columns: entry ID, name, deleted flavors count, and space saved.
- Onscreen feedback improvements including progress updates and multistream notifications.

## [1.0.0] - 2025-06-27

### Added
- Initial release of the `delete-flavors` script, published on GitHub.
- Deletes all non-source flavors for specified entries (basic `isOriginal == True` check).
- Supports identifying entries by tag, category ID, or list of entry IDs.
