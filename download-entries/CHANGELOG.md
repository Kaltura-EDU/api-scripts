# Changelog – download-entries.py

## [v2.0.1] – 2026-08-20
### Added
- Pre-flight download-size estimate. Before downloading, the script sums the source-file sizes of all matched entries (and their child entries) from the flavor-asset metadata it already fetches, then prints e.g. `This download will take up about 12.34 GB, beginning...` (sizes shown in GB, or MB when under 1 GB, with comma-grouped thousands). It compares the estimate against the real free space on the destination drive and only pauses to ask `Continue anyway? [y/N]` when the download would not comfortably fit (10% headroom); comfortable downloads just print the total and proceed. Entries whose size can't be determined in advance (e.g. images with no source flavor) are reported as a separate "unknown size" count rather than silently undercounted.

### Changed
- Login failures now show a readable message instead of a raw Python traceback: a wrong Partner ID or Admin Secret (`START_SESSION_ERROR`) prints a clear "could not log in — double-check both values, and use the Administrator (not User) secret" message and exits cleanly, and a network error reaching Kaltura prints a separate "could not reach Kaltura" message.
- Default download folder renamed from `kaltura_downloads` to `output` (still configurable via the `DOWNLOAD_FOLDER` global). When downloading into per-term subdirectories, those subfolders are now created under `output/`.

### Fixed
- Thread-safety: the Kaltura client is not safe to share across threads (concurrent use corrupts its per-request state, surfacing as `AttributeError: 'NoneType' object has no attribute 'get'` in `doQueue`). Each worker thread — in both the new size-estimate pass and the download pass — now builds and reuses its own client via `threading.local`, instead of sharing the single main-thread client. The main thread still uses its own client for category resolution and entry fetching.
- Running out of disk space mid-download now stops the run cleanly with a clear message (including a note that macOS Finder's "Available" figure can include unusable "purgeable" space) instead of a raw `OSError: [Errno 28]` traceback. The partial file being written is removed rather than left behind, entries no longer burn all their retry attempts against a full disk, and remaining category batches are skipped once space runs out. Files already downloaded are left intact.

## [v2.0.0] – 2026-07-06
### Added
- **Category name search**: new dedicated option to search by category name (separate from category ID). Uses Kaltura's `freeText` API with client-side exact-name filtering to match KMC search behavior. When multiple categories share the same name, the script lists them with their full paths and prompts the user to choose.
- **Comma-delimited support for all search fields**: all five search modes (tag, category ID, category name, entry ID, owner user ID) now accept comma-delimited values. Multiple values are combined with OR logic.
- **Subdirectory option**: when multiple comma-delimited values are entered for tags, category IDs, category names, or owner IDs, the script offers to download each term's results into a separate named subdirectory, each with its own CSV report.
- **Download-more loop**: after each download batch completes, the script asks whether to download more and returns to the search menu without requiring credentials to be re-entered.
- **Multithreaded downloads**: entry downloads now run in a configurable thread pool (default: 5 workers, set via `MAX_WORKERS` global). Achieved gigabit-level throughput in testing.
- **Per-entry retry wrapper**: each worker thread retries the full entry processing pipeline (including child entries) up to `RETRY_ATTEMPTS` times with exponential backoff before giving up.
- **Accurate completion reporting**: the final summary line now distinguishes between a fully successful run and one with failures, and reports failure counts per batch when using subdirectories.

### Changed
- Search menu expanded from 4 to 5 options; category ID and category name are now separate choices.
- Caffeinate is now scoped to active download sessions only — it starts when a download begins and terminates when it finishes, rather than running between sessions in the download-more loop.
- Child entry progress messages now use an indented `↳` prefix instead of repeating the parent entry's index number, which was confusing in multithreaded output.

### Fixed
- `get_entries` now retries each page fetch on `KalturaException` and `KalturaClientException` (e.g., network timeouts), and prints a progress line per batch when fetching entries upfront. Previously, a single timeout would crash the script.
- `get_child_entries` now retries on both `KalturaException` and `KalturaClientException` with exponential backoff. Previously it only caught `KalturaException` and had no retry logic.

## [v1.6.0] – 2026-05-01
### Added
- Duplicate filename handling: if two entries produce the same filename (e.g., multiple "Person's Zoom Meeting" recordings), the entry ID is appended to the second file's name to keep both and prevent silent overwrites.
- CSV download report: after each run, a timestamped CSV is saved to the download folder (`YYYY-MM-DD-HHMM_download_report.csv`) listing every entry processed with metadata fields matching a KMC-style export: entry ID, name, description, owner, creator ID, creation date, last updated, duration, media type, tags, categories, download status, and the actual filename written to disk. The report is flushed after each entry so partial results are preserved if the run is interrupted.

## [v1.5.0] – 2026-04-30
### Added
- macOS sleep prevention: the script now launches `caffeinate -i` at the start of a download run and terminates it when the run completes, preventing the computer from sleeping mid-download.

## [v1.4.0] – 2026-04-29
### Added
- Resume support: files that already exist in the download folder are skipped, allowing interrupted runs to be safely restarted.

## [v1.3.0] – 2025-11-20
### Added
- Enhanced security: Admin Secret is now hidden during terminal input using `getpass`. (Galen Davis, UCSD)

## [v1.2.0] - 2025-05-05
### Changed
- Main function now prompts user for Partner ID and Admin Secret
- Updated README
### Removed
- Commented out global variables for Partner ID and Admin Secret, which are now requested by the main function

## [v1.1.0] – 2025-03-21
### Added
- `REMOVE_SUFFIX` global variable to optionally clean up filenames by removing "(Source)" and trailing underscores/dashes.
- Filtering logic to exclude non-media entries (e.g., playlists) from download processing.
- Download progress now numbered for easier tracking.

### Changed
- Simplified main download loop
- Updated README to reflect new functionality and behavior.

## [v1.0.0] – 2025-02-24
- Initial version of script to download Kaltura source files based on tag, category ID, entry ID(s), or owner ID.
- Basic serial download implementation with retry logic and child entry support.
