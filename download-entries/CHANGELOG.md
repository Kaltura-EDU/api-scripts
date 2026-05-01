# Changelog – download-entries.py

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
