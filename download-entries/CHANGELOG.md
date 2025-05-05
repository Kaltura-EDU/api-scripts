# Changelog – download-entries.py

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
