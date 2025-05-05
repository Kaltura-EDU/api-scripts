# Changelog

## [v1.2.0] - 2025-05-05
### Changed
- Main function now prompts user for Partner ID and Admin Secret
- Updated README
### Removed
- Commented out global variables for Partner ID and Admin Secret, which are now requested by the main function

## [v1.1.0] - 2025-04-24
### Added
- Friendly fallback and message for SSL certificate errors.
- Compatibility update to use timezone-aware datetime (avoids deprecation warnings in Python 3.12+).
- Prints the total number of entries found before downloads begin.
- Numbered progress indicator for each caption file downloaded (e.g., `42. Downloaded: ...`).
