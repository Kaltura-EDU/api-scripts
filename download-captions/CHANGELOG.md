# Changelog


## [1.2.0] - 2025-04-25
### Added
- Optional creation of TXT files from SRT files (stripping out timecode information)
- Separate numbered progress indicator for TXT file creation

## [1.1.0] - 2025-04-24
### Added
- Friendly fallback and message for SSL certificate errors.
- Compatibility update to use timezone-aware datetime (avoids deprecation warnings in Python 3.12+).
- Prints the total number of entries found before downloads begin.
- Numbered progress indicator for each caption file downloaded (e.g., `42. Downloaded: ...`).
