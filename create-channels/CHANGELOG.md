## Changelog for create-channels.py

### [1.2.0] - 2025-10-16

#### Added
* Output report is now saved to a `reports/` subfolder (created automatically if it doesn't exist).
* Input CSV filename is now configurable via `.env` (`INPUT_CSV_FILENAME`).
* CSV column headers are now configurable via `.env`, allowing flexibility in input file schema.

#### Changed
* Refactored script to load all session and global variables from a `.env` file (e.g. credentials, parent ID, configuration).
* Improved header detection logic to gracefully handle Byte Order Mark (BOM) issues in exported CSV files.

### [1.1.0] - 2025-05-04

#### Added

* Duplicate channel name detection via `get_existing_channel_names()`
* CSV row validation before processing to ensure clean input
* Warnings when `members` field is empty
* Stricter error messages for missing or invalid fields

#### Changed

* Refactored to fail early if any duplicate channel names are detected
* Required fields are checked before any API action is taken
* `PARENT_ID` is now cast to `int` to ensure type compatibility
* Added global variable `FULL_NAME_PREFIX` for cleaner configuration

### \[1.0.0] - 2025-04-22

* Initial release with support for basic bulk channel creation via CSV
* Supported owners, members, privacy settings, and output CSV summary
