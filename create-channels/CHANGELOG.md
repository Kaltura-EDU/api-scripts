## Changelog for create-channels.py

### \[1.1.0] - 2025-05-04

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
