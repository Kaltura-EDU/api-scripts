## Changelog for create-channels.py

### [2.0.0] - 2026-10-06

#### Added
* Optional `managers`, `moderators`, and `contributors` CSV columns alongside `members`, so each channel can be set up with every role at once. Each takes comma-separated user IDs, and the column names are configurable (`KALTURA_CHANNEL_MANAGERS_HEADER`, `KALTURA_CHANNEL_MODERATORS_HEADER`, `KALTURA_CHANNEL_CONTRIBUTORS_HEADER`). A user listed under more than one role gets the highest one. The owner is left out of the role lists, since they already have full control. The confirmation list shows how many users each channel gets per role.
* `KALTURA_CHANNEL_PRIVACY` sets privacy for every channel, so the CSV no longer needs a `privacy` column repeating the same value. If the CSV does have the column named in `KALTURA_PRIVACY_SETTING_HEADER`, its values win row by row, and blank cells fall back to `KALTURA_CHANNEL_PRIVACY`. The confirmation list shows each channel's privacy, and the results CSV has a new `privacy` column.
* Before anything else, the script shows where each setting comes from: which CSV column it reads for the channel name, owner, privacy, and each user role, which roles aren't used, and which CSV columns it ignores. A mistyped header in `.env` shows up as MISSING, with the real column under Ignored.
* The script lists the channels it's about to create and asks for confirmation before creating anything. It logs in and checks for duplicate names first, so those problems surface before you're asked.
* Network retries: every API call goes through `call_with_retry`, configured with `REQUEST_TIMEOUT` (120), `MAX_NETWORK_RETRIES` (5), and `NETWORK_RETRY_DELAY` (5). The Kaltura SDK's own silent retries are now printed, so a slow connection doesn't look like a hang.
* A warning when `.env` values contain invisible characters (e.g. zero-width spaces from copy-pasting). They are removed before use.
* A warning if `ADMIN_SECRET` is set in the environment; it is always ignored.

#### Changed
* **Breaking:** `.env` settings now start with `KALTURA_` (e.g. `PARTNER_ID` → `KALTURA_PARTNER_ID`, `USER_ID` → `KALTURA_USER`, `SERVICE_URL` → `KALTURA_SERVICE_URL`, `MEDIA_SPACE_BASE_URL` → `KALTURA_MEDIASPACE_URL`, and the `..._HEADER` settings), and `INPUT_CSV_FILENAME` is now `INPUT_FILENAME`. If `.env` still has old names, the script stops and lists what to rename. It also flags a leftover `ADMIN_SECRET` line.
* **Breaking:** the input CSV is read from the `input/` folder next to the script, and results go to `output/` next to the script, no matter where you run it from. A leading `input/` in `INPUT_FILENAME` still works.
* `KALTURA_MEDIASPACE_URL` accepts either the site URL or one ending in `/channel/`.
* If a user can't be added (e.g. a mistyped user ID), the script reports it and moves on instead of stopping partway. Failures are listed at the end and in a new `usersFailed` column of the results CSV, which also has `managersAdded`, `moderatorsAdded`, and `contributorsAdded` columns.
* Results file renamed to `YYYY-MM-DD-HHMM_create-channels.csv`.
* CSV rows are validated before the admin secret prompt, so a bad CSV fails right away.
* Numeric settings with a typo now give a clear error instead of a traceback. `KALTURA_PARTNER_ID` and `KALTURA_PARENT_ID` are checked to be numbers.
* `requirements.txt` now lists `python-dotenv` and `requests`, which the script imports.
* `.gitignore` now covers `input/` and `output/`; an empty `input/` folder ships with the script.
* README rewritten for admins new to the API: `.env` quick-start, how to show hidden files, required vs. optional settings, CSV format, and upgrade notes.

#### Fixed
* Corrected the documented channel setting values. `DEFAULT_PERMISSION_LEVEL` is 0=Manager, 1=Moderator, 2=Contributor, 3=Member (the old docs said 1=Contributor through 4=Manager, and 4 actually means "no access"). `APPEAR_IN_LIST` has no "2 = KMC Only" option. `USER_JOIN_POLICY` is 1=Join automatically, 2=Request to join, 3=Not allowed (not Open/Restricted/Private). The defaults were always correct.

### [1.4.1] - 2026-08-20

#### Changed
* Login failures now show a readable message instead of a raw Python traceback: a wrong Partner ID or Admin Secret (`START_SESSION_ERROR`) prints a clear "could not log in — double-check both values, and use the Administrator (not User) secret" message and exits cleanly, and a network error reaching Kaltura prints a separate "could not reach Kaltura" message.

### [1.4.0] - 2026-07-01

#### Changed
* Admin secret is now entered at runtime via a secure `getpass` prompt instead of being stored in `.env`.
* Added empty admin secret guard to exit cleanly rather than producing a cryptic API error.
* Wrapped all logic in a `main()` function with a top-level `try/except` for clean error output.
* Validation of required env vars (`PARTNER_ID`, `PARENT_ID`, `MEDIA_SPACE_BASE_URL`) and the input CSV file now happens before the admin secret prompt, so configuration errors are caught immediately.
* Replaced `raise ValueError` and `raise FileNotFoundError` with `print()`/`return` for user-friendly error messages instead of raw tracebacks.
* Fixed `load_dotenv()` to use the script's own directory rather than the current working directory.
* Reordered imports to follow PEP 8 convention (stdlib before third-party); moved all imports to the top of the file.
* Renamed `filter` variable in `get_existing_channel_names()` to `cat_filter` to avoid shadowing the Python builtin.
* Fixed redundant `f` prefix on a string with no interpolated values (flake8 F541).
* Fixed two E501 line-length violations.
* Removed `ADMIN_SECRET` from `.env.example`; restructured into session variables, script variables, and CSV column headers sections using standard section header format.
* Updated README to reflect the above changes.

### [1.3.0] - 2026-04-30

#### Changed
* Output filename timestamp format corrected from `%Y-%m-%dT%H%M` to `%Y-%m-%d-%H%M` (removed the `T` separator) for consistency with other scripts in the repository.

### [1.2.1] - 2026-02-04

#### Changed
* Made `MEDIA_SPACE_BASE_URL` a required environment variable and removed the hardcoded default value to improve portability.
* The script now ensures that the generated `channelLink` in the output CSV correctly includes the `/channel/` path.
* Improved comments in `.env.example` for clarity on required variables.

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