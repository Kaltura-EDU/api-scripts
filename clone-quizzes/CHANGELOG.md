# Changelog

## [v1.4.0] - 2026-10-08
### Changed
- Every Kaltura API call now goes through a `call_with_retry` helper: transient network errors (timeouts, connection resets) are retried with a growing delay instead of ending the run, while real API errors still surface normally. Tune with `REQUEST_TIMEOUT` (default 120), `MAX_NETWORK_RETRIES` (default 5) and `NETWORK_RETRY_DELAY` (default 5) in `.env`; see `.env.example`.

## [v1.3.1] - 2026-08-20

### Changed
* Login failures now show a readable message instead of a raw Python traceback: a wrong Partner ID or Admin Secret (`START_SESSION_ERROR`) prints a clear "could not log in — double-check both values, and use the Administrator (not User) secret" message and exits cleanly, and a network error reaching Kaltura prints a separate "could not reach Kaltura" message.

## [v1.3.0] - 2026-07-01

### Changed
* Admin secret is now entered at runtime via a secure `getpass` prompt instead of being typed visibly in the terminal.
* `PARTNER_ID`, `ENTRY_IDS`, and optional `TAG` are now read from `.env` instead of being entered interactively.
* Added `load_dotenv()` using the script's own directory (`Path(__file__).with_name(".env")`) so the `.env` file is always found regardless of where the script is called from.
* Added validation of `PARTNER_ID` and `ENTRY_IDS` before the admin secret prompt, so configuration errors are caught immediately.
* Added empty admin secret guard.
* Moved output CSV to an `output/` subfolder (created automatically) instead of the script's current working directory.
* Updated `KalturaFilterPager` page size to 500 (was using Kaltura's default of 30, which would silently truncate quizzes with more than 30 questions).
* Renamed `get_kaltura_client()` to `build_client()` for consistency with other scripts in the repository.
* Replaced old-style `from KalturaClient.Base import KalturaConfiguration` with the standard `from KalturaClient import KalturaClient, KalturaConfiguration`.
* Added `USER_ID`, `SERVICE_URL`, and `PRIVILEGES` as configurable env vars instead of hardcoded values (`"admin"`, `"https://www.kaltura.com/"`, `"all:*,disableentitlement"`).
* Reordered imports to follow PEP 8 convention (stdlib before third-party); removed unused `cuePointType` debug print loop.
* Updated docstring to `"""` style and accurate description.
* Fixed 4 flake8 violations (trailing whitespace, missing space after comma, blank lines, missing newline at EOF).
* Added `.env.example`, `requirements.txt`, and `README.md`.

## [v1.2.0] - 2026-04-30
### Changed
- Output filename format updated: timestamp moved to the beginning of the filename for consistent chronological sorting. New format: `YYYY-MM-DD-HHMM_QuizzesCloned.csv`.

## [v1.1.0] - 2025-05-05
### Changed
- Main function now prompts user for Partner ID and Admin Secret
- Updated get_kaltura_client function to pass variables from user input, simplified to bring in line with other scripts in repo