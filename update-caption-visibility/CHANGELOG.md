# Changelog

All notable changes to `update-caption-visibility.py` will be documented in this file.

## [1.2.1] - 2026-10-08
### Changed
- Output now goes to the `output/` folder next to the script (created automatically), not the launch folder: the results CSV.

## [1.2.0] - 2026-10-08
### Changed
- Every Kaltura API call now goes through a `call_with_retry` helper: transient network errors (timeouts, connection resets) are retried with a growing delay instead of ending the run, while real API errors still surface normally. Tune with `REQUEST_TIMEOUT` (default 120), `MAX_NETWORK_RETRIES` (default 5) and `NETWORK_RETRY_DELAY` (default 5) in `.env`; see `.env.example`.

## [1.1.2] - 2026-10-08
### Changed
- The Admin Secret is now prompted at runtime with a hidden `getpass` prompt instead of being typed into the script file, so it never sits in the script folder.

## [1.1.1] - 2026-08-20
### Changed
- Login failures now show a readable message instead of a raw Python traceback: a wrong Partner ID or Admin Secret (`START_SESSION_ERROR`) prints a clear "could not log in — double-check both values, and use the Administrator (not User) secret" message and exits cleanly, and a network error reaching Kaltura prints a separate "could not reach Kaltura" message.

## [1.1.0] - 2025-05-25
### Added
- Support for filtering entries by **category ID** and **comma-delimited entry ID list**, in addition to tag.
- Interactive user prompt for selecting the filtering method.
- Customizable `CAPTION_LABEL` as a global variable.
- Output log to timestamped CSV file with visibility update results.
- `README.md` and `requirements.txt` created to support reproducible setup.

### Changed
- Refactored session initialization to use compact syntax.
- Capitalized and grouped all configuration constants.
- Replaced legacy command-line argument handling with interactive prompts.
- Function `process_entries_with_tag()` renamed to `get_entries()` for broader support.

## [1.0.0] - 2024-05-20
### Initial version
- Hidden ASR captions labeled "English (auto-generated)" for entries filtered by tag.
