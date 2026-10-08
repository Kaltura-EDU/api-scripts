# Changelog

## [v1.3.0] - 2026-10-08
### Changed
- Every Kaltura API call now goes through a `call_with_retry` helper: transient network errors (timeouts, connection resets) are retried with a growing delay instead of ending the run, while real API errors still surface normally. Tune with `REQUEST_TIMEOUT` (default 120), `MAX_NETWORK_RETRIES` (default 5) and `NETWORK_RETRY_DELAY` (default 5) in `.env`; see `.env.example`.

## [v1.2.1] - 2026-08-20
### Changed
- Login failures now show a readable message instead of a raw Python traceback: a wrong Partner ID or Admin Secret (`START_SESSION_ERROR`) prints a clear "could not log in — double-check both values, and use the Administrator (not User) secret" message and exits cleanly, and a network error reaching Kaltura prints a separate "could not reach Kaltura" message.

## [v1.2.0] - 2026-04-30
### Changed
- Output filename format standardized: timestamp moved to the beginning of each filename and format updated to `YYYY-MM-DD-HHMM` for consistent chronological sorting across all scripts in the repository.

## [v1.1.0] - 2025-05-05
### Changed
- Main function now prompts user for Partner ID and Admin Secret
- Updated get_kaltura_client function to pass variables from user input, simplified to bring in line with other scripts in repo
- Updated README
