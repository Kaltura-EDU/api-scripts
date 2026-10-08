# Changelog – show-category-affilations.py

This changelog was started on 2026-08-20; earlier versions of the script predate it and are not documented here.

## [1.1.0] - 2026-10-08
### Changed
- Every Kaltura API call now goes through a `call_with_retry` helper: transient network errors (timeouts, connection resets) are retried with a growing delay instead of ending the run, while real API errors still surface normally. Tune with `REQUEST_TIMEOUT` (default 120), `MAX_NETWORK_RETRIES` (default 5) and `NETWORK_RETRY_DELAY` (default 5) in `.env`; see `.env.example`.

## [1.0.2] - 2026-10-08
### Changed
- The Admin Secret is now prompted at runtime with a hidden `getpass` prompt instead of being typed into the script file, so it never sits in the script folder.

## [1.0.1] - 2026-08-20
### Changed
- Login failures now show a readable message instead of a raw Python traceback: a wrong Partner ID or Admin Secret (`START_SESSION_ERROR`) prints a clear "could not log in — double-check both values, and use the Administrator (not User) secret" message and exits cleanly, and a network error reaching Kaltura prints a separate "could not reach Kaltura" message.
