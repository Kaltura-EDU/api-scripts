# Changelog

## [1.1.1] - 2026-10-08
### Changed
- Input CSVs are now read from the `input/` folder next to the script (a bare filename in `.env`, a leading `input/`, or an absolute path all still work; a missing file prints the full path that was checked).

## [1.1.0] - 2026-10-08
### Changed
- Every Kaltura API call now goes through a `call_with_retry` helper: transient network errors (timeouts, connection resets) are retried with a growing delay instead of ending the run, while real API errors still surface normally. Tune with `REQUEST_TIMEOUT` (default 120), `MAX_NETWORK_RETRIES` (default 5) and `NETWORK_RETRY_DELAY` (default 5) in `.env`; see `.env.example`.

## [1.0.2] - 2026-10-08
### Changed
- The Admin Secret is now prompted at runtime with a hidden `getpass` prompt instead of being read from `.env`, so it no longer sits in the script folder. If `ADMIN_SECRET` is still set in `.env` the script warns and ignores it; delete that line. `.env.example` and the README no longer list it.

## [1.0.1] - 2026-08-20
### Changed
- Login failures now show a readable message instead of a raw Python traceback: a wrong Partner ID or Admin Secret (`START_SESSION_ERROR`) prints a clear "could not log in — double-check both values, and use the Administrator (not User) secret" message and exits cleanly, and a network error reaching Kaltura prints a separate "could not reach Kaltura" message.

## [1.0.0] - 2026-05-08

Initial release.

Bulk-publishes Kaltura entries to categories from a CSV. Originally developed to retry failed entry publications from a canvas-to-mediaspace migration run. Features include configurable column name mapping, optional row filtering by status value, parallel publishing via thread pool, and retry with exponential backoff.
