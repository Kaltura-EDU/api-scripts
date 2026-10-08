# Changelog

## [1.1.1] - 2026-10-08
### Changed
- Input CSVs are now read from the `input/` folder next to the script (a bare filename in `.env`, a leading `input/`, or an absolute path all still work; a missing file prints the full path that was checked).

## [1.1.0] - 2026-10-08
### Changed
- Every Kaltura API call now goes through a `call_with_retry` helper: transient network errors (timeouts, connection resets) are retried with a growing delay instead of ending the run, while real API errors still surface normally. Tune with `REQUEST_TIMEOUT` (default 120), `MAX_NETWORK_RETRIES` (default 5) and `NETWORK_RETRY_DELAY` (default 5) in `.env`; see `.env.example`.

## [1.0.4] - 2026-10-08
### Changed
- The Admin Secret is now prompted at runtime with a hidden `getpass` prompt instead of being read from `.env`, so it no longer sits in the script folder. If `ADMIN_SECRET` is still set in `.env` the script warns and ignores it; delete that line. `.env.example` and the README no longer list it.

## [1.0.3] - 2026-09-24
### Changed
- Login failures now show a readable message instead of a raw Python traceback: a wrong Partner ID or Admin Secret (`START_SESSION_ERROR`) prints a clear "could not log in — double-check both values, and use the Administrator (not User) secret" message and exits cleanly, and a network error reaching Kaltura prints a separate "could not reach Kaltura" message.

## [1.0.2] - 2026-09-24

### Changed
- Replaced macOS-only `caffeinate` with the cross-platform `wakepy` library to prevent sleep during runs. Keeps the system awake on Windows and Linux as well as macOS. Requires `wakepy` (added to `requirements.txt`).

## [1.0.1] - 2026-05-08

### Fixed
- `channel_members.csv` now includes an `add_status` column (`ok` or `error`) for every member row, making it possible to identify users who were not successfully added to a channel without relying on terminal output.
- `publish_status` in `published_entries.csv` now distinguishes `already_published` (entry was already in the channel — not a true failure) from `error` (genuine API failure). Previously both were recorded as `error`.

## [1.0.0] - 2026-05-08

Initial release.

Written under emergency conditions following a Canvas outage at UC San Diego to bulk-migrate Canvas media galleries to Kaltura MediaSpace channels. Features include:

- Two-level concurrent processing (per-course and per-member/entry thread pools)
- Automatic retry with exponential backoff for transient API errors
- Resume-on-interrupt via a lightweight state file
- Incremental CSV output flushed after each course completes
- macOS sleep prevention via `caffeinate`
- Duplicate channel detection before any changes are made
