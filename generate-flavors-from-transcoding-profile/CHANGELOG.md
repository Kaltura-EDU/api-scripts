# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [1.1.0] - 2026-10-08
### Changed
- Every Kaltura API call now goes through a `call_with_retry` helper: transient network errors (timeouts, connection resets) are retried with a growing delay instead of ending the run, while real API errors still surface normally. Tune with `REQUEST_TIMEOUT` (default 120), `MAX_NETWORK_RETRIES` (default 5) and `NETWORK_RETRY_DELAY` (default 5) in `.env`; see `.env.example`.

## [1.0.2] - 2026-10-08
### Changed
- The Admin Secret is now prompted at runtime with a hidden `getpass` prompt instead of being read from `.env`, so it no longer sits in the script folder. If `ADMIN_SECRET` is still set in `.env` the script warns and ignores it; delete that line. `.env.example` and the README no longer list it.

## [1.0.1] - 2026-08-20

### Changed
- Login failures now show a readable message instead of a raw Python traceback: a wrong Partner ID or Admin Secret (`START_SESSION_ERROR`) prints a clear "could not log in — double-check both values, and use the Administrator (not User) secret" message and exits cleanly, and a network error reaching Kaltura prints a separate "could not reach Kaltura" message.

## [1.0.0] - 2026-03-30

### Added
- Initial release of `generate-flavors-from-transcoding-profile`.
- Fetches flavor params defined in a specified transcoding profile via `conversionProfileAssetParams.list`.
- For each entry, compares existing flavor assets against the profile and queues missing or failed flavors via `flavorAsset.convert`.
- Skips flavors already in active states (READY, QUEUED, CONVERTING, WAIT_FOR_CONVERT, IMPORTING, VALIDATING, EXPORTING).
- Supports entry selection by `ENTRY_IDS`, `TAGS`, `CATEGORY_IDS`, or CSV file.
- Writes a preview CSV before converting and a results CSV after.
- Parallel processing via `MAX_WORKERS` (default: 5).
- Progress logging every 25 entries for large batches.
