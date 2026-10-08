# Changelog

## [1.1.0] - 2026-10-08
### Changed
- Every Kaltura API call now goes through a `call_with_retry` helper: transient network errors (timeouts, connection resets) are retried with a growing delay instead of ending the run, while real API errors still surface normally. Tune with `REQUEST_TIMEOUT` (default 120), `MAX_NETWORK_RETRIES` (default 5) and `NETWORK_RETRY_DELAY` (default 5) in `.env`; see `.env.example`.

## [1.0.3] - 2026-10-08
### Changed
- The Admin Secret is now prompted at runtime with a hidden `getpass` prompt instead of being read from `.env`, so it no longer sits in the script folder. If `ADMIN_SECRET` is still set in `.env` the script warns and ignores it; delete that line. `.env.example` and the README no longer list it.

## [1.0.2] - 2026-08-20
### Changed
- Login failures now show a readable message instead of a raw Python traceback: a wrong Partner ID or Admin Secret (`START_SESSION_ERROR`) prints a clear "could not log in — double-check both values, and use the Administrator (not User) secret" message and exits cleanly, and a network error reaching Kaltura prints a separate "could not reach Kaltura" message.

## [1.0.1] - 2026-05-08

### Fixed
- Permission level values returned by the Kaltura SDK are `KalturaCategoryUserPermissionLevel` objects, not plain integers. Role lookup now uses `.getValue()` for correct conversion.

### Added
- `timestamp` column in output CSV records when each action completed.
- Parallel member cache building: before processing, the script fetches all members for every unique category via `categoryUser.list` (using `THREAD_COUNT` threads), eliminating per-user `categoryUser.get` calls during processing. Significantly faster when many rows share the same categories.
- Per-category and per-25-category progress output during cache build, including elapsed time and estimated time remaining.
- Per-row progress output during processing, including elapsed time and estimated time remaining every 25 rows.

## [1.0.0] - 2026-05-08

Initial release.

Bulk-manages Kaltura channel membership from a CSV. Supports adding, removing, verifying, and changing the role of users (member, manager, contributor, moderator, owner). Includes ownership transfer via `category.update()`, detailed result descriptions (including before/after role for `change_role`), parallel processing via thread pool, and retry with exponential backoff.
