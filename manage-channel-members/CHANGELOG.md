# Changelog

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
