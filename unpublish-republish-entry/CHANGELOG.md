# Changelog

## [1.3.0] - 2026-11-04
### Changed
- The category is now chosen by what `.env` contains instead of requiring `USE_CATEGORY_NAME`: if `CATEGORY_ID` is set it is used directly; otherwise, if `CHANNEL_NAME` (e.g. a Canvas course ID like `15712`) is set, the category is looked up by name; if neither is set, the script prompts. An explicit `USE_CATEGORY_NAME=True/False` still overrides this.
- `FULL_NAME_PREFIX` and `CANVAS_COURSE_ID` are accepted as older names for `CATEGORY_PATH_PREFIX` and `CHANNEL_NAME`.
- README `.env` example and setting descriptions updated to match.

## [1.2.2] - 2026-10-30
### Fixed
- `CATEGORY_ID` in `.env` is now actually read; previously the script ignored it and always prompted for a category ID. Set `USE_CATEGORY_NAME=True` to use the channel-name lookup instead.

## [1.2.1] - 2026-10-08
### Changed
- The default `USER_ID` is now the neutral `api-user` instead of a personal account name; set your own in `.env`.

## [1.2.0] - 2026-10-08
### Changed
- Every Kaltura API call now goes through a `call_with_retry` helper: transient network errors (timeouts, connection resets) are retried with a growing delay instead of ending the run, while real API errors still surface normally. Tune with `REQUEST_TIMEOUT` (default 120), `MAX_NETWORK_RETRIES` (default 5) and `NETWORK_RETRY_DELAY` (default 5) in `.env`; see `.env.example`.

## [1.1.2] - 2026-10-08
### Changed
- The Admin Secret is now prompted at runtime with a hidden `getpass` prompt instead of being read from `.env`, so it no longer sits in the script folder. If `ADMIN_SECRET` is still set in `.env` the script warns and ignores it; delete that line. `.env.example` and the README no longer list it.

## [1.1.1] - 2026-08-20
### Changed
- Login failures now show a readable message instead of a raw Python traceback: a wrong Partner ID or Admin Secret (`START_SESSION_ERROR`) prints a clear "could not log in — double-check both values, and use the Administrator (not User) secret" message and exits cleanly, and a network error reaching Kaltura prints a separate "could not reach Kaltura" message.

## [1.1.0] - 2025-11-04
### Changed
- Updated script to use `.env` file for configuration, including support for multiple `ENTRY_IDS`.
- Replaced interactive prompts with environment variables where applicable.
- Switched category lookup to support full name matching with customizable prefix.

### Added
- `.env.example` template with all required variables.
- Support for batch unpublish/republish using comma-delimited `ENTRY_IDS`.
- New `README.md` instructions for .env-based usage.

## [1.0.0] - 2025-11-01
### Added
- Initial version: unpublish and republish a Kaltura entry by removing and re-adding it to a given category.
