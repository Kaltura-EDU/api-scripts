# Changelog

## [1.3.1] – 2026-10-08
### Changed
- Output now goes to the `output/` folder next to the script (created automatically), not the launch folder: the results CSV.

## [1.3.0] – 2026-10-08
### Changed
- Every Kaltura API call now goes through a `call_with_retry` helper: transient network errors (timeouts, connection resets) are retried with a growing delay instead of ending the run, while real API errors still surface normally. Tune with `REQUEST_TIMEOUT` (default 120), `MAX_NETWORK_RETRIES` (default 5) and `NETWORK_RETRY_DELAY` (default 5) in `.env`; see `.env.example`.

## [1.2.1] – 2026-08-20
### Changed
- Login failures now show a readable message instead of a raw Python traceback: a wrong Partner ID or Admin Secret (`START_SESSION_ERROR`) prints a clear "could not log in — double-check both values, and use the Administrator (not User) secret" message and exits cleanly, and a network error reaching Kaltura prints a separate "could not reach Kaltura" message.

## [1.2.0] – 2026-07-01
### Changed
- Source and destination channels can now be identified by either category ID or channel name. Set `SOURCE_CATEGORY_ID` or `SOURCE_CATEGORY_NAME` (and the equivalent `DESTINATION_*` pair) in `.env` — not both.
- The script validates the ID/name inputs before prompting for the admin secret, so configuration errors are caught immediately.
- If a name matches multiple categories, the script lists the conflicting IDs and instructs the user to use the ID variable instead.
- Updated `.env.example` and README to document the new ID/name options.

## [1.1.0] – 2026-06-30
### Changed
- Admin secret is now entered at runtime via a secure prompt (no echo) instead of being stored in `.env`.
- Source and destination category IDs are now set in `.env` (`SOURCE_CATEGORY_ID`, `DESTINATION_CATEGORY_ID`) instead of being entered interactively during the script run.
- Before duplicating, the script displays the number of playlists found in the source category and asks for confirmation.
- Updated `.env.example` to reflect the above changes: removed `ADMIN_SECRET`, added `SOURCE_CATEGORY_ID` and `DESTINATION_CATEGORY_ID`, and grouped variables into session variables and script variables.

## [1.0.0] – 2025-07-07
### Added
- Initial release of `duplicate-playlists.py`.
- Duplicates all Kaltura playlists within a specified category and reassigns them to a new category ID.
- User provides original and destination category IDs during script run.
- Outputs a CSV file listing duplicated playlists and their associated category IDs.
- Includes `.env.example` for environment variable setup.
- Added README with detailed usage instructions.
