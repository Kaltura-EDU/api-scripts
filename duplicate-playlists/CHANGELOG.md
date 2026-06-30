# Changelog

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
