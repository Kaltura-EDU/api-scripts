# Changelog

All notable changes to the `create-channel.py` script will be documented here.

---

## [v1.5.0] - 2026-11-04
### Added
- The script shows the channel it's about to create and who it will add, then asks for confirmation before creating anything. It logs in first, so a wrong secret fails before you're asked to confirm.
- Each run writes a record to `output/YYYY-MM-DD-HHMM_create-channel.csv`: the channel ID and URL, plus every user, their role, and whether they were added.
- Network retries: every API call goes through `call_with_retry`, configured with `REQUEST_TIMEOUT` (120), `MAX_NETWORK_RETRIES` (5), and `NETWORK_RETRY_DELAY` (5).
- `KALTURA_SERVICE_URL` setting (default `https://www.kaltura.com`).
### Changed
- **Breaking:** `.env` settings now start with `KALTURA_` (e.g. `PARTNER_ID` → `KALTURA_PARTNER_ID`, `USER_ID` → `KALTURA_USER`, `MEDIA_SPACE_URL` → `KALTURA_MEDIASPACE_URL`, `OWNER` → `KALTURA_CHANNEL_OWNER`, `MANAGERS` → `KALTURA_MANAGERS`). If `.env` still has old names, the script stops and lists what to rename. It also flags a leftover `ADMIN_SECRET` line.
- User IDs are matched without regard to capitalization when removing duplicates and skipping the owner, so `JDoe` and `jdoe` are treated as the same user.
- If one user can't be added (e.g. a mistyped ID), the script reports it and keeps going. It still prints the channel ID and URL and lists the failures at the end. Before, it crashed partway, leaving a channel you had to track down.
- README rewritten for admins new to the API: `.env` quick-start, how to show hidden files, required vs. optional settings, and upgrade notes.
### Fixed
- A blank owner, description, or parent ID is no longer sent to Kaltura as an empty value. Those fields are now set only when filled in.
- Numeric settings with a typo now give a clear error instead of a traceback.

## [v1.4] - 2026-10-19
### Added
- `MANAGERS`, `MODERATORS`, and `CONTRIBUTORS` `.env` variables (comma-separated Kaltura user IDs) to assign those permission levels at creation, alongside `MEMBERS`. A user listed more than once gets the highest level; the owner is skipped.
### Fixed
- Corrected the documented setting values. `DEFAULT_PERMISSION_LEVEL` is 0=Manager, 1=Moderator, 2=Contributor, 3=Member (the old docs said 1=Contributor through 4=Manager). `CHANNEL_PRIVACY` is 1=Anyone, 2=Any logged-in user, 3=Channel members only. `USER_JOIN_POLICY` is 1=Join automatically, 2=Request to join, 3=Not allowed. `APPEAR_IN_LIST` has no `2` option. The defaults were always correct.

## [v1.3.1] - 2026-08-20
### Changed
- Login failures now show a readable message instead of a raw Python traceback: a wrong Partner ID or Admin Secret (`START_SESSION_ERROR`) prints a clear "could not log in — double-check both values, and use the Administrator (not User) secret" message and exits cleanly, and a network error reaching Kaltura prints a separate "could not reach Kaltura" message.

## [v1.3] - 2026-06-30
### Changed
- Admin secret is now entered at runtime via a secure `getpass` prompt instead of being stored in `.env`.
- Added validation for required fields (`PARTNER_ID`, `CHANNEL_NAME`, `MEDIA_SPACE_URL`) with clear error messages before any API calls are made.
- Added empty admin secret guard to exit cleanly rather than producing a cryptic API error.
- Wrapped all logic in a `main()` function with a top-level `try/except`, so unhandled API errors print cleanly instead of producing a raw traceback.
- Fixed `load_dotenv()` to use the script's own directory rather than the current working directory, so the `.env` file is always found regardless of where the script is called from.
- Moved `MEDIA_SPACE_URL` from session variables to script variables in `.env.example`; removed `ADMIN_SECRET`; applied standard section header format; added descriptive comments to all variables.
- Reordered imports to follow PEP 8 convention (stdlib before third-party).

## [v1.2] - 2025-05-08
### Added
- Implemented `.env` file support for secure configuration management using `python-dotenv`.
- Added `.env.example` to serve as a configuration template.
- Added `.gitignore` to exclude virtual environments and secrets from version control.
- Added `README.md` with installation steps and a configuration reference table.
### Changed
- Refactored `create-channel.py` to load all configuration from environment variables instead of local Python variables.
- Updated `requirements.txt` to include `python-dotenv`.

## [v1.1] - 2025-05-07
### Changed
- Replaced hardcoded Kaltura category property assignments with global variables
  (e.g., USER_JOIN_POLICY, MODERATION, etc.) to allow full configuration from the top of the script.
- Integrated flake8-friendly formatting: wrapped long lines, formatted list comprehensions, and cleaned print statements.
- Ensured `member_list` is always defined, even if `MEMBERS` is left blank (prevents NameError).
- Improved script output formatting: aligned key labels and added generated channel URL.
- Added a module-level docstring to describe the script’s purpose.

---

## [v1.0] - 2025-03-19
### Added
- Initial version of `create-channel.py` script
- Creates a new MediaSpace channel (Kaltura category) with preset privacy and ownership properties
- Supports optional assignment of members at time of creation
- Outputs basic confirmation and category ID
