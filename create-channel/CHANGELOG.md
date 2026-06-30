# Changelog

All notable changes to the `create-channel.py` script will be documented here.

---

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
