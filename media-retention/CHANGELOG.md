# Changelog

All notable changes to this project will be documented in this file.

## [0.2.0] - 2025-11-07
### Changed
- Replaced `caffeinate` functionality with `wakepy` in `media-retention-report.py` and `include-flavor-calculations.py` to add cross-platform functionality for Linux and PC
  - Additional performance knobs for `KEEP_SYSTEM_AWAKE` and `KEEP_DISPLAY_AWAKE` when `PREVENT_COMPUTER_SLEEP` is set to `True`
- Updated `README.md` and `.env.example`

## [0.1.0] - 2025-09-05
### Added
- Initial release with:
  - `media-retention-report.py`
  - `include-flavor-calculations.py`
  - `retention-summary.py`
  - Shared `.env.example` and `README.md`