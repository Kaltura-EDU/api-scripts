# Changelog

All notable changes to `update-caption-visibility.py` will be documented in this file.

## [1.1.0] - 2025-05-25
### Added
- Support for filtering entries by **category ID** and **comma-delimited entry ID list**, in addition to tag.
- Interactive user prompt for selecting the filtering method.
- Customizable `CAPTION_LABEL` as a global variable.
- Output log to timestamped CSV file with visibility update results.
- `README.md` and `requirements.txt` created to support reproducible setup.

### Changed
- Refactored session initialization to use compact syntax.
- Capitalized and grouped all configuration constants.
- Replaced legacy command-line argument handling with interactive prompts.
- Function `process_entries_with_tag()` renamed to `get_entries()` for broader support.

## [1.0.0] - 2024-05-20
### Initial version
- Hidden ASR captions labeled "English (auto-generated)" for entries filtered by tag.
