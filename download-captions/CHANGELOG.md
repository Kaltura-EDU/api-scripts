# Changelog

## [1.4.0] - 2026-05-02
### Changed
- Replaced the `CONVERT_TO_TXT` boolean with a new `OUTPUT_FORMAT` variable that accepts three values: `srt` (default — saves original caption file only), `txt` (converts to plain text and deletes the original), or `both` (saves the original caption file and a TXT transcript).
- Deletion of the source caption file is now handled in `download_captions` rather than inside `convert_caption_to_txt`, making the conversion function side-effect-free.
- Updated debug output to display `OUTPUT_FORMAT` instead of `CONVERT_TO_TXT`.

### Backward Compatibility
- Existing `.env` files using `CONVERT_TO_TXT=true` will continue to work; the script maps `CONVERT_TO_TXT=true` → `OUTPUT_FORMAT=txt` and `CONVERT_TO_TXT=false` → `OUTPUT_FORMAT=srt` automatically when `OUTPUT_FORMAT` is not set.

## [1.3.0] - 2025-09-03
### Changed
- Switched all configuration to use `.env` including: `CATEGORY_IDS`, `TAGS`, `ENTRY_IDS`, `OWNER`, `INCLUDE_CHILD_CATEGORIES`, `CONVERT_TO_TXT`, `INCLUDE_CAPTION_LABEL_IN_FILENAMES`, and `USER`.
- Replaced all command-line arguments—now everything is controlled via environment variables.
- Enhanced the category search logic:
  - If `INCLUDE_CHILD_CATEGORIES=true`, fetch subcategory IDs using `category.list` with `ancestorIdIn`, then iterate `categoryEntry.list` per category ID.
- Improved user feedback during execution:
  - After announcing starting conditions, display a “Fetching entries…” progress message to prevent the appearance of hanging.
- Streamlined output formatting:
  - Removed the verbose summary like “55 entries found via categoryEntry (per-category scan). Sample: [...]”.
  - Unified enumeration so each caption file’s operations appear once:
    1. Downloaded
       Converted to TXT
       Deleted (if conversion applied and cleanup enabled)
- Smarter caption format handling:
  - Supports various source formats (e.g., `.srt`, `.vtt`, `.dfxp`).
  - When `CONVERT_TO_TXT=true`, only `.txt` is retained—source captions are deleted post-conversion.
  - When `CONVERT_TO_TXT=false`, only the original caption format is downloaded and preserved.
- Added logic to skip children of multi-stream entries:
  - If an entry has a parent, only process the parent to avoid duplicates.
- Optional filename simplification:
  - When `INCLUDE_CAPTION_LABEL_IN_FILENAMES=false`, caption filenames omit long labels like `English__auto-generated`.
- Introduced a new `.env` variable `USER`—allows tagging API actions for tracking/audit logs (e.g., `api-gbdavis`).

### Fixed
- Resolved trailing double-enumeration issue during download-convert steps.
- Ensured cleanup messages (“Deleted: ...”) only appear if cleanup was performed.

## [1.2.0] - 2025-04-25
### Added
- Optional creation of TXT files from SRT files (stripping out timecode information)
- Separate numbered progress indicator for TXT file creation

## [1.1.0] - 2025-04-24
### Added
- Friendly fallback and message for SSL certificate errors.
- Compatibility update to use timezone-aware datetime (avoids deprecation warnings in Python 3.12+).
- Prints the total number of entries found before downloads begin.
- Numbered progress indicator for each caption file downloaded (e.g., `42. Downloaded: ...`).
