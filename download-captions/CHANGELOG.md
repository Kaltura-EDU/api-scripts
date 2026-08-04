# Changelog

## [1.7.0] - 2026-07-28
### Added
- **Audio description support.** Kaltura stores captions and audio descriptions as the same "caption asset" type; the script now downloads audio descriptions too. They are identified by the asset's `usage` field (`KalturaCaptionAssetUsage` = 1), which is more reliable than the label — only exactly `usage=1` is treated as an audio description, so unknown/new usage values fail safe as captions.
- **Requires KalturaApiClient 22.0.0+** for audio-description detection — the `usage` field was added to the SDK in 22.0.0; `requirements.txt` now pins `>=22.0.0`. In the SDK, `usage` is a `KalturaCaptionAssetUsage` enum object read via `.getValue()` (handled accordingly). If the installed SDK is older *and* the run needs to tell audio descriptions apart from other captions, the script exits with a clear "upgrade your SDK" message instead of silently misclassifying them.
- README onboarding section: how to create `.env` by copying `.env.example` (including the macOS Finder ⌘-Shift-. tip for showing hidden dotfiles), a clearer Required-vs-optional variable breakdown, and a dedicated Query filters section.

### Changed
- **`OUTPUT_FORMAT` now applies to captions only.** Audio descriptions are always saved in their original format and never converted to TXT — stripping their timecodes (the timing is the content) produced meaningless text. So `OUTPUT_FORMAT=txt` or `both` no longer yields garbled audio-description transcripts; the original file is kept instead, with a note in the output.
- **Replaced `SKIP_AUTO_GENERATED` with three "include" toggles** for choosing which caption-asset types to download: `INCLUDE_ASR_CAPTIONS` (default true), `INCLUDE_NON_ASR_CAPTIONS` (default true), and `INCLUDE_AUDIO_DESCRIPTIONS` (default false). At least one must be true or the script exits with a clear error. ASR captions are still identified by `AUTO_GENERATED_LABEL`, which is now required only when `INCLUDE_ASR_CAPTIONS` and `INCLUDE_NON_ASR_CAPTIONS` differ (so the two must be told apart).

### Migration
- If your `.env` from 1.6.0 used `SKIP_AUTO_GENERATED=true`, replace it with `INCLUDE_ASR_CAPTIONS=false` (and set `INCLUDE_AUDIO_DESCRIPTIONS=true` if you also want audio descriptions). `SKIP_AUTO_GENERATED` is no longer read.

## [1.6.0] - 2026-07-28
### Changed
- Each run now writes into a timestamped subfolder of `output/` named for the processing date and time (e.g. `output/2026-07-28_142530/`), keeping batches separated and preventing repeated runs from overwriting each other. The entry creation date has been dropped from individual filenames (which now start with the entry ID), shortening them.

### Added
- Two new filename-component toggles, `INCLUDE_CREATION_DATE_IN_FILENAMES` (default false; uses the **entry** creation date, not the caption track's) and `INCLUDE_CAPTION_NAME_IN_FILENAMES` (default true; the entry/video title), joining the existing `INCLUDE_CAPTION_LABEL_IN_FILENAMES`. The entry ID is always included; at least one of the three toggles must be true or the script exits with a clear error. When multiple caption tracks on one entry would otherwise map to the same filename (e.g. the label is omitted), a numeric suffix is added so none overwrite each other.
- New `SKIP_AUTO_GENERATED` toggle to skip machine (ASR) caption tracks, paired with `AUTO_GENERATED_LABEL` (default `(auto-generated)`) — the label suffix KMC appends to auto-generated captions (KMC > Settings > Reach > Service Parameters). The value is the suffix only and is matched as a case-insensitive substring, so a single setting catches auto-generated tracks across all languages (e.g. `English (auto-generated)`, `Spanish (auto-generated)`). Enabling `SKIP_AUTO_GENERATED` with an empty `AUTO_GENERATED_LABEL` exits with a clear error rather than skipping every track.
- Automatic retry with linear backoff for transient network failures (timeouts, connection resets). All Kaltura API calls — including `session.start` — now go through a `call_with_retry` helper. Because `KalturaClientException` is not a subclass of `KalturaException`, these network errors were previously not retried. Configurable via `REQUEST_TIMEOUT` (default 120), `MAX_NETWORK_RETRIES` (default 5), and `NETWORK_RETRY_DELAY` (default 5) in `.env`.
- `OUTPUT_FORMAT` is now validated at startup; an invalid value exits with a clear error instead of silently downloading and then deleting every file.
- `requests` added to `requirements.txt` (used directly for network-error handling).

### Changed
- Removed the `DOWNLOAD_FOLDER` option. Captions are now always written to a fixed `output` folder next to the script. `output/` has been added to `.gitignore`.

## [1.5.0] - 2026-06-30
### Changed
- Admin secret is now entered at runtime via a secure prompt (no echo) instead of being stored in `.env`.
- Added validation so that submitting an empty admin secret exits with a clear error message rather than a cryptic API failure.
- Renamed `USER` to `KALTURA_USER` to avoid a silent collision with the system `USER` environment variable on macOS/Linux, which caused `.env` values for this variable to be ignored.
- Removed `ADMIN_SECRET` from `.env.example`; added a comment noting it is entered at runtime.
- Grouped `.env.example` variables into session variables, script variables, and query filters.
- Fixed all PEP 8 E501 line-length violations so the script passes `flake8` without warnings.
- Updated README to reflect the above changes.

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
- Introduced a new `.env` variable `USER` (renamed to `KALTURA_USER` in 1.5.0)—allows tagging API actions for tracking/audit logs (e.g., `api-gbdavis`).

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
