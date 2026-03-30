# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [1.0.0] - 2026-03-30

### Added
- Initial release of `generate-flavors-from-transcoding-profile`.
- Fetches flavor params defined in a specified transcoding profile via `conversionProfileAssetParams.list`.
- For each entry, compares existing flavor assets against the profile and queues missing or failed flavors via `flavorAsset.convert`.
- Skips flavors already in active states (READY, QUEUED, CONVERTING, WAIT_FOR_CONVERT, IMPORTING, VALIDATING, EXPORTING).
- Supports entry selection by `ENTRY_IDS`, `TAGS`, `CATEGORY_IDS`, or CSV file.
- Writes a preview CSV before converting and a results CSV after.
- Parallel processing via `MAX_WORKERS` (default: 5).
- Progress logging every 25 entries for large batches.
