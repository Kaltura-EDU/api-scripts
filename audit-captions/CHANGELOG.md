# Changelog – audit-captions.py

## [v1.0.0] – 2026-09-28

Initial release.

### Features
- Audits Kaltura entries for a ready (fully processed) caption asset, chosen via a CSV of entry IDs, tag(s), or typed entry ID(s) at an interactive runtime prompt.
- Optional `CHECK_EAD` toggle also checks for a ready Extended Audio Description asset, independently of the regular caption check. Captions and EAD are distinguished by the caption asset's `usage` field, not by name; requires `KalturaApiClient>=21.20.0` (pinned in `requirements.txt`), the oldest version confirmed to have this field.
- Writes a timestamped output CSV with `entryId`, `title`, `captions`, `EAD` columns (`Y`/`N`; `EAD` left blank when `CHECK_EAD` is off, to distinguish "not checked" from "checked, has none").
- Entries are audited concurrently (`MAX_WORKERS`, default 3), one Kaltura session per worker thread, with API calls paced across all workers via `AUDIT_RATE_PER_SEC` (default 5/sec).
- Keeps the computer awake for the duration of the run via `wakepy`.
- Read-only: does not download, modify, or delete anything.
