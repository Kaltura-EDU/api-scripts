# Changelog

## [1.0.0] - 2026-05-08

Initial release.

Written under emergency conditions following a Canvas outage at UC San Diego to bulk-migrate Canvas media galleries to Kaltura MediaSpace channels. Features include:

- Two-level concurrent processing (per-course and per-member/entry thread pools)
- Automatic retry with exponential backoff for transient API errors
- Resume-on-interrupt via a lightweight state file
- Incremental CSV output flushed after each course completes
- macOS sleep prevention via `caffeinate`
- Duplicate channel detection before any changes are made
