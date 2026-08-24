# Changelog – upload-entries.py

## [v1.0.0] – 2026-08-24
### Added
- Initial release. Bulk-uploads every media file dropped into a local `input/` folder to Kaltura, creating one media entry per file. Batch-wide metadata (owner, tags, description, co-editors, co-publishers, category memberships, conversion profile) is configured once in `.env`.
- **Reliable large-file uploads via chunked upload.** The Kaltura Python SDK sends an upload as a single streamed request with a 120-second default timeout and silently retries the whole file five times on any timeout, so large videos time out and retry endlessly. This script instead splits each file into `UPLOAD_CHUNK_MB` chunks (default 5 MB) and uploads them with Kaltura's resumable `uploadToken` flow (first chunk `resume=False`; subsequent chunks `resume=True` at their byte offset; final chunk `finalChunk=True`), then creates the entry with the correct media type and binds the uploaded bytes via `media.addContent`. `REQUEST_TIMEOUT` defaults to 600s so a single chunk never times out.
- **Automatic media-type detection** from file extension (video / audio / image); unrecognized files are listed and skipped rather than uploaded as the wrong type.
- **Parallel uploads.** Files upload concurrently across a configurable `MAX_WORKERS` pool (default 4). Because the Kaltura client is not thread-safe, each worker builds and reuses its own client/session via `threading.local`, and CSV rows are written through a thread-safe writer. Default is conservative (4) because upload throughput is bound by outbound bandwidth.
- **Live throughput readout.** A live MB/s figure on the progress bar (single-worker) or on each file's completion line (parallel), plus an "effective MB/s" total for the run. Also recorded per file in the CSV report.
- **Safe re-runs.** Successfully uploaded files are moved to `input/_uploaded/` and failures to `input/_failed/` (toggle with `MOVE_ON_SUCCESS`), so re-running never double-uploads.
- **Category assignment** by numeric ID (`CATEGORY_IDS`) and/or full path (`CATEGORY_NAMES`, resolved to IDs at startup with a disambiguation prompt for duplicate names), applied via `categoryEntry.add`.
- **`DRY_RUN` preview mode** that lists exactly what would be uploaded (and flags unrecognized files) without creating anything in Kaltura.
- Timestamped CSV report written to `output/` after every run: status, entry ID, owner, tags, categories, size, average upload speed, and any error.
- House conventions: admin secret is prompted at runtime (never read from or written to disk), friendly `START_SESSION_ERROR` / network-error login messages, and `call_with_retry` with `REQUEST_TIMEOUT` / `MAX_NETWORK_RETRIES` / `NETWORK_RETRY_DELAY` knobs for transient network failures.
