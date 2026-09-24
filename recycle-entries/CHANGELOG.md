# Changelog – recycle-entries.py

## [1.0.0] – 2026-09-24

Initial public release. The script was used internally at UC San Diego in March and April 2026 to recycle more than 40,000 entries before it was published here.

### Features
- Recycles entries in bulk from a CSV of entry IDs. Every other column in the input CSV is kept in the output CSV, with added columns showing the result for each entry: success/failure, a stable `failure_reason`, the Kaltura error code, and a timestamp.
- `DRY_RUN` reports what would happen without recycling anything.
- `VALIDATE_ENTRY_EXISTS` looks each entry up before recycling it, so missing entries get a clear failure reason.
- Child entries (multi-stream or Zoom multi-layout, where `rootEntryId != entryId`) are skipped automatically, with a note to recycle the parent instead.
- Duplicate entry IDs in the input are recycled once and marked as skipped after that.
- On a Mac, the script keeps the computer from sleeping while it runs (via `caffeinate`, which stops by itself when the script ends), so long runs aren't cut short.
- Output is written as it goes, one row per finished entry, so nothing is lost if the run is stopped with Ctrl+C or interrupted.
- The admin secret is typed in when the script starts (input is hidden) and is never read from `.env`, so it can't travel with the folder when it's shared. The login is checked once up front, so a wrong secret or Partner ID stops the run right away with a readable message instead of a traceback.

### Pacing under Kaltura's recycle throttle
Kaltura throttles recycles. Calls above the limit come back `ACTION_BLOCKED`, which looks like a permission error but isn't. On UCSD's account, runs at ~22 calls/sec held steady at about 2 successful recycles per second, and ~91% of entries were blocked. The script therefore:
- spaces calls evenly across all workers at `RECYCLE_RATE_PER_SEC` (default `1.5`), and
- retries `ACTION_BLOCKED` entries with growing waits (`BLOCKED_RETRIES=4`, `BLOCKED_RETRY_DELAY=10`). An entry still blocked after its retries is recorded as `failure_reason=action_blocked`.

Network errors (timeouts, dropped connections) are retried separately (`MAX_RETRIES`, `BACKOFF_BASE_SEC`). API calls use their own connect and read timeouts (`REQUEST_CONNECT_TIMEOUT_SEC`, `REQUEST_TIMEOUT_SEC`) instead of the SDK's fixed 120 seconds.

### Known limitations
See "Known limitations" in the README: legacy `0_` entries and some Zoom-ingested entries can't be reliably recycled.
