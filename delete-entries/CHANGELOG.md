# Changelog for delete-entries.py

### [1.2.0] - 2026-09-03

#### Added
* Transient network failures (timeouts, dropped connections) are now retried automatically with linear backoff, via the standard `call_with_retry` helper. Tunable with the new `MAX_NETWORK_RETRIES` (default `5`) and `NETWORK_RETRY_DELAY` (default `5`) variables. Previously a single blip mid-run marked an entry `FAILED: connection error` with no second attempt.
* The admin session is now started immediately after the secret prompt, so a wrong secret or partner ID fails fast with the readable login message. Previously the session was built lazily inside worker threads, where the `SystemExit` it raises escaped the top-level `except Exception` handler and printed a raw traceback — once per worker.

* API messages and entry names are now XML-unescaped before display. An error read `Action &quot;delete&quot; in service &quot;baseentry&quot; is blocked`; it now reads `Action "delete" in service "baseentry" is blocked`. This also applies to the `entry_name` column, so a name like `Ben & Jerry's` no longer lands in the CSV as `Ben &amp; Jerry&#39;s`.
* Runs now end with a summary that groups failures by error code, with the message shown once per code and a wrapped hint for codes that have a known next step (`ACTION_BLOCKED`, `ENTRY_ID_NOT_FOUND`). A run where 162 entries fail identically no longer requires scrolling back through 162 identical lines to see what happened.

* Requests are now paced at `DELETE_RATE_PER_SEC` (default `2.5`) by a shared rate limiter, and entries rejected with `ACTION_BLOCKED` are retried with linear backoff (`BLOCKED_RETRIES`, `BLOCKED_RETRY_DELAY`). Kaltura throttles deletes and rejects the excess rather than queueing it: on a production account, a run attempting ~21.6 calls/sec landed only 14.5% of its deletes, with successes holding at ~3.1-3.6/sec and spread evenly across the run — the signature of a throttle, not of undeletable entries. The same entry was observed returning `ACTION_BLOCKED` in one run and deleting normally a minute later. Pacing below the ceiling lets nearly every call land on the first pass instead of requiring repeated re-runs.

* A `delete` whose response is lost to a network timeout is no longer reported as a failure. The call is not idempotent: the lost request can reach Kaltura and delete the entry before the response times out, so the automatic retry then gets `ENTRY_ID_NOT_FOUND` for an entry that was successfully deleted. That combination — a network retry on this call, followed by `ENTRY_ID_NOT_FOUND` — is now recorded as `DELETED (first attempt timed out)` rather than `FAILED`, so the operator is not sent chasing an entry that is already gone. A genuine bad ID, with no retry involved, still reports as a failure. Observed once in a 162-entry run on 2026-09-03.

#### Removed
* `FORCE_DELETE` has been removed from the script, `.env.example` and the README. It passed `force=1` to `baseEntry.delete`, but no delete action in the Kaltura API accepts a `force` parameter — `baseEntry.delete` takes only `entryId`, and the only `force*` parameters in the SDK are `forceProxy` on `serve`/`getUrl`. Kaltura silently discarded the field, so the flag never had any effect, and the documentation claiming it clears `ACTION_BLOCKED` entries was incorrect. Removing it is not a behavior change.

#### Changed
* Entry IDs are now resolved *before* the admin secret prompt, so a missing `ENTRY_IDS` / wrong `ENTRY_ID_COLUMN_HEADER` is caught before the user types a secret. An empty CSV column now names the file and column header instead of reporting "no valid entries to delete".
* Duplicate entry IDs in the input are collapsed (order preserved) and the count reported; previously a repeated ID was processed twice, with the second pass logging a spurious failure.
* Non-2xx HTTP responses (gateway errors, HTML error pages) are now treated as failures. Previously such a response carried no Kaltura error block, so an entry read as found-but-empty and was passed through to deletion.
* Kaltura error codes are now read only from the response's `<error>` block, so a field of the entry itself can never be mistaken for an error code.
* `force=1` is now sent only with `delete`; it is not a parameter of `recycle`.
* The SDK session request now honors `REQUEST_TIMEOUT_SEC` (`config.requestTimeout`), which previously applied only to the raw entry calls.
* `KalturaClientException` is now caught by class rather than by comparing `type(e).__name__` to a string.
* `Ctrl-C` now exits with a clean `[ABORTED] Interrupted by user.` message, and unhandled errors exit non-zero instead of returning success.

### [1.1.1] - 2026-08-20

#### Changed
* Login failures now show a readable message instead of a raw Python traceback: a wrong Partner ID or Admin Secret (`START_SESSION_ERROR`) prints a clear "could not log in — double-check both values, and use the Administrator (not User) secret" message and exits cleanly, and a network error reaching Kaltura prints a separate "could not reach Kaltura" message.

### [1.1.0] - 2026-07-01

#### Changed
* Admin secret is now entered at runtime via a secure `getpass` prompt instead of being stored in `.env`.
* Added empty admin secret guard to exit cleanly rather than producing a cryptic API error.
* Wrapped all main logic in a `main()` function with a top-level `try/except` for clean error output.
* Fixed `load_dotenv()` to use the script's own directory (`Path(__file__).with_name(".env")`) instead of `find_dotenv()`, which could accidentally pick up a `.env` from a parent directory.
* Moved timestamp, output directory, and CSV path setup into `main()` so the clock starts when the script runs rather than when the module loads.
* Replaced bare `exit()` calls with `sys.exit()` for consistency with the helper functions, which already used `sys.exit(2)`.
* Renamed the lookup-phase progress counter from `completed` to `looked_up` to avoid a shadowed variable when the action-phase counter uses the same name.
* Removed `# Path relative to script directory` comment from `load_entry_ids_from_csv()` (no longer needed now that `load_dotenv()` uses the script directory by default).
* Removed `ADMIN_SECRET` from `.env.example`; restructured variables into session variables and script variables sections using standard section header format.
* Added clarifying comment to `MAX_WORKERS` explaining that `10` is reasonable for large batches (code default is `1` when the variable is unset).
* Updated README: removed `ADMIN_SECRET` from configuration table, replaced prose instructions with a structured table, added `FORCE_DELETE`, `MAX_WORKERS`, and timeout variables, updated step 8 to describe the admin secret prompt, moved reports path reference to `output/` subfolder.

### [1.0.0] - 2025-08-28

#### Added
* Initial release of `delete-entries.py`.
* Accepts entry IDs via `ENTRY_IDS` (comma-separated) or a CSV file (`CSV_FILENAME` + `ENTRY_ID_COLUMN_HEADER`).
* Optional lookup phase (`LOOKUP_BEFORE_ACTION`) fetches entry metadata before deletion.
* `DRY_RUN` mode writes a preview CSV without making any destructive API calls.
* Confirmation prompt requires typing `DELETE` or `RECYCLE` before any entries are affected.
* `FORCE_DELETE` flag passes `force=1` to handle entries in error states.
* Concurrent API calls via `ThreadPoolExecutor` with per-thread Kaltura sessions.
* Incremental result CSV writing so partial results are preserved if the run is interrupted.
