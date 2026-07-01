# Changelog for delete-entries.py

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
