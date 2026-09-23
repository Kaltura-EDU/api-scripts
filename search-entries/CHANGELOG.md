# Changelog – search-entries.py

This changelog was started on 2026-08-20; earlier versions of the script predate it and are not documented here.

## [1.3.0] - 2026-09-23
### Added
- Child entries are now included. Kaltura hides child entries — e.g. the extra layouts of a Zoom meeting recorded in more than one layout, or the second stream of a dual-screen recording — from normal searches, so previously they never appeared in results. The script now looks up the children of every matched entry (in parallel across `MAX_WORKERS`) and lists them in the CSV directly under their parent. Children don't need to match your filters themselves. Set `INCLUDE_CHILDREN=False` to skip the lookup, which costs one extra API request per matched entry.
- New CSV columns right after `entry_id`: `relationship` (`parent` / `child` / `standalone`), `parent_entry_id` (moved here from near the end), `child_count`, and `child_entry_ids`.
- The console summary reports entries with children, child entries, and child duration separately from the matched entries, so a recording's extra layouts don't inflate the matched totals.
- The run now prints every filter that's set in `.env` — plus the date range and defaults that affect results, such as `STATUS` being READY-only when blank — before searching and again with the totals, so a value left over from an earlier search is easy to spot.
- Non-video children (e.g. documents) show their entry type in `media_type` instead of `None`.

## [1.2.0] - 2026-09-10
### Added
- Parallel fetching: result pages are now downloaded several at a time across `MAX_WORKERS` threads (default 10), each with its own Kaltura session. Large scans — especially `OWNER_STARTS_WITH` / `OWNER_ENDS_WITH`, which have to check every entry in the date range — can finish much faster. Set `MAX_WORKERS=1` to fetch one page at a time; lower it if Kaltura starts timing out under load.

### Changed
- Auto-chunking around the 10,000-entry cap now counts each date range up front (one tiny request per range) and then fetches, instead of discovering an oversized range partway through. Ranges are split at 9,000 entries to leave headroom.
- The CSV is now always sorted by created date, newest first. Previously, auto-chunked runs listed each chunk oldest-month-first.

## [1.1.0] - 2026-09-10
### Added
- `OWNER_STARTS_WITH` and `OWNER_ENDS_WITH` filters: match entries whose owner's user ID starts or ends with any of the given terms (comma = OR, not case-sensitive) — e.g. `OWNER_ENDS_WITH=@ucsd.edu`. Kaltura's API only supports exact owner matches, so these are applied client-side after the query; used on their own they scan every entry in the date range, so pair them with a `CREATED_AFTER`/`CREATED_BEFORE` window when possible.

## [1.0.1] - 2026-08-20
### Changed
- Login failures now show a readable message instead of a raw Python traceback: a wrong Partner ID or Admin Secret (`START_SESSION_ERROR`) prints a clear "could not log in — double-check both values, and use the Administrator (not User) secret" message and exits cleanly, and a network error reaching Kaltura prints a separate "could not reach Kaltura" message.
