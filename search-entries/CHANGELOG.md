# Changelog – search-entries.py

This changelog was started on 2026-08-20; earlier versions of the script predate it and are not documented here.

## [1.6.0] - 2026-10-07
### Changed
- A blank `STATUS` now returns entries of any status (including `DELETED`, `ERROR_*`, `PENDING`, etc.) instead of only `READY`. Set `STATUS` to narrow it. If you relied on the old behavior, set `STATUS=READY`. Child entries use the same setting. The CSV already included a `status` column, so each row shows its status.

## [1.5.0] - 2026-10-05
### Changed
- CSV columns reordered so the most-used come first: `created_at`, `updated_at`, `entry_id`, `owner_id`, `creator_id`, `name`, `duration_sec`, then the parent-child columns (`relationship`, `parent_entry_id`, `child_count`, `child_entry_ids`) and the rest in their previous order.

### Fixed
- `display_in_search` showed a Python object description (e.g. `<KalturaClient.Plugins.Core.KalturaEntryDisplayInSearchType object at 0x…>`) instead of a value. It now shows the setting's name: `PARTNER_ONLY`, `KALTURA_NETWORK`, `NONE`, `SYSTEM`, or `RECYCLED`. As a safety net, any other SDK enum value that reaches the CSV is written as its value, never as an object description.

## [1.4.0] - 2026-10-05
### Added
- `PLAYLIST_ID` filter: search only the entries in one or more playlists (comma = any of them). Manual playlists use the entries that were added to them; rule-based playlists are run at search time. External and interactive-path playlists, and playlist IDs that aren't found, are skipped with a message, and if no entries are left the script stops without writing a CSV. It combines with the other filters like any filter; with `ENTRY_ID`, only entries in both are searched. Results aren't in playlist order, and the CSV has no playlist columns.

### Changed
- Searches limited to specific entries (`ENTRY_ID` or `PLAYLIST_ID`) now look the IDs up directly, 100 per request and in parallel, across the whole date range, instead of going through date-range splitting.

## [1.3.1] - 2026-10-05
### Fixed
- Invisible characters (e.g. zero-width spaces, easily picked up when copy-pasting IDs) are now removed from `.env` values. Previously a line that looked blank, like `ENTRY_ID=`, could still hold them and silently become a filter that matched nothing — e.g. a search for `OWNER_ID=e2heinzm` returned 0 entries while the KMC showed 21. The script now prints a warning naming any variables it cleaned.

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
