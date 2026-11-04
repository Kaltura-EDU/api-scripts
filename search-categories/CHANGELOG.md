# Changelog – search-categories.py

## [1.1.2] - 2026-11-04
### Changed
- The Kaltura session is now closed (`session.end`) when the script exits, whether it finishes, hits an error, or you stop it with Ctrl+C. Before, the session stayed valid until it expired on its own, so a copied or leaked session key kept working. You'll see "Kaltura session closed." at the end.

## [1.1.1] - 2026-10-29
### Fixed
- The `privacy_context` CSV column was blank for MediaSpace channels, because Kaltura stores their context in a different field. It now shows `MediaSpace`.

## [1.1.0] - 2026-10-29
### Changed
- Searching by `OWNER_ID` is now fast. Kaltura cannot filter categories by owner, so the script used to download every category and check the owner afterward. It now first asks Kaltura for the categories that user manages (an owner is normally a manager too), then confirms the owner. Set `OWNER_PREFILTER=False` to go back to scanning everything, which also finds owners who are not managers. If `MANAGER_ID` is filled in, it is used instead.

## [1.0.0] - 2026-10-29
### Added
- First version. Searches Kaltura categories (including MediaSpace channels and galleries) using filters set in `.env`, lists them on screen, and saves every match to a CSV in the `output/` folder.
- Filters Kaltura handles itself: category ID, parent, everything underneath a category, tree level, full path (equals or begins with), word search, reference ID, tags (any or all), managers, members, privacy, who sees it in lists, inheritance, contribution policy, privacy context, status, member and pending-member counts, and created/updated dates. A comma-separated list means "any of these".
- Filters applied after Kaltura answers, because Kaltura cannot do them: category name (equals, begins with, contains, ends with, does not contain), path contains / does not contain, owner, admin tags, moderated or not, and entry, direct-entry and subcategory counts (for example `ENTRIES_MAX=0` finds empty categories).
- Searches run with the `all:*,disableentitlement` privileges so MediaSpace categories appear, and a search larger than Kaltura's 10,000-result cap is split by creation date automatically.
- The Admin Secret is always asked for when you run the script.
