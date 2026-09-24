# Changelog

## [v2.0.0] - 2026-09-24
### Changed
- **Settings now live in a `.env` file** instead of the Python file. Copy `.env.example` to `.env` to get started (see README). The source and destination partner IDs can be set there as `KALTURA_SOURCE_PARTNER_ID` and `KALTURA_DEST_PARTNER_ID`; if left blank, the script asks for them.
- Admin secrets are typed in hidden (`getpass`) and are never read from `.env`.
- All API calls retry automatically on network timeouts and dropped connections (`REQUEST_TIMEOUT`, `MAX_NETWORK_RETRIES`, `NETWORK_RETRY_DELAY`).
- ASR captions are now recognized by a label suffix (`AUTO_GENERATED_LABEL`, default `(auto-generated)`) matched in any language, instead of one exact label. The old `CAPTION_LABEL` setting is still honored.
- The script asks for confirmation before copying anything.
- The CSV report is saved in an `output` folder and now includes child entries, the destination parent ID, per-entry counts of thumbnails, captions, attachments, and cue points copied, and a status column.
- `requirements.txt` now requires `KalturaApiClient>=22.0.0`, `python-dotenv`, and `requests`.

### Removed
- `COPY_QUIZ_ANSWERS`. It never had any effect: quiz answers were always skipped. Quiz answers (learner submissions) are not copied.

### Fixed
- Audio descriptions are now copied as audio descriptions. Previously they arrived at the destination as ordinary caption tracks.
- The source's default thumbnail stays the default. Previously the last thumbnail copied became the default.
- One failed entry no longer stops the whole run; it is logged in the CSV and the script moves on.
- Leaving `DESTINATION_OWNER` blank no longer sends an empty owner to Kaltura.
- Cue point counts were doubled.
- A child entry listed alongside its parent (e.g. by entry ID) was copied twice.
- Entries with no tags, or quiz questions with no answer options, could crash the copy.
- Unknown cue point types are skipped with a warning instead of failing the entry.
- Cue points, captions, thumbnails, attachments, flavors, and child entries are now paginated past 500.
- Captions and attachments were each fetched twice per entry.

## [v1.3.2] - 2026-09-24
### Changed
- Login failures now show a readable message instead of a raw Python traceback: a wrong Partner ID or Admin Secret (`START_SESSION_ERROR`) prints a clear "could not log in — double-check both values, and use the Administrator (not User) secret" message and exits cleanly, and a network error reaching Kaltura prints a separate "could not reach Kaltura" message. Applies to both the source and destination logins.

## [v1.3.1] - 2026-09-24
### Changed
- Updated `DEST_PID` to be `dest_pid` since the all-caps version is commented out in the main branch.

## [v1.3.0] - 2026-04-30
### Changed
- Output filename format standardized: timestamp moved to the beginning of the filename and format updated to `YYYY-MM-DD-HHMM`. New format: `YYYY-MM-DD-HHMM_CrossInstanceDuplication.csv`.

## [v1.2.0] - 2025-05-05
### Changed
- Main function now prompts user for Partner ID and Admin Secret for both source and destination.

## [v1.1.0] - 2025-04-26
### Added
- Added detailed docstring to the script.
- Improved command-line user prompts and feedback for a cleaner user experience.
- Added clearer debug and completion messages.

### Changed
- Improved CSV writing structure for Flake8 compatibility.
- Adjusted status printing to avoid redundant entry counts.

### Fixed
- Fixed duplicate "entries found" message.
- Fixed small formatting issues for Flake8 compliance.
- Implemented pagination when retrieving entries to handle large result sets. (Otherwise only 30 entries would transfer.)
- Updated search behavior for categories to use `categoryAncestorIdIn` instead of `categoriesIdsMatchOr`. (Otherwise entries that were in subcategories of the category entered wouldn't be duplicated.)

---

Galen Davis  
Senior Education Technology Specialist  
UC San Diego  
  
*and*  
  
Andy Clark  
Systems Administrator, Learning Systems  
Baylor University  
  
*Last updated 2026-09-24*
