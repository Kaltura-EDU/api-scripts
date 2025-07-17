# Check Kaltura Entry Replacements

This Python script identifies Kaltura media entries that have been replaced by looking for `media::updatecontent` actions in the audit trail. It compares each replacement timestamp against the original entry's creation time and only includes entries with confirmed replacements in the output file.

## Use Case

At UC San Diego, this script is used by Multimedia Services to flag instructional videos that have been replaced. When faculty or instructional designers request updates to previously delivered media, it's important to track that work for billing and scope management. This tool helps identify which videos were updated and when, so the team can follow up appropriately.

## What It Does

- Uses environment variables from a `.env` file to configure the search.
- Retrieves entries based on combinations of:
  - `OWNER_ID`
  - `CREATOR_ID`
  - `TAGS` (comma-delimited OR logic)
  - `CATEGORY_IDS` (comma-delimited OR logic)
  - `DATE_START` / `DATE_END` (ISO format, YYYY-MM-DD)
- Filters audit logs to include only `media::updatecontent` events that occur at least `MIN_DELAY_MINUTES` after entry creation.
- Limits the number of replacements per entry to `MAX_REPLACEMENTS`.
- Outputs one row per entry, with replacement timestamps and users shown in separate columns.
- Timestamps are displayed in your chosen `TIMEZONE`.
- Adds a second sheet (`Search_Terms`) identifying the search parameters used to generate the results.

## Output

An Excel file named like 2025-07-16-1012_ReplacementsAudit.xlsx, with:

- A `Results` sheet showing one row per entry, including replacement timestamps and user IDs.
- A `Search_Terms` sheet summarizing the search terms used during the query (excluding sensitive values like `PARTNER_ID` and `ADMIN_SECRET`).

Columns in the Results sheet:

- `entry_id`
- `title`
- `creator_id`
- `owner_id`
- `creation_time`
- `replacement01`, `replacement01_user`, ... Up to the number of replacements defined by `MAX_REPLACEMENTS`

## Requirements

Install dependencies with:

```bash
pip install -r requirements.txt
```

Contents of `requirements.txt`:

```
pytz
pandas
KalturaApiClient
lxml
openpyxl
python-dotenv
```

## Configuration

This script requires a `.env` file in the same directory. Use `.env.example` as a template (copy and rename it to `.env` before running the script). Key variables include:

```env
PARTNER_ID
ADMIN_SECRET
USER_ID
SERVICE_URL
PRIVILEGES

OWNER_ID
CREATOR_ID
TAGS
CATEGORY_IDS
DATE_START
DATE_END
TIMEZONE
MIN_DELAY_MINUTES
MAX_REPLACEMENTS
```

Remember that in the .env file you should not use quotation marks when assigning values, e.g.
```
PARTNER_ID=123456
```
Default values for some of the variables are set in the Python script. For example, if you don't set a value for `MAX_REPLACEMENTS` it will default to 3. These defaults are identified in `.env.example`. 

### Notes on logic:
- If multiple filters are provided, they are combined with **AND** logic. For example, searching for something by `OWNER_ID` and `TAGS` will only return entries that are owned by the user AND have the tag.
- WITHIN `TAGS` and `CATEGORY_IDS`, however, comma-separated values are treated with **OR** logic. So you can search for entries that have EITHER this tag or that tag, or are in this category or that category.

### Notes on `TIMEZONE`:
Answers the question *"What timezone are you in?"*

We've provided possible values for all US timezones in `.env.example`, i.e.
- `America/New_York`
- `America/Chicago`
- `America/Denver`
- `America/Los_Angeles`
- `America/Anchorage`
- `Pacific/Honolulu`  

These are used for the timestamps in the .xlsx output to ensure that they match up with your institution's timezone. 

You can find a full list of possible values at https://en.wikipedia.org/wiki/List_of_tz_database_time_zones.


### Notes on `MIN_REPLACEMENT_DELAY_MINUTES`:
Answers the question *"How many minutes after the entry's creation should we consider a  replacement event as official?"*

This variable is necessary because of the different ways that Kaltura entries can be created. For example, an entry created via API might first create the entry "shell" (baseEntry.create), and then run a separate API action to add the media content to the entry (media.update). But this "update event" occurs at the same time (or seconds after) the entry is created and should be ignored for the purposes of this script. 

Accordingly, use this variable to identify how many minutes after the entry's creation an event can be considered a legitimate "replacement." 


### Notes on `MAX_REPLACEMENTS`:
Answers the question *"How many replacement events do you want reported in the spreadsheet?"*

For legibility, it seems to make the most sense to have one row per entry so it's easy to determine how many entries showed up in your results. Accordingly, the number of columns will depend on the number of replacement events. This variable allows you to set the number of replacement events to consider. 


## Running the Script

From the command line:
```bash
python3 audit-replacements.py
```

## Important Notes

- Only entries that have one or more replacement events (defined as `media::updatecontent`) **after** creation and beyond the configured delay will appear in the spreadsheet.
- This script depends on the Kaltura **Audit Trail** module, which must be enabled in your environment.
- The Audit Trail only tracks actions from the time it was activated onward; historical data is not backfilled.
