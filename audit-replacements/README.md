# Check Kaltura Entry Replacements

This Python script identifies Kaltura media entries that have been replaced by looking for `media::updatecontent` actions in the audit trail. It compares each replacement timestamp against the original entry's creation time and only includes entries with confirmed replacements in the output file.

## Use Case

At UC San Diego, this script is used by Multimedia Services to flag instructional videos that have been replaced. When faculty or instructional designers request updates to previously delivered media, it's important to track that work for billing and scope management. This tool helps identify which videos were updated and when, so the team can follow up appropriately.

## What It Does

- Prompts the user to search by:
  - Tag
  - Category ID
  - Comma-delimited list of Entry IDs
- Retrieves entries based on the selected method.
- Fetches audit trail data for each entry.
- Compares `media::updatecontent` timestamps to each entry's creation date.
- Outputs only entries that have been replaced (i.e., where an update occurred after creation).
- Lists **both creation and replacement events** per entry.
- Formats timestamps in **Pacific Time (PT)**.

Note: Only entries that have been replaced — meaning they have one or more `media::updatecontent` actions recorded after their creation date — will be included in the resulting Excel file. Entries with no replacement history will be skipped entirely.

## Output

An Excel file named like `2025-06-11-1453_ReplacementsAudit.xlsx`, with the following columns:

- `entry_id`
- `title`
- `action` (either `creation` or `replacement`)
- `user_id`
- `timestamp` (Pacific Time, suffixed with "PT")

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
```

## Configuration

At the top of the script, you must provide:

```python
PARTNER_ID = ""        # Your Kaltura partner ID
ADMIN_SECRET = ""      # Your Kaltura admin secret
```

The script creates an admin session using:
```python
privileges="all:*,disableentitlement"
```

## Running the Script

From the command line:
```bash
python3 audit-replacements.py
```

Then follow the prompt to enter your preferred search method.

## Notes

- Some older entries may not have creation events logged, and thus will not appear unless they’ve been replaced since the Audit Trail was enabled.
- Playlists and non-media entries are automatically excluded.

## Important Notes

This script depends on the Kaltura **Audit Trail** module, which must be enabled in your Kaltura environment. If your Kaltura account does not have Audit Trail enabled, calls to `auditTrail.list` may fail, and the script will not return any replacement data. Also note that even once Audit Trail is enabled, entries are only tracked *from that point forward* — actions on entries that occurred before Audit Trail was activated will not appear in the logs.
