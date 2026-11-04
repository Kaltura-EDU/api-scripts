# Delete Non-Source Flavors

Frees up Kaltura storage by deleting the extra copies ("flavors") that Kaltura makes of each video at different sizes and qualities. It keeps each entry's **source** flavor (the original upload), plus any flavors you choose to keep. It's built for bulk cleanups, which aren't practical one entry at a time in the KMC.

Before deleting anything, the script writes a **preview** spreadsheet (CSV) of exactly what it plans to delete and asks you to type `DELETE` to go ahead. Afterward it writes a **results** CSV showing what was deleted and how much space was saved.

## ⚠️ Warning

This script **permanently deletes flavors**, and it can't be undone. Deleted flavors are **not** recycled. Always review the preview CSV before typing `DELETE`. If you ever need flavors back, Kaltura can regenerate them from the source (for example with `media.convert` or `flavorAsset.add`).

## How it decides what to keep

For each entry, and for each stream of a multi-stream entry:

1. The flavor marked as the original (`isOriginal`) is the source.
2. If none is marked, a flavor tagged `source` is the source.
3. If neither exists, the **largest** flavor is treated as the source, and the script prints a warning. Check those entries in the preview.

The source, and any flavor whose flavor params ID is in `ADDITIONAL_FLAVORS_TO_KEEP`, is kept. Everything else is deleted. Entries with only one flavor are skipped.

## How to run this script

1. Download this folder to your computer, somewhere that's easy to find.
2. Set up your `.env` file (see the next section).
3. Create and/or activate your virtual environment. You can find [instructions on this](https://github.com/Kaltura-EDU/api-scripts) in the main README for this GitHub repository.
4. Open a command-line application (e.g. Terminal on a Mac or Command Prompt on Windows) and navigate to this folder.
5. Type `pip install -r requirements.txt` to install what the script needs. You only need to do this once.
6. Type `python3 delete-nonsource-flavors.py` to run the script.
7. Enter your **Administrator secret** when asked (what you type stays hidden). If you didn't choose entries in `.env`, the script asks which entries to process.
8. Open the preview CSV in the `output` folder and check it. Then type `DELETE` to go ahead, or anything else to cancel.

## Setting up your `.env`

The `.env` file holds your settings, so you never have to edit the Python file.

1. Make a copy of `.env.example` and name it `.env`. In a terminal you can type `cp .env.example .env`, or duplicate and rename the file in Finder or File Explorer.
2. Open `.env` in any text editor and fill in the values. Each setting has a comment explaining it.

**Can't see the file?** Files whose names start with a dot are hidden by default. On a Mac, press **⌘ + Shift + .** in Finder to show them. On Windows, open File Explorer and choose **View → Show → Hidden items**.

**Your admin secret never goes in `.env`.** The script asks for it each time it runs, so it's never saved on your computer.

### Choosing entries

Leave these blank and the script asks you. To skip the question, set **one** of them:

- `INPUT_FILENAME`: a CSV of entry IDs saved in the `input` folder (best for long lists). The entry IDs go in a column headed **Entry ID**, or set `COLUMN_HEADER_ENTRY_ID` to your column's header. A CSV with just one column works whatever its header says. Other columns in your CSV are copied into the reports.
- `ENTRY_IDS`: entry IDs, comma-separated.
- `TAGS` and/or `CATEGORY_IDS`: comma-separated. If you set both, an entry must match both.

### Other settings

| Setting | Default | What it does |
|---|---|---|
| `KALTURA_PARTNER_ID` | *(asks you)* | Your partner ID. |
| `KALTURA_USER` | `admin` | The user ID recorded in Kaltura's logs. |
| `ADDITIONAL_FLAVORS_TO_KEEP` | *(blank)* | Flavor params IDs to keep in addition to the source, comma-separated. Find them in KMC > Settings > Transcoding > (profile) > Flavors. |
| `MAX_WORKERS` | `10` | How many entries to work on at once. Lower it if you see errors from Kaltura. |
| `PROGRESS_EVERY` | `100` | How often to print a progress line. |
| `SKIP_ENTRY_METADATA` | `true` | Skips looking up each entry's name and owner, which is faster. Set to `false` to include them in the reports. |
| `SKIP_CONFIRMATION` | `false` | Set to `true` to delete without typing `DELETE`. Only use this once you trust the preview. |
| `REQUEST_TIMEOUT`, `MAX_NETWORK_RETRIES`, `NETWORK_RETRY_DELAY` | `120`, `5`, `5` | How patiently the script handles a slow or dropped connection. The defaults are fine for most people. |

Settings from older versions (`PARTNER_ID`, `USER_ID`, `SERVICE_URL`, `CSV_FILENAME`, `ENTRY_ID_COLUMN_HEADER`, `REQUEST_TIMEOUT_SEC`) still work. Delete any `ADMIN_SECRET` line from an older `.env`.

## The reports

Each run writes two CSVs to the `output` folder, named with the date and time:

- `…_deleted_flavors_PREVIEW.csv`: the plan, written before anything is deleted.
- `…_deleted_flavors_RESULT.csv`: what actually happened.

Each row is one entry (`PARENT`) or one stream of a multi-stream entry (`CHILD`). Columns include the source flavor kept and why it was chosen, the flavors to delete, the kilobytes saved, and a `status`:

- `READY` (preview only): flavors will be deleted.
- `DELETED`, `PARTIAL`, or `FAILED` (results only): what happened. See the `error` column for details.
- `SKIPPED_SINGLE_FLAVOR` or `SKIPPED_NO_SOURCE_DETECTED`: nothing was deleted.
- `ERROR`: the entry couldn't be checked.

---

*Last updated 2026-09-24*
