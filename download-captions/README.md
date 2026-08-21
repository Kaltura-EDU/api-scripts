# Description
This script downloads **captions and audio descriptions** from one or more Kaltura media entries. (In Kaltura, both are stored as "caption assets.") You can download them in their original formats (SRT, VTT, etc.), convert them to plain TXT transcripts, or save both formats at once, and you can choose which asset types to pull: machine (ASR) captions, other captions, and/or audio descriptions. You can select the entries using a tag, a category ID, a specific owner, or a comma-delimited list of entry IDs. The script also handles multi-stream entries and can be configured to skip child entries if desired.

Each run writes its files into a timestamped subfolder of `output/` named for the processing date and time (e.g. `output/2026-07-28_142530/`), so batches stay separated and repeated runs don't overwrite each other. Filenames by default include entry ID, entry title, and caption label, but you can optionally shorten them further by excluding the caption label. E.g.
```
output/2026-07-28_142530/1_xuw9zvsc_XSE1_5B_Axial_Compression__English.srt
output/2026-07-28_142530/1_xuw9zvsc_XSE1_5B_Axial_Compression__Spanish.srt
```
The filename still carries enough metadata (entry ID, title, and label) to identify which caption file came from which entry.

The script supports pagination, so all matching entries will be included — not just the first 30. Each caption downloaded will also show a numbered message, helping you track progress when working with large batches. Note that the number of caption files downloaded may exceed the number of entries, since each entry may have multiple caption tracks.

# Prerequisites
- **Python 3** (3.8 or newer recommended).
- The Python packages in `requirements.txt` (installed in step 7 below).
- **KalturaApiClient 22.0.0 or newer** — required only if you want to download **audio descriptions**, which are identified by a caption-asset field (`usage`) that older SDKs don't expose. `requirements.txt` pins this automatically; if you have an older environment, upgrade with `pip install -U KalturaApiClient`. (Captions-only use works on older SDKs, but newer is recommended.)
- A Kaltura **partner ID** and **admin secret** (the admin secret is entered at runtime, never stored).

# Configuration (.env)
The script reads its settings from a `.env` file in the same folder.

## Setting up your `.env`
The repo includes a ready-made **`.env.example`** with every variable and an explanatory comment. The easiest way to start:

1. **Make a copy of `.env.example` and name the copy `.env`.** You can duplicate it in your file manager and rename, or from a terminal run:
   ```bash
   cp .env.example .env
   ```
2. Open `.env` in any text editor and fill in your values (at minimum, `PARTNER_ID`).

> **Can't see the file?** Files that begin with a dot (like `.env` and `.env.example`) are hidden by default. In macOS **Finder**, press **⌘ + Shift + .** (Command-Shift-Period) to toggle hidden files on and off. In Windows **File Explorer**, enable **View → Show → Hidden items**.

Your `.env` holds your own settings and is git-ignored, so it's never uploaded — only `.env.example` is tracked.

## Variables

**Required — the script won't run without these:**
- `PARTNER_ID` : Your Kaltura partner ID (in KMC under **Settings > Integration Settings**).
- **Admin secret** : *Not* stored in `.env` — the script prompts you for it at runtime (hidden input) so it's never written to disk.
- **At least one query filter** — one of `ENTRY_IDS`, `CATEGORY_IDS`, `CATEGORY_NAMES`, `TAGS`, or `OWNER` (see [Query filters](#query-filters) below) so the script knows which entries to process.
- **At least one caption-asset type** — one of `INCLUDE_ASR_CAPTIONS`, `INCLUDE_NON_ASR_CAPTIONS`, or `INCLUDE_AUDIO_DESCRIPTIONS` set to `true` (all default appropriately, so you only need to change these to *narrow* what you download).

**Optional:**
- `KALTURA_SERVICE_URL` : The Kaltura service URL (default: https://www.kaltura.com).
- `KALTURA_USER` : The Kaltura user ID to act as (default: admin user).
- `OUTPUT_FORMAT` : Controls what gets saved **for captions**. Options: `srt` (default — keeps original caption file only), `txt` (converts to plain text and deletes original), or `both` (keeps original caption file and saves a TXT transcript). **Audio descriptions are not affected** — they are always kept in their original format and never converted to TXT, since stripping their timecodes would make the text meaningless. Replaces the legacy `CONVERT_TO_TXT` variable; existing `.env` files using `CONVERT_TO_TXT=true` will continue to work.
- **Caption-asset types** — choose which kinds of caption asset to download. **At least one must be `true`** (see Required, above). In Kaltura, captions and audio descriptions are all "caption assets"; the script sorts each one into exactly one bucket:
  - `INCLUDE_ASR_CAPTIONS` : Machine/auto-generated captions — a caption whose label contains `AUTO_GENERATED_LABEL` (default: true).
  - `INCLUDE_NON_ASR_CAPTIONS` : Every other caption (default: true). "Non-ASR" only means the label isn't marked machine-generated; it does **not** guarantee the captions are accurate or human-made.
  - `INCLUDE_AUDIO_DESCRIPTIONS` : Audio descriptions (default: false), identified by the asset's `usage` field (`usage=1`), which is more reliable than the label. **Requires KalturaApiClient 22.0.0+** — the `usage` field doesn't exist in older SDKs (see the install step below).
  - `AUTO_GENERATED_LABEL` : The suffix KMC appends to machine-caption labels (default: `(auto-generated)`). Configured in KMC under **Settings > Reach > Service Parameters** ("machine captions label suffix"). Enter **only the suffix** (e.g. `(auto-generated)`), *not* a full label like `English (auto-generated)` — it's matched as a case-insensitive substring, so the same value catches machine tracks in every language. Only needed when `INCLUDE_ASR_CAPTIONS` and `INCLUDE_NON_ASR_CAPTIONS` differ (so the script must tell them apart).
- `INCLUDE_CHILD_CATEGORIES` : Set to `true` to include entries from child categories when using category ID (default: false).
- **Filename parts** — the entry ID is *always* included in each filename; the three toggles below add optional components. **At least one must be `true`**, or the script exits with an error (otherwise filenames would be just the bare entry ID):
  - `INCLUDE_CREATION_DATE_IN_FILENAMES` : Set to `true` to prepend the entry's creation date, e.g. `2023-08-01` (default: false). This is the **entry (video) creation date** (`entry.createdAt`), *not* the caption track's own creation date, so it's the same for every caption track on a given entry.
  - `INCLUDE_CAPTION_NAME_IN_FILENAMES` : Set to `false` to exclude the entry (video) title, e.g. `XSE1_5B_Axial_Compression` (default: true).
  - `INCLUDE_CAPTION_LABEL_IN_FILENAMES` : Set to `false` to exclude the caption track label, e.g. `English` (default: true).
- `SKIP_CHILD_ENTRIES` : Set to `true` to skip child entries in multi-stream entries (default: false).
- `SUBFOLDER_PER_SEARCH_TERM` : Set to `true` to give each search term its own subfolder inside the run folder, named after the term as entered — e.g. `output/<timestamp>/19452/…` (default: false, everything flat). Applies to `CATEGORY_IDS`, `CATEGORY_NAMES`, `TAGS`, and `OWNER`; `ENTRY_IDS` are always kept flat. For `CATEGORY_NAMES`, the subfolder uses the name you typed, not the internal Kaltura category ID it resolves to.
- `DEBUG` : Set to `true` to enable debug output for troubleshooting (default: false).
- `REQUEST_TIMEOUT` : Seconds before an individual API request times out (default: 120).
- `MAX_NETWORK_RETRIES` : How many times to retry a call after a transient network error (default: 5).
- `NETWORK_RETRY_DELAY` : Base seconds between retries; grows linearly as `delay × attempt` (default: 5).

### Query filters
Set **at least one** of these to tell the script which entries to process. Each accepts comma-delimited values. Values within a single variable are OR'd (e.g. `TAGS=math,biology` matches either tag); if you set more than one variable, they're prioritized in this order and only the first is used: `ENTRY_IDS` > `CATEGORY_IDS`/`CATEGORY_NAMES` > `TAGS` > `OWNER`.
- `ENTRY_IDS` : Specific entry IDs, e.g. `1_ab2cd3ef,1_gh4ij5kl`.
- `CATEGORY_IDS` : Category IDs (combine with `INCLUDE_CHILD_CATEGORIES` to include subcategories).
- `CATEGORY_NAMES` : A convenience alternative to `CATEGORY_IDS` — enter category **names** and the script resolves each to its ID. **Category names aren't guaranteed unique in Kaltura**: if a name matches more than one category, the script stops and lists the matching IDs and full paths so you can put the specific ID in `CATEGORY_IDS` instead. Matching is exact and case-sensitive. Any resolved IDs are combined with `CATEGORY_IDS` if you set both.
- `TAGS` : Entry tags.
- `OWNER` : The user ID that owns the entries.

Downloaded files are written to a timestamped subfolder of `output/` next to the script (created automatically, and git-ignored so they're never uploaded). By default everything from a run lands flat in that one folder; set `SUBFOLDER_PER_SEARCH_TERM=true` to split results into one subfolder per search term.

# How to Run the Script
1. Download the files in this folder (at minimum **download-captions.py**, **requirements.txt**, and **.env.example**) to your computer. Ensure they end up in the same folder.
2. Create your `.env` file by copying `.env.example` (see [Setting up your `.env`](#setting-up-your-env) above), then fill in your values.
3. Open a command line interface, such as Terminal on a Mac or Command Prompt in Windows.
4. Navigate to wherever you put your files (e.g. `cd /path/to/project`).
5. Set up a virtual environment if you haven't already: `python3 -m venv venv`
6. Activate your virtual environment (Windows: `venv\\Scripts\\activate` Mac: `source venv/bin/activate`)
7. Install the needed modules: `pip install -r requirements.txt`
   - **Downloading audio descriptions requires KalturaApiClient 22.0.0 or newer** (older versions can't distinguish audio descriptions from captions). If you already have an older environment, upgrade with `pip install -U KalturaApiClient`. The script will stop with a clear message if your SDK is too old and your settings need that distinction.
8. Run the script: `python3 download-captions.py`
9. When prompted, enter your Kaltura admin secret. The input will not be visible as you type.

# Output
As the script runs, it will display numbered messages for each caption file downloaded, converted, or deleted, helping you track progress when working with large batches. Messages will indicate the original format of captions downloaded, whether conversion to TXT was performed, and if original files were deleted. Progress numbering allows easy identification of captions processed.

## Troubleshooting SSL Errors
If you receive an SSL certificate error when downloading captions, your system may be missing the trusted certificate store.
- macOS users: Run /Applications/Python\ 3.x/Install\ Certificates.command in Terminal (replace 3.x with your Python version).
- Alternatively, the script now includes a friendly message when this issue occurs.

---

Galen Davis  
Senior Education Technology Specialist  
UC San Diego  

*and* 

Andy Clark  
Systems Administrator, Learning Systems  
Baylor University  

*Last updated 2026-07-28*
