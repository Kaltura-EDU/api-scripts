# Description
This script allows you to download all caption assets from one or more Kaltura media entries. You can download captions in their original formats (SRT, VTT, etc.), convert them to plain TXT transcripts, or save both formats at once. You can select the entries using a tag, a category ID, or a comma-delimited list of entry IDs. The script also handles multi-stream entries and can be configured to skip child entries if desired.

Each run writes its files into a timestamped subfolder of `output/` named for the processing date and time (e.g. `output/2026-07-28_142530/`), so batches stay separated and repeated runs don't overwrite each other. Filenames by default include entry ID, entry title, and caption label, but you can optionally shorten them further by excluding the caption label. E.g.
```
output/2026-07-28_142530/1_xuw9zvsc_XSE1_5B_Axial_Compression__English.srt
output/2026-07-28_142530/1_xuw9zvsc_XSE1_5B_Axial_Compression__Spanish.srt
```
The filename still carries enough metadata (entry ID, title, and label) to identify which caption file came from which entry.

The script supports pagination, so all matching entries will be included — not just the first 30. Each caption downloaded will also show a numbered message, helping you track progress when working with large batches. Note that the number of caption files downloaded may exceed the number of entries, since each entry may have multiple caption tracks.

# Configuration (.env)
The script requires a `.env` file to be created in the same folder with the following environment variables:

**Required:**
- `PARTNER_ID` : Your Kaltura partner ID.
- Admin secret: entered at runtime via a secure prompt (not stored in `.env`).

**Optional:**
- `KALTURA_SERVICE_URL` : The Kaltura service URL (default: https://www.kaltura.com).
- `KALTURA_USER` : The Kaltura user ID to act as (default: admin user).
- `OUTPUT_FORMAT` : Controls what gets saved. Options: `srt` (default — keeps original caption file only), `txt` (converts to plain text and deletes original), or `both` (keeps original caption file and saves a TXT transcript). Replaces the legacy `CONVERT_TO_TXT` variable; existing `.env` files using `CONVERT_TO_TXT=true` will continue to work.
- `INCLUDE_CHILD_CATEGORIES` : Set to `true` to include entries from child categories when using category ID (default: false).
- **Filename parts** — the entry ID is *always* included in each filename; the three toggles below add optional components. **At least one must be `true`**, or the script exits with an error (otherwise filenames would be just the bare entry ID):
  - `INCLUDE_CREATION_DATE_IN_FILENAMES` : Set to `true` to prepend the entry's creation date, e.g. `2023-08-01` (default: false). This is the **entry (video) creation date** (`entry.createdAt`), *not* the caption track's own creation date, so it's the same for every caption track on a given entry.
  - `INCLUDE_CAPTION_NAME_IN_FILENAMES` : Set to `false` to exclude the entry (video) title, e.g. `XSE1_5B_Axial_Compression` (default: true).
  - `INCLUDE_CAPTION_LABEL_IN_FILENAMES` : Set to `false` to exclude the caption track label, e.g. `English` (default: true).
- `SKIP_CHILD_ENTRIES` : Set to `true` to skip child entries in multi-stream entries (default: false).
- `SKIP_AUTO_GENERATED` : Set to `true` to skip machine (ASR) caption tracks (default: false).
- `AUTO_GENERATED_LABEL` : The suffix KMC appends to auto-generated caption labels (default: `(auto-generated)`). Configured in KMC under **Settings > Reach > Service Parameters** ("machine captions label suffix"). Enter **only the suffix** (e.g. `(auto-generated)`), *not* a full label like `English (auto-generated)` — it's matched as a case-insensitive substring, so the same value catches auto-generated tracks in every language. Only used when `SKIP_AUTO_GENERATED=true`.
- `DEBUG` : Set to `true` to enable debug output for troubleshooting (default: false).
- `REQUEST_TIMEOUT` : Seconds before an individual API request times out (default: 120).
- `MAX_NETWORK_RETRIES` : How many times to retry a call after a transient network error (default: 5).
- `NETWORK_RETRY_DELAY` : Base seconds between retries; grows linearly as `delay × attempt` (default: 5).

Captions are always written to an `output` folder next to the script (created automatically, and excluded from version control via `.gitignore`).

# How to Run the Script
1. Download **download-captions.py** and **Requirements.txt** to your computer. Ensure they end up in the same folder.
2. Create a `.env` file in the same folder and add your configuration variables as described above.
3. Open a command line interface, such as Terminal on a Mac or Command Prompt in Windows.
4. Navigate to wherever you put your files (e.g. `cd /path/to/project`).
5. Set up a virtual environment if you haven't already: `python3 -m venv venv`
6. Activate your virtual environment (Windows: `venv\\Scripts\\activate` Mac: `source venv/bin/activate`)
7. Install the needed modules: `pip install -r requirements.txt`
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
