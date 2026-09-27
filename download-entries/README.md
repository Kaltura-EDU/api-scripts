# download-entries.py

## Description
This script allows you to download source files from Kaltura media entries based on one of five search criteria:
- Tag(s)
- Category ID(s)
- Category name(s)
- Entry ID(s)
- Owner user ID(s)

All fields accept comma-delimited values, and multiple values are treated as OR. When multiple comma-delimited values are used for tags, category IDs/names, or owner IDs, the script optionally downloads each group's results into a separate subdirectory named after the search term.

The script is configured with a `.env` file in the same folder (see **Configuration** below). By default it downloads into a folder named `output`, created next to the script.

Before downloading, the script estimates the total size of the source files (including any child entries) and prints it on screen — for example, `This download will take up about 12.34 GB, beginning...`. If the estimate would not comfortably fit in the free space on the destination drive, it warns you and asks whether to continue; otherwise it just shows the total and proceeds. Note that on macOS the free-space figure (like Finder's) can include "purgeable" space that isn't actually usable right now, so treat a tight fit with caution.

Downloads are multithreaded (default: 5 concurrent workers) for fast throughput. The number of workers is configurable via `MAX_WORKERS` in `.env`.

## Configuration
Copy `.env.example` to `.env` in the same folder as the script and edit the values. (On macOS, press **Cmd+Shift+.** in Finder to show hidden dotfiles like `.env`.) Every setting is optional and has a sensible default, so a run works even with an empty `.env`.

| Variable | Default | What it does |
|---|---|---|
| `PARTNER_ID` | *(prompted)* | Your numeric Kaltura Partner ID. Not secret, so it can live in `.env`. If blank, you're asked for it each run. |
| `DOWNLOAD_FOLDER` | `output` | Where files are saved. A name relative to the script, or a full path (e.g. an external drive). |
| `MAX_WORKERS` | `5` | Simultaneous downloads. Higher can be faster but may hit rate limits. |
| `RETRY_ATTEMPTS` | `3` | How many times to retry a failing entry before giving up. |
| `REMOVE_SUFFIX` | `true` | Strip Kaltura's `(Source)` suffix and trailing dashes/underscores from filenames. |
| `APPEND_CREATED_DATE` | `false` | Prepend the entry's created date and time (`YYYY-MM-DD-HHMM`, 24-hour) to the front of every filename. |
| `APPEND_ENTRY_ID` | `false` | Prepend the entry ID to every filename (guarantees a unique name per entry). If the created date is also on, the ID comes right after it. |

Your **Admin Secret is never stored in `.env`** — the script always prompts for it at runtime.

The two `APPEND_*` options help when many entries share the same title. Even with both off, identically named entries are never overwritten: the script automatically appends the entry ID whenever names would otherwise collide.

After each batch of downloads completes, the script asks whether you'd like to download more, bringing you back to the search menu without needing to re-enter credentials.

## Features
- **Five search modes**: tag, category ID, category name, entry ID, or owner user ID — all accepting comma-delimited values with OR logic
- **Category name lookup**: searches by exact category name using Kaltura's freeText API (same as KMC search); disambiguates when multiple categories share the same name
- **Subdirectory option**: when multiple search terms are entered for tags, category IDs/names, or owner IDs, optionally downloads each term's results into its own named subfolder with its own CSV report
- **Multithreaded downloads**: uses a configurable thread pool (default: 5 workers) for fast parallel downloads
- **Download-more loop**: after each run completes, offers the option to download another batch without restarting the script or re-entering credentials
- **Sleep prevention scoped to active downloads**: prevents the computer (macOS, Windows, or Linux) from sleeping during a download run; releases automatically between sessions
- **Retry logic**: API calls (entry listing, flavor URL lookups, child entry lookups) and individual entry downloads all retry automatically with exponential backoff on failure
- **Filters out non-media entries** (e.g., playlists) automatically
- **`.env` configuration**: output folder, worker count, retry count, filename cleanup, and filename options are all set in a `.env` file (Partner ID too, optionally); the Admin Secret is always prompted, never stored
- **Optional filename tagging**: prepend the created date-time (`YYYY-MM-DD-HHMM`) and/or entry ID to the front of every filename via `APPEND_CREATED_DATE` / `APPEND_ENTRY_ID`
- **Optionally removes `(Source)` and trailing underscores/dashes** from filenames via `REMOVE_SUFFIX` (default: `true`)
- **Handles duplicate filenames safely**: when multiple entries share the same name — including several downloading at once — the entry ID is appended so every entry is kept as its own file and nothing is silently overwritten
- **Handles child entries** (e.g., clips or derivatives)
- **Supports category hierarchy**: providing a category ID includes entries from all subcategories
- **Skips files that already exist** in the download folder, so interrupted runs can be safely resumed
- **Timestamped CSV report** (`YYYY-MM-DD-HHMM_download_report.csv`) saved after each batch with KMC-style metadata: entry ID, name, description, owner, creator ID, creation date, last updated, duration, media type, tags, categories, download status, and filename written to disk
- **Masked admin secret input** (not displayed when entered at the prompt)
- **Accurate completion reporting**: distinguishes between fully successful runs and runs with failures

## Caveats
- Some users may experience API hanging or slow responses. If that happens, try running the script while connected to your institution's VPN. (In testing, this resolved download hangs.)
- Kaltura's API may return more entries than expected when searching by tag if the tag is broadly applied across your repository.
- The Kaltura Python client is not fully thread-safe. Occasional errors under high concurrency are handled by the per-entry retry wrapper; if you see persistent failures, try lowering `MAX_WORKERS`.

## How to Run the Script
1. Download `download-entries.py`, `requirements.txt`, and `.env.example` into the same folder.
2. Open a terminal or command line window.
3. Navigate to the folder where the script is saved:
   ```
   cd /path/to/your/folder
   ```
4. Set up a virtual environment (optional but recommended):
   ```
   python3 -m venv venv
   ```
5. Activate the virtual environment:
   - On macOS/Linux:
     ```
     source venv/bin/activate
     ```
   - On Windows:
     ```
     venv\Scripts\activate
     ```
6. Install the required Python modules:
   ```
   pip install -r requirements.txt
   ```
7. Set up your configuration:
   ```
   cp .env.example .env
   ```
   Then open `.env` and fill in the values you want (all are optional — see **Configuration** above).
8. Run the script:
    ```
    python3 download-entries.py
    ```

---

Galen Davis  
Senior Education Technology Specialist  
UC San Diego  

*and* 

Andy Clark  
Systems Administrator, Learning Systems  
Baylor University  

*Last updated 2026-09-16*
