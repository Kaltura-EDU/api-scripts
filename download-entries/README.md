# download-entries.py

## Description
This script allows you to download source files from Kaltura media entries based on one of five search criteria:
- Tag(s)
- Category ID(s)
- Category name(s)
- Entry ID(s)
- Owner user ID(s)

All fields accept comma-delimited values, and multiple values are treated as OR. When multiple comma-delimited values are used for tags, category IDs/names, or owner IDs, the script optionally downloads each group's results into a separate subdirectory named after the search term.

The default download folder is `kaltura_downloads`, created in the same directory as the script. You can change this using global variables at the top of the script.

Downloads are multithreaded (default: 5 concurrent workers) for fast throughput. The number of workers is configurable via the `MAX_WORKERS` global variable.

After each batch of downloads completes, the script asks whether you'd like to download more, bringing you back to the search menu without needing to re-enter credentials.

## Features
- **Five search modes**: tag, category ID, category name, entry ID, or owner user ID — all accepting comma-delimited values with OR logic
- **Category name lookup**: searches by exact category name using Kaltura's freeText API (same as KMC search); disambiguates when multiple categories share the same name
- **Subdirectory option**: when multiple search terms are entered for tags, category IDs/names, or owner IDs, optionally downloads each term's results into its own named subfolder with its own CSV report
- **Multithreaded downloads**: uses a configurable thread pool (default: 5 workers) for fast parallel downloads
- **Download-more loop**: after each run completes, offers the option to download another batch without restarting the script or re-entering credentials
- **Caffeinate scoped to active downloads**: prevents macOS from sleeping during a download run; terminates between sessions
- **Retry logic**: API calls (entry listing, flavor URL lookups, child entry lookups) and individual entry downloads all retry automatically with exponential backoff on failure
- **Filters out non-media entries** (e.g., playlists) automatically
- **Optionally removes `(Source)` and trailing underscores/dashes** from filenames via a `REMOVE_SUFFIX` global variable (default: `True`)
- **Handles duplicate filenames**: if multiple entries share the same name, the entry ID is appended to keep filenames unique and prevent silent overwrites
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
1. Download `download-entries.py` and `requirements.txt` into the same folder.
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
7. Run the script:
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

*Last updated 2026-07-06*
