# download-entries.py

## Description
This script allows you to download source files from Kaltura media entries based on one of four search criteria:
- A tag
- A category ID
- A comma-delimited list of entry IDs
- An owner's user ID

The default download folder is `kaltura_downloads`, created in the same directory as the script. You can change this using a global variable at the top of the script.

The script downloads entries sequentially (not threaded) for simplicity and reliability.

## Features
- Filters out non-media entries (e.g., playlists) automatically
- Optionally removes `(Source)` and trailing underscores/dashes from filenames via a `REMOVE_SUFFIX` global variable (default: `True`)
- Handles child entries (e.g., clips or derivatives)
- Supports category hierarchy — if you provide a category ID, the script will also include entries from any subcategories

## Caveats
- Some users may experience API hanging or slow responses. If that happens, try running the script while connected to your institution's VPN. (In testing, this resolved download hangs.)
- Kaltura's API may return more entries than expected when searching by tag if the tag is broadly applied across your repository.

## How to Run the Script
1. Download `download-entries.py` and `requirements.txt` into the same folder.
2. Open `download-entries.py` in a text editor.
3. Add values for `PARTNER_ID` and `ADMIN_SECRET` based on your own Kaltura instance.
4. Save your changes.
5. Open a terminal or command line window.
6. Navigate to the folder where the script is saved:
   ```
   cd /path/to/your/folder
   ```
7. Set up a virtual environment (optional but recommended):
   ```
   python3 -m venv venv
   ```
8. Activate the virtual environment:
   - On macOS/Linux:
     ```
     source venv/bin/activate
     ```
   - On Windows:
     ```
     venv\Scripts\activate
     ```
9. Install the required Python modules:
   ```
   pip install -r requirements.txt
   ```
10. Run the script:
    ```
    python3 download-entries.py
    ```

---

Galen Davis  
Senior Education Technology Specialist  
UC San Diego  
Last updated 21 March 2025
