# Description
This script allows you to download all caption assets from one or more Kaltura media entries. Optionally you can elect to convert them to TXT files and delete  the original caption files, leaving you only with TXT files. It supports downloading captions in their original formats, such as SRT, VTT, and others. You can select the entries using a tag, a category ID, or a comma-delimited list of entry IDs. The script also handles multi-stream entries and can be configured to skip child entries if desired. Filenames by default include creation date, entry ID, entry title, and caption label, but you can optionally shorten filenames by excluding the caption label. E.g.
```
2024-05-22_1_xuw9zvsc_XSE1_5B_Axial_Compression__English.srt
2024-05-22_1_xuw9zvsc_XSE1_5B_Axial_Compression__Spanish.srt
2024-05-22_1_xuw9zvsc_XSE1_5B_Axial_Compression__English__auto-generated_.srt
```
The filename might end up being long, but it'll have lots of metadata that will help you identify what caption file is from what entry.

The script supports pagination, so all matching entries will be included — not just the first 30. Each caption downloaded will also show a numbered message, helping you track progress when working with large batches. Note that the number of caption files downloaded may exceed the number of entries, since each entry may have multiple caption tracks.

# Configuration (.env)
The script requires a `.env` file to be created in the same folder with the following environment variables:

**Required:**
- `PARTNER_ID` : Your Kaltura partner ID.
- `ADMIN_SECRET` : Your Kaltura admin secret key.

**Optional:**
- `KALTURA_SERVICE_URL` : The Kaltura service URL (default: https://www.kaltura.com).
- `USER` : The Kaltura user ID to act as (default: admin user).
- `DOWNLOAD_FOLDER` : Folder where captions will be saved (default: current folder).
- `CONVERT_TO_TXT` : Set to `true` to convert captions to TXT files and delete originals (default: false).
- `INCLUDE_CHILD_CATEGORIES` : Set to `true` to include entries from child categories when using category ID (default: false).
- `INCLUDE_CAPTION_LABEL_IN_FILENAMES` : Set to `false` to exclude caption labels from filenames for shorter names (default: true).
- `SKIP_CHILD_ENTRIES` : Set to `true` to skip child entries in multi-stream entries (default: false).
- `DEBUG` : Set to `true` to enable debug output for troubleshooting (default: false).

# How to Run the Script
1. Download **download-captions.py** and **Requirements.txt** to your computer. Ensure they end up in the same folder.
2. Create a `.env` file in the same folder and add your configuration variables as described above.
3. Open a command line interface, such as Terminal on a Mac or Command Prompt in Windows.
4. Navigate to wherever you put your files (e.g. `cd /path/to/project`).
5. Set up a virtual environment if you haven't already: `python3 -m venv venv`
6. Activate your virtual environment (Windows: `venv\\Scripts\\activate` Mac: `source venv/bin/activate`)
7. Install the needed modules: `pip install -r requirements.txt`
8. Run the script: `python3 download-captions.py`

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

*Last updated 2025-09-03*
