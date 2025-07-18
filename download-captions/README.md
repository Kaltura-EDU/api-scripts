# Description
This script allows you to download all caption assets from one or more Kaltura media entries, and optionally allows you to create TXT files from the SRT files. You can select the entries using a tag, a category ID, or a comma-delimited list of entry IDs. The filenames take this format: creation date, entry ID, entry title, and lastly the caption label. E.g.
```
2024-05-22_1_xuw9zvsc_XSE1_5B_Axial_Compression__English.srt
2024-05-22_1_xuw9zvsc_XSE1_5B_Axial_Compression__Spanish.srt
2024-05-22_1_xuw9zvsc_XSE1_5B_Axial_Compression__English__auto-generated_.srt
```
It might end up being long, but it'll have lots of metadata that will help you identify what caption file is from what entry. 

The script now supports pagination, so all matching entries will be included — not just the first 30. Each caption downloaded will also show a numbered message, helping you track progress when working with large batches. Note that the number of caption files downloaded may exceed the number of entries, since each entry may have multiple caption tracks. 

# How to Run the Script
1. Download **download-captions.py** and **Requirements.txt** to your computer. Ensure they end up in the same folder.
2. Open **download-captions.py** with a text editor.
3. Open a command line interface, such as Terminal on a Mac or Command Prompt in Windows.
4. Navigate to wherever you put your files (e.g. `cd /path/to/project`).
5. Set up a virtual environment if you haven't already: `python3 -m venv venv`
6. Activate your virtual environment (Windows: `venv\\Scripts\\activate` Mac: `source venv/bin/activate`)
7. Install the needed modules: `pip install -r requirements.txt`
8. Run the script: `python3 download-captions.py`

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

*Last updated 2025-05-05*
