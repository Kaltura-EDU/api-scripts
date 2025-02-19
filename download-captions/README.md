# Description
This script allows you to download all caption assets from a Kaltura media entry. You can select the entries using a tag, a category ID, or a comma-delimited list of entry IDs. The filenames take this format: creation date, entry ID, entry title, and lastly the caption label. E.g.
```
2024-05-22_1_xuw9zvsc_XSE1_5B_Axial_Compression__English.srt
2024-05-22_1_xuw9zvsc_XSE1_5B_Axial_Compression__Spanish.srt
2024-05-22_1_xuw9zvsc_XSE1_5B_Axial_Compression__English__auto-generated_.srt
```
It might end up being long, but it'll have lots of metadata that will help you identify what caption file is from what entry. 

# How to Run the Script
1. Download **download-captions.py** and **Requirements.txt** to your computer. Ensure they end up in the same folder.
2. Open **download-captions.py** with a text editor.
3. Add values for `partner_id` and `admin_secret` based on your own instance of Kaltura.
4. Save the changes.
5. Open a command line interface, such as Terminal on a Mac or Command Prompt in Windows.
6. Navigate to wherever you put your files (e.g. `cd /path/to/project`).
7. Set up a virtual environment if you haven't already: `python3 -m venv venv`
8. Install the needed modules: `pip install -r requirements.txt`
9. Activate your virtual environment (Windows: `venv\\Scripts\\activate` Mac: `source venv/bin/activate`)
10. Run the script: `python3 download-captions.py`


Galen Davis, Senior Education Technology Specialist
UC San Diego
19 February 2025
