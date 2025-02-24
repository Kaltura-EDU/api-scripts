# Description
This script allows you to download source files from entries based on a tag, a category ID, or a comma-delimited list of entry IDs. 


# How to Run the Script
1. Download **download-entries.py** and **requirements.txt** to your computer. Ensure they end up in the same folder.
2. Open **download-entries.py** with a text editor.
3. Add values for `partner_id` and `admin_secret` based on your own instance of Kaltura.
4. Save the changes.
5. Open a command line interface, such as Terminal on a Mac or Command Prompt in Windows.
6. Navigate to wherever you put your files (e.g. `cd /path/to/project`).
7. Set up a virtual environment if you haven't already: `python3 -m venv venv`
8. Install the needed modules: `pip install -r requirements.txt`
9. Activate your virtual environment (Windows: `venv\\Scripts\\activate` Mac: `source venv/bin/activate`)
10. Run the script: `python3 download-entries.py`


Galen Davis, Senior Education Technology Specialist
UC San Diego
24 February 2025
