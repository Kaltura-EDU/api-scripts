# Description
This script allows you to rename Kaltura media entries in batches. You can add something to the entries' titles at the beginning or at the end (a prefix or a suffix). You can select what entries you'd like to rename by using one of the following:

- a tag
- a category
- a comma-delimited list of entry IDs

# One-Time Instructions
1. Download **rename-entries.py** and **Requirements.txt** to your computer.
2. Open **rename-entries.py** with a text editor.
3. Add values for `partner_id` and `admin_secret` based on your own instance of Kaltura.
4. Save the changes.
5. Open a command line interface, such as Terminal on a Mac or Command Prompt in Windows.
6. Navigate to wherever you put your files (e.g. `cd /path/to/project`).
7. Set up a virtual environment if you haven't already: `python3 -m venv venv`
8. Install the needed modules: `pip install -r requirements.txt`
# Run the script
6. Activate your virtual environment (Windows: `venv\\Scripts\\activate` Mac: `source venv/bin/activate`)
7. Run the script: `python3 rename-entries.py`


Galen Davis, Senior Education Technology Specialist
UC San Diego
29 January 2025
