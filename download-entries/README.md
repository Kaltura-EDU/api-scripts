# Description
This script allows you to download source files from entries based on a tag, a category ID, a comma-delimited list of entry IDs, or an owner's user ID. 

You're able to change the name of the download folder if you wish, but the default is "kaltura_downloads". It will be created in the folder where you keep download-entries.py.

The script is currently set up to download files serially. I did this to avoid the complexity of trying to "thread" or queue downloads (which creates its own set of headaches). 


# Caveat
I ran into some issues with the script hanging at certain points. I was at home (I work remotely), but once I connected to my institution's VPN, downloads did just fine with no hanging. It may be the case that your ISP doesn't like you running too many API calls that download data!

# How to Run the Script
1. Download **download-entries.py** and **requirements.txt** to your computer. Ensure they end up in the same folder.
2. Open **download-entries.py** with a text editor.
3. Add values for `PARTNER_ID` and `ADMIN_SECRET` based on your own instance of Kaltura.
4. Save the changes.
5. Open a command line interface, such as Terminal on a Mac or Command Prompt in Windows.
6. Navigate to wherever you put your files (e.g. `cd /path/to/project`).
7. Set up a virtual environment if you haven't already: `python3 -m venv venv`
8. Activate your virtual environment (Windows: `venv\\Scripts\\activate` Mac: `source venv/bin/activate`)
9. Install the needed modules: `pip install -r requirements.txt`
10. Run the script: `python3 download-entries.py`


Galen Davis, Senior Education Technology Specialist  
UC San Diego  
24 February 2025  
