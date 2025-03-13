# Kaltura Category Affiliation Reporter

This Python script retrieves category affiliations for one or more Kaltura users, including their role in each category and the category hierarchy. It could be helpful for quick visual checks or for generating CSV reports for audit, troubleshooting, or support purposes.

## Features

- Accepts one or more Kaltura user IDs (comma-delimited)
- Shows the user's role in each category (Owner, Manager, Moderator, Contributor, Member, None), category id, and category name.
- CSV output includes the full hierarchical path
- Configurable output options:
  - CSV or visual-only (no file)
  - One combined file for all users or separate files per user


# How to Run the Script
1. Download **show-category-affiliations.py** and **requirements.txt** to your computer. Ensure they end up in the same folder.
2. Open **show-category-affiliations.py** with a text editor.
3. Add values for `PARTNER_ID` and `ADMIN_SECRET` based on your own instance of Kaltura.
4. Save the changes.
5. Open a command line interface, such as Terminal on a Mac or Command Prompt in Windows.
6. Navigate to wherever you put your files (e.g. `cd /path/to/project`).
7. Set up a virtual environment if you haven't already:  
`python3 -m venv venv`
8. Activate your virtual environment  
Windows: `venv\\Scripts\\activate`  
Mac: `source venv/bin/activate`
9. Install the needed modules:  
`pip install -r requirements.txt`
10. Run the script:  
`python3 show-category-affiliations.py`


Galen Davis, Senior Education Technology Specialist  
UC San Diego  
12 March 2025  