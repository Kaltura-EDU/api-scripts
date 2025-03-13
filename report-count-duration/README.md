# Description
This script allows you to generate a report that summarizes the number of Kaltura video entries and their total duration. You can filter by **category ID**, **tag**, or a combination of both (entries must match *both* filters if both are used).

The script breaks the search into smaller time intervals (e.g., by month or week) to avoid hitting Kaltura's 10,000-entry API limit, and you can customize this behavior using the `RESTRICTION_INTERVAL` variable. In other words, if your date range returns more than 10,000 entries, then you'll need to find a way to "chunk" those API calls.

The output includes:
- A summary of counts and total durations for each interval
- A final total displayed in minutes, hours, days, months, and years
- Two CSV files (summary and detailed entry list), named based on your selected interval

# Caveat
* If your interval size is too broad (e.g., `RESTRICTION_INTERVAL = 1` for yearly), and your dataset is large, you may still hit the 10,000-entry cap and the script will exit early with a warning. If that happens, try increasing the value of the `RESTRICTION_INTERVAL` variable. This will make the script pull smaller datasets, hopefully returning fewer entries per API call and staying underneath the "10,000 entries per call" limit. 
* You may not need to be on a VPN to run this script, but if you encounter connection errors or hanging behavior, a VPN may help — especially when running large volumes of API calls. In other words, if you're working from home, it's possible your ISP may not like you making so many API calls to the same client. :) 
* Category filtering uses `categoriesIdsMatchOr`, so if your category ID has subcategories, entries in those subcategories will also be included.

# How to Run the Script
1. Download **report-count-duration.py** and **requirements.txt** to your computer. Make sure they are in the same folder.
2. Open **report-count-duration.py** with a text editor.
3. Set values for the following global variables near the top of the script:
   - `PARTNER_ID`
   - `ADMIN_SECRET`
   - `USER_ID` (optional)
   - `CATEGORY_ID` (optional)
   - `TAG` (optional)
   - `START_DATE` and `END_DATE`
   - `RESTRICTION_INTERVAL` (choose 1=Yearly, 2=Monthly, 3=Weekly, 4=Daily) (Note that this variable should be an integer and requires no quotation marks.)
4. Save your changes.
5. Open a terminal or command prompt.
6. Navigate to the folder where your files are stored:  
   `cd /path/to/project`
7. (Optional but recommended) Set up a virtual environment:  
   `python3 -m venv venv`
8. Activate the virtual environment:  
   - Mac: `source venv/bin/activate`  
   - Windows: `venv\\Scripts\\activate`
9. Install the necessary packages:  
   `pip install -r requirements.txt`
10. Run the script:  
    `python3 report-count-duration.py`

Galen Davis, Senior Education Technology Specialist  
UC San Diego  
12 March 2025
