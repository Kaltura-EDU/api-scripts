# Description
This script allows you to generate a report that summarizes the number of Kaltura video entries and their total duration. You can filter by **category ID**, **tag**, or a combination of both (entries must match *both* filters if both are used).

To avoid hitting Kaltura’s 10,000-entry API result cap, the script automatically breaks the search into smaller time intervals (e.g., by year, month, week, or day), based on your selected `RESTRICTION_INTERVAL`. You will be prompted for this when the script runs.

The output includes:
- A summary of entry counts and total durations for each time interval
- Final totals shown onscreen in minutes, hours, days, months, and years
- Two CSV files (summary and detailed list of entries), with filenames that include a timestamp and the tag/category used

# Caveats
- If your interval size is too broad (e.g., `RESTRICTION_INTERVAL = 1` for yearly), and your dataset is too large, Kaltura may return an error and refuse to run the query. If that happens, the script will exit gracefully and suggest trying a smaller interval (e.g., weekly or daily).
- You may not need to be on a VPN to run this script. However, if you experience connection issues or inconsistent API behavior, connecting via VPN may help — especially when running large batches of requests.
- Category filtering uses `categoriesIdsMatchOr`, so entries from subcategories will also be included if their parent category matches your filter.

# How to Run the Script
1. Download **report-count-duration.py** and **requirements.txt** to your computer. Make sure they are in the same folder.
2. Open a terminal or command prompt.
3. Navigate to the folder where your files are stored:  
   `cd /path/to/project`
4. (Optional but recommended) Set up a virtual environment:  
   `python3 -m venv venv`
5. Activate the virtual environment:  
   - Mac/Linux: `source venv/bin/activate`  
   - Windows: `venv\\Scripts\\activate`
6. Install the necessary packages:  
   `pip install -r requirements.txt`
7. Run the script:  
   `python3 report-count-duration.py`
8. You’ll be prompted for:
   - A tag (optional)
   - A category ID (optional)
   - A start and end date
   - An interval type (1=Yearly, 2=Monthly, 3=Weekly, 4=Daily)

# Output CSVs
CSV files are saved automatically in your working directory. Filenames include:
- The interval type
- The tag and/or category ID used (if any)
- A timestamp so repeated runs won’t overwrite previous results

Example filenames:  
video_summary_uc_san_diego_podcast_216135363_month_20250312_143012.csv  
video_details_uc_san_diego_podcast_216135363_month_20250312_143012.csv  

# Contact
Galen Davis  
Senior Education Technology Specialist  
UC San Diego  
12 March 2025
