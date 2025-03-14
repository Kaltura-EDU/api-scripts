# Description
This script generates a report summarizing the number of Kaltura video entries and their total duration. You can filter by **category ID**, **tag**, or both (entries must match *both* filters if both are used).

To avoid hitting Kaltura’s 10,000-entry API result cap, the script automatically breaks the search into smaller time intervals (year, month, week, or day) based on your selected `RESTRICTION_INTERVAL`. You will be prompted for this value when the script runs.

The output includes:
- A summary of entry counts and total durations for each time interval
- Final totals displayed onscreen in minutes, hours, days, months, and years
- Two CSV files:
  - A summary report per interval
  - A detailed list of matching entries

# Caveats
- If your interval size is too broad (e.g., `RESTRICTION_INTERVAL = 1` for yearly), and your dataset is too large, Kaltura may return an error and refuse to run the query. If that happens, the script will exit gracefully and recommend using a smaller interval (e.g., weekly or daily).
- You may not need to be on a VPN to run this script (if you're working from home), but if you encounter connection errors or timeouts — especially during large queries — connecting via VPN may help. It's possible your ISP won't like you making a ton of API calls to the same place. 
- Category filtering uses `categoriesIdsMatchOr`, meaning entries in subcategories will also be included if their parent category matches your filter.

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
6. Install the required packages:  
   `pip install -r requirements.txt`
7. Run the script:  
   `python3 report-count-duration.py`
8. You’ll be prompted for:
   - A tag (optional)
   - A category ID (optional)
   - A start and end date (leave end date blank if you want it to be today)
   - A restriction interval (1 = Yearly, 2 = Monthly, 3 = Weekly, 4 = Daily)

# Output CSVs
CSV files are saved automatically in your working directory. Filenames include:
- The interval type
- The tag and/or category ID used (if any)
- A timestamp to avoid overwriting previous runs

Example filenames:  
`video_summary_uc_san_diego_podcast_216135363_month_20250312_143012.csv`  
`video_details_uc_san_diego_podcast_216135363_month_20250312_143012.csv`

The **detailed CSV** includes the following columns:
- `entryId`
- `name`
- `duration_sec`
- `created_at`
- `owner_id` (the Kaltura user ID of the entry owner)

# Contact
Galen Davis  
Senior Education Technology Specialist  
UC San Diego  
Last updated 14 March 2025
