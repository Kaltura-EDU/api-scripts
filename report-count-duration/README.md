# Description
This script generates a report summarizing the number of Kaltura video entries and their total duration. You can filter by **category ID**, **tag**, or both (entries must match *both* filters if both are used).

To avoid hitting Kaltura’s 10,000-entry API result cap, the script automatically breaks the search into smaller time intervals (year, month, week, or day) based on your selected `RESTRICTION_INTERVAL`. You will be prompted for this value when the script runs.

The script output:
- Onscreen:
  - A summary of entry counts and total durations for each time interval
  - Final totals displayed in minutes, hours, days, months, and years
- Two CSV files:
  - A summary report per interval
  - A detailed list of matching entries
 
### What Does “Restriction Interval” Mean?

So, you may have encountered the issue that Kaltura doesn't like it when some sort of API action returns more than 10,000 entries. It'll throw an exception. So when you need to deal with more than 10,000 entries, you need to break up the API action in some way. In this case, because you have to search a date range, the way to restrict that search is by time period.

Setting the `RESTRICTION_INTERVAL` is like like telling the script:  
* When you perform the search with my search terms, break it up into separate searches based on the `RESTRICTION_INTERVAL` - it should return fewer than 10,000 results for each search.

If you choose a broader interval like **year**, the script will try to pull all matching entries from an entire year at once — which may be too many. But if you pick a narrower interval like **month**, **week**, or **day**, the script will break your search into smaller chunks, making it more likely to stay under the limit and avoid errors. But it will increase the amount of time it will take the script to run. 

If you're unsure, start with **monthly (2)** — it’s usually a good balance between performance and safety. If you still get an error about too many results, try **weekly (3)** or **daily (4)**.

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
   - An owner user ID (optional) 
   - A tag (optional)
   - A category ID (optional)
   - A start and end date (optional — leave both blank to search from the beginning of your repository)
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
- `duration` (formatted as HH:MM:SS)
- `created_at` (formatted in local time)
- `updated_at` (formatted in local time)
- `owner_id` (the Kaltura user ID of the entry owner)
- `original_filename` (cleaned version of the uploaded source file name, extracted from the flavorAsset URL)


# Timezone Configuration
Timestamps in the detailed CSV are formatted based on the `TIMEZONE` global variable at the top of the script (default is `"US/Pacific"`). You can change this to match your region. Common options include:
- `US/Pacific`
- `US/Mountain`
- `US/Central`
- `US/Eastern`
- `US/Alaska`
- `US/Hawaii`


# Earliest Repository Date

If the user leaves both start and end dates blank, the script will search from the earliest known entry in your Kaltura repository. You can configure this value at the top of the script by editing the `EARLIEST_START_DATE` global variable. It should be entered in `YYYY-MM-DD` format. 

Example:
```python
EARLIEST_START_DATE = "2017-09-18"
```

This keeps things clean, user-friendly, and consistent with your other script behavior.


# Contact
Galen Davis  
Senior Education Technology Specialist  
UC San Diego  
Last updated 26 March 2025
