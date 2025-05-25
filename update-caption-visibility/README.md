# update-caption-visibility.py

This script hides captions with a specific label (e.g., "English (auto-generated)") from displaying in the Kaltura player.

## Features

- Prompts user to select how to identify target media entries:
  - By **tag**
  - By **category ID**
  - By **comma-delimited list of entry IDs**
- Checks all matching entries for captions that match the configured `CAPTION_LABEL`
- Hides those captions by setting `displayOnPlayer = False`
- Prompts for confirmation before applying changes
- Outputs changes and skipped captions to a timestamped CSV log

## Requirements

This script requires the following Python packages:
- `KalturaApiClient`
- `lxml`
- `pytz`

Install them using a `requirements.txt` file or directly via pip:
```bash
pip install -r requirements.txt
```

## Recommended Setup

1. Create and activate a virtual environment:
```bash
python3 -m venv env
source env/bin/activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Run the script:
```bash
python3 update-caption-visibility.py
```

## Configuration

Edit the following variables at the top of the script before running:

```python
PARTNER_ID = ""     # Your Kaltura partner ID
ADMIN_SECRET = ""   # Your admin secret from KMC
USER_ID = "your-email@yourdomain.edu"
CAPTION_LABEL = "English (auto-generated)"  # Can be customized per environment
```

## Output

- A CSV file is created in the current directory, named like:
  ```
  2025-05-20-1530_captionUpdates.csv
  ```
- Each row logs:
  - Entry ID
  - Entry Name
  - Caption Asset ID
  - Caption Label
  - Action taken (or "No Change")
  - Timestamp

## Disclaimer

This script does not delete any captions. It only updates their visibility in the player UI.

Galen Davis  
Senior Education Technology Specialist  
UC San Diego  
25 May 2025
