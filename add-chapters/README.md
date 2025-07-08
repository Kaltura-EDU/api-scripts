# Add Chapters to Kaltura Entries

This script reads a CSV file of chapter metadata and adds chapter markers (cue points) to specified Kaltura media entries using the Kaltura API.

## Setup

1. **Download the Files**
Clone or download this repository to your computer. You can either:
- Use the green **Code** button on GitHub to **Download ZIP**, then extract it
- Or, if you're familiar with Git, clone the repo:
  ```bash
  git clone https://github.com/your-org/add-chapters.git
  cd add-chapters
  ```

2. **Install dependencies** (in your virtual environment):
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure your environment**:

   - Rename the `.env.example` file to just `.env`:
     ```bash
     cp .env.example .env
     ```
   - Edit `.env` to include your Kaltura credentials and CSV filename.

## CSV Format
Your `chapter_input.csv` must contain a header row with these fields:

```
entry_id,timecode,chapter_title,chapter_description,search_tags
```

- `timecode` must be in `HH:MM:SS` format with **two digits** for each unit.

## Running the Script

Run the script from the command line:

```bash
python add-chapters.py
```

Each chapter will be added as a `KalturaThumbCuePoint` with subtype `CHAPTER`.

## Notes

- The script loads credentials from `.env`. Do not commit `.env` to version control.
- A `.gitignore` file should exclude `.env`, `venv/`, and `__pycache__/`.
