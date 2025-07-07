This script duplicates all Kaltura playlists that are associated with a particular category and reassigns them to a new category ID provided by the user. It's designed to address a known limitation (at least in Canvas) where courses with 10 or more playlists fail to copy any playlists when the Media Gallery is imported. The script outputs a `CSV file` summarizing which playlists were successfully duplicated and reassigned.

# Instructions for use

## Initial setup (only needed once)
1. Download all of the files for this project and put them in their own folder on your computer.
2. Rename `.env.example` to just `.env`. The .env file stores sensitive configuration values like your Kaltura partner ID, admin secret, and API URL. These values are loaded securely when the script runs.
3. Open .env in a text editor and assign values to all the variables based on your Kaltura instance.
4. Open a Terminal window and navigate to the folder in which the script is stored, e.g.
```bash
cd /Users/username/Documents/kalturaAPI/duplicate-playlists
```
5. Create a virtual environment:
```bash
python3 -m venv venv
```
6. Activate the virtual environment:
```bash
source venv/bin/activate
```
7. Install dependencies:
```bash
pip install -r requirements.txt
```

## Running the Script
Before beginning, you'll need to know the category IDs of the original Media Gallery and the destination Media Gallery.
1. From the terminal, navigate to the script's folder, e.g.
```bash
cd /Users/username/Documents/kalturaAPI/duplicate-playlists
```
2. If needed, activate your virtual environment:
```bash
source venv/bin/activate
```
3. From the terminal, within the script's folder, run:
```bash
python3 duplicate-playlists.py
```

After the script runs, it will generate a CSV file in the same folder. This file summarizes the operation for each playlist, including:

- The original playlist ID and name
- The new (duplicated) playlist ID
- The original and destination category IDs

Galen Davis  
Senior Education Technology Specialist   
UC San Diego
7 July 2025
