# Bulk Kaltura Channel Creation

This script allows you to create multiple Kaltura MediaSpace channels in bulk by reading from a CSV input file. Each channel will be created under a specified parent category with designated owners and members.

## What It Does
- Creates Kaltura channels using the Kaltura API
- Assigns an owner and adds members to each channel
- Outputs a CSV summary of all created channels, including direct MediaSpace links (leveraging the MEDIA_SPACE_BASE_URL variable)

## CSV Input File
Your input file should be named `channelDetails.csv` and placed in the same
directory as the script.

### Required Columns
| Column       | Description                                                                 |
|--------------|-----------------------------------------------------------------------------|
| `channelName` | The name of the channel to be created                                        |
| `owner`       | The Kaltura user ID of the channel owner                                     |
| `members`     | Comma-separated list of member user IDs. Use quotes if listing multiple.     |
| `privacy`     | Channel privacy level: `1` = Public, `2` = Authenticated, `3` = Private      |

You can use `channelDetails.csv` as a template. Be sure not to change the filename, and keep it in the same directory as the script.

## Required Configuration
Before running the script, you **must** edit the following variables at the top of the file:

- `PARTNER_ID`: Your Kaltura partner ID (integer)
- `ADMIN_SECRET`: Your admin secret key (string)
- `USER_ID`: your user ID (optional; actions will be assigned to this user)
- `PARENT_ID`: The category ID under which new channels will be created. Usually the "channels" category in your MediaSpace instance, but it depends on your channel creation preferences. For example, if you're creating a bunch of galleries, you might want to put it under the "galleries" category. 
- `MEDIA_SPACE_BASE_URL`: The base URL of your MediaSpace instance, probably (but not necessarily) ending in `/channel/`.
  Example: `https://mediaspace.ucsd.edu/channel/`


## 🦖 Getting Started

### 1. Install dependencies

Before running the script, make sure you have Python 3 installed and install the required packages:

```bash
pip install -r requirements.txt
```

### 2. Download the script and CSV template

Clone or download the repo, then edit the provided `channelDetails_template.csv` with your own data. Save it as `channelDetails.csv` in the same directory as the script.

### 3. Edit global variables

Open the script file and fill in the required values at the top:

- `PARTNER_ID`
- `ADMIN_SECRET`
- `USER_ID` *(optional but recommended)*
- `PARENT_ID`
- `MEDIA_SPACE_BASE_URL`

### 4. Run the script

Once everything is filled in and saved, run:

```bash
python3 create-channels.py
```

After the script runs, a timestamped results file will be created in the same directory. It lists all channels created, along with their IDs, members, and direct MediaSpace links.

## Author
Galen Davis  
Senior Education Technology Specialist, UC San Diego  
22 April 2025
