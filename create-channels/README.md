# Bulk Kaltura Channel Creation

This script allows you to create multiple Kaltura MediaSpace channels in bulk by reading from a CSV input file. Each channel will be created under a specified parent category with designated owners and members.

## What It Does
- Creates Kaltura channels using the Kaltura API
- Assigns owners and adds members to each channel
- Outputs a CSV summary of all created channels, including direct MediaSpace links (leveraging the MEDIA_SPACE_BASE_URL variable

## 📂 CSV Input File
Your input file should be named `channelDetails.csv` and placed in the same
directory as the script.

### Required Columns
| Column       | Description                                                                 |
|--------------|-----------------------------------------------------------------------------|
| `channelName` | The name of the channel to be created                                        |
| `owner`       | The Kaltura user ID of the channel owner                                     |
| `members`     | Comma-separated list of member user IDs. Use quotes if listing multiple.     |
| `privacy`     | Channel privacy level: `1` = Public, `2` = Authenticated, `3` = Private      |

You can use [`channelDetails_template.csv`](channelDetails_template.csv) as a starting point.

## ⚙️ Required Configuration
Before running the script, you **must** edit the following variables at the top
of the file:

- `PARTNER_ID`: Your Kaltura partner ID (integer)
- `ADMIN_SECRET`: Your admin secret key (string)
- `USER_ID`: Your user ID (used for generating the session)
- `PARENT_ID`: The category ID under which new channels will be created. 
  Usually the "Channels" category in your MediaSpace instance.

```python
PARENT_ID = None  # Replace with the numeric ID of your parent category
