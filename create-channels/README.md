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

You can use `channelDetails_template.csv` as a template.

## Required Configuration
Before running the script, you **must** edit the following variables at the top of the file:

- `PARTNER_ID`: Your Kaltura partner ID (integer)
- `ADMIN_SECRET`: Your admin secret key (string)
- `USER_ID`: your user ID (optional; actions will be assigned to this user)
- `PARENT_ID`: The category ID under which new channels will be created. Usually the "channels" category in your MediaSpace instance, but it depends on your channel creation preferences. For example, if you're creating a bunch of galleries, you might want to put it under the "galleries" category. 
- `MEDIA_SPACE_BASE_URL`: The base URL of your MediaSpace instance, probably (but not necessarily) ending in `/channel/`.
  Example: `https://mediaspace.ucsd.edu/channel/`

## Author
Galen Davis  
Senior Education Technology Specialist, UC San Diego  
22 April 2025
