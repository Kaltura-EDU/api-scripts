# Create Channel Script

This script creates a new MediaSpace channel (Kaltura Category) using the Kaltura API. It supports configuration via environment variables.

## Setup

1.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

2.  **Configure Environment:**
    Copy the example file to `.env`:
    ```bash
    cp .env.example .env
    ```
    Edit `.env` and fill in your API credentials and channel settings. Note that `CHANNEL_NAME` and `MEDIA_SPACE_URL` are required — the script will exit with an error if either is missing.

## Configuration Reference

The following integer values are used to configure channel behavior in the `.env` file.

| Variable | Value | Meaning |
| :--- | :--- | :--- |
| **CHANNEL_PRIVACY** | `1` | Anyone can see the content |
| | `2` | Any logged-in user can see the content |
| | `3` | Channel members only |
| **USER_JOIN_POLICY** | `1` | Join automatically |
| | `2` | Request to join |
| | `3` | Not allowed |
| **APPEAR_IN_LIST** | `1` | Partner only (Visible to everyone) |
| | `3` | Category members only (Hidden from non-members) |
| **INHERITANCE_TYPE** | `1` | Inherit from parent |
| | `2` | Manual (Override parent settings) |
| **DEFAULT_PERMISSION_LEVEL** | `0` | Manager |
| | `1` | Moderator |
| | `2` | Contributor |
| | `3` | Member |
| **CONTRIBUTION_POLICY** | `1` | All Members |
| | `2` | Members with Contribution Permission |
| **MODERATION** | `0` | No Moderation |
| | `1` | Moderation Required |

## Assigning Users

Set any of `MANAGERS`, `MODERATORS`, `CONTRIBUTORS`, and `MEMBERS` in `.env` to a comma-separated list of Kaltura user IDs (e.g. `MODERATORS=jdoe,asmith`). A user listed in more than one is assigned the highest level (Manager > Moderator > Contributor > Member). The `OWNER` is already a manager and is skipped.

## Usage

Run the script using Python:

```bash
python create-channel.py
```

When prompted, enter your Kaltura admin secret. The input will not be visible as you type.
