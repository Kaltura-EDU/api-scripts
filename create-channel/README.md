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
    Edit `.env` and fill in your API credentials and channel preferences.

## Configuration Reference

The following integer values are used to configure channel behavior in the `.env` file.

| Variable | Value | Meaning |
| :--- | :--- | :--- |
| **CHANNEL_PRIVACY** | `1` | Open (Membership open to all) |
| | `2` | Restricted (Membership by request/invite) |
| | `3` | Private (Membership by invite only) |
| **USER_JOIN_POLICY** | `1` | Auto Join |
| | `2` | Request to Join |
| | `3` | Not Allowed |
| **APPEAR_IN_LIST** | `1` | Partner Only (Visible to everyone) |
| | `2` | KMC Only |
| | `3` | Category Members Only (Hidden from non-members) |
| **INHERITANCE_TYPE** | `1` | Inherit from parent |
| | `2` | Manual (Override parent settings) |
| **DEFAULT_PERMISSION_LEVEL** | `1` | Contributor |
| | `2` | Moderator |
| | `3` | Member |
| | `4` | Manager |
| **CONTRIBUTION_POLICY** | `1` | All Members |
| | `2` | Members with Contribution Permission |
| **MODERATION** | `0` | No Moderation |
| | `1` | Moderation Required |

## Usage

Run the script using Python:

```bash
python create-channel.py
```