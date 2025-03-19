# Create Kaltura Channel (MediaSpace)

This script creates a new Kaltura MediaSpace channel (category) using the Kaltura API. It supports configuration of core channel properties including privacy level, membership policies, and default permissions. Optionally, you can assign users as members, moderators, or contributors when the channel is created.

## Overview

The script performs the following actions:

1. Starts an admin session (`ks`) with full privileges.
2. Creates a new Kaltura category representing a MediaSpace channel.
3. Optionally adds members, moderators, and contributors via `categoryUser.add`.
4. Outputs confirmation of the channel and any assigned users.

## Configuration

Be sure to set the global variables at the top of the script that are specific to your instance before running:

```python
# Session variables
PARTNER_ID = ""
ADMIN_SECRET = ""
USER_ID = ""  # Optional; used for the KS, not required for the channel
```

## Channel Configuration Options

| Variable                    | Default     | Required? | Description |
|----------------------------|-------------|-----------|-------------|
| `PARENT_ID`                | `[blank]`   | Yes       | Enter the category ID that will be the new channel's parent. This is so it will show up properly in your instance of MediaSpace. |
| `PRIVACY_CONTEXT`          | `MediaSpace`| Yes       | This is UCSD's default privacy context. It may be different for you. |
| `CHANNEL_NAME`             | `[blank]`   | Yes       | Name of the new channel to be created. |
| `CHANNEL_DESCRIPTION`      | `[blank]`   | No        | Optional description for the channel. |
| `OWNER`                    | `[blank]`   | Yes       | User ID that will be listed as the channel owner. |
| `MEMBERS`                 | `[blank]`   | No        | Comma-separated list of user IDs to add as members (permission level: Member). |
| `MODERATORS`              | `[blank]`   | No        | Comma-separated list of user IDs to add as moderators (permission level: Moderator). |
| `CONTRIBUTORS`            | `[blank]`   | No        | Comma-separated list of user IDs to add as contributors (permission level: Contributor). |
| `CHANNEL_PRIVACY`          | `3`         | Yes       | 1 = Public, 2 = Visible only to Authenticated Users, 3 = Private |
| `USER_JOIN_POLICY`         | `3`         | Yes       | 1 = Auto Join, 2 = Request to Join, 3 = Not Allowed |
| `APPEAR_IN_LIST`           | `3`         | Yes       | 1 = Partner Only, 3 = Category Members Only |
| `INHERITANCE_TYPE`         | `2`         | Yes       | 1 = Inherit, 2 = Manual assignment of members |
| `DEFAULT_PERMISSION_LEVEL`| `3`         | Yes       | 0 = Manager, 1 = Moderator, 2 = Contributor, 3 = Member, 4 = None |
| `CONTRIBUTION_POLICY`      | `2`         | Yes       | 1 = All users, 2 = Members with contribution permission |
| `MODERATION`               | `0`         | No        | 0 = No moderation, 1 = Moderation enabled |


## Output

At the end of the script execution, you'll see output indicating:
- The created channel's name and ID
- The assigned owner
- Any members, moderators, or contributors that were added

Example:
```
Created channel: 123456789 (History 101)
Added member: user1 to channel 123456789
Added moderator: mod1 to channel 123456789
Script execution complete.
```

## Requirements

- Python 3.x
- Kaltura Python API Client Library and lxml (`pip install KalturaApiClient lxml`)
- Admin credentials with access to your Kaltura Partner account

## Notes

- The `privacyContext` must be set to `"MediaSpace"` for `appearInList` and other entitlement behaviors to work correctly.
- The script assigns users even if they do not already exist in the Kaltura user list (though some functionality may depend on user creation later).

## Contact

Galen Davis  
Senior Education Technology Specialist, UC San Diego  
Last updated 19 March 2025
