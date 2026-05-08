# manage-channel-members

Bulk-manages membership in Kaltura categories (MediaSpace channels) from a CSV. Supports adding, removing, verifying, and changing the role of users, with configurable concurrency and retry.

## Prerequisites

- Python 3.8+
- A Kaltura admin API secret
- The following Python packages:

```
pip install KalturaApiClient python-dotenv
```

## Setup

1. Copy `.env.example` to `.env` and fill in your credentials.
2. Create an `input/` directory and place your CSV there.
3. Run the script.

## Input CSV

| Column | Required | Description |
|---|---|---|
| `username` | Yes | Kaltura/MediaSpace username |
| `category_id` | Yes | Kaltura category ID |
| `action` | Yes | `add`, `remove`, `verify`, or `change_role` |
| `role` | See below | `member`, `manager`, `contributor`, `moderator`, or `owner` |

**`role` requirements by action:**

| Action | `role` field |
|---|---|
| `add` | Required |
| `remove` | Ignored |
| `verify` | Optional — if provided, verifies both presence and role |
| `change_role` | Required |

## Actions

### `add`
Adds the user to the channel with the specified role. If `role` is `owner`, updates `category.owner` via the Kaltura API. See [Ownership notes](#ownership-notes) below.

### `remove`
Removes the user from the channel. Returns an error if the user is the channel owner (ownership must be transferred first).

### `verify`
Checks whether the user is in the channel. If `role` is provided, also checks whether their current role matches. The `result` field in the output will indicate both.

Example results:
- `in channel as manager (matches expected)`
- `in channel as member (expected manager)`
- `not in channel (expected member)`
- `not in channel`

### `change_role`
Changes the user's role. The `result` field records both the original and new role:

```
role changed from member to manager
```

Returns an error if the user is not in the channel, or if the user is the current channel owner (see [Ownership notes](#ownership-notes)).

## Ownership notes

Setting a user as owner (`role=owner`) updates the `category.owner` field via `category.update()`. Unlike the MediaSpace web UI, **the Kaltura API does not automatically demote the previous owner** — their `categoryUser` entry remains unchanged.

To transfer ownership and demote the old owner, include both operations in your CSV:

```csv
username,category_id,action,role
new_owner,123456,add,owner
old_owner,123456,change_role,manager
```

Row order in the CSV is not guaranteed in parallel execution. If both rows target the same category, consider running them sequentially (set `THREAD_COUNT=1`) or in two separate passes.

You cannot change the role of the current channel owner using `change_role` — the script will return an error directing you to transfer ownership first.

## Running

```bash
# Uses INPUT_CSV_FILENAME from .env
python3 manage-channel-members.py

# Or pass the file as an argument
python3 manage-channel-members.py input/members.csv
```

## Output

A timestamped report CSV is written to `output/`:

| Column | Description |
|---|---|
| `username` | The user that was processed |
| `category_id` | The target category |
| `action` | The action from the input file |
| `role` | The role from the input file (if any) |
| `result` | Detailed outcome: success message, role change description, or error |

## Configuration

| Variable | Default | Description |
|---|---|---|
| `PARTNER_ID` | — | Kaltura partner ID |
| `ADMIN_SECRET` | — | Kaltura admin secret |
| `USER_ID` | — | API user ID |
| `SERVICE_URL` | `https://www.kaltura.com` | Kaltura API endpoint |
| `SESSION_EXPIRY` | `86400` | Session TTL in seconds |
| `INPUT_CSV_FILENAME` | — | Path to input CSV |
| `THREAD_COUNT` | `10` | Parallel API requests (5–15 recommended) |
| `MAX_RETRIES` | `4` | Total attempts per operation (1 = no retry) |
