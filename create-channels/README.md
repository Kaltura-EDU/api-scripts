# Bulk Kaltura Channel Creation

Create many MediaSpace channels at once from a spreadsheet (CSV). Each row
becomes one channel with its own owner, privacy setting, and users
(managers, moderators, contributors, and members). When it's done, you get a
CSV listing every new channel with a direct MediaSpace link.

## What it does

* Checks every row of your CSV before creating anything (missing fields,
  invalid privacy values)
* Stops if any channel name already exists in MediaSpace, so you don't end
  up with duplicates
* Shows you the list of channels it's about to create and asks you to
  confirm
* Creates each channel, sets its owner, and adds its managers, moderators,
  contributors, and members
* If a user can't be added (e.g. a mistyped user ID), reports it and keeps
  going
* Saves a results CSV to the `output/` folder

## Getting started

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Set up your `.env` file

The script reads its settings from a file named `.env` in the script folder.
Make one by copying the example:

```bash
cp .env.example .env
```

(Or duplicate `.env.example` in your file manager and rename the copy to
`.env`.)

> **Can't see `.env` or `.env.example`?** Files whose names start with a dot
> are hidden by default. On macOS, press **⌘ + Shift + .** in Finder to show
> them. On Windows, open File Explorer and choose **View → Show → Hidden
> items**.

Open `.env` in any text editor and fill in the values. Every setting is
explained in comments right in the file.

**Required:**

* `KALTURA_PARTNER_ID`: your Kaltura Partner ID (KMC → Settings → Integration
  Settings)
* `KALTURA_PARENT_ID`: the category ID new channels go under, usually your
  MediaSpace site's "channels" category (KMC → Content → Categories)
* `KALTURA_MEDIASPACE_URL`: your MediaSpace address, e.g.
  `https://mediaspace.example.edu`
* `INPUT_FILENAME`: the name of your CSV file (see step 3)
* A privacy setting, either `KALTURA_CHANNEL_PRIVACY` for every channel or a
  `privacy` column in your CSV (see step 3). `.env.example` starts you with
  `KALTURA_CHANNEL_PRIVACY=3` (channel members only).

**Optional:**

* `KALTURA_USER`: the user ID the session runs as (usually your own), so
  changes show up under your name in Kaltura's logs
* `KALTURA_FULL_NAME_PREFIX`: the full path of the parent category, used to
  spot channel names that already exist. The default,
  `MediaSpace>site>channels>`, is right for most sites.
* Channel settings applied to every channel (who can join, who can find the
  channel, moderation, and so on). The defaults make private,
  invitation-only channels.
* Network settings (`REQUEST_TIMEOUT`, `MAX_NETWORK_RETRIES`,
  `NETWORK_RETRY_DELAY`). The defaults are fine for almost everyone.

**Your admin secret never goes in `.env`.** The script asks for it each time
it runs, and nothing you type is shown on screen.

### 3. Prepare your CSV

Put your CSV in the `input/` folder next to the script, and set
`INPUT_FILENAME` in `.env` to its filename (e.g.
`INPUT_FILENAME=channelDetails.csv`).

The CSV needs these columns:

| Column        | Required | What goes in it                                                   |
|---------------|----------|-------------------------------------------------------------------|
| `channelName` | Yes      | The channel's name                                                |
| `owner`       | Yes      | The owner's Kaltura user ID                                       |
| `privacy`     | No       | Who can see the content: `1` = anyone, `2` = any logged-in user, `3` = channel members only. Overrides `KALTURA_CHANNEL_PRIVACY` for that row. |
| `managers`     | No       | Users with full control, including channel settings and who belongs |
| `moderators`   | No       | Users who can add media and approve or reject media others submit |
| `contributors` | No       | Users who can add media to the channel                             |
| `members`      | No       | Users who can view the channel                                     |

The four user columns each take Kaltura user IDs separated by commas (put
the cell in quotes if your editor doesn't do it for you). Include only the
columns you need. If someone is listed under more than one role, they get
the highest one. You don't need to list the owner: they already have full
control, and the script leaves them out if you do.

Example:

```csv
channelName,owner,managers,contributors,members,privacy
Biology 101 Lectures,jdoe,tasmith,"bchen,dlee","student1,student2",3
Campus Events,events-admin,,,,1
```

**Privacy: one value for all, or one per channel.** To give every channel
the same privacy, set `KALTURA_CHANNEL_PRIVACY` in `.env` and leave the
`privacy` column out. To set it per channel, include the `privacy` column.
Its values win, and any blank cell falls back to `KALTURA_CHANNEL_PRIVACY`.
The script tells you which source it's using before it asks you to confirm.

If your CSV uses different column names, change the `KALTURA_..._HEADER`
settings at the bottom of `.env` to match.

### 4. Run the script

```bash
python3 create-channels.py
```

First the script shows which CSV column it reads for each setting and which
columns it ignores. If something says MISSING, fix the matching
`KALTURA_..._HEADER` line in `.env`. Then enter your admin secret when
asked. The script checks your CSV, logs in, checks for existing channel
names, then lists the channels it will create and asks you to confirm. When it finishes, the results CSV is in the
`output/` folder, named like `2026-10-06-1430_create-channels.csv`.

## Upgrading from v1.x

As of v2.0.0, `.env` settings start with `KALTURA_` (e.g. `PARTNER_ID` →
`KALTURA_PARTNER_ID`, `USER_ID` → `KALTURA_USER`, `MEDIA_SPACE_BASE_URL` →
`KALTURA_MEDIASPACE_URL`), and `INPUT_CSV_FILENAME` is now `INPUT_FILENAME`.
If your `.env` still has the old names, the script stops and lists exactly
what to rename. It also tells you to delete any leftover `ADMIN_SECRET` line.

Your CSV now goes in the `input/` folder.

## Author

Galen Davis
Senior Education Technology Specialist, UC San Diego
