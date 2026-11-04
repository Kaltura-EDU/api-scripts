# Create Channel

Creates a new MediaSpace channel from settings you fill in once in a text
file. You can also add the channel's managers, moderators, contributors, and
members in the same step, which saves adding people one at a time in
MediaSpace.

You don't need API experience to use it. You fill in a settings file, run one
command, type your admin secret, and confirm.

## Before you start

- Python 3 installed
- Your Kaltura **Partner ID** and **Administrator secret** (KMC → Settings →
  Integration Settings). Use the Administrator secret, not the User secret.
- Your MediaSpace site's URL

## 1. Install the requirements

In a terminal, from this folder:

```bash
pip install -r requirements.txt
```

## 2. Set up your `.env`

The `.env` file holds your settings. Make your own copy of the example:

```bash
cp .env.example .env
```

Or, in your file manager, duplicate `.env.example` and rename the copy to
`.env`.

**Can't see the file?** Files whose names start with a dot are hidden by
default. On a Mac, press **⌘ + Shift + .** in Finder to show them. On
Windows, in File Explorer choose **View → Show → Hidden items**.

Open `.env` in any text editor and fill in the values. Every setting has a
comment explaining it.

**Required**

| Setting | What it is |
| :--- | :--- |
| `KALTURA_PARTNER_ID` | Your Kaltura Partner ID (a number) |
| `KALTURA_MEDIASPACE_URL` | Your MediaSpace site, e.g. `https://mediaspace.example.edu` |
| `KALTURA_CHANNEL_NAME` | The new channel's name |

**Recommended**

| Setting | What it is |
| :--- | :--- |
| `KALTURA_CHANNEL_OWNER` | The owner's Kaltura user ID |
| `KALTURA_PARENT_ID` | The category the channel goes under, usually your MediaSpace site's channels category |

**Optional:** people to add. Each setting takes a comma-separated list of
user IDs, e.g. `jdoe,asmith`.

| Setting | Role |
| :--- | :--- |
| `KALTURA_MANAGERS` | Full control, including settings and who belongs |
| `KALTURA_MODERATORS` | Add media and approve media others submit |
| `KALTURA_CONTRIBUTORS` | Add media |
| `KALTURA_MEMBERS` | View the channel |

If someone is in more than one list, they get the highest role. The owner
already has full control, so there's no need to list them.

Everything else (privacy, who can join, moderation, and so on) has sensible
defaults for a private channel. See the comments in `.env.example` to change
them.

**Your admin secret never goes in `.env`.** The script asks for it each time.

## 3. Run it

```bash
python create-channel.py
```

1. Type your admin secret when asked. It won't show on screen as you type.
2. The script lists the channel it's about to create and who it will add.
   Check it for anything left over from an earlier run, then type `y` to go
   ahead.
3. It prints the new channel's ID and link.

If a user ID is wrong, the channel is still created and everyone else is
still added. The summary lists anyone who couldn't be added, so you can fix
the ID and add them in MediaSpace.

Each run saves a record to `output/YYYY-MM-DD-HHMM_create-channel.csv`: the
channel's ID and link, plus every user and whether they were added.

## Upgrading from an earlier version

Version 1.4.0 renamed the settings in `.env`. They now start with `KALTURA_`
(for example, `PARTNER_ID` is now `KALTURA_PARTNER_ID`, and `OWNER` is now
`KALTURA_CHANNEL_OWNER`). If your `.env` still has the old names, the script
stops and lists exactly what to rename. Also delete any `ADMIN_SECRET` line.
