# reassign-owners.py

Changes who owns Kaltura media entries, in bulk. Useful when someone leaves,
when a department account takes over a set of recordings, or when entries
were uploaded under the wrong account.

There are three ways to pick which entries move:

| Mode | You provide | What moves |
|------|-------------|------------|
| `owner_map` | A CSV of old user → new user | **Every** entry each old user owns |
| `entry_map` | A CSV of entry ID → new owner | Only the entries you list |
| `tag` | A tag and one new owner | Every entry with that tag |

Always do a dry run first: the script reports what it *would* change without
changing anything.

---

## Getting started

1. Install the requirements:

   ```
   pip install -r requirements.txt
   ```

2. **Create your `.env`.** Copy `.env.example` to a new file named `.env`
   (`cp .env.example .env`, or duplicate and rename it in your file manager),
   then fill in `PARTNER_ID` and `USER_ID`.

   > **Can't see `.env`?** Files starting with a dot are hidden by default.
   > On a Mac, press **⌘ + Shift + .** in Finder to show them. On Windows,
   > choose **View → Show → Hidden items** in File Explorer.

3. Put your CSV in the `input/` folder next to the script (not needed for
   `tag` mode).

4. Run it:

   ```
   python3 reassign-owners.py
   ```

   The script asks for your **admin secret** every time it runs (typing is
   hidden). Find it in KMC → Settings → Integration Settings; use the
   **Administrator** secret, not the User secret. It is never stored in
   `.env`.

   If `MODE` or `INPUT_FILENAME` is blank in `.env`, the script asks for
   those too. Fill them in to skip the questions on later runs. Before
   changing anything, it shows how many entries it found and asks you to
   type `yes`.

---

## Input CSV

### `entry_map`: specific entries

Required columns:

- `entry_id`: the Kaltura entry ID
- `owner_new`: the user ID who should own it

Optional, **recommended** column:

- `owner_old`: who you expect owns the entry now

```
entry_id,owner_new,owner_old
1_abcd1234,multimedia@ucsd.edu,jsmith
1_efgh5678,multimedia@ucsd.edu,adoe
```

You don't need `owner_old`: the script looks up each entry's current owner
either way and records it in the results. Including it gives you a record of
what you *meant* to move, and catches a stale list. If an entry's real owner
differs from `owner_old`, the results flag it in the `note` column and the
run ends with a warning. By default the entry is still reassigned. Set
`SKIP_OWNER_MISMATCH=true` to leave those entries alone instead.

### `owner_map`: everything a user owns

Both columns are required, because the script finds the entries *by* the old
username:

- `old_username`: the current owner's user ID
- `new_username`: the user ID who should take over

```
old_username,new_username
jsmith,multimedia@ucsd.edu
adoe,multimedia@ucsd.edu
```

### Different column names?

If your CSV uses other headers, set them in `.env` instead of renaming the
columns: `COLUMN_HEADER_ENTRY_ID`, `COLUMN_HEADER_OWNER` and
`COLUMN_HEADER_OWNER_OLD` for `entry_map`; `COLUMN_HEADER_OLD` and
`COLUMN_HEADER_NEW` for `owner_map`.

---

## `.env` settings

**Required:** `PARTNER_ID`, `USER_ID`.

**Usually set:**

| Variable | What it does |
|----------|--------------|
| `MODE` | `owner_map`, `entry_map`, or `tag`. Blank = ask each run |
| `INPUT_FILENAME` | Your CSV's name inside `input/`. Blank = ask each run |
| `DRY_RUN` | `true` (default) reports without changing anything. Set `false` to make real changes |
| `TAG`, `TAG_NEW_OWNER` | For `tag` mode. Blank = ask each run |

**Optional:** column header names, `SKIP_OWNER_MISMATCH`, worker count,
retry and timeout settings, user validation, and timezone. Each one is
explained in `.env.example`; the defaults are fine for most runs.

---

## Results

Everything goes in the `output/` folder next to the script, timestamped so
runs never overwrite each other:

```
YYYY-MM-DD-HHMM_reassignOwners_dryRun.csv
YYYY-MM-DD-HHMM_reassignOwners_dryRun_summary.txt
YYYY-MM-DD-HHMM_reassignOwners_dryRun_errors.txt
```

(`dryRun` becomes `live` when `DRY_RUN=false`.)

**The results CSV** has one row per entry:

```
entry_id,entry_name,owner_old,owner_expected,owner_new,success,error,note
```

- `owner_old`: who actually owned the entry before the run
- `owner_expected`: the `owner_old` value from your CSV (`entry_map` only;
  blank otherwise)
- `success`: `success` or `fail`
- `error`: why it failed, if it did
- `note`: things worth a look, such as an owner mismatch or an entry that
  already belonged to the new owner

**The summary** lists the settings used, per-user entry counts, totals, and
(for `entry_map`) how many owner mismatches were found.

**The error log** lists every failed entry, plus a warning line for each
owner mismatch.

---

## Good to know

- **Dry run first, every time.** Then check the results CSV before setting
  `DRY_RUN=false`.
- **`owner_map` moves everything.** Every entry each old user owns is
  reassigned. Use `entry_map` if you only want some of them.
- **Large accounts:** if one user owns 10,000 or more entries, Kaltura may
  cap the list. The script warns you when that happens.
- **Network hiccups** (timeouts, dropped connections) are retried
  automatically (`MAX_NETWORK_RETRIES`, `NETWORK_RETRY_DELAY`,
  `REQUEST_TIMEOUT`). A failed update is retried up to `MAX_RETRIES` times
  before it is logged as a failure and the run moves on.
- **Pasted IDs:** invisible characters, such as zero-width spaces copied
  from web pages, are removed from `.env` values and CSV cells
  automatically. The script tells you if it found any in `.env`.
- Keep `.env` out of version control (`.gitignore` already does this), and
  keep your admin secret out of it entirely.

---

Galen Davis  
Senior Education Technology Specialist  
UC San Diego
