# Kaltura Category Search

Find the Kaltura categories (and MediaSpace channels and galleries) that match what you're looking for, without clicking around the KMC or building API calls by hand. You describe the search in a settings file, run the script, and get a list on screen plus a CSV spreadsheet.

Some things you can ask:

- Every channel whose name contains "chem"
- Every channel nobody has put media in (empty categories)
- Everything underneath a particular category
- Categories created this year, owned by a particular person, or tagged a certain way
- Channels that are open to anyone vs. members only

## What you need

- Python 3 and the packages in `requirements.txt`
- Your Kaltura **Partner ID** and **Administrator Secret** (KMC → Settings → Integration Settings). The script asks for the secret each time you run it and never saves it.

The script signs in with `all:*,disableentitlement` privileges, so MediaSpace channels and galleries show up too. (Without that, an admin sign-in sees almost none of them.)

## Setting up your `.env`

1. Copy `.env.example` to `.env` (`cp .env.example .env`, or duplicate and rename the file).
2. Open `.env` in a text editor and fill in the filters you want. Leave everything else blank.

> **Can't see `.env`?** Files starting with a dot are hidden. On a Mac, press **⌘ + Shift + .** in Finder. On Windows, use File Explorer → **View → Show → Hidden items**.

### Required vs. optional

| Setting | Needed? |
|---|---|
| `PARTNER_ID` | Optional. The script asks if it's blank. |
| Admin Secret | Always asked for when you run the script. |
| Search filters | Optional, but with none filled in the script offers to list **every** category. |
| Everything else | Optional; the defaults are fine. |

### How the filters combine

- Put several values in one filter, separated by commas, and a category matching **any** of them is included (OR). `TAG_ANY=biology,chemistry`
- Fill in several different filters and a category must match **all** of them (AND). `PRIVACY=MEMBERS_ONLY` plus `ENTRIES_MAX=0` means members-only categories that are also empty.

Every filter is explained in `.env.example`. A few are applied after Kaltura answers (the category name, owner, admin tags, moderation and the entry-count filters) because Kaltura can't filter on them; those still work, they just can't shrink the amount Kaltura sends back.

### Examples

All channels:
```
FULL_NAME_BEGINS_WITH=MediaSpace>site>channels
```

Empty channels:
```
FULL_NAME_BEGINS_WITH=MediaSpace>site>channels
ENTRIES_MAX=0
```

Channels with "chem" in the name, newest first:
```
FULL_NAME_BEGINS_WITH=MediaSpace>site>channels
CATEGORY_NAME_CONTAINS=chem
ORDER_BY=CREATED_AT_DESC
```

Everything under category 12345:
```
ANCESTOR_ID=12345
```

## Running it

```
python3 -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
python3 search-categories.py
```

## Results

- The matches are listed on screen (the first 50; change `PRINT_LIMIT`).
- Every match is saved to the `output/` folder next to the script, named like `2026-10-29-1430_category-search.csv`. The CSV has the category ID, full path, entry and member counts, owner, privacy settings, tags, reference ID and dates.
- `output/` is ignored by git, so your data never gets committed.

## Troubleshooting

- **"Could not log in"**: check the Partner ID and use the *Administrator* secret, not the User secret.
- **No results, but you expected some**: look at the "Settings filled in .env" recap the script prints first. A leftover value in an old filter is the usual cause. Filters are combined with AND.
- **Slow or timing out**: raise `REQUEST_TIMEOUT` or lower the number of results with tighter filters.
