# Canvas to MediaSpace Migration Script

Bulk-migrates Canvas media galleries to Kaltura MediaSpace channels. For each course, the script:

1. Locates the course's existing Kaltura category under Canvas (including embedded/InContext subcategories)
2. Creates a new private MediaSpace channel using the SIS course ID as the channel name
3. Assigns the primary instructor as channel **owner**, additional teachers as **managers**, and all other enrollees as **members**
4. Publishes all associated media entries to the new channel
5. Writes three output CSVs: channel mapping, member assignments, and published entries

The script supports configurable concurrency, automatic retry with exponential backoff, resume-on-interrupt, and macOS sleep prevention for long runs.

---

> **Important:** This script was written for UC San Diego's specific Kaltura/Canvas integration under emergency conditions. Column names, category path conventions, and environment settings reflect UCSD's environment. Before using it at another institution, read the [Adapting to Your Environment](#adapting-to-your-environment) section carefully — and consider using an AI coding assistant to help with the changes.

---

## Prerequisites

- Python 3.8+
- A Kaltura admin API secret with `all:*,disableentitlement` privileges
- The following Python packages:

```
pip install KalturaApiClient python-dotenv pytz
```

## Setup

1. Copy `.env.example` to `.env` and fill in your values (see [Configuration](#configuration)).
2. Create an `input/` directory and place your two CSV files there.
3. Run the script.

## Input Files

The script requires two CSVs. Column names are those used at UCSD and will likely need to be changed to match your export format (see [Adapting to Your Environment](#adapting-to-your-environment)).

### Courses CSV

| Column | Description |
|---|---|
| `course_id` | SIS course ID; used as the MediaSpace channel name |
| `canvas_course_id` | Numeric Canvas course ID; used to find the Kaltura category |
| `courseDisplayName` | Human-readable course name (recorded in output only) |
| `primary_instructor_username` | Kaltura username; set as channel owner |

### Users CSV

| Column | Description |
|---|---|
| `username` | Kaltura/MediaSpace username |
| `sis_course_id` | Matches `course_id` in the courses CSV |
| `role` | Canvas role (e.g., `Student`, `Teacher`, `TA`) |

## Configuration

Copy `.env.example` to `.env` and set the following:

### Required

| Variable | Description |
|---|---|
| `PARTNER_ID` | Your Kaltura partner ID |
| `ADMIN_SECRET` | Your Kaltura admin secret |
| `USER_ID` | API user ID |
| `PARENT_ID` | Category ID of your MediaSpace channels root |
| `FULL_NAME_PREFIX` | Full category path prefix for MediaSpace channels (e.g., `MediaSpace>site>channels>`) |
| `CANVAS_CAT_PREFIX` | Full category path prefix for Canvas courses in Kaltura (e.g., `Canvas_Prod>site>channels>`) |
| `MEDIA_SPACE_BASE_URL` | Your MediaSpace base URL including `/channel/` path |
| `COURSES_CSV_FILENAME` | Path to your courses CSV |
| `USERS_CSV_FILENAME` | Path to your users CSV |

### Tuning

| Variable | Default | Description |
|---|---|---|
| `THREAD_COUNT` | `5` | Courses processed concurrently. Raise cautiously (3–10 recommended). |
| `MEMBER_THREADS` | `10` | Member additions and entry publications per course in parallel (5–15 recommended). |
| `SESSION_EXPIRY` | `86400` | Kaltura session TTL in seconds (86400 = 24 hours). |
| `MAX_RETRIES` | `4` | Total API call attempts per operation (1 = no retry). |

## Running

```bash
# Uses COURSES_CSV_FILENAME and USERS_CSV_FILENAME from .env
python3 canvas-to-mediaspace.py

# Or pass CSV paths as arguments (overrides .env)
python3 canvas-to-mediaspace.py input/courses.csv input/users.csv
```

If a run is interrupted, re-run with the same input files to resume automatically. To force a fresh run, delete `output/.run_state.json`.

## Output

Three CSVs are written to `output/` as courses complete (not at the end):

| File | Contents |
|---|---|
| `<timestamp>_channel_mapping.csv` | One row per course: channel ID, URL, Canvas category info |
| `<timestamp>_channel_members.csv` | One row per user: username, Canvas role, MediaSpace role |
| `<timestamp>_published_entries.csv` | One row per entry: entry ID, name, creator, publish status |

---

## Adapting to Your Environment

This script was written quickly for UCSD's specific setup. Here is what is most likely to need changing at another institution.

### 1. `.env` values

At minimum, update:

- `PARTNER_ID`, `ADMIN_SECRET`, `USER_ID` — your credentials
- `PARENT_ID` — find this by browsing your Kaltura category tree; it's the numeric ID of your MediaSpace channels root category
- `FULL_NAME_PREFIX` — the full Kaltura path down to (and including the trailing `>`) your MediaSpace channels root
- `CANVAS_CAT_PREFIX` — same, but for where your Canvas course categories live in Kaltura
- `MEDIA_SPACE_BASE_URL` — your institution's MediaSpace domain
- `PRIVACY_CONTEXT` — may differ from `MediaSpace` depending on your setup

### 2. CSV column names

Your Canvas/SIS exports likely use different column headers. The script reads these column names directly; update `load_courses()` and `load_users()` in the script to match your actual headers.

### 3. Canvas category lookup logic

The function `find_canvas_root_category()` assumes your Canvas courses appear in Kaltura at the path `<CANVAS_CAT_PREFIX><canvas_course_id>` — where `canvas_course_id` is the numeric Canvas course ID. If your institution names these categories differently (e.g., using the SIS course ID, or a different nesting structure), this function will need to be updated.

### Using AI to adapt the script

This is a good candidate for AI-assisted adaptation. Share the script and your `.env.example` with Claude, ChatGPT, or a similar tool, and describe:

- Your CSV column names and a sample row or two
- Your Kaltura category path structure (you can find this in the Kaltura Management Console under Content > Categories)
- Any differences in how your Canvas integration names course categories

The AI can identify exactly what needs to change and produce a working adapted version quickly.

---

## Notes

- The script sets the channel owner via `category.owner` at creation time. A separate `categoryUser.add` call is not made for the owner.
- Entries published in InContext (embedded) subcategories are discovered via `ancestorIdIn` and included alongside main gallery entries. Duplicates across subcategories are deduplicated before publishing.
- The state file (`output/.run_state.json`) stores only completed course IDs, not row data. All CSV output is written and flushed incrementally as each course completes.
