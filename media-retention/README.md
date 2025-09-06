

# Media Retention Toolkit

This folder contains three Python scripts designed to help analyze and act on media retention policies in a Kaltura environment. Together, they allow you to:

1. Generate a list of candidate entries subject to retention rules.
2. Enrich those entries with storage impact calculations.
3. Summarize the results in a human-friendly report.

All scripts share a common `.env` configuration file for credentials, input/output filenames, and performance knobs.

## Media Retention Policy

The scripts are designed with the following media retention policy in mind:

1. Any media entries created between 2 and 4 years ago that have not been watched in two years or more will have all of its flavors deleted except for the source flavor.
2. Any media entries created more than 4 years ago that have not been watched in 4 or more years will be deleted.

---

## Prerequisites

- Python 3.10+ (tested with 3.12)
- A Kaltura API account with sufficient permissions

Set up a `.env` file by copying `.env.example` and filling in values for your environment.

```
cp .env.example .env
```

---

## Scripts

### 1. `media-retention-report.py`

Generates a CSV of candidate entries subject to 2-year, 4-year, or non-ready retention rules.  
- Input: one or more KMC export files (all, quizzes, YouTube)  
- Output: candidates CSV with basic metadata  
- Notable `.env` variables:  
  - `KMC_EXPORT_ALL_FILENAME`, `KMC_EXPORT_QUIZ_FILENAME`, `KMC_EXPORT_YOUTUBE_FILENAME`  
  - `REPORT_DATE`  
  - `OUTPUT_CSV_FILENAME`, `OUTPUT_TIMESTAMP`  
  - `REPORT_LOOKUP_WORKERS` (performance)

---

### 2. `include-flavor-calculations.py`

Enriches the candidates CSV with flavor-level detail:  
- Adds `number_of_flavors` and `bytes_saved` columns  
- Uses `flavorAsset.list` to sum storage impact  
- Applies policy-specific rules:  
  - 4-year and non-ready entries: sum all flavors  
  - 2-year entries: sum all but the source flavor  
- Notable `.env` variables:  
  - `FLAVOR_INPUT_FILENAME`, `FLAVOR_OUTPUT_FILENAME`  
  - `FLAVOR_LOOKUP_WORKERS` (performance)

---

### 3. `retention-summary.py`

Summarizes candidate data into high-level counts and totals.  
- Breaks down by policy (2-year, 4-year, non-ready, total)  
- Reports counts of entries, unique users, media types/subtypes, and total duration  
- If `bytes_saved` column is present (from flavor enrichment), also reports:  
  - Bytes, MB, GB, TB saved  
- Notable `.env` variables:  
  - `SUMMARY_INPUT_FILENAME`, `SUMMARY_OUTPUT_FILENAME`, `SUMMARY_OUTPUT_TIMESTAMP`  
  - `LENGTH_EQUALS`, `LENGTH_LESS_THAN`, `LENGTH_GREATER_THAN`

---

## Performance Knobs

Shared knobs in `.env` (used by long-running scripts):  
- `HTTP_RETRIES`, `THROTTLE_MS`  
- `PROGRESS_EVERY_SEC`, `PROGRESS_STYLE`  
- `PREVENT_COMPUTER_SLEEP` (macOS only; uses `caffeinate`)  

Script-specific knobs:  
- `REPORT_LOOKUP_WORKERS` → `media-retention-report.py`  
- `FLAVOR_LOOKUP_WORKERS` → `include-flavor-calculations.py`  
- `FLUSH_EVERY`, `PROGRESS_VERBOSE_RETRY` → report script only

---

## Workflow

Typical workflow for a retention audit:

1. Export 3 CSVs from the KMC: 
    1. a report of all entries created at least 2 years ago
    2. a report of all entries created at least 2 years ago that are YouTube entries
    3. a report of all entries created at least 2 years ago that are quizzes
2. Fill out necessary variables in .env and run `media-retention-report.py` to generate a report of media retention policy candidates.  
3. Fill out necessary variables in .env and run `include-flavor-calculations.py` to enrich with flavor/storage data.  
4. Fill out necessary variables in .env and run `retention-summary.py` to produce a summary table for decision makers.

---

## Notes

- Long runs can take hours on large repositories (~800k entries may take ~12 hours). This is why these scripts are separated at present.  
- Progress meters give real-time feedback, including ETA and API call rates.  
- `PREVENT_COMPUTER_SLEEP=True` is recommended to ensure long runs don't stop because your computer goes to sleep.  