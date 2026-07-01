# clone-quizzes.py

Clones Kaltura quiz entries along with their quiz question cue points. Provide a list of entry IDs in `.env`, run the script, and each quiz will be duplicated — including all of its questions — as a new entry. An optional tag can be applied to every cloned entry. Results are written to a timestamped CSV in the `output/` subfolder.

## What It Does

- Clones each quiz entry via `baseEntry.clone()`
- Identifies all `quiz.QUIZ_QUESTION` cue points on the original entry and clones them to the new entry via `cuePoint.clone()`
- Optionally adds a tag to each cloned entry
- Prints a per-entry summary to the terminal
- Writes a timestamped CSV to `output/` listing the original entry ID, new entry ID, title, and number of questions cloned

## Getting Started

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Set up configuration

Copy `.env.example` to `.env` and fill in your values:

```bash
cp .env.example .env
```

### 3. Run the script

```bash
python3 clone-quizzes.py
```

When prompted, enter your Kaltura admin secret. The input will not be visible as you type.

## Configuration

| Variable | Required | Description |
| :--- | :--- | :--- |
| `PARTNER_ID` | Yes | Your Kaltura partner ID (integer) |
| `USER_ID` | No | Kaltura user ID for the session (typically your email address) |
| `SERVICE_URL` | No | Kaltura API URL (default: `https://www.kaltura.com/`) |
| `PRIVILEGES` | No | Session privileges (default: `all:*,disableentitlement`) |
| `ENTRY_IDS` | Yes | Comma-separated list of quiz entry IDs to clone |
| `TAG` | No | A tag to add to each cloned entry (leave blank to skip) |

Your admin secret is **not** stored in `.env` — you will be prompted for it securely at runtime.

## Author

Galen Davis  
Senior Education Technology Specialist, UC San Diego
