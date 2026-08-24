# upload-entries.py

Bulk-uploads media files from a local `input` folder into Kaltura, creating one
new media entry per file. Metadata you want applied to the whole batch — tags,
owner, collaborators, category memberships, description — is set once in a
`.env` file. Video, audio, and image files can be mixed in the same batch; the
media type is detected automatically from each file's extension.

---

## Quick start

1. **Install the requirements** (once):

   ```bash
   pip install -r requirements.txt
   ```

2. **Create your settings file** by copying the example:

   ```bash
   cp .env.example .env
   ```

   > **Heads-up for Mac users:** the leading dot makes `.env` a *hidden* file, so
   > it won't show up in Finder by default. Press **Cmd + Shift + .** in Finder to
   > reveal hidden files, or just edit `.env` from the Terminal.

3. **Open `.env` and fill in your `PARTNER_ID`.** That's the only required
   value. (Your Admin Secret is *not* stored in the file — the script prompts
   for it each time you run, so it never touches disk.) Set any of the optional
   metadata you want, too.

4. **Drop your media files into the `input` folder.**

5. **Run it:**

   ```bash
   python upload-entries.py
   ```

The script lists what it found, asks for your Admin Secret, uploads each file,
and writes a timestamped CSV report to `output/`.

> **Tip:** the first time, set `DRY_RUN=true` in `.env`. The script will list
> exactly what it *would* upload (and flag any unrecognized files) without
> creating anything in Kaltura. Set it back to `false` when you're ready.

---

## What gets configured in `.env`

Only `PARTNER_ID` is required. Everything else is optional — leave a blank and
it's simply skipped. Full explanations live in the comments of `.env.example`;
the highlights:

| Setting | What it does |
| --- | --- |
| `PARTNER_ID` | **Required.** Your numeric Kaltura account ID. |
| `OWNER_ID` | User ID that should own the new entries. |
| `TAGS` | Comma-separated tags added to every entry. |
| `DESCRIPTION` | Description applied to every entry. |
| `COEDITORS` | Collaborators who may **edit** (KMC "co-editors"). |
| `COPUBLISHERS` | Collaborators who may **publish** (KMC "co-publishers"). |
| `CATEGORY_IDS` / `CATEGORY_NAMES` | Categories to add every entry to (by numeric ID and/or full path). |
| `CONVERSION_PROFILE_ID` | Optional transcoding profile; blank = account default. |
| `MOVE_ON_SUCCESS` | Move uploaded files out of `input/` so re-runs don't duplicate them (on by default). |
| `DRY_RUN` | Preview without uploading. |
| `UPLOAD_CHUNK_MB` | See "Reliability" below. |

---

## How files are handled

- **Media type is auto-detected** from the file extension. Common video, audio,
  and image formats are recognized; anything unrecognized is listed and skipped
  (never uploaded as the wrong type).
- **Each entry is named** after its filename (without the extension).
- **After a successful upload**, the file is moved to `input/_uploaded/` so a
  second run won't upload it again. **Failed** files move to `input/_failed/` so
  you can retry just those by moving them back into `input/`. (Turn this off with
  `MOVE_ON_SUCCESS=false`.)
- **A CSV report** is written to `output/` after every run, with one row per
  file: status, new entry ID, owner, tags, categories, size, average upload
  speed, and any error.
- **Live speed readout.** During a run you'll see the throughput as it happens —
  a live MB/s figure on the progress bar (single-worker) or on each file's
  completion line (parallel), plus an "effective MB/s" total at the end. Handy
  for tuning `UPLOAD_CHUNK_MB` / `MAX_WORKERS` to your connection.

---

## Why this uses "chunked" uploads (the reliability part)

If you've tried to script Kaltura uploads before and watched large videos hang
forever, this is why: the Kaltura Python SDK sends an upload as a **single**
streamed request with a **120-second default timeout**, and silently retries the
*whole file* five times on any timeout. Any file that takes longer than two
minutes to transfer times out and retries endlessly.

This script sidesteps that by splitting each file into small chunks
(`UPLOAD_CHUNK_MB`, default 5 MB) and uploading them one at a time using
Kaltura's resumable upload flow, so no single request ever approaches the
timeout. The full sequence per file is:

1. `uploadToken.add()` — reserve an upload token
2. `uploadToken.upload()` — send the bytes, chunk by chunk
3. `media.add()` — create the entry with the correct media type
4. `media.addContent()` — bind the uploaded bytes to the entry

**If an upload still stalls** on a slow or flaky connection, lower
`UPLOAD_CHUNK_MB` (e.g. to `2`). On a fast, stable connection you can raise it
for fewer round trips.

---

## Requirements

- Python 3.8+
- The packages in `requirements.txt` (`pip install -r requirements.txt`)
- A Kaltura Partner ID and **Administrator** secret (KMC → Settings →
  Integration Settings)
