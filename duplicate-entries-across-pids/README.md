# Duplicate Entries Across PIDs

Copies videos (and other media) from one Kaltura partner ID (PID) to another: for example, from a test instance to production, or from one campus instance to another. It copies each entry's media along with its thumbnails, captions and audio descriptions, attachments, chapters and slides, hotspots, and quiz questions. Multi-stream recordings keep their parent/child structure.

You choose which entries to copy by **tag**, by **category ID**, or by a **list of entry IDs**. The script shows its progress as it works and saves a spreadsheet (CSV) that maps each original entry to its new copy.

# What gets copied

This script has been tested in our production environment with:

- "normal" video entries
- audio-only entries
- images
- multi-stream entries (parent and child streams)
- quizzes (questions and settings; see the note on quiz answers below)
- chapters and slides
- thumbnails
- hotspots
- captions, including audio descriptions
- ASR (machine) captions and their transcripts (optional)
- attachments

It may not cover every configuration out there. Don't hesitate to reach out to me at gbdavis@ucsd.edu if something needs adding or changing.

# How to run this script

1. Download this folder to your computer, somewhere that's easy to find.
2. Set up your `.env` file (see the next section).
3. Create and/or activate your virtual environment. You can find [instructions on this](https://github.com/Kaltura-EDU/api-scripts) in the main README for this GitHub repository.
4. Open a command-line application (e.g. Terminal on a Mac or Command Prompt on Windows) and navigate to this folder.
5. Type `pip install -r requirements.txt` to install what the script needs. You only need to do this once.
6. Type `python3 duplicate-entries-across-pids.py` to run the script.
7. Follow the onscreen prompts. The script asks for the **Administrator secret** of both partners (what you type stays hidden), how you want to choose entries, and then asks you to confirm before it copies anything.

# Setting up your `.env`

The `.env` file holds your settings, so you never have to edit the Python file.

1. Make a copy of `.env.example` and name it `.env`. In a terminal you can type `cp .env.example .env`, or duplicate and rename the file in Finder or File Explorer.
2. Open `.env` in any text editor and fill in the values. Each setting has a comment explaining it.

**Can't see the file?** Files whose names start with a dot are hidden by default. On a Mac, press **⌘ + Shift + .** in Finder to show them. On Windows, open File Explorer and choose **View → Show → Hidden items**.

**Your admin secrets never go in `.env`.** The script asks for them each time it runs, so they're never saved on your computer.

## Required settings

None. If you leave `KALTURA_SOURCE_PARTNER_ID` and `KALTURA_DEST_PARTNER_ID` blank, the script asks for them when it runs. Filling them in just saves typing.

## Optional settings

| Setting | Default | What it does |
|---|---|---|
| `KALTURA_SOURCE_PARTNER_ID` | *(asks you)* | The partner to copy **from**. |
| `KALTURA_DEST_PARTNER_ID` | *(asks you)* | The partner to copy **to**. |
| `KALTURA_USER` | `admin` | The user ID recorded in Kaltura's logs. New entries are owned by this user unless `DESTINATION_OWNER` is set. |
| `COPY_ASR_CAPTIONS` | `true` | Set to `false` to skip machine (ASR) captions and their transcript attachments. |
| `AUTO_GENERATED_LABEL` | `(auto-generated)` | How the script spots machine captions: any caption whose label contains this text. Only matters when `COPY_ASR_CAPTIONS=false`. |
| `COPY_ATTACHMENTS` | `true` | Set to `false` to skip attachments. |
| `DESTINATION_OWNER` | *(blank)* | Owner of the new entries. The user doesn't have to exist in the destination partner. |
| `DESTINATION_COEDITORS` | *(blank)* | Co-editors for the new entries, comma-separated (e.g. `user1,user2`). |
| `DESTINATION_COPUBLISHERS` | *(blank)* | Co-publishers for the new entries, comma-separated. |
| `DESTINATION_TAG` | `duplicated_entry` | Tag(s) added to every new entry, on top of its existing tags. |
| `REQUEST_TIMEOUT`, `MAX_NETWORK_RETRIES`, `NETWORK_RETRY_DELAY` | `120`, `5`, `5` | How patiently the script handles a slow or dropped connection. The defaults are fine for most people. |

# The CSV report

Each run saves a report in the `output` folder next to the script, named like `2026-09-24-1405_CrossInstanceDuplication.csv`. It has one row per copied entry, including child entries, with:

- the source and destination entry IDs (and parent IDs for multi-stream entries)
- the owner, co-editors, co-publishers, and tags applied
- how many thumbnails, captions, attachments, and cue points were copied
- a **status** column: `ok`, or the error message if that entry failed

If one entry fails, the script logs it and moves on to the next. Check the status column when the run finishes.

# Additional notes

- **If you choose not to copy ASR captions, their transcript attachments aren't copied either.** When Kaltura generates machine captions, it also adds a transcript (.txt) and a .json file as attachments. With `COPY_ASR_CAPTIONS=false`, those are skipped too.
- **Quiz answers (learner submissions) are never copied.** Quiz questions and quiz settings are copied, but previous learners' answers are not. When copying a quiz to another instance, you almost always want it to start fresh.
- **Ad, Event, and Code cue points haven't been tested as thoroughly as the others.** We haven't used these features at UCSD yet.
- **The script copies the largest flavor of each video.** Usually that's the original upload, but some transcoding profiles can produce a flavor larger than the original. (Images have no flavors, so the original image file is copied instead.)
- **Child entries may take longer to finish processing than their parent.** When copying a multi-stream entry, the destination may show only the main stream at first. Give the other streams time to finish processing. The CSV lists every child's new entry ID.
- **Download settings aren't copied.** If users could download flavors of the original entry, you'll need to turn that on again for the new entry.
- **Large result sets are fine.** The script pages through results, so searches that return more than 500 entries still copy everything.

---

Galen Davis  
Senior Education Technology Specialist  
gbdavis@ucsd.edu  
UC San Diego  

*and*

Andy Clark  
Systems Administrator, Learning Systems  
Baylor University  

*Last updated 2026-09-24*
