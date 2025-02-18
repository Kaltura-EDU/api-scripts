# Short Description

This script copies entries from one Kaltura PID to another Kaltura PID. It does this by manually copying and storing all of the relevant data from the original and creating a new entry at the destination PID using all that data. It shows its progress on the command line and generates a .csv indicating source and destination entry IDs. 

# Caveat

This script has been successfuly tested in our production environment, but it may not account for the variety of configurations out there.

I've attempted to account for a variety of use cases, but I may have missed something. Don't hesitate to reach out to me at gbdavis@ucsd.edu to let me know if I need to add or or change anything. My use cases so far have included:

- "normal" video entries
- quizzes
- chapters
- thumbnails
- hotspots
- audio-only entries
- multi-stream entries
- attachments
- captions
- ASR transcripts (technically a type of attachment)
- images

# How to Run This Script

1. Download the Python file (duplicate-entries-across-pids.py) to your computer, ideally in a folder to which it's easy to navigate.
2. Set values for any configurable variables (described in more detail below).
3. Create and/or activate your virtual environment. You can find [instructions on this](https://github.com/Kaltura-EDU/api-scripts) in the main Readme in this github site.
4. Open a command line application (e.g. Command Prompt or Terminal) and navigate to the folder.
5. Type `pip install -r requirements.txt` and allow the script to install the needed modules into your virtual environment.
6. Type `python3 duplicate-entries-across-pids.py` (you only need to do this once).
7. Follow the onscreen prompts.

# Configurable Variables

- **COPY_QUIZ_ANSWERS** (Default: False)
  - Set to True if "quiz answer" cue points should be copied to the new instance; False to exclude them. (See the note below for more information.)
- **COPY_ASR_CAPTIONS** (Default: False)
  - Set to True to copy auto-generated captions to the destination entry as well as the transcript and accompanying .json file (which are treated as "attachments"). Leave as False to skip ASR captions.
  - If you set it to False, be sure to update the value of `CAPTION_LABEL` so the script knows what caption label to look out for (and *not* copy over).
- **CAPTION_LABEL** (Default: "English: auto-generated)")
  - If you intend to exclude ASR captions from the entry duplication (i.e. `COPY_ASR_CAPTIONS = False`), ensure that the value of this variable matches the label for your ASR captions.
- **COPY_ATTACHMENTS** (Default: True)
  - Set to false if you do not want the source attachments to be copied to the destination entry.
  - As noted above, remember that if `COPY_ASR_CAPTIONS = False`, the ASR transcript and .json file will not copy to the new entry (despite their being a subtype of an attachment). 
- **DESTINATION_TAG** (Default: "duplicated_entry")
  - The script will tag the destination entries with the value of this variable.
- **DESTINATION_OWNER** (Default: "admin")
  - Assigns the entry at the destination to the owner specified.
  - Username does not have to be a valid user.
- **DESTINATION_COEDITORS** (Default: "")
  - Adds co-editors to the destination entry. None by default (empty).
  - Enter a comma-delimited list within the quotation marks (e.g. `DESTINATION_COEDITORS = "user1, user2, user3"`).
  - Usernames do not have to be valid users. 
 - **DESTINATION_COPUBLISHERS** (Default: "")
  - Adds co-publishers to the destination entry. None by default (empty).
  - Enter a comma-delimited list within the quotation marks (e.g. `DESTINATION_COPUBLISHERS = "user1, user2, user3"`).
  - Usernames do not have to be valid users. 

# Additional Notes

- **If you elect not to copy ASR captions, the related .txt and .json attachments will also not copy.** At least in our instance, when ASR captions are generated for an entry, it adds two attachments: a .txt file and a .json file. These appear to be exclusively related to the ASR captions. If you set `COPY_ASR_CAPTIONS` to True, then those attachment files WILL show up in the destination. 
- **This script does not copy quiz answers by default.** This will attempt to copy cuePoints of every other kind to the destination PID, however. ("cuePoints" can be one of the following: a hotspot, an advertisement, a quiz answer (submission), a quiz question, a code point (for running little scripts at a certain point in a video), an event, or a chapter/slide.) Personally, I find it unlikely that, when copying quizzes to another instance, you want to preserve previous submissions to the quiz. However, if you DO want to copy quiz answers for some reason, you'll need to change the value of COPY_QUIZ_ANSWERS to True. Doing so MAY work, but be aware of two things: 1) User IDs in the destination PID might not exist, causing failures (i.e. each quiz answer must have a valid userId at the destinatiopn PID); and 2) analytics might be inaccurate if all quiz answers are assigned to a single user (e.g. "admin"). If you need to copy quiz answers, be aware of these limitations. Testing is recommended before using it in production.
- **Ad, Event, and Code cuePoints haven't been as thoroughly tested as the others.** We've never really used these features at UCSD (at least not yet) so I don't feel confident that my API-created cuePoints of this type were "real" enough. 
- **With the exception of image entries, the script selects the largest flavor (by file size) from the source entry to create the destination entry.** For most entries, the original source flavor can be identified because its `flavorParamsId` is 0. However, multistream entries don't always follow this pattern, making it less reliable to determine the true source. Instead, this script selects the flavor with the highest `flavorAsset.sizeInBytes` value. This approach generally works well, but be aware that certain transcoding profiles might result in a flavor that is larger than the original source file, which could affect the copy. Just something to keep in mind! (Image entries don't have any flavors, so instead its `downloadUrl` is used to retrieve the source.)
- **Children may process more slowly than their parents.** If you’re copying a multistream entry (e.g., a dual-stream recording), be aware that child entries (such as a 1080p webcam video) may take longer to process than the parent entry (e.g., a static screen share). While the script correctly duplicates both parent and child entries, they may not become "ready" on the front end at the same time. (So if you log into the destination front end and only see the parent stream, you may just need to wait longer.) You can confirm successful copying by running a `baseEntry.list` API call with `parentEntryIdEqual` set to the new parent entry’s ID. Be patient! 
- **If downloads have been enabled for the source entry, this won't be the case for the destination entry.** If a user has gone to the "downloads" tab and offered up flavors for front-end viewers to download, this won't be replicated in the destination. I haven't figured this one out yet!

Galen Davis, Senior Education Technology Specialist (gbdavis@ucsd.edu)
UC San Diego
13 Feburary 2025
