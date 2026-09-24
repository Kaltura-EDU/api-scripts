"""
This script duplicates Kaltura media entries from one partner ID (PID) to
another using the Kaltura API. It supports selecting entries by tag, by
category ID, or by a list of specific entry IDs. The script copies entries
along with associated metadata, thumbnails, captions (including audio
descriptions), attachments, and cue points. For entries with parent/child
relationships (multi-stream recordings), it preserves the hierarchy.

Key features:
- Optional copying of ASR (auto-generated) captions and attachments.
- Reassigns the destination entry owner, co-editors, and co-publishers as
  specified.
- Adds additional destination tags if configured.
- Generates a CSV report logging the source and destination entry IDs,
  what was copied for each entry, and any errors.

Configuration is managed through a .env file next to this script (see
README and .env.example). For security, the admin secrets are NOT read from
.env; the script prompts for them interactively at runtime so they are never
stored on disk.

This script assumes access to admin-level Kaltura credentials (admin secret
keys) for both the source and destination environments.

Author: Galen Davis
Last updated: September 24, 2026
"""

import csv
import getpass
import os
import time
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv
from KalturaClient import KalturaClient, KalturaConfiguration
from KalturaClient.Plugins.Core import (
    KalturaMediaEntry, KalturaFilterPager, KalturaBaseEntryFilter,
    KalturaSessionType, KalturaUrlResource, KalturaEntryReplacementOptions,
    KalturaFlavorAssetFilter, KalturaSourceType, KalturaThumbAsset,
    KalturaThumbAssetFilter, KalturaMediaType
)
from KalturaClient.Plugins.Attachment import (
    KalturaAttachmentAssetFilter, KalturaAttachmentAsset
)
from KalturaClient.Plugins.Caption import (
    KalturaCaptionAsset, KalturaCaptionAssetFilter
)
from KalturaClient.Plugins.CuePoint import (
    KalturaCuePointFilter, KalturaQuestionType
)
from KalturaClient.Plugins.ThumbCuePoint import KalturaThumbCuePoint
from KalturaClient.Plugins.AdCuePoint import KalturaAdCuePoint
from KalturaClient.Plugins.CodeCuePoint import KalturaCodeCuePoint
from KalturaClient.Plugins.EventCuePoint import KalturaEventCuePoint
from KalturaClient.Plugins.Quiz import (
    KalturaAnswerCuePoint, KalturaQuestionCuePoint, KalturaQuiz,
    KalturaOptionalAnswer
)
from KalturaClient.Plugins.Annotation import KalturaAnnotation
from KalturaClient.Plugins.Transcript import KalturaTranscriptAsset
from KalturaClient.exceptions import KalturaException, KalturaClientException

# Load .env alongside this script, not relying on current working directory
load_dotenv(dotenv_path=Path(__file__).with_name(".env"), override=False)


def _env_bool(key, default="false"):
    return os.getenv(key, default).strip().lower() in ("1", "true", "yes", "y")


def _env_list(key):
    # Comma-delimited .env value -> list of trimmed, non-empty items.
    return [v.strip() for v in os.getenv(key, "").split(",") if v.strip()]


# ---------- Session configuration (from .env) ----------
# NOTE: the admin secrets are intentionally NOT read from .env. They are
# prompted for interactively at runtime (see main) so they are never stored
# on disk. If a partner ID is left blank here, the script prompts for it.
SOURCE_PARTNER_ID = os.getenv("KALTURA_SOURCE_PARTNER_ID", "").strip()
DEST_PARTNER_ID = os.getenv("KALTURA_DEST_PARTNER_ID", "").strip()
SERVICE_URL = os.getenv(
    "KALTURA_SERVICE_URL", "https://www.kaltura.com/"
).strip()
KALTURA_USER = os.getenv("KALTURA_USER", "admin").strip() or "admin"

# ---------- Reliability knobs (from .env) ----------
# Seconds before an individual API request times out.
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "120"))
# Retries for transient network errors before giving up.
MAX_NETWORK_RETRIES = int(os.getenv("MAX_NETWORK_RETRIES", "5"))
# Base seconds between retries (grows linearly: delay × attempt).
NETWORK_RETRY_DELAY = int(os.getenv("NETWORK_RETRY_DELAY", "5"))

# ---------- What to copy (from .env) ----------
COPY_ASR_CAPTIONS = _env_bool("COPY_ASR_CAPTIONS", "true")
# ASR captions are recognized by this text appearing anywhere in the caption
# label (case-insensitive), e.g. "English (auto-generated)". CAPTION_LABEL is
# the pre-2.0 name of this setting and is still honored.
AUTO_GENERATED_LABEL = (
    os.getenv("AUTO_GENERATED_LABEL")
    or os.getenv("CAPTION_LABEL")
    or "(auto-generated)"
).strip()
COPY_ATTACHMENTS = _env_bool("COPY_ATTACHMENTS", "true")

# ---------- Destination settings (from .env) ----------
# Blank DESTINATION_OWNER = owned by KALTURA_USER (the session user).
DESTINATION_OWNER = os.getenv("DESTINATION_OWNER", "").strip()
DESTINATION_COEDITORS = _env_list("DESTINATION_COEDITORS")
DESTINATION_COPUBLISHERS = _env_list("DESTINATION_COPUBLISHERS")
DESTINATION_TAG = ",".join(_env_list("DESTINATION_TAG"))

# Page size for every list call; results are paginated past this.
PAGE_SIZE = 500

OUTPUT_DIR = Path(__file__).with_name("output")
CSV_FILENAME = OUTPUT_DIR / (
    f"{datetime.now().strftime('%Y-%m-%d-%H%M')}_CrossInstanceDuplication.csv"
)

CSV_HEADER = [
    "source entry ID", "title", "source parent entry ID",
    "destination entry ID", "destination parent entry ID",
    "destination owner", "destination coeds", "destination copubs",
    "destination tags", "thumbnails copied", "captions copied",
    "attachments copied", "cue points copied", "status"
]


def debug_timer(start_time, message):
    # Logs the time elapsed since start_time with a debug message.
    elapsed_time = time.time() - start_time
    print(f"⏱ {elapsed_time:.2f}s - {message}")


def call_with_retry(fn, *args, **kwargs):
    """Call fn(*args, **kwargs), retrying with linear backoff on transient
    network failures (timeouts, connection resets). Kaltura raises those as
    KalturaClientException, which is NOT a subclass of KalturaException, so
    it is caught here separately; requests' own errors are covered too. A
    real, well-formed API error (KalturaException) is never retried here —
    callers handle those."""
    for attempt in range(1, MAX_NETWORK_RETRIES + 1):
        try:
            return fn(*args, **kwargs)
        except (
            KalturaClientException,
            requests.exceptions.RequestException,
        ) as exc:
            if attempt == MAX_NETWORK_RETRIES:
                raise
            delay = NETWORK_RETRY_DELAY * attempt
            print(
                f"    [network error: {exc}; retry "
                f"{attempt}/{MAX_NETWORK_RETRIES} in {delay}s]"
            )
            time.sleep(delay)


def list_all(list_fn, list_filter):
    """Page through a Kaltura list call and return every object."""
    pager = KalturaFilterPager()
    pager.pageSize = PAGE_SIZE
    objects = []
    page = 1
    while True:
        pager.pageIndex = page
        result = call_with_retry(list_fn, list_filter, pager)
        if not result.objects:
            break
        objects.extend(result.objects)
        if len(result.objects) < PAGE_SIZE:
            break
        page += 1
    return objects


def get_kaltura_client(partner_id, admin_secret):
    config = KalturaConfiguration(partner_id)
    config.serviceUrl = SERVICE_URL
    config.requestTimeout = REQUEST_TIMEOUT
    client = KalturaClient(config)
    try:
        ks = call_with_retry(
            client.session.start,
            admin_secret, KALTURA_USER, KalturaSessionType.ADMIN, partner_id,
            privileges="all:*,disableentitlement"
        )
    except Exception as e:
        if getattr(e, "code", "") == "START_SESSION_ERROR":
            print(
                "\n❌ Could not log in to Kaltura. Partner ID "
                f"[{partner_id}] and the Admin Secret were not accepted.\n"
                "   Double-check both values — the secret must be the "
                "Administrator secret (not the User secret),\n"
                "   copied exactly from KMC → Settings → "
                "Integration Settings.\n"
            )
        elif type(e).__name__ == "KalturaClientException":
            print(
                "\n❌ Could not reach Kaltura to start a session.\n"
                f"   {e}\n   Check your internet connection and try again.\n"
            )
        else:
            print(f"\n❌ Could not start Kaltura session: {e}\n")
        raise SystemExit(1)
    client.setKs(ks)
    return client


def get_entries(client, method, identifier):
    """Retrieve entries based on tag, category, or entry IDs."""
    if not identifier:
        print("⚠️ No identifier provided. Exiting.")
        return []

    filter = KalturaBaseEntryFilter()
    if method == "tag":
        filter.tagsLike = identifier
        print(f"🔎 Searching entries by tag: {identifier}")
    elif method == "category":
        filter.categoryAncestorIdIn = str(identifier)
        print(f"🔎 Searching entries under Category ID: {identifier}")
    elif method == "entry_ids":
        filter.idIn = ",".join(
            eid.strip() for eid in identifier.split(",") if eid.strip()
        )
        print("🔎 Searching entries by specific IDs.")
    else:
        print("❌ Invalid method.")
        return []

    try:
        return list_all(client.baseEntry.list, filter)
    except Exception as e:
        print(f"❌ API error while retrieving entries: {e}")
        return []


def get_child_entries(client, parent_entry_id):
    """Retrieve all child entries of a given parent entry."""
    child_filter = KalturaBaseEntryFilter()
    child_filter.parentEntryIdEqual = parent_entry_id

    try:
        return list_all(client.baseEntry.list, child_filter)
    except Exception as e:
        print(f"Error retrieving children for entry {parent_entry_id}: {e}")
        return []


def get_source_url(client, entry_id):
    # Retrieve the best flavor asset URL for a given entry by selecting the
    # largest available file.

    flavor_filter = KalturaFlavorAssetFilter()
    flavor_filter.entryIdEqual = entry_id

    try:
        flavors = list_all(client.flavorAsset.list, flavor_filter)
        if not flavors:
            return None

        # Select the flavor with the largest file size
        best_flavor = max(flavors, key=lambda f: f.sizeInBytes or 0)
        return call_with_retry(client.flavorAsset.getUrl, best_flavor.id)
    except Exception as e:
        print(
            f"⚠️ Warning: Could not retrieve flavor asset for entry "
            f"{entry_id}. Error: {e}"
            )
        return None


def cue_type_of(cue):
    return str(getattr(cue.cuePointType, "value", cue.cuePointType))


def get_cuepoints(client, entry_id):
    # Retrieve all cue points for a given entry. Quiz answers are learner
    # submissions, not quiz content, so they are never copied.
    try:
        cuepoint_filter = KalturaCuePointFilter()
        cuepoint_filter.entryIdEqual = entry_id
        cuepoints = []

        for cp in list_all(client.cuePoint.cuePoint.list, cuepoint_filter):
            cue_type_str = cue_type_of(cp)
            if cue_type_str == "quiz.QUIZ_ANSWER":
                continue

            cuepoints.append(cp)

            # Ensure we're retrieving optionalAnswers for quiz questions
            if cue_type_str == "quiz.QUIZ_QUESTION":
                answer_count = (
                    len(cp.optionalAnswers) if (
                        hasattr(cp, "optionalAnswers")
                        and cp.optionalAnswers
                    )
                    else 0
                )
                print(
                    f"Retrieved quiz question {cp.id} with {answer_count} "
                    f"answer options."
                    )

        return cuepoints

    except Exception as e:
        print(f"⚠️ Error retrieving cue points for entry {entry_id}: {e}")
        return []


def create_cue_point_instance(cue):
    """Creates a new cue point instance based on its type."""
    cue_type_map = {
        "annotation.Annotation": KalturaAnnotation,
        "adCuePoint.Ad": KalturaAdCuePoint,
        "answerCuePoint.Answer": KalturaAnswerCuePoint,
        "codeCuePoint.Code": KalturaCodeCuePoint,
        "eventCuePoint.Event": KalturaEventCuePoint,
        "quiz.QUIZ_QUESTION": KalturaQuestionCuePoint,
        "quiz.QUIZ_ANSWER": KalturaAnswerCuePoint,
        "thumbCuePoint.Thumb": KalturaThumbCuePoint,
    }

    cue_type_str = cue_type_of(cue)

    if cue_type_str not in cue_type_map:
        print(
            f"⚠️ Skipping cue point {cue.id}: unknown cuePointType "
            f"{cue_type_str}"
            )
        return None

    cue_class = cue_type_map[cue_type_str]
    new_cue = cue_class()

    # Copy over basic fields
    new_cue.cuePointType = cue.cuePointType
    new_cue.isPublic = (
        cue.isPublic.getValue()
        if hasattr(cue, "isPublic")
        and cue.isPublic
        and hasattr(cue.isPublic, "getValue")
        else False
    )

    return new_cue


def copy_cuepoints(client_source, client_dest, source_entry_id, dest_entry_id):
    # Copy cue points from source to destination entry.

    cuepoints = get_cuepoints(client_source, source_entry_id)
    copied_count = 0  # Track copied cue points

    print(f"Total cue points found: {len(cuepoints)}")

    for cue in cuepoints:
        cue_type_str = cue_type_of(cue)

        new_cue = create_cue_point_instance(cue)
        if not new_cue:
            continue

        # Copy relevant fields
        new_cue.entryId = dest_entry_id
        new_cue.startTime = cue.startTime
        new_cue.userId = cue.userId
        new_cue.tags = cue.tags
        new_cue.systemName = cue.systemName
        new_cue.partnerData = cue.partnerData
        new_cue.partnerSortValue = cue.partnerSortValue
        new_cue.thumbOffset = cue.thumbOffset
        new_cue.description = getattr(cue, "description", "")
        new_cue.title = getattr(cue, "title", "")
        new_cue.subType = getattr(cue, 'subType', None)
        new_cue.forceStop = cue.forceStop
        new_cue.text = getattr(cue, "text", "")
        new_cue.endTime = getattr(cue, "endTime", 0)
        new_cue.duration = getattr(cue, "duration", 0)

        # Ensure quiz questions retain their answers
        if cue_type_str == "quiz.QUIZ_QUESTION":
            new_cue.question = (
                cue.question if cue.question else "[Missing Question]"
            )
            new_cue.questionType = getattr(
                cue, "questionType",
                KalturaQuestionType.MULTIPLE_CHOICE_ANSWER
                )

            # Copy optionalAnswers
            new_cue.optionalAnswers = [
                KalturaOptionalAnswer(
                    isCorrect=answer.isCorrect,
                    key=answer.key,
                    text=getattr(answer, "text", ""),
                    weight=answer.weight
                ) for answer in (getattr(cue, "optionalAnswers", None) or [])
            ]

        try:
            # Add cue point to Kaltura
            added_cue = call_with_retry(
                client_dest.cuePoint.cuePoint.add, new_cue
            )
            copied_count += 1
            print(f"✅ Copied cue point {added_cue.id} (Type: {cue_type_str})")
        except Exception as e:
            print(f"❌ Failed to copy {cue_type_str} {cue.id}: {e}")

    print(f"{copied_count} cuepoints copied for entry {source_entry_id}")
    return copied_count


def is_asr_caption(caption):
    label = caption.label if isinstance(caption.label, str) else ""
    return AUTO_GENERATED_LABEL.lower() in label.lower()


def get_captions(client, entry_id):
    # Retrieve captions for an entry, optionally excluding auto-generated
    # captions. Audio descriptions are caption assets too, and are included.

    caption_filter = KalturaCaptionAssetFilter()
    caption_filter.entryIdEqual = entry_id

    captions = []
    try:
        for caption in list_all(
            client.caption.captionAsset.list, caption_filter
        ):
            # Skip ASR captions if the flag is False
            if not COPY_ASR_CAPTIONS and is_asr_caption(caption):
                print(f"⏭️ Skipping auto-generated caption: {caption.label}")
                continue

            captions.append(caption)
    except Exception as e:
        print(f"Error retrieving captions for entry {entry_id}: {e}")

    return captions


def copy_captions(client_source, client_dest, source_entry_id, dest_entry_id):
    captions = get_captions(client_source, source_entry_id)
    print(f"Retrieved {len(captions)} captions for entry {source_entry_id}")

    if not captions:
        print(f"⚠️ No captions found for entry {source_entry_id}.")
        return 0

    copied_count = 0
    for caption in captions:
        try:
            new_caption = KalturaCaptionAsset()
            new_caption.language = caption.language
            new_caption.format = caption.format
            new_caption.isDefault = caption.isDefault
            new_caption.label = caption.label
            new_caption.displayOnPlayer = caption.displayOnPlayer
            new_caption.accuracy = caption.accuracy
            # usage tells an audio description apart from a caption. Without
            # it, an audio description arrives as an ordinary caption track.
            usage = getattr(caption, "usage", None)
            if usage not in (None, NotImplemented):
                new_caption.usage = usage
            added_caption = call_with_retry(
                client_dest.caption.captionAsset.add,
                dest_entry_id, new_caption
                )
            caption_url = call_with_retry(
                client_source.caption.captionAsset.getUrl, caption.id
            )
            caption_resource = KalturaUrlResource()
            caption_resource.url = caption_url
            call_with_retry(
                client_dest.caption.captionAsset.setContent,
                added_caption.id, caption_resource
                )

            copied_count += 1
            print(
                f"Copied caption {caption.id} ({caption.label}) to new entry "
                f"{dest_entry_id}"
                )
        except Exception as e:
            print(f"❌ Failed to copy caption {caption.id}: {e}")

    print(f"{copied_count} captions copied for entry {source_entry_id}")
    return copied_count


def get_thumbnails(client, entry_id):
    # Retrieve thumbnails for an entry.
    thumb_filter = KalturaThumbAssetFilter()
    thumb_filter.entryIdEqual = entry_id

    try:
        return list_all(client.thumbAsset.list, thumb_filter)
    except Exception as e:
        print(f"⚠️ Error retrieving thumbnails for entry {entry_id}: {e}")
        return []


def copy_thumbnails(
        client_source, client_dest, source_entry_id, dest_entry_id
        ):
    # Copy thumbnails from source to destination entry. The source's default
    # thumbnail (tagged "default_thumb") becomes the destination's default;
    # if none is tagged, the last one copied is used.
    thumbnails = get_thumbnails(client_source, source_entry_id)
    default_ids = [
        t.id for t in thumbnails
        if isinstance(t.tags, str) and "default_thumb" in t.tags
    ]
    copied_count = 0

    for thumb in thumbnails:
        try:
            # Step 1: Add a new thumbnail asset to the destination entry
            new_thumb = KalturaThumbAsset()
            added_thumb = call_with_retry(
                client_dest.thumbAsset.add, dest_entry_id, new_thumb
            )

            # Step 2: Get source thumbnail URL
            thumb_url = call_with_retry(
                client_source.thumbAsset.getUrl, thumb.id
            )

            # Step 3: Set content for the new thumbnail
            thumb_resource = KalturaUrlResource()
            thumb_resource.url = thumb_url
            call_with_retry(
                client_dest.thumbAsset.setContent,
                added_thumb.id, thumb_resource
            )
            copied_count += 1

            # Step 4: Set as default
            if thumb.id in default_ids or not default_ids:
                call_with_retry(
                    client_dest.thumbAsset.setAsDefault, added_thumb.id
                )
                print(
                    f"Copied and set default thumbnail {thumb.id} to new "
                    f"entry {dest_entry_id}"
                    )
            else:
                print(
                    f"Copied thumbnail {thumb.id} to new entry "
                    f"{dest_entry_id}"
                    )
        except Exception as e:
            print(f"❌ Failed to copy thumbnail {thumb.id}: {e}")

    return copied_count


def get_attachments(client, entry_id):
    # Retrieve attachments for an entry.

    attachment_filter = KalturaAttachmentAssetFilter()
    attachment_filter.entryIdEqual = entry_id

    try:
        return list_all(
            client.attachment.attachmentAsset.list, attachment_filter
        )
    except Exception as e:
        print(f"⚠️ Error retrieving attachments for entry {entry_id}: {e}")
        return []


def copy_attachments(
        client_source, client_dest, source_entry_id, dest_entry_id
        ):
    # Copy attachments from source to destination entry, ensuring transcript
    # attachments follow COPY_ASR_CAPTIONS.

    if not COPY_ATTACHMENTS:
        print(
            f"⏭️ Skipping attachment copying for entry {source_entry_id} "
            f"(disabled)."
            )
        return 0

    attachments = get_attachments(client_source, source_entry_id)
    print(f"Retrieved {len(attachments)} attachments for {source_entry_id}")
    if not attachments:
        print(f"⚠️ No attachments found for entry {source_entry_id}.")
        return 0

    copied_count = 0

    for attachment in attachments:
        try:
            # If COPY_ASR_CAPTIONS is False, skip transcript-related
            # attachments
            if (
                not COPY_ASR_CAPTIONS
                and isinstance(attachment, KalturaTranscriptAsset)
            ):
                print(
                    f"⏭️ Skipping transcript attachment {attachment.id} "
                    f"(ASR captions are disabled).")
                continue

            # Step 1: Get the attachment URL
            attachment_url = call_with_retry(
                client_source.attachment.attachmentAsset.getUrl,
                attachment.id
            )
            if not attachment_url:
                print(
                    f"⚠️ Skipping attachment {attachment.id}: Unable to "
                    f"retrieve URL."
                    )
                continue

            # Step 2: Add a new attachment asset to the destination entry
            new_attachment = KalturaAttachmentAsset()
            new_attachment.entryId = dest_entry_id
            new_attachment.title = getattr(
                attachment, "title", "Untitled Attachment"
                )
            new_attachment.tags = getattr(attachment, "tags", "")
            new_attachment.fileExt = getattr(attachment, "fileExt", "")
            new_attachment.format = getattr(attachment, "format", None)
            new_attachment.partnerData = getattr(
                attachment, "partnerData", ""
                )
            new_attachment.description = getattr(
                attachment, "description", ""
                )
            new_attachment.filename = getattr(
                attachment, "filename", "unnamed_file"
                )

            added_attachment = call_with_retry(
                client_dest.attachment.attachmentAsset.add,
                dest_entry_id, new_attachment
            )

            # Step 3: Set the content for the new attachment asset
            attachment_resource = KalturaUrlResource()
            attachment_resource.url = attachment_url
            call_with_retry(
                client_dest.attachment.attachmentAsset.setContent,
                added_attachment.id, attachment_resource
                )

            print(
                f"Copied {type(attachment).__name__} {attachment.id} "
                f"({new_attachment.filename}) to entry {dest_entry_id}"
                  )
            copied_count += 1
        except Exception as e:
            print(f"❌ Failed to copy attachment {attachment.id}: {e}")

    return copied_count


def get_sorted_entries(client, entry_ids):
    # Retrieve entries and sort them so parents are processed before children.
    entries = []
    parent_map = {}

    for entry_id in entry_ids:
        try:
            entry = call_with_retry(client.baseEntry.get, entry_id)
            entries.append(entry)
            if entry.parentEntryId:
                parent_map[entry.id] = entry.parentEntryId
        except Exception as e:
            print(f"⚠️ Error retrieving entry {entry_id}: {e}")

    if not entries:
        print("⚠️ No entries retrieved for sorting. Exiting.")
        return []

    def get_depth(entry):
        # Recursively determines the "depth" of an entry.
        depth = 0
        current_id = entry.id
        while current_id in parent_map:
            depth += 1
            current_id = parent_map[current_id]
        return depth

    # Sort entries based on depth: parents (depth 0) first, children (higher
    # depth) later
    return sorted(entries, key=get_depth)


def merge_tags(tags):
    # Append DESTINATION_TAG to an entry's existing tags.
    tags = tags if isinstance(tags, str) else ""
    return ",".join(t for t in (tags, DESTINATION_TAG) if t)


def apply_destination_users(client_dest, dest_entry_id):
    # Assign co-editors and co-publishers to the new entry.
    if not (DESTINATION_COEDITORS or DESTINATION_COPUBLISHERS):
        return

    try:
        update_entry = KalturaMediaEntry()
        if DESTINATION_COEDITORS:
            update_entry.entitledUsersEdit = ",".join(DESTINATION_COEDITORS)
        if DESTINATION_COPUBLISHERS:
            update_entry.entitledUsersPublish = ",".join(
                DESTINATION_COPUBLISHERS
            )

        call_with_retry(
            client_dest.baseEntry.update, dest_entry_id, update_entry
        )
        print(f"Assigned coeditors: {DESTINATION_COEDITORS}")
        print(f"Assigned copublishers: {DESTINATION_COPUBLISHERS}")
    except Exception as e:
        print(
            f"⚠️ Failed to assign coeditors/copublishers for "
            f"{dest_entry_id}: {e}"
            )


def copy_image_entry(client_source, client_dest, source_entry, counts,
                     entry_id_mapping):
    new_entry = KalturaMediaEntry()
    new_entry.name = source_entry.name
    new_entry.description = source_entry.description
    new_entry.tags = merge_tags(source_entry.tags)
    new_entry.mediaType = KalturaMediaType(KalturaMediaType.IMAGE)
    new_entry.sourceType = KalturaSourceType.URL
    if DESTINATION_OWNER:
        new_entry.userId = DESTINATION_OWNER

    copied_entry = call_with_retry(client_dest.media.add, new_entry)
    entry_id_mapping[source_entry.id] = copied_entry.id
    print(
        f"✅ Created new image entry {copied_entry.id} from "
        f"{source_entry.id}"
          )

    if source_entry.downloadUrl:
        resource = KalturaUrlResource()
        resource.url = source_entry.downloadUrl
        conversion_profile_id = 0
        advanced_options = KalturaEntryReplacementOptions()

        try:
            call_with_retry(
                client_dest.media.updateContent,
                copied_entry.id, resource, conversion_profile_id,
                advanced_options
            )
            print(
                f"✅ Image content downloaded from {source_entry.id} and "
                f"uploaded to new entry {copied_entry.id}"
                )
        except Exception as e:
            print(
                f"❌ ERROR: Failed to update content for image "
                f"{source_entry.id}: {e}"
                )
    else:
        print(
            f"⚠️ WARNING: No download URL found for image "
            f"{source_entry.id}. Cannot copy content."
            )

    apply_destination_users(client_dest, copied_entry.id)

    counts["attachments"] = copy_attachments(
        client_source, client_dest, source_entry.id, copied_entry.id
    )
    return copied_entry


def copy_entry(client_source, client_dest, entry, dest_parent_id, counts,
               entry_id_mapping):
    start_time = time.time()

    debug_timer(start_time, "Started entry duplication process.")

    source_entry = call_with_retry(client_source.baseEntry.get, entry.id)

    # Special case for images
    media_type = getattr(source_entry, "mediaType", None)
    if getattr(media_type, "value", None) == KalturaMediaType.IMAGE:
        copied_entry = copy_image_entry(
            client_source, client_dest, source_entry, counts, entry_id_mapping
        )
        debug_timer(start_time, "Completed image copying.")
        return copied_entry

    # Proceed with normal video/audio duplication
    new_entry = KalturaMediaEntry()
    new_entry.name = entry.name
    new_entry.description = entry.description
    new_entry.tags = merge_tags(entry.tags)
    if DESTINATION_OWNER:
        new_entry.userId = DESTINATION_OWNER
    new_entry.mediaType = media_type
    new_entry.sourceType = KalturaSourceType.FILE
    new_entry.blockAutoTranscript = True

    # If this is a child entry, attach it to its copied parent
    if dest_parent_id:
        new_entry.parentEntryId = dest_parent_id

    debug_timer(start_time, "Finished preparing new entry metadata.")

    # Check if the source entry is a quiz before duplicating
    capabilities = getattr(source_entry, "capabilities", "")
    is_quiz = isinstance(capabilities, str) and "quiz.quiz" in capabilities

    # Copy entry
    copied_entry = call_with_retry(client_dest.baseEntry.add, new_entry)
    entry_id_mapping[entry.id] = copied_entry.id  # Store mapping
    debug_timer(start_time, "Completed baseEntry.add() for new entry.")

    apply_destination_users(client_dest, copied_entry.id)

    source_url = get_source_url(client_source, entry.id)

    if source_url:
        resource = KalturaUrlResource()
        resource.url = source_url
        call_with_retry(
            client_dest.baseEntry.updateContent, copied_entry.id, resource
        )
        debug_timer(start_time, "Completed updateContent() for new entry.")
    else:
        print(
            f"⚠️ WARNING: No flavor found for {entry.id}. The new entry "
            f"{copied_entry.id} has no media."
            )

    if is_quiz:
        try:
            source_quiz = call_with_retry(
                client_source.quiz.quiz.get, entry.id
            )
            quiz = KalturaQuiz()
            quiz.allowAnswerUpdate = source_quiz.allowAnswerUpdate
            quiz.allowDownload = source_quiz.allowDownload
            quiz.attemptsAllowed = source_quiz.attemptsAllowed
            quiz.scoreType = source_quiz.scoreType
            quiz.showCorrectAfterSubmission = (
                source_quiz.showCorrectAfterSubmission
            )
            quiz.showGradeAfterSubmission = (
                source_quiz.showGradeAfterSubmission
            )
            quiz.uiAttributes = source_quiz.uiAttributes

            call_with_retry(client_dest.quiz.quiz.add, copied_entry.id, quiz)
            debug_timer(
                start_time,
                "Converted new entry into a quiz with original settings."
                )
        except (KalturaException, KalturaClientException) as e:
            print(
                f"❌ Failed to copy quiz settings for entry "
                f"{copied_entry.id}: {e}"
                )

    counts["thumbnails"] = copy_thumbnails(
        client_source, client_dest, entry.id, copied_entry.id
    )
    debug_timer(start_time, "Completed thumbnail copying.")

    counts["captions"] = copy_captions(
        client_source, client_dest, entry.id, copied_entry.id
    )
    debug_timer(start_time, "Completed caption copying.")

    counts["attachments"] = copy_attachments(
        client_source, client_dest, entry.id, copied_entry.id
    )
    debug_timer(start_time, "Completed attachment copying.")

    counts["cuepoints"] = copy_cuepoints(
        client_source, client_dest, entry.id, copied_entry.id
    )
    debug_timer(start_time, "Completed cuepoint copying.")

    return copied_entry


def duplicate_entry(client_source, client_dest, entry, csv_rows,
                    entry_id_mapping, dest_parent_id=""):
    """Copy one entry (and, recursively, its children), logging a CSV row
    for each. One failed entry is logged and does not stop the run."""
    if entry.id in entry_id_mapping:
        print(
            f"⏭️ {entry.id} was already copied (as a child of its parent) "
            f"to {entry_id_mapping[entry.id]}."
            )
        return

    counts = {"thumbnails": 0, "captions": 0, "attachments": 0,
              "cuepoints": 0}
    try:
        copied_entry = copy_entry(
            client_source, client_dest, entry, dest_parent_id, counts,
            entry_id_mapping
        )
        status = "ok"
    except Exception as e:
        print(f"❌ ERROR: Failed to copy entry {entry.id}: {e}")
        copied_entry = None
        status = f"error: {e}"

    csv_rows.append([
        entry.id,
        entry.name,
        entry.parentEntryId if entry.parentEntryId else "",
        # A partly-copied entry may exist at the destination even on error
        entry_id_mapping.get(entry.id, ""),
        dest_parent_id,
        DESTINATION_OWNER or KALTURA_USER,
        ",".join(DESTINATION_COEDITORS),
        ",".join(DESTINATION_COPUBLISHERS),
        DESTINATION_TAG,
        counts["thumbnails"],
        counts["captions"],
        counts["attachments"],
        counts["cuepoints"],
        status
    ])

    if not copied_entry:
        return

    # Copy child entries (Multistream support). Children are hidden from
    # normal list results, so they are looked up per parent here.
    for child in get_child_entries(client_source, entry.id):
        print(f"👶 Found child entry {child.id}, copying...")
        duplicate_entry(
            client_source, client_dest, child, csv_rows, entry_id_mapping,
            dest_parent_id=copied_entry.id
        )


def write_to_csv(csv_rows):
    OUTPUT_DIR.mkdir(exist_ok=True)
    with open(CSV_FILENAME, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(CSV_HEADER)
        writer.writerows(csv_rows)


def prompt_partner_id(value, label):
    if value:
        print(f"{label} Partner ID (from .env): {value}")
        return value
    return input(f"Enter the {label} Partner ID: ").strip()


def prompt_admin_secret(label):
    secret = getpass.getpass(
        f"Enter the {label} Admin Secret (input is hidden): "
    ).strip()
    if not secret:
        print("Error: Admin secret cannot be empty.")
        raise SystemExit(1)
    return secret


def main():
    for key in ("ADMIN_SECRET", "SOURCE_ADMIN_SECRET", "DEST_ADMIN_SECRET",
                "KALTURA_SOURCE_ADMIN_SECRET", "KALTURA_DEST_ADMIN_SECRET"):
        if os.getenv(key):
            print(
                f"⚠️ {key} is set in your .env or environment and is being "
                "ignored. Admin secrets are always typed in at runtime; "
                "please delete it from .env."
            )

    # Prompt for source PID and Admin Secret
    source_pid = prompt_partner_id(SOURCE_PARTNER_ID, "Source")
    source_admin_secret = prompt_admin_secret("Source")
    client_source = get_kaltura_client(source_pid, source_admin_secret)

    # Prompt for destination PID and Admin Secret
    dest_pid = prompt_partner_id(DEST_PARTNER_ID, "Destination")
    dest_admin_secret = prompt_admin_secret("Destination")
    client_dest = get_kaltura_client(dest_pid, dest_admin_secret)

    # Ask the user how they want to select entries
    print("\nWhat do you want to use to duplicate entries?")
    print("[1] A tag")
    print("[2] A category ID")
    print("[3] A comma-delimited list of entry IDs")

    method_mapping = {
        "1": ("tag", "Enter the tag name: "),
        "2": ("category", "Enter the category ID: "),
        "3": ("entry_ids", "Enter the entry IDs (comma-separated): ")
    }

    method_choice = input(
        "Enter the number corresponding to your choice: "
        ).strip()

    # Validate user input and unpack method and prompt text
    if method_choice not in method_mapping:
        print("Error: Invalid choice. Please enter 1, 2, or 3.")
        return

    method, prompt_text = method_mapping[method_choice]
    identifier = input(prompt_text).strip()

    # Ensure an identifier was provided
    if not identifier:
        print("Error: You must provide a valid identifier.")
        return

    start_time = time.time()

    entries = get_entries(client_source, method, identifier)
    if not entries:
        print("⚠️ No entries matched your search. Exiting script.")
        return

    csv_rows = []
    entry_id_mapping = {}

    print(f"✅ {len(entries)} entries found.")
    print("🤰 Sorting entries by parent-child hierarchy...")
    sorted_entries = get_sorted_entries(
        client_source, [entry.id for entry in entries]
    )
    print(f"✅ Sorted {len(sorted_entries)} entries.")
    if not sorted_entries:
        return

    confirm = input(
        f"\nCopy {len(sorted_entries)} entries (plus any child entries) "
        f"from PID {source_pid} to PID {dest_pid}? [y/N]: "
    ).strip().lower()
    if confirm not in ("y", "yes"):
        print("Cancelled. Nothing was copied.")
        return

    for idx, entry in enumerate(sorted_entries, start=1):
        entry_start = time.time()  # Track per-entry time

        print("\n" + "-" * 80)
        print(
            f"🚀 Processing {idx}/{len(sorted_entries)} | "
            f"entry id: {entry.id} | title: {entry.name}"
        )
        print("-" * 80 + "\n")

        # A child listed alongside its parent is copied under the parent;
        # a child listed on its own is copied as a standalone entry.
        dest_parent_id = (
            entry_id_mapping.get(entry.parentEntryId, "")
            if entry.parentEntryId else ""
        )
        duplicate_entry(
            client_source, client_dest, entry, csv_rows, entry_id_mapping,
            dest_parent_id=dest_parent_id
        )
        print(
            f"⏱ {time.time() - entry_start:.2f}s - Completed entry {entry.id}"
            )

    write_to_csv(csv_rows)
    failed = sum(1 for row in csv_rows if row[-1] != "ok")
    print(
        f"\n📄 CSV log saved as {CSV_FILENAME}\n"
        f"✅ {len(csv_rows) - failed} entries copied, {failed} failed."
    )
    print(f"⏱ {time.time() - start_time:.2f}s - Script execution complete.")


if __name__ == "__main__":
    main()
