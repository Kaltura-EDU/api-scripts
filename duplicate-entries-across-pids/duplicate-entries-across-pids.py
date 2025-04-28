"""
This script duplicates Kaltura media entries from one partner ID (PID) to
another using the Kaltura API. It supports selecting entries by tag, by
category ID, or by a list of specific entry IDs. The script copies entries
along with associated metadata, thumbnails, captions, attachments, and cue
points. For entries with parent/child relationships (multi-stream recordings),
it preserves the hierarchy.

Key features:
- Optional copying of quiz data (questions/answers) and ASR (auto-generated)
  captions.
- Reassigns the destination entry owner, co-editors, and co-publishers as
  specified.
- Adds additional destination tags if configured.
- Generates a CSV report logging the source and destination entry IDs.

This script assumes access to admin-level Kaltura credentials (admin secret
keys) for both the source and destination environments.

Author: Galen Davis
Last updated: April 26, 2025
"""

import csv
import time
from datetime import datetime
from KalturaClient import KalturaClient, KalturaConfiguration
from KalturaClient.Plugins.Core import (
    KalturaMediaEntry, KalturaFilterPager, KalturaBaseEntryFilter,
    KalturaSessionType, KalturaUrlResource, KalturaEntryReplacementOptions,
    KalturaFlavorAssetFilter, KalturaSourceType, KalturaThumbAsset,
    KalturaThumbAssetFilter
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
from KalturaClient.exceptions import KalturaException

# ---- CONFIGURABLE VARIABLES ----
SOURCE_PID = ""
SOURCE_ADMIN_SECRET = ""
DEST_PID = ""
DEST_ADMIN_SECRET = ""
COPY_QUIZ_ANSWERS = False
COPY_ASR_CAPTIONS = True
CAPTION_LABEL = "English (auto-generated)"
COPY_ATTACHMENTS = True
DESTINATION_OWNER = ""
DESTINATION_COEDITORS = ""
DESTINATION_COPUBLISHERS = ""
DESTINATION_TAG = ""
# -- END CONFIGURABLE VARIABLES --

CSV_FILENAME = (
    f"CrossInstanceDuplication_{datetime.now().strftime('%Y%m%d%H%M')}.csv"
)


def debug_timer(start_time, message):
    # Logs the time elapsed since start_time with a debug message.
    elapsed_time = time.time() - start_time
    print(f"⏱ {elapsed_time:.2f}s - {message}")


def get_kaltura_client(partner_id, admin_secret):
    config = KalturaConfiguration(partner_id)
    config.serviceUrl = "https://www.kaltura.com/"
    client = KalturaClient(config)
    ks = client.session.start(
        admin_secret, "admin", KalturaSessionType.ADMIN, partner_id,
        privileges="all:*,disableentitlement"
    )
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
        filter.idIn = identifier
        print("🔎 Searching entries by specific IDs.")
    else:
        print("❌ Invalid method.")
        return []

    pager = KalturaFilterPager()
    pager.pageSize = 500

    entries = []
    page = 1
    while True:
        try:
            pager.pageIndex = page
            result = client.baseEntry.list(filter, pager)

            if not result.objects:
                break

            entries.extend(result.objects)

            if len(result.objects) < pager.pageSize:
                break

            page += 1

        except KalturaException as e:
            print(f"❌ API error while retrieving entries: {e}")
            break

    return entries


def get_child_entries(client, parent_entry_id):
    """Retrieve all child entries of a given parent entry."""
    child_filter = KalturaBaseEntryFilter()
    child_filter.parentEntryIdEqual = parent_entry_id
    pager = KalturaFilterPager()
    pager.pageSize = 500

    try:
        children = client.baseEntry.list(child_filter, pager).objects
        return children if children else []
    except Exception as e:
        print(f"Error retrieving children for entry {parent_entry_id}: {e}")
        return []


def get_destination_users():
    return {
        "coed": [
            uid.strip() for uid in DESTINATION_COEDITORS.split(",")
            ] if DESTINATION_COEDITORS else [],
        "copub": [
            uid.strip() for uid in DESTINATION_COPUBLISHERS.split(",")
            ] if DESTINATION_COPUBLISHERS else []
    }


def get_source_url(client, entry_id):
    # Retrieve the best flavor asset URL for a given entry by selecting the
    # largest available file.

    flavor_filter = KalturaFlavorAssetFilter()
    flavor_filter.entryIdEqual = entry_id
    pager = KalturaFilterPager()
    pager.pageSize = 500

    flavors = client.flavorAsset.list(flavor_filter, pager).objects
    if not flavors:
        return None

    # Select the flavor with the largest file size
    best_flavor = max(flavors, key=lambda f: f.sizeInBytes, default=None)

    try:
        return (
            client.flavorAsset.getUrl(best_flavor.id) if best_flavor else None
        )
    except KalturaException as e:
        print(
            f"⚠️ Warning: Could not retrieve flavor asset for entry "
            f"{entry_id}. Error: {e}"
            )
        return None


def get_cuepoints(client, entry_id):
    # Retrieve all cue points for a given entry, storing parent-child
    # relationships.
    try:
        cuepoint_filter = KalturaCuePointFilter()
        cuepoint_filter.entryIdEqual = entry_id
        pager = KalturaFilterPager()
        pager.pageSize = 500
        response = client.cuePoint.cuePoint.list(cuepoint_filter, pager)

        cuepoints = []
        parent_child_map = {}  # Map parent IDs to a list of child IDs

        if response.objects:
            for cp in response.objects:
                cue_type_str = (
                    cp.cuePointType.value if hasattr(
                        cp.cuePointType, 'value'
                        ) else str(cp.cuePointType)
                )

                if (
                    cue_type_str == "quiz.QUIZ_ANSWER"
                    and not COPY_QUIZ_ANSWERS
                ):
                    continue  # Skip storing answers if disabled

                cuepoints.append(cp)

                if (
                    cue_type_str == "quiz.QUIZ_ANSWER"
                    and COPY_QUIZ_ANSWERS
                    and cp.parentId
                ):
                    parent_child_map.setdefault(cp.parentId, []).append(cp.id)

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

        return cuepoints, parent_child_map

    except Exception as e:
        print(f"⚠️ Error retrieving cue points for entry {entry_id}: {e}")
        return [], {}


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

    cue_type_str = (
        cue.cuePointType.value
        if hasattr(cue.cuePointType, "value")
        else str(cue.cuePointType)
    )

    if cue_type_str not in cue_type_map:
        print(
            f"❌ Unknown cuePointType encountered: {cue.cuePointType} "
            f"(Raw: {cue})"
            )
        raise ValueError(f"Unknown cuePointType: {cue.cuePointType}")

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


def copy_cuepoints(
        client_source, client_dest, source_entry_id, dest_entry_id,
        entry_id_mapping
        ):
    # Copy cue points from source to destination entry, preserving
    # relationships.

    cuepoints, parent_child_map = get_cuepoints(client_source, source_entry_id)
    copied_count = 0  # Track copied cue points

    print(f"Total cue points found: {len(cuepoints)}")

    for cue in cuepoints:
        cue_type_str = str(
            getattr(cue.cuePointType, 'value', cue.cuePointType)
            )
        if cue_type_str == "quiz.QUIZ_ANSWER":
            continue  # Skip quiz answers in first pass

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
                ) for answer in getattr(cue, "optionalAnswers", [])
            ]

        try:
            # Add cue point to Kaltura
            added_cue = client_dest.cuePoint.cuePoint.add(new_cue)
            entry_id_mapping[cue.id] = added_cue.id
            copied_count += 1
            print(f"✅ Copied cue point {added_cue.id} (Type: {cue_type_str})")
            copied_count += 1
        except Exception as e:
            print(f"❌ Failed to copy {cue_type_str} {cue.id}: {e}")

    print(f"{copied_count} cuepoints copied for entry {source_entry_id}")
    return copied_count  # ✅ Return the correct count


def get_captions(client, entry_id):
    # Retrieve captions for an entry, optionally excluding auto-generated
    # captions.

    caption_filter = KalturaCaptionAssetFilter()
    caption_filter.entryIdEqual = entry_id
    pager = KalturaFilterPager()
    pager.pageSize = 500

    captions = []
    try:
        caption_assets = (
            client.caption.captionAsset.list(caption_filter, pager).objects
        )
        for caption in caption_assets:
            # Skip ASR captions if the flag is False
            if not COPY_ASR_CAPTIONS and caption.label == CAPTION_LABEL:
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
            added_caption = client_dest.caption.captionAsset.add(
                dest_entry_id, new_caption
                )
            caption_url = client_source.caption.captionAsset.getUrl(caption.id)
            caption_resource = KalturaUrlResource()
            caption_resource.url = caption_url
            client_dest.caption.captionAsset.setContent(
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
    pager = KalturaFilterPager()
    pager.pageSize = 500

    try:
        thumbnails = client.thumbAsset.list(thumb_filter, pager).objects
        return thumbnails
    except Exception as e:
        print(f"⚠️ Error retrieving thumbnails for entry {entry_id}: {e}")
        return []


def copy_thumbnails(
        client_source, client_dest, source_entry_id, dest_entry_id
        ):
    # Copy thumbnails from source to destination entry.
    thumbnails = get_thumbnails(client_source, source_entry_id)

    for thumb in thumbnails:
        try:
            # Step 1: Add a new thumbnail asset to the destination entry
            new_thumb = KalturaThumbAsset()
            added_thumb = client_dest.thumbAsset.add(dest_entry_id, new_thumb)

            # Step 2: Get source thumbnail URL
            thumb_url = client_source.thumbAsset.getUrl(thumb.id)

            # Step 3: Set content for the new thumbnail
            thumb_resource = KalturaUrlResource()
            thumb_resource.url = thumb_url
            client_dest.thumbAsset.setContent(added_thumb.id, thumb_resource)

            # Step 4: Set as default
            client_dest.thumbAsset.setAsDefault(added_thumb.id)
            print(
                f"Copied and set default thumbnail {thumb.id} to new entry "
                f"{dest_entry_id}"
                )
        except Exception as e:
            print(f"❌ Failed to copy thumbnail {thumb.id}: {e}")


def get_attachments(client, entry_id):
    # Retrieve attachments for an entry.

    attachment_filter = KalturaAttachmentAssetFilter()
    attachment_filter.entryIdEqual = entry_id
    pager = KalturaFilterPager()
    pager.pageSize = 500

    try:
        attachments = (
            client.attachment.attachmentAsset.list(
                attachment_filter, pager
                ).objects
        )
        return attachments if attachments else []
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

    copied_count = 0

    try:
        # Step 1: Retrieve all attachments for the source entry
        attachment_filter = KalturaAttachmentAssetFilter()
        attachment_filter.entryIdEqual = source_entry_id
        pager = KalturaFilterPager()
        pager.pageSize = 500

        attachments = (
            client_source.attachment.attachmentAsset.list(
                attachment_filter, pager
                ).objects
        )
        if not attachments:
            print(f"⚠️ No attachments found for entry {source_entry_id}.")
            return

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

                # Step 2: Get the attachment URL
                attachment_url = (
                    client_source.attachment.attachmentAsset.getUrl(
                        attachment.id
                        )
                )
                if not attachment_url:
                    print(
                        f"⚠️ Skipping attachment {attachment.id}: Unable to "
                        f"retrieve URL."
                        )
                    continue

                # Step 3: Add a new attachment asset to the destination entry
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

                added_attachment = (
                    client_dest.attachment.attachmentAsset.add(
                        dest_entry_id, new_attachment
                        )
                )

                # Step 4: Set the content for the new attachment asset
                attachment_resource = KalturaUrlResource()
                attachment_resource.url = attachment_url
                client_dest.attachment.attachmentAsset.setContent(
                    added_attachment.id, attachment_resource
                    )

                print(
                    f"Copied {type(attachment).__name__} {attachment.id} "
                    f"({new_attachment.filename}) to entry {dest_entry_id}"
                      )
                copied_count += 1
            except Exception as e:
                print(f"❌ Failed to copy attachment {attachment.id}: {e}")

    except Exception as e:
        print(
            f"❌ Error retrieving attachments for entry {source_entry_id}: {e}"
            )

    return copied_count


def get_sorted_entries(client, entry_ids):
    # Retrieve entries and sort them so parents are processed before children.
    entries = []
    parent_map = {}

    for entry_id in entry_ids:
        try:
            entry = client.baseEntry.get(entry_id)
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
    sorted_entries = sorted(entries, key=get_depth)

    if not sorted_entries:
        print("⚠️ Sorting returned an empty list.")

    return sorted_entries


def copy_entry(
        client_source, client_dest, entry, new_partner_id,
        DESTINATION_COEDITORS, DESTINATION_COPUBLISHERS, DESTINATION_TAG,
        csv_entries, entry_id_mapping
        ):
    start_time = time.time()

    debug_timer(start_time, "Started entry duplication process.")

    source_entry = client_source.media.get(entry.id)
    new_entry = KalturaMediaEntry()
    new_entry.name = entry.name
    new_entry.description = entry.description
    new_entry.tags = entry.tags

    # Special case for images (mediaType == 2)
    if source_entry.mediaType.value == 2:
        new_entry.mediaType = 2
        new_entry.sourceType = KalturaSourceType.URL

        try:
            copied_entry = client_dest.media.add(new_entry)
            print(
                f"✅ Created new image entry {copied_entry.id} from "
                f"{source_entry.id}"
                  )

        except Exception as e:
            print(
                f"❌ ERROR: Failed to create image entry {source_entry.id}: {e}"
                )
            return None

        if source_entry.downloadUrl:
            resource = KalturaUrlResource()
            resource.url = source_entry.downloadUrl
            conversion_profile_id = 0
            advanced_options = KalturaEntryReplacementOptions()

            try:
                client_dest.media.updateContent(
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

        # Process attachments for images
        attachments = get_attachments(client_source, source_entry.id)
        copy_attachments(
            client_source, client_dest, source_entry.id, copied_entry.id
        )
        print(
            f"Attachments for {source_entry.id}: Retrieved {len(attachments)}"
            )
        debug_timer(start_time, "Completed attachment copying.")

        # Apply destination owner and tags to images
        try:
            update_entry = KalturaMediaEntry()
            update_entry.userId = DESTINATION_OWNER

            if DESTINATION_TAG:
                update_entry.tags = (
                    (copied_entry.tags or "") + f",{DESTINATION_TAG}"
                )

            client_dest.baseEntry.update(copied_entry.id, update_entry)
            print(f"✅ Assigned owner & tag for image {copied_entry.id}")

            # Apply co-editors & co-publishers
            if DESTINATION_COEDITORS or DESTINATION_COPUBLISHERS:
                try:
                    update_permissions = KalturaMediaEntry()
                    if DESTINATION_COEDITORS:
                        update_permissions.entitledUsersEdit = (
                            ",".join(DESTINATION_COEDITORS) if (
                                isinstance(DESTINATION_COEDITORS, list)
                             ) else DESTINATION_COEDITORS
                        )

                    if DESTINATION_COPUBLISHERS:
                        update_permissions.entitledUsersPublish = (
                            ",".join(DESTINATION_COPUBLISHERS) if (
                                isinstance(DESTINATION_COPUBLISHERS, list)
                             ) else DESTINATION_COPUBLISHERS
                        )

                    client_dest.baseEntry.update(
                        copied_entry.id, update_permissions
                        )
                    print(
                        f"Assigned co-editors & co-publishers for image "
                        f"{copied_entry.id}"
                        )

                except Exception as e:
                    print(
                        f"⚠️ ERROR: Failed to assign co-editors/copublishers "
                        f"for image {copied_entry.id}: {e}"
                        )

        except Exception as e:
            print(
                f"⚠️ ERROR: Failed to assign owner/tag for image "
                f"{copied_entry.id}: {e}"
                )

        return copied_entry

    else:
        # Proceed with normal video/audio duplication
        if DESTINATION_TAG:
            new_entry.tags += f",{DESTINATION_TAG}"

        new_entry.userId = DESTINATION_OWNER
        new_entry.mediaType = entry.mediaType
        new_entry.sourceType = KalturaSourceType.FILE
        new_entry.blockAutoTranscript = True

        # If this is a child entry, set parentEntryId
        if entry.parentEntryId:
            new_entry.parentEntryId = (
                entry_id_mapping.get(entry.parentEntryId, "")
            )

        debug_timer(start_time, "Finished preparing new entry metadata.")

        # Check if the source entry is a quiz before duplicating
        is_quiz = (
            hasattr(source_entry, "capabilities")
            and "quiz.quiz" in source_entry.capabilities
        )

        # Copy entry
        copied_entry = client_dest.baseEntry.add(new_entry)
        entry_id_mapping[entry.id] = copied_entry.id  # Store mapping
        debug_timer(start_time, "Completed baseEntry.add() for new entry.")

        # Assign coeditors and copublishers to the new entry
        if DESTINATION_COEDITORS or DESTINATION_COPUBLISHERS:
            try:
                update_entry = KalturaMediaEntry()

                if DESTINATION_COEDITORS:
                    update_entry.entitledUsersEdit = (
                        ",".join(DESTINATION_COEDITORS) if (
                            isinstance(DESTINATION_COEDITORS, list)
                         ) else DESTINATION_COEDITORS
                    )

                if DESTINATION_COPUBLISHERS:
                    update_entry.entitledUsersPublish = (
                        ",".join(DESTINATION_COPUBLISHERS) if (
                            isinstance(DESTINATION_COPUBLISHERS, list)
                         ) else DESTINATION_COPUBLISHERS
                    )

                client_dest.baseEntry.update(copied_entry.id, update_entry)
                debug_timer(
                    start_time,
                    "Assigned coeditors and copublishers to the new entry."
                    )
                print(f"Assigned coeditors: {update_entry.entitledUsersEdit}")
                print(
                    f"Assigned copublishers: "
                    f"{update_entry.entitledUsersPublish}"
                    )

            except Exception as e:
                print(
                    f"⚠️ Failed to assign coeditors/copublishers for "
                    f"{copied_entry.id}: {e}"
                    )

        source_url = get_source_url(client_source, entry.id)

        if source_url:
            resource = KalturaUrlResource()
            resource.url = source_url
            client_dest.baseEntry.updateContent(copied_entry.id, resource)

        debug_timer(start_time, "Completed updateContent() for new entry.")

        if is_quiz:
            try:
                source_quiz = client_source.quiz.quiz.get(entry.id)
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

                client_dest.quiz.quiz.add(copied_entry.id, quiz)
                debug_timer(
                    start_time,
                    "Converted new entry into a quiz with original settings."
                    )
            except KalturaException as e:
                print(
                    f"❌ Failed to copy quiz settings for entry "
                    f"{copied_entry.id}: {e}"
                    )

        copy_thumbnails(client_source, client_dest, entry.id, copied_entry.id)
        debug_timer(start_time, "Completed thumbnail copying.")

        captions = get_captions(client_source, entry.id)
        copy_captions(
            client_source, client_dest, entry.id, copied_entry.id
        )
        print(f"Captions for {entry.id}: Retrieved {len(captions)}")
        debug_timer(start_time, "Completed caption copying.")

        attachments = get_attachments(client_source, entry.id)
        copy_attachments(
            client_source, client_dest, entry.id, copied_entry.id
        )
        print(f"Attachments for {entry.id}: Retrieved {len(attachments)}")
        debug_timer(start_time, "Completed attachment copying.")

        copy_cuepoints(
            client_source, client_dest, entry.id, copied_entry.id,
            entry_id_mapping
        )
        debug_timer(start_time, "Completed cuepoint copying.")

        # Copy child entries (Multistream support)
        children = get_child_entries(client_source, entry.id)
        for child in children:
            print(f"👶 Found child entry {child.id}, copying...")
            copy_entry(
                client_source, client_dest, child, new_partner_id,
                DESTINATION_COEDITORS, DESTINATION_COPUBLISHERS,
                DESTINATION_TAG, csv_entries, entry_id_mapping
                )

    return copied_entry


def write_to_csv(entries):
    for entry_id, entry_data in entries.items():
        with open(CSV_FILENAME, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([
                "source entry ID", "title", "parent entry ID",
                "destination entry id", "destination owner",
                "destination coeds", "destination copubs", "destination tags"
            ])

            for entry in entries.values():
                writer.writerow([
                    entry["source_id"],
                    entry["title"],
                    entry["parent_id"],
                    entry["dest_id"],
                    DESTINATION_OWNER,
                    ",".join(DESTINATION_COEDITORS.split(","))
                    if DESTINATION_COEDITORS else "",
                    ",".join(DESTINATION_COPUBLISHERS.split(","))
                    if DESTINATION_COPUBLISHERS else "",
                    DESTINATION_TAG
                ])


def main():
    client_source = get_kaltura_client(SOURCE_PID, SOURCE_ADMIN_SECRET)
    client_dest = get_kaltura_client(DEST_PID, DEST_ADMIN_SECRET)

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

    csv_entries = {}
    entry_id_mapping = {}

    print(f"✅ {len(entries)} entries found.")
    print("🤰 Sorting entries by parent-child hierarchy...")
    sorted_entries = get_sorted_entries(
        client_source, [entry.id for entry in entries]
    )
    print(f"✅ Sorted {len(sorted_entries)} entries.")

    for idx, entry in enumerate(sorted_entries, start=1):
        entry_start = time.time()  # Track per-entry time

        print("\n" + "-" * 80)
        print(
            f"🚀 Processing {idx}/{len(sorted_entries)} | "
            f"entry id: {entry.id} | title: {entry.name}"
        )
        print("-" * 80 + "\n")

        copied_entry = copy_entry(
            client_source, client_dest, entry, DEST_PID,
            DESTINATION_COEDITORS, DESTINATION_COPUBLISHERS, DESTINATION_TAG,
            csv_entries, entry_id_mapping
        )

        if copied_entry:
            csv_entries[entry.id] = {
                "source_id": entry.id,
                "title": entry.name,
                "parent_id": entry.parentEntryId if (
                    entry.parentEntryId
                 ) else "",
                "dest_id": copied_entry.id,
                "owner": DESTINATION_OWNER,
                "coed": DESTINATION_COEDITORS,
                "copub": DESTINATION_COPUBLISHERS,
                "captions_duplicated": 0,
                "cuepoints_duplicated": 0,
                "attachments_duplicated": 0
            }
        print(
            f"⏱ {time.time() - entry_start:.2f}s - Completed entry {entry.id}"
            )

    write_to_csv(csv_entries)
    print(f"📄 CSV log saved as {CSV_FILENAME}")
    print(f"⏱ {time.time() - start_time:.2f}s - Script execution complete.")


if __name__ == "__main__":
    main()
