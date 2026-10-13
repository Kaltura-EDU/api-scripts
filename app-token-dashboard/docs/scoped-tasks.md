# Scoped script library

The local AppToken dashboard now provides eight independently authored, focused versions of tasks in the read-only [Kaltura-EDU/api-scripts repository](https://github.com/Kaltura-EDU/api-scripts). They use Python's standard library, without the SDK or external packages. Each downloaded runner contains one fixed task manifest and uses an AppToken; it never asks for or uses the Administrator Secret.

Choose a script in the dashboard, enter the PID and provisioning secret, review its permissions, and download and unzip the single bundle, which includes the provisioning files plus `run_task.py` and, where applicable, `input.csv`. The task's permission set is fixed; selecting a different task resets the review. Custom permission groups remain available for creating tokens without a task runner.

## Permission categories

The library uses **read**, **read + write/edit**, and **read + write/edit + delete** as navigation categories describing the role's capabilities. They are not cumulative bundles. A delete-entry task receives entry-read and entry-delete permissions without metadata-edit permissions. A count/duration inventory uses `media.list`, so it does not need analytics permissions. Read-only tasks receive no create, update, rename, or delete grants.

Kaltura sometimes combines operations in one named permission. Caption visibility is an update operation, but `CAPTION_MODIFY` includes caption deletion, so this task appears in the delete-capable category. The runner itself only calls caption list/update. That local guard does not make the remote token incapable of deletion if someone uses it outside the runner. Likewise, `CONTENT_MANAGE_METADATA` covers more fields than the runner's name update. The consulted catalog provides no separate caption-visibility or entry-name-only permission.

Every role includes `BASE_USER_SESSION_PERMISSION`; additional grants are:

| Task | Additional permission names | Content API actions | Input |
| --- | --- | --- | --- |
| Search entries | `CONTENT_MANAGE_BASE` | `media.list` | Optional CLI filters |
| Count & duration | `CONTENT_MANAGE_BASE` | `media.list` | Optional CLI filters |
| Audit captions | `CONTENT_MANAGE_BASE`, `CAPTION_SEARCH` | `caption_captionAsset.list` | `entry_id` |
| Download captions | `CONTENT_MANAGE_BASE`, `CAPTION_SEARCH` | `caption_captionAsset.list`, `.getUrl` | `entry_id` |
| Download original media | `CONTENT_MANAGE_BASE`, `CONTENT_MANAGE_DOWNLOAD` | `flavorAsset.list`, `.getUrl` | `entry_id` |
| Rename entries | `CONTENT_MANAGE_BASE`, `CONTENT_MANAGE_METADATA` | `baseEntry.get`, `.update` (name only) | `entry_id,expected_name,new_name` |
| Set caption visibility | `CONTENT_MANAGE_BASE`, `CAPTION_SEARCH`, `CAPTION_MODIFY` | `caption_captionAsset.list`, `.update` (`displayOnPlayer` only) | `entry_id,caption_id,expected_label,display_on_player` |
| Delete entries | `CONTENT_MANAGE_BASE`, `CONTENT_MANAGE_DELETE` | `baseEntry.get`, `.delete` | `entry_id,expected_name` |

The machine-readable per-task manifest is [tasks.json](../tasks.json), alongside the dashboard. The permission name snapshot is [kaltura-reference-catalog.txt](../permissions/kaltura-reference-catalog.txt). Each downloaded bundle includes its own `PERMISSIONS.md`, with the exact role permissions, actions, reference path, limitations, and verification status.

**Verification status:** these are least-privilege candidates for the documented operations, reviewed offline. No target-account calls were made. The permission catalog lists names, not every action/property mapping. In particular, caption `getUrl` access must be checked on the intended account before describing caption downloads as operationally verified. `CAPTION_SEARCH` follows this repository's existing scoped read-only caption workflow; the general Kaltura caption guide mentions the broader `CAPTION_PLUGIN_PERMISSION`, which this project's account previously rejected. The runners stop on denial and never fall back to the admin secret, add broad plugin permissions automatically, or expand grants. An operator must confirm successful access and account-specific minimality.

## Provision and run

From the extracted bundle folder:

```powershell
python provision_app_token.py --dry-run
python provision_app_token.py
```

Provisioning alone uses the Administrator Secret, read from `.env`, to create the custom role and AppToken. The script saves the token credentials to `credentials/<role-name>.env`. Delete the provisioning `.env` afterward. The runner reads the new token credential file; a separate path can be provided with `--credentials`.

For CSV tasks, replace the sample row with reviewed targets. Expected names/labels must match exactly; `display_on_player` is `0` or `1`. CSV targets are explicit to avoid granting additional category, user-directory, or discovery access.

```powershell
python run_task.py --input input.csv --dry-run
python run_task.py --input input.csv --preview
python run_task.py --input input.csv --execute
```

The default mode, like `--dry-run`, is entirely offline and does not load credentials. `--preview` starts a scoped session and performs live reads, producing a report without mutations or downloading assets. `--execute` performs the task; write and delete tasks first produce `plan.csv` and require typed `APPLY` or `DELETE` confirmation in the terminal. The script rechecks expected names/labels immediately before each mutation, logs each attempt before sending it, stops on the first error, and never automatically retries a write. These checks reduce mistakes but do not provide a server-side atomic compare-and-update operation. An interrupted or timed-out request may have succeeded: inspect Kaltura and the audit log before resuming.

For search/count reports, omit `--input` and use optional filters:

```powershell
python run_task.py --start 2026-01-01 --end 2026-01-31 --owner example-user --preview
python run_task.py --start 2026-01-01 --end 2026-01-31 --execute
```

Other filters are `--tag` and `--category`. Date boundaries use UTC and are inclusive. The runner refuses incomplete pagination and the 10,000-match boundary; narrow the date window or filters rather than relying on a truncated report. It does not reproduce the reference count/duration script's automatic time chunking, optional source-filename URL calls, or flavor-size output. Every run gets a separate timestamped report directory under `reports/`, or a location chosen with `--output`.

Caption downloads preserve the original file format. Media downloads include ready original flavors only, excluding documents, thumbnails, external media, and descendant discovery. Downloads use HTTPS, reject insecure redirects, and refuse file overwrites. Signed URLs and sessions are never written to reports. Both downloads are read operations with respect to Kaltura; they write files locally.

## Authentication and authority

The runner performs `session.startWidgetSession`, computes `SHA256(widgetKS + tokenValue)`, and exchanges it with `appToken.startSession`. The created token fixes its role, identity, session type, and duration. Task presets use admin-type sessions restricted by `setrole:<new-role-ID>` to support server automation; they do not add `all:*`, `edit:*`, `disableentitlement`, or `list:*`. Custom-token mode still permits the existing explicit `list:*` option.

The runner checks PID, expected session type, SHA256, and exact permission names recorded in the token `.env` before network access. This is a consistency check of local records, not remote attestation; editing the local manifest cannot change the server's role. Account entitlements, enabled plugins, and content access controls remain relevant. Each content call also passes through the runner's fixed task action allowlist, and authentication has no admin-secret fallback.

The primary API guides are [AppTokens](https://kaltura.md/raw/KALTURA_APPTOKENS_API.md), [Captions](https://kaltura.md/raw/KALTURA_CAPTIONS_AND_TRANSCRIPTS_API.md), and [User management](https://kaltura.md/raw/KALTURA_USER_MANAGEMENT_API.md). Their upstream Markdown copies were consulted when the raw guide endpoint could not be fetched. The reference snapshot is commit `5dda3191f4a5daaa2cbf227e9a16e1ae1b3c1688`, retrieved on October 12, 2026. Source blob hashes were checked against that tree. The reference repository was never modified.

## Remaining reference scripts

The [reference audit](reference-script-audit.md) inventories all 30 Python files in the snapshot. This first release integrates eight tasks; the others remain documented candidates. Multi-account copying needs separate source and destination tokens with different roles. Channel membership, quiz attempt deletion, cue points, custom metadata, ingestion, and conversion need additional operation/property mappings before minimum roles can be defined confidently. They are not offered as executable presets with guessed broad permissions.
