# delete-entries.py

This script permanently deletes Kaltura media entries by entry ID using the Kaltura API. It is designed for use by administrators who need to remove a large number of entries quickly, particularly in cases where batch deletion through the KMC is not feasible.

## ⚠️ WARNING

This script **permanently deletes entries** and cannot be undone. They are **not** put in a "recycled" status. Use with caution. Entries listed as parents will automatically remove associated child entries as well.

---

## Features

- Accepts a comma-separated list of Kaltura entry IDs
- Uses `baseEntry.get()` to collect entry metadata:
  - Entry ID
  - Entry name
  - Owner user ID
  - Duration (in seconds)
- Confirms intent to delete
- Uses `baseEntry.delete()` to permanently remove entries
- Gracefully handles:
  - Already deleted entries
  - Missing or invalid entry IDs
- Outputs a timestamped CSV report with:
  - `entry_id`
  - `entry_name`
  - `owner_user_id`
  - `duration_seconds`
  - `status` (`OK`, `NOT FOUND`, `DELETED`, `ALREADY DELETED`)

---

## Requirements

- Python 3.x
- `KalturaApiClient` (install with `pip install KalturaApiClient`)
- `lxml` (install with `pip install lxml`)

---

## Usage

1. Download and open the script file and update the following configuration variables at the top:

```python
PARTNER_ID = ''
ADMIN_SECRET = ''
USER_ID = 'your_admin_user_id'
```

2. Run the script:

```bash
python3 delete-entries.py
```

3. When prompted, paste a comma-separated list of entry IDs to delete.

4. Review the entries listed in the terminal. A report CSV will be created with a name like:

```
deleted_entries_20250516_104028.csv
```

5. Type `DELETE` to confirm and proceed with deletion.

---

## Output

A CSV report will be saved in the same directory as the script. The final status column indicates whether each entry was successfully deleted, not found, or skipped.

---

Galen Davis  
Senior Education Technology Specialist  
UC San Diego  
16 May 2025
