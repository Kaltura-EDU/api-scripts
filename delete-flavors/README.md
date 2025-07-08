# delete-flavors.py

This script helps administrators efficiently manage Kaltura media storage by permanently removing all redundant flavors (transcoded versions) of media entries, keeping only the source flavor. It is designed for use by administrators who need to remove a large number of flavors quickly, since batch deletion through the KMC is not feasible.

## ⚠️ WARNING

This script **permanently deletes flavors** and cannot be undone. They are **not** put in a "recycled" status. Use with caution.

---

## Requirements

- Python 3.x
- `KalturaApiClient` (install with `pip install KalturaApiClient`)
- `lxml` (install with `pip install lxml`)
- `python-dotenv` (install with `pip install python-dotenv`)