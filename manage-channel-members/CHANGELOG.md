# Changelog

## [1.0.0] - 2026-05-08

Initial release.

Bulk-manages Kaltura channel membership from a CSV. Supports adding, removing, verifying, and changing the role of users (member, manager, contributor, moderator, owner). Includes ownership transfer via `category.update()`, detailed result descriptions (including before/after role for `change_role`), parallel processing via thread pool, and retry with exponential backoff.
