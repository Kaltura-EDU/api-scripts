# Changelog

## [v1.2.0] - 2025-05-05
### Changed
- Main function now prompts user for Partner ID and Admin Secret for both source and destination.

- ## [v1.1.0] - 2025-04-26
### Added
- Added detailed docstring to the script.
- Improved command-line user prompts and feedback for a cleaner user experience.
- Added clearer debug and completion messages.

### Changed
- Improved CSV writing structure for Flake8 compatibility.
- Adjusted status printing to avoid redundant entry counts.

### Fixed
- Fixed duplicate "entries found" message.
- Fixed small formatting issues for Flake8 compliance.
- Implemented pagination when retrieving entries to handle large result sets. (Otherwise only 30 entries would transfer.)
- Updated search behavior for categories to use `categoryAncestorIdIn` instead of `categoriesIdsMatchOr`. (Otherwise entries that were in subcategories of the category entered wouldn't be duplicated.)

---

Galen Davis  
Senior Education Technology Specialist  
UC San Diego  
  
*and*  
  
Andy Clark  
Systems Administrator, Learning Systems  
Baylor University  
  
*Last updated 2025-05-05*
