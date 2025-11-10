# Changelog

## [1.1.0] - 2025-11-04
### Changed
- Updated script to use `.env` file for configuration, including support for multiple `ENTRY_IDS`.
- Replaced interactive prompts with environment variables where applicable.
- Switched category lookup to support full name matching with customizable prefix.

### Added
- `.env.example` template with all required variables.
- Support for batch unpublish/republish using comma-delimited `ENTRY_IDS`.
- New `README.md` instructions for .env-based usage.

## [1.0.0] - 2025-11-01
### Added
- Initial version: unpublish and republish a Kaltura entry by removing and re-adding it to a given category.
