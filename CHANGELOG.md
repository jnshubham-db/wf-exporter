# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
## [Unreleased]

## [0.3.1]
### Added
- **Dual Asset Processing**: Support for processing both workflows and pipelines in a single run
- **Pipeline Export Support**: Complete pipeline export functionality with legacy and glob-based processing
- **Library Export Control**: Granular control over library artifact downloads with global and individual settings
- **Enhanced Configuration**: New `global_settings` and `pipelines` sections in config.yml
- **Shared Utilities**: Generalized core logic for better code reusability
- **Comprehensive Testing**: Full test suite for Phase 3 functionality
- Support for wheel (.whl) files with automatic directory management
- Enhanced wheel file handling and placement in required directories

### Changed
- **BREAKING**: Main function now processes both workflows and pipelines sequentially instead of requiring separate runs
- Enhanced error handling and logging with better progress reporting
- Improved path transformation logic for consistent artifact placement

### Fixed
- Resolved mutual exclusion error when both workflows and pipelines are active in config
- Fixed binary data handling for Python and SQL file downloads
- Improved wheel file download fallback mechanisms
- Bug fixes for future releases

## [0.2.0]
### Added
- Support for wheel (.whl) files with automatic directory management
- Enhanced wheel file handling and placement in required directories
- Added support for python script task, wheel file task and sql task
- Added support for exporting py and sql files in respective directories

### Fixed
- Fixed bug related to yml detection and src file detection. Updated its logic.

## [0.1.2] - 2025-07-17

### Fixed
- Fixed the logic to remove databricks yml
- Handles the databricks.yml file more gracefully

## [0.1.1] - 2025-07-17

### Fixed
- Fixed authentication fallback in Databricks environment when Spark is not available
- Now uses provided host and token parameters as fallback when Spark session is unavailable


## [0.1.0] - 2025-07-17
### Added
- Initial release of wf-exporter
- Support for both local and Databricks environments
- Export multiple jobs in a single operation
- Parameterization support for exported YAML files
- Auto-fetch job permissions and notebook paths
- Core workflow extraction functionality
- CLI interface with configuration management
- Comprehensive logging system

[Unreleased]: https://github.com/yourusername/wf-exporter/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/yourusername/wf-exporter/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/yourusername/wf-exporter/releases/tag/v0.1.0 

