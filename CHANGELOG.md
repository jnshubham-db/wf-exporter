# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Added
- Support for wheel (.whl) files with automatic directory management
- Enhanced wheel file handling and placement in required directories

### Fixed
- Bug fixes for future releases

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