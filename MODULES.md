# Module Documentation

This document provides detailed information about each module in the WF Exporter package for developers who want to understand or contribute to the codebase.

## 📁 Package Structure

```
src/wfExporter/
├── __init__.py             # Package interface and exports
├── main.py                 # Main programmatic entry point
├── cli_entry.py            # CLI entry point
├── cli/                    # CLI management module
├── config/                 # Configuration management module
├── core/                   # Core business logic module
├── logging/                # Logging utilities module
├── processing/             # File and YAML processing module
└── workflow/               # Workflow extraction module
```

## 🎯 Core Modules

### `main.py` - Main Entry Point

**Purpose**: Provides the main programmatic entry point for the package.

**Key Functions**:
- `main(config_path, databricks_host, databricks_token)` - Main function that initializes and runs the export process

**Usage**:
```python
from wfExporter.main import main
main('config/config.yml')
```

### `cli_entry.py` - CLI Entry Point

**Purpose**: Handles command-line interface operations and argument parsing.

**Key Functions**:
- `cli_main()` - CLI entry point with argument parsing
- Supports multiple authentication methods
- Provides comprehensive help and error handling

**Features**:
- Command-line argument parsing
- Authentication option handling
- Error reporting and user guidance

### `core/databricks_exporter.py` - Main Business Logic

**Purpose**: Orchestrates the entire workflow export process using the Facade pattern.

**Key Classes**:
- `DatabricksExporter` - Main coordinator class

**Key Methods**:
- `__init__()` - Initialize with configuration and credentials
- `setup()` - Set up CLI and authentication
- `run()` - Execute the complete export workflow
- `process_job()` - Process individual jobs
- `_clean_existing_files()` - Clean up before generation
- `_prepare_file_mapping()` - Prepare file mappings

**Responsibilities**:
- Environment detection
- Component initialization
- Job processing orchestration
- Error handling and logging
- File operations coordination

## 🛠️ Utility Modules

### `cli/cli_manager.py` - CLI Management

**Purpose**: Manages Databricks CLI operations for both local and Databricks environments.

**Key Classes**:
- `DatabricksCliManager` - CLI operations manager

**Key Methods**:
- `_detect_environment()` - Detect local vs Databricks environment
- `install_cli()` - Install or locate Databricks CLI
- `setup_authentication()` - Configure authentication
- `test_authentication()` - Verify authentication
- `generate_yaml_src_files_from_job_id()` - Generate bundle files

**Features**:
- Automatic environment detection
- CLI installation (Databricks environments)
- Multiple authentication methods
- Bundle generation commands

### `config/config_manager.py` - Configuration Management

**Purpose**: Centralized configuration loading and access using singleton pattern.

**Key Classes**:
- `ConfigManager` - Singleton configuration manager

**Key Methods**:
- `_load_config()` - Load configuration from YAML
- `get_active_jobs()` - Get list of active workflows
- `get_initial_paths()` - Get configured paths
- `get_log_directory_path()` - Get log directory path
- `get_replacements()` - Get value replacement mappings
- `get_spark_conf_transformations()` - Get Spark config rules

**Features**:
- Singleton pattern implementation
- Multiple config file location support
- Path placeholder resolution
- Comprehensive configuration access

### `logging/log_manager.py` - Logging Utilities

**Purpose**: Provides centralized logging with configurable levels and colored output.

**Key Classes**:
- `LogManager` - Logging manager with file and console handlers

**Key Methods**:
- `_create_colored_console_handler()` - Set up colored console output
- `_setup_file_handler()` - Set up file logging
- `debug()`, `info()`, `warning()`, `error()`, `critical()` - Logging methods

**Features**:
- Configurable log levels
- Colored console output
- File logging with timestamps
- Configurable log directory
- Instance tracking to prevent duplicates

### `workflow/workflow_extractor.py` - Workflow Extraction

**Purpose**: Handles interactions with Databricks SDK for retrieving workflow information.

**Key Classes**:
- `WorkflowExtractor` - Workflow data extraction manager

**Key Methods**:
- `get_job_workflow_tasks()` - Retrieve job workflow tasks
- `get_job_acls()` - Get job permissions
- `_extract_notebook_tasks()` - Extract notebook task information
- `_extract_dlt_tasks()` - Extract DLT pipeline tasks

**Features**:
- Databricks SDK integration
- Workflow task extraction
- Permission management
- Support for multiple task types

### `processing/` - File and YAML Processing

#### `yaml_serializer.py` - YAML Processing

**Purpose**: Handles YAML file manipulation including loading, updating, and dumping.

**Key Classes**:
- `YamlSerializer` - YAML processing manager

**Key Methods**:
- `load_update_dump_yaml()` - Main YAML processing pipeline
- `_apply_replacements()` - Apply value replacements
- `_apply_spark_conf_transformations()` - Apply Spark config transformations
- `_update_notebook_paths()` - Update notebook path references

**Features**:
- YAML file manipulation
- Value replacement processing
- Spark configuration transformations
- Path transformations
- Custom formatting preservation

#### `export_file_handler.py` - File Operations

**Purpose**: Handles file operations such as moving, mapping, and transforming file paths.

**Key Classes**:
- `ExportFileHandler` - File operations manager

**Key Methods**:
- `move_files_to_directory()` - Move files based on mapping
- `transform_notebook_path()` - Transform notebook paths
- `map_src_file_name()` - Map source file names
- `convert_string()` - Standardize string formatting

**Features**:
- File path transformations
- Directory operations
- File mapping and moving
- Pattern-based replacements

## 🔧 Entry Points and Configuration

### Package Interface (`__init__.py`)

**Exports**:
- `main` - Main programmatic entry point
- `cli_main` - CLI entry point
- `DatabricksExporter` - Main exporter class
- All utility classes for advanced usage

### CLI Configuration (`pyproject.toml`)

**Entry Point**:
```toml
[tool.poetry.scripts]
wf-export = "wfExporter.cli_entry:cli_main"
```

## 🏗️ Architecture Patterns

### Facade Pattern
- `DatabricksExporter` acts as a facade, providing a simplified interface to complex subsystems

### Singleton Pattern
- `ConfigManager` ensures configuration is loaded only once and shared across components

### Dependency Injection
- All components receive their dependencies (logger, config) through constructor injection

### Separation of Concerns
- Each module has a single, well-defined responsibility
- Clear interfaces between modules
- No circular dependencies

## 🧪 Testing and Development

### Development Script
- `dev_test.py` - Simple script for testing changes during development

### Key Development Patterns
- All classes accept logger instances for consistent logging
- Configuration is centralized and injected
- Error handling with proper exception propagation
- Comprehensive logging at appropriate levels

## 🔄 Data Flow

1. **Initialization**: `DatabricksExporter` loads config and initializes components
2. **Authentication**: CLI manager sets up Databricks authentication
3. **Job Processing**: For each active job:
   - Extract workflow definition
   - Generate bundle files
   - Move and organize files
   - Apply YAML transformations
   - Save processed results
4. **Cleanup**: Log summary and cleanup temporary files

## 🎨 Code Style and Standards

- **Type Hints**: Comprehensive type annotations throughout
- **Docstrings**: Detailed documentation for all classes and methods
- **Error Handling**: Proper exception handling with informative messages
- **Logging**: Structured logging at appropriate levels
- **Configuration**: Externalized configuration with sensible defaults
- **Modularity**: Clear separation of concerns and single responsibility principle

---

This modular architecture ensures maintainability, testability, and extensibility while providing a clean interface for users. 