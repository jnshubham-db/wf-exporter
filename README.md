# WF Exporter

WF Exporter is a comprehensive tool designed to export Databricks jobs from your workspace and save them as YAML files with full Databricks Asset Bundles (DABS) compatibility. This powerful utility streamlines the process of migrating from workspace-based development to Infrastructure as Code (IaC) practices.

## What It Does

This tool transforms your existing Databricks workspace assets into a structured, version-controlled format by providing:

1. **Auto Extraction of Workflows YAML** - Automatically converts Databricks jobs into DABS-compatible YAML configuration files
2. **Auto Extraction of Notebooks** - Downloads and organizes all associated notebooks into their respective directory structures  
3. **Variable Substitution** - Performs intelligent path and value replacements to make exported assets compatible with DABS variable systems
4. **CLI Support** - Provides command-line interface for batch processing and automation of workflow exports
5. **Databricks Apps Support** - Offers a clean, user-friendly web interface for interactive extraction and management

The tool supports both local development environments and Databricks notebook environments, making it versatile for different development workflows and deployment scenarios.

## 🚀 Features

- **Automatic Environment Detection** - Works in both local and Databricks environments
- **Multiple Authentication Methods** - Profile-based, environment variables, or auto-detection
- **Dual Asset Type Support** - Process both workflows and pipelines in a single run
- **Library Export Control** - Granular control over library artifact downloads
- **Modular Architecture** - Clean, maintainable codebase with separated concerns
- **Configurable Paths** - Customize output directories and log locations
- **YAML Processing** - Advanced transformations and replacements
- **Bundle Generation** - Creates Databricks Asset Bundle compatible files

## 📋 Prerequisites

### Required Files

1. **`config/config.yml`** - Main configuration file
2. **`config/databricks.yml`** - Databricks bundle configuration file

### System Requirements

- Python 3.11+
- Databricks CLI (auto-installed in Databricks environments)
- Access to Databricks workspace

## 📥 Installation

### Download from GitHub Releases
1. Create a folder
```bash
mkdir ~/wf_exporter
cd ~/wf_exporter
```

2. Download and install the binary
```bash
# Get the latest release wheel file
curl -L -o wfexporter-0.4.1-py3-none-any.whl \
  "https://github.com/jnshubham-db/wf-exporter/releases/download/v0.4.1/wfexporter-0.4.1-py3-none-any.whl"
```

```bash
# Install the wheel
pip install wfexporter-0.4.3-py3-none-any.whl
```

3. Install the workflows, apps and local sample files
```bash
wf-export install
```

## 🎯 Usage

### Command Line Interface

```bash
# Basic usage with config file
wf-export run --config config/config.yml

# With explicit credentials
wf-export run --config config/config.yml --host <databricks-host> --token <databricks-token>

# Help
wf-export --help
```

### Databricks Workflow Integration

1. **Upload wheel file** to Databricks workspace
2. **Create a job** with the following configuration:

```python
# Install the wheel in your job
%pip install /path/to/wfexporter-1.0.6-py3-none-any.whl

# Use in your notebook
from wfExporter import main
main('config/config.yml')
```

### Programmatic Usage

```python
from wfExporter import main, DatabricksExporter

# Simple usage
main('config/config.yml')

# Advanced usage
exporter = DatabricksExporter('config/config.yml')
exporter.run()
```

## ⚙️ Configuration

### config.yml Structure

```yaml
initial_variables:
  # Output directory for generated files
  v_start_path: "/path/to/exports"
  
  # CSV mapping file path
  v_resource_key_job_id_mapping_csv_file_path: "{v_start_path}/bind_scripts/resource_key_job_id_mapping.csv"
  
  # Backup directory for YAML files
  v_backup_jobs_yaml_path: "{v_start_path}/backup_jobs_yaml/"
  
  # Log directory (configurable)
  v_log_directory_path: "{v_start_path}/logs"
  
  # Logging level
  v_log_level: DEBUG  # DEBUG, INFO, WARNING, ERROR, CRITICAL
  
  # Databricks CLI configuration (optional for local environments)
  v_databricks_cli_path: "databricks"  # CLI executable path
  v_databricks_config_profile: "TEST"  # Profile name
  
  # Databricks bundle file path
  v_databricks_yml_path: "/path/to/config/databricks.yml"

# Global settings (optional)
global_settings:
  export_libraries: true  # Global control for library artifact export

# Define workflows to export
workflows:
  - job_name: "My-Workflow"
    job_id: 123456789
    is_existing: true
    is_active: true
    export_libraries: true  # Override global setting for this workflow
    
# Define pipelines to export (optional)
pipelines:
  - pipeline_name: "My-Pipeline"
    pipeline_id: "pipeline_123456"
    is_existing: true
    is_active: true
    export_libraries: false  # Override global setting for this pipeline

# Value replacements in YAML files
value_replacements:
  "old_value": "new_value"
  
# Path transformations
path_replacement:
  "^/Workspace/Repos/[^/]+/": "../"
  "^/Repos/[^/]+/": "../"
```

### New Configuration Features

#### Library Export Control
- **Global Setting**: `global_settings.export_libraries` controls library export for all assets
- **Individual Override**: Each workflow/pipeline can override the global setting
- **Hierarchy**: Global `false` overrides individual `true` settings (safety first)

#### Dual Asset Processing
- **Workflows**: Traditional Databricks job definitions
- **Pipelines**: DLT (Delta Live Tables) pipeline definitions  
- **Simultaneous Processing**: Both asset types can be processed in a single run
- **Independent Control**: Each asset type can be enabled/disabled separately

### Configuration Options Explained

| Variable | Description | Example |
|----------|-------------|---------|
| `v_start_path` | Base directory for all generated files | `/path/to/exports` |
| `v_log_directory_path` | Directory for log files | `{v_start_path}/logs` |
| `v_log_level` | Logging verbosity level | `DEBUG`, `INFO`, `WARNING` |
| `v_databricks_cli_path` | Path to Databricks CLI (local only) | `databricks` |
| `v_databricks_config_profile` | Databricks profile name (local only) | `TEST`, `PROD` |
| `v_databricks_yml_path` | Path to databricks.yml bundle file | `/path/to/databricks.yml` |

### databricks.yml Structure

```yaml
bundle:
  name: WF_EXPORTER_BUNDLE

include:
  - environments/*.yml
  - resources/*.yml

targets:
  dev:
    mode: development
    default: true
    workspace:
      # host: $DATABRICKS_HOST
```

## 📁 Project Structure

```
wf-exporter/
├── config/
│   ├── config.yml          # Main configuration
│   └── databricks.yml      # Databricks bundle config
├── src/wfExporter/         # Main package
│   ├── cli/                # CLI management
│   ├── config/             # Configuration handling
│   ├── core/               # Main business logic
│   ├── logging/            # Logging utilities
│   ├── processing/         # File and YAML processing
│   └── workflow/           # Workflow extraction
├── dev_test.py             # Development testing script
└── pyproject.toml          # Package configuration
```

For detailed module documentation, see [MODULES.md](MODULES.md).

## 🔧 Development

### Development Testing

```bash
# Test your changes
poetry run python dev_test.py
```

### Building from Source

```bash
poetry build
# Output: dist/wfexporter-1.0.6-py3-none-any.whl
```

## 🤝 Authentication Options

1. **Config Profile** (Recommended for local development)
   ```yaml
   v_databricks_config_profile: "your-profile"
   ```

2. **Environment Variables**
   ```bash
   export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
   export DATABRICKS_TOKEN="your-token"
   ```

3. **Command Line Arguments**
   ```bash
   wf-export --host <host> --token <token>
   ```

4. **Auto-detection** (Databricks notebooks - no setup required)

## 📝 Output Structure

After running, your configured directory will contain:

```
exports/
├── src/                    # Generated source files
├── resources/              # Generated YAML files
├── backup_jobs_yaml/       # Backup of original YAML
├── logs/                   # Log files
└── bind_scripts/           # Resource mapping files
```

## 🐛 Troubleshooting

- **Authentication Issues**: Verify your Databricks credentials and profile configuration
- **File Permissions**: Ensure write access to output directories
- **Missing Config**: Check that both `config.yml` and `databricks.yml` exist and are properly formatted
- **CLI Not Found**: For local environments, ensure Databricks CLI is installed and accessible

---

**Version**: 0.4.1 | **Python**: 3.11+ | **License**: MIT
