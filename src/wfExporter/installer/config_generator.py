"""
Configuration generator for WF Exporter.

This module generates sample configuration files for users.
"""

from pathlib import Path
from typing import List


class ConfigGenerator:
    """Generates sample configuration files for WF Exporter."""
    
    def __init__(self):
        """Initialize the config generator."""
        pass
    
    def generate_samples(self, target_directory: Path) -> List[Path]:
        """
        Generate sample configuration files in the target directory.
        
        Args:
            target_directory: Directory to create files in
            
        Returns:
            List of created file paths
        """
        created_files = []
        
        # Generate config.yml
        config_yml_path = target_directory / "config.yml"
        with open(config_yml_path, 'w') as f:
            f.write(self._get_config_yml_content())
        created_files.append(config_yml_path)
        
        # Generate databricks.yml
        databricks_yml_path = target_directory / "databricks.yml"
        with open(databricks_yml_path, 'w') as f:
            f.write(self._get_databricks_yml_content())
        created_files.append(databricks_yml_path)
        
        # Generate sample_export.py
        sample_export_path = target_directory / "sample_export.py"
        with open(sample_export_path, 'w') as f:
            f.write(self._get_sample_export_content())
        created_files.append(sample_export_path)
        
        return created_files
    
    def _get_config_yml_content(self) -> str:
        """Get content for config.yml file."""
        return """initial_variables:
  v_start_path: /Workspace/Applications/wf_exporter/exports/
  v_resource_key_job_id_mapping_csv_file_path: '{v_start_path}/bind_scripts/resource_key_job_id_mapping.csv'
  v_backup_jobs_yaml_path: '{v_start_path}/backup_jobs_yaml/'
  v_log_level: INFO
  v_databricks_yml_path: /Workspace/Applications/wf_exporter/wf_config/databricks.yml
  v_log_directory_path: '{v_start_path}/logs'

spark_conf_key_replacements:
- search_key: spark.hadoop.fs.azure.account.key.storage.dfs.core.windows.net
  target_key: spark.sql.shuffle.partitions
  target_value: '{existing_value}\\nspark.hadoop.fs.azure.account.key.${var.v_storage_account}.dfs.core.windows.net
    ${var.v_storage_account_secret}'

path_replacement:
  ^/Workspace/Repos/[^/]+/: ../
  ^/Repos/[^/]+/: ../
  ^/Workspace/: ../
  ^/Shared/: ../
  ^/: ../

global_settings:
  export_libraries: true

value_replacements:
  ${: $${

workflows:
  - job_name: "Sample Workflow"
    job_id: 123456789
    is_existing: true
    is_active: true
    export_libraries: true

pipelines:
  - pipeline_name: "Sample Pipeline"
    pipeline_id: 987654321
    is_existing: true
    is_active: true
    export_libraries: true
"""
    
    def _get_databricks_yml_content(self) -> str:
        """Get content for databricks.yml file."""
        return """bundle:
  name: WF_EXPORTER_BUNDLE

include:
  - resources/*.yml


targets:
  dev:
    mode: development
    default: true
    workspace:

"""
    
    def _get_sample_export_content(self) -> str:
        """Get content for sample_export.py file."""
        return '''#!/usr/bin/env python3
"""
Sample script demonstrating programmatic usage of WF Exporter.

This script shows different ways to use the WF Exporter programmatically.
"""

import os
from pathlib import Path

def main():
    """Main function demonstrating WF Exporter usage."""
    print("🚀 WF Exporter Sample Script")
    print("=" * 40)
    
    # Method 1: Using the main function with config file
    from wfExporter.main import main as wf_main
    
    # Use local config file
    config_path = "./config.yml"
    
    if Path(config_path).exists():
        print(f"📋 Using config file: {config_path}")
        wf_main(config_path=config_path)
    else:
        print(f"⚠️  Config file not found: {config_path}")
        print("💡 Run this script from directory containing config.yml")
        print("💡 Or modify the config_path variable to point to your config file")
    
    # Method 2: Using DatabricksExporter directly
    # from wfExporter.core.databricks_exporter import DatabricksExporter
    # exporter = DatabricksExporter(config_path="./config.yml")
    # exporter.run_workflow_export()
    # exporter.run_pipeline_export()
    
    print("✅ Sample script completed!")

if __name__ == "__main__":
    main()
'''

    def _get_run_py_content(self) -> str:
        """Get content for run.py file (Databricks workflow runner)."""
        return '''#!/usr/bin/env python3
"""
WF Exporter Workflow Runner

This script is the main entry point for the WF Exporter workflow 
when deployed to Databricks workspace.
"""

import sys
import os
from pathlib import Path

def setup_whl_import():
    """Set up imports from the uploaded WHL file."""
    current_dir = Path(__file__).parent
    
    # Look for WHL files in the current directory
    whl_files = list(current_dir.glob("*.whl"))
    
    if whl_files:
        whl_file = whl_files[0]  # Use the first WHL file found
        print(f"Found WHL file: {whl_file}")
        
        # Add WHL file to Python path
        if str(whl_file) not in sys.path:
            sys.path.insert(0, str(whl_file))
            print(f"Added {whl_file} to Python path")
    else:
        print("Warning: No WHL files found in current directory")
        print(f"Current directory: {current_dir}")
        print(f"Files in directory: {list(current_dir.iterdir())}")

def main():
    """Main function to run the WF Exporter."""
    print("🚀 Starting WF Exporter Workflow")
    print("=" * 50)
    
    # Setup WHL imports
    setup_whl_import()
    
    # Get config file path
    config_path = Path(__file__).parent / "config.yml"
    
    if not config_path.exists():
        print(f"❌ Error: Config file not found at {config_path}")
        sys.exit(1)
    
    print(f"📋 Using config file: {config_path}")
    
    try:
        # Import and run the exporter
        from wfExporter.main import main as export_main
        
        print("✅ Successfully imported wfExporter")
        print("🔄 Starting export process...")
        
        # Run the export with the config file
        export_main(config_path=str(config_path))
        
        print("🎉 Export process completed successfully!")
        
    except ImportError as e:
        print(f"❌ Failed to import wfExporter: {e}")
        print("💡 Make sure the WHL file is properly uploaded and accessible")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Export process failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
''' 