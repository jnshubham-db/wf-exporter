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
            f.write(self._get_config_yml_content_local(target_directory=target_directory))
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
    
    def _get_config_yml_content_local(self, target_directory: Path) -> str:
        """Get content for config.yml file."""
        return """initial_variables:
  v_start_path: """+target_directory+"""/exports/
  v_resource_key_job_id_mapping_csv_file_path: '{v_start_path}/bind_scripts/resource_key_job_id_mapping.csv'
  v_backup_jobs_yaml_path: '{v_start_path}/backup_jobs_yaml/'
  v_log_level: INFO
  v_databricks_yml_path: """+target_directory+"""/databricks.yml
  v_log_directory_path: '{v_start_path}/logs'
  v_databricks_cli_path: "databricks"
  v_databricks_config_profile: DEFAULT


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
        return '''
from wfExporter.main import main
import sys


if __name__ == "__main__":
    workspace_url = spark.conf.get('spark.databricks.workspaceUrl')
    url = f"https://{workspace_url}"
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()    
    config_path = None
    
    if "--config_path" in sys.argv:
        try:
            index = sys.argv.index("--config_path")
            config_path = sys.argv[index + 1]
        except IndexError:
            print("Error: --config_path specified but no value provided.")
            sys.exit(1)


        main(
            config_path=config_path,
            databricks_host=url,
            databricks_token=token
        )
''' 