"""
YAML processing module for Databricks Workflow Exporter.

This module handles YAML file manipulation including loading, updating,
and dumping YAML files with custom formatting and transformations.
"""

import os
import yaml
from typing import Any, Dict, Optional, Tuple

from ..logging.log_manager import LogManager
from ..workflow.workflow_extractor import WorkflowExtractor


class YamlSerializer:
    """
    A class for processing and manipulating YAML files.
    
    Handles:
    - YAML file loading and dumping
    - Content transformations and replacements
    - Custom formatting and style preservation
    """
    
    # Default replacements
    DEFAULT_REPLACEMENTS = {
        "${": "$${",
        "azuesstqadbbigdata.": "${var.v_storage_account}.",
        "{{secrets/attmx-storage-secrets/storage-key}}": "${var.v_storage_account_secret}"
    }
    
    def __init__(self, logger: Optional[LogManager] = None):
        """
        Initialize the YamlSerializer.
        
        Args:
            logger: Logger instance for logging operations
        """
        self.logger = logger or LogManager()
    
    class CustomDumper(yaml.Dumper):
        """Custom Dumper class to enhance list indentation and preserve formatting."""
        
        def increase_indent(self, flow=False, indentless=False):
            """Ensure proper indentation for lists."""
            return super(YamlSerializer.CustomDumper, self).increase_indent(flow, False)

        def represent_scalar(self, tag, value, style=None):
            """Preserve double quotes for strings containing double quotes and handle multiline strings."""
            if isinstance(value, str):
                if '\n' in value:
                    # Use literal block style for multiline strings
                    style = '|'
                elif '"' in value:
                    # Preserve double quotes
                    style = '"'
            return super(YamlSerializer.CustomDumper, self).represent_scalar(tag, value, style)
    
    def replace_keyword_in_values(self, data: Any, old_value: str, new_value: str, current_key: str = None) -> Any:
        """
        Recursively replaces occurrences of old_value with new_value in a nested dictionary or list.
        Excludes file path fields from replacement to preserve downloaded file paths.
        
        Args:
            data: The data structure to process
            old_value: The value to replace
            new_value: The replacement value
            current_key: The current key being processed (for path exclusion)
            
        Returns:
            The processed data structure with replacements made
        """
        # Define path fields that should be excluded from replacements
        PATH_FIELDS = {
            'notebook_path',  # notebook task paths
            'python_file',    # spark python task paths  
            'path',          # general path field (sql task file path, library paths, etc.)
            'file'           # file references
        }
        
        if isinstance(data, dict):
            result = {}
            for key, value in data.items():
                # Skip replacement for path-related fields
                if key in PATH_FIELDS:
                    result[key] = value  # Keep original path value without replacement
                    self.logger.debug(f"Skipping replacement for path field: {key} = {value}")
                else:
                    result[key] = self.replace_keyword_in_values(value, old_value, new_value, key)
            return result
        elif isinstance(data, list):
            return [self.replace_keyword_in_values(item, old_value, new_value, current_key) for item in data]
        elif isinstance(data, str):
            # Only replace if we're not in a path field context
            if current_key in {'notebook_path', 'python_file', 'path', 'file'}:
                return data  # Keep original path
            return data.replace(old_value, new_value)
        else:
            return data

    def replace_null_with_string_null(self, data: Any) -> Any:
        """
        Recursively replaces all `None` values in a nested structure with the string 'null'.
        
        Args:
            data: The data structure to process
            
        Returns:
            The processed data structure with None values replaced
        """
        if isinstance(data, dict):
            return {key: self.replace_null_with_string_null(value) for key, value in data.items()}
        elif isinstance(data, list):
            return [self.replace_null_with_string_null(item) for item in data]
        elif data is None:
            return "null" 
        else: 
            return data
    
    def load_update_dump_yaml(self, workflow_manager: WorkflowExtractor, 
                             yml_file: str, modified_yml_file: str, 
                             job_id: str, job_resource_name: str, 
                             mapping_filtered_df: Dict[str, str], 
                             replacements: Optional[Dict[str, str]] = None,
                             config_manager: Optional['ConfigManager'] = None) -> Tuple[str, str]:
        """
        Loads a YAML file, updates its content based on specific rules, and writes the updated content to a new file.
        
        Args:
            workflow_manager: Workflow manager for retrieving job permissions
            yml_file: Path to the input YAML file
            modified_yml_file: Path to the output YAML file
            job_id: The Databricks job ID
            job_resource_name: The resource name in the YAML
            mapping_filtered_df: Dictionary mapping old paths to new paths
            replacements: Dictionary of value replacements to apply
            config_manager: Configuration manager for accessing transformations
            
        Returns:
            Tuple of (error_message, status) - ("0", "success") if successful
        """
        replacements = replacements or self.DEFAULT_REPLACEMENTS

        try:
            self.logger.debug(f"Loading YAML file: {yml_file}")
            
            # Load the YAML file
            with open(yml_file, "r") as file:
                yaml_data = yaml.safe_load(file)

            # Remove pause_status if schedule exists
            if 'schedule' in yaml_data['resources']['jobs'][job_resource_name]:
                if 'pause_status' in yaml_data['resources']['jobs'][job_resource_name]['schedule']:
                    del yaml_data['resources']['jobs'][job_resource_name]['schedule']['pause_status']
                    self.logger.debug("Removed pause_status from schedule")

            self.logger.debug("Adding permissions to yaml file")
            # Add 'permissions' if not present
            if 'permissions' not in yaml_data['resources']['jobs'][job_resource_name]:
                yaml_data['resources']['jobs'][job_resource_name]['permissions'] = workflow_manager.get_job_acls(job_id)
            self.logger.debug("Permissions added successfully")

            self.logger.debug("Replacing keyword variables in yaml file")
            # Apply string replacements to the entire YAML data
            for pattern, replacement in replacements.items():
                if not pattern.startswith(r'('):  # Skip regex patterns
                    yaml_data = self.replace_keyword_in_values(yaml_data, pattern, replacement)
            self.logger.debug("Keyword variables replaced successfully")
            
            self.logger.debug("Replacing null with none in yaml file")
            # Replace None with 'null'
            yaml_data = self.replace_null_with_string_null(yaml_data)
            self.logger.debug("Successfully replaced null with none")
            
            # Update notebook_paths
            self.logger.debug("Updating notebook paths in yaml file")
            self.logger.debug(f"Path mappings: {mapping_filtered_df}")
            for i, task in enumerate(yaml_data["resources"]["jobs"][job_resource_name]['tasks']):
                if task.get('notebook_task') is not None:
                    v_notebook_path = task.get('notebook_task').get('notebook_path')
                    if v_notebook_path in mapping_filtered_df:
                        yaml_data["resources"]["jobs"][job_resource_name]['tasks'][i]['notebook_task']['notebook_path'] = mapping_filtered_df[v_notebook_path]
            self.logger.debug("Successfully updated notebook paths")
            
            # Update paths for all task types
            self.logger.debug("Updating paths for all task types")
            for i, task in enumerate(yaml_data["resources"]["jobs"][job_resource_name]['tasks']):
                
                # Update spark_python_task paths
                if task.get('spark_python_task') is not None:
                    python_file = task.get('spark_python_task').get('python_file')
                    if python_file and python_file in mapping_filtered_df:
                        yaml_data["resources"]["jobs"][job_resource_name]['tasks'][i]['spark_python_task']['python_file'] = mapping_filtered_df[python_file]
                        self.logger.debug(f"Updated spark_python_task file: {python_file} -> {mapping_filtered_df[python_file]}")
                
                # Update sql_task paths
                if task.get('sql_task') is not None:
                    sql_task = task.get('sql_task')
                    if sql_task.get('file') is not None:
                        sql_file_path = sql_task.get('file').get('path')
                        if sql_file_path and sql_file_path in mapping_filtered_df:
                            yaml_data["resources"]["jobs"][job_resource_name]['tasks'][i]['sql_task']['file']['path'] = mapping_filtered_df[sql_file_path]
                            self.logger.debug(f"Updated sql_task file: {sql_file_path} -> {mapping_filtered_df[sql_file_path]}")
                
                # Update libraries (whl files) for all task types
                if task.get('libraries') is not None:
                    for j, library in enumerate(task['libraries']):
                        if library.get('whl') is not None:
                            whl_path = library['whl']
                            if whl_path in mapping_filtered_df:
                                yaml_data["resources"]["jobs"][job_resource_name]['tasks'][i]['libraries'][j]['whl'] = mapping_filtered_df[whl_path]
                                self.logger.debug(f"Updated library whl: {whl_path} -> {mapping_filtered_df[whl_path]}")
            
            # Update job-level environments (for serverless configurations)
            if 'environments' in yaml_data["resources"]["jobs"][job_resource_name]:
                self.logger.debug("Updating job-level environment dependencies")
                for i, environment in enumerate(yaml_data["resources"]["jobs"][job_resource_name]['environments']):
                    if environment.get('spec') and environment['spec'].get('dependencies'):
                        for j, dependency in enumerate(environment['spec']['dependencies']):
                            if isinstance(dependency, str) and dependency in mapping_filtered_df:
                                yaml_data["resources"]["jobs"][job_resource_name]['environments'][i]['spec']['dependencies'][j] = mapping_filtered_df[dependency]
                                self.logger.debug(f"Updated environment dependency: {dependency} -> {mapping_filtered_df[dependency]}")
            
            self.logger.debug("Successfully updated all task type paths")
            
            self.logger.debug("Updating cluster conf in yaml file")
            try:
                job_clusters = yaml_data["resources"]["jobs"][job_resource_name].get('job_clusters', [])
                
                # Get spark configuration transformations from config
                if config_manager:
                    spark_transformations = config_manager.get_spark_conf_transformations()
                    
                    # Process each job cluster
                    for cluster_index, job_cluster in enumerate(job_clusters):
                        if 'new_cluster' in job_cluster:
                            self.logger.debug(f"Processing job cluster {cluster_index}")
                            spark_conf = job_cluster['new_cluster'].get('spark_conf', {})
                            
                            # Apply remove and replace transformations
                            for transformation in spark_transformations:
                                search_key = transformation.get('search_key')
                                target_key = transformation.get('target_key')
                                target_value = transformation.get('target_value')
                                
                                if search_key and search_key in spark_conf:                        
                                    del spark_conf[search_key]
                                    self.logger.debug(f"Removed spark conf key: {search_key} from cluster {cluster_index}")
                                    
                                    existing_value = spark_conf.get(target_key, 'auto')
                                    new_value = target_value.replace('{existing_value}', existing_value)
                                    
                                    spark_conf[target_key] = new_value
                                    self.logger.debug(f"Updated target key {target_key} in cluster {cluster_index} with new configuration")
                                    break  # Only apply the first matching transformation
                        else:
                            self.logger.debug(f"Skipping job cluster {cluster_index} - no new_cluster configuration")
                            
            except Exception as e:
                self.logger.error(f"Error updating spark conf: {str(e)}")
                return str(e), "failed"
            self.logger.debug("Successfully updated cluster conf")
            
            self.logger.debug(f"Writing yaml file to {modified_yml_file}")
            # Write back to the YAML file
            with open(modified_yml_file, "w") as file:
                yaml.dump(yaml_data, file, default_flow_style=False, sort_keys=False, Dumper=self.CustomDumper)
            
            self.logger.debug(f"Successfully processed YAML file: {modified_yml_file}")
            return "0", "success"
            
        except Exception as e:
            self.logger.error(f"Error updating the YAML file: {str(e)}")
            return str(e), "failed" 

    def load_update_dump_yaml_generic(self, workflow_manager: 'WorkflowExtractor', 
                                     yml_file: str, modified_yml_file: str, 
                                     resource_id: str, resource_name: str,
                                     resource_type: str,  # 'job' or 'pipeline'
                                     mapping_dict: Dict[str, str], 
                                     replacements: Optional[Dict[str, str]] = None,
                                     config_manager: Optional['ConfigManager'] = None,
                                     backup_yaml_path: Optional[str] = None) -> Tuple[str, str]:
        """
        Generic method to load, update, and dump YAML files for both workflows and pipelines.
        
        Args:
            workflow_manager: Workflow manager for retrieving permissions
            yml_file: Path to the input YAML file
            modified_yml_file: Path to the output YAML file
            resource_id: The Databricks resource ID (job_id or pipeline_id)
            resource_name: The resource name in the YAML
            resource_type: Type of resource ('job' or 'pipeline')
            mapping_dict: Dictionary mapping old paths to new paths
            replacements: Dictionary of value replacements to apply
            config_manager: Configuration manager for accessing transformations
            
        Returns:
            Tuple of (error_message, status) - ("0", "success") if successful
        """
        try:
            self.logger.debug(f"Loading and updating {resource_type} YAML file: {yml_file}")
            
            # Load YAML file
            with open(yml_file, 'r', encoding='utf-8') as file:
                yaml_data = yaml.safe_load(file)
            
            # Backup the original file to proper backup directory
            if backup_yaml_path:
                import shutil
                # Ensure backup directory exists
                if not os.path.exists(backup_yaml_path):
                    os.makedirs(backup_yaml_path)
                    self.logger.debug(f"Created backup directory: {backup_yaml_path}")
                
                # Create backup in the proper backup directory
                backup_file = os.path.join(backup_yaml_path, os.path.basename(yml_file))
                shutil.copy2(yml_file, backup_file)
                self.logger.debug(f"Copied YAML file to backup directory: {backup_file}")
            else:
                # Fallback to old behavior if no backup path provided
                import shutil
                backup_file = yml_file.replace('.yml', '_backup.yml')
                shutil.copy2(yml_file, backup_file)
                self.logger.debug(f"Copied YAML file to backup directory: {backup_file}")
            
            if resource_type == 'job':
                # Add permissions for jobs
                self.logger.debug("Adding permissions to yaml file")
                job_permissions = workflow_manager.get_job_acls(resource_id)
                if job_permissions:
                    yaml_data["resources"]["jobs"][resource_name]['permissions'] = job_permissions
                    self.logger.debug("Permissions added successfully")
                else:
                    self.logger.debug("No permissions found to add")
            elif resource_type == 'pipeline':
                # Add permissions for pipeline
                self.logger.debug("Adding permissions to pipeline yaml file")
                pipeline_permissions = workflow_manager.get_pipeline_acls(resource_id)
                if pipeline_permissions:
                    yaml_data["resources"]["pipelines"][resource_name]['permissions'] = pipeline_permissions
                    self.logger.debug("Pipeline permissions added successfully")
                else:
                    self.logger.debug("No pipeline permissions found to add")
            
            # Apply value replacements
            if replacements:
                self.logger.debug("Replacing keyword variables in yaml file")
                # Apply string replacements to the entire YAML data (same as job processing)
                for pattern, replacement in replacements.items():
                    if not pattern.startswith(r'('):  # Skip regex patterns
                        yaml_data = self.replace_keyword_in_values(yaml_data, pattern, replacement)
                self.logger.debug("Keyword variables replaced successfully")
            
            # Replace null with none
            self.logger.debug("Replacing null with none in yaml file")
            yaml_content_str = yaml.dump(yaml_data, default_flow_style=False, sort_keys=False)
            yaml_content_str = yaml_content_str.replace(': null', ': none')
            yaml_data = yaml.safe_load(yaml_content_str)
            self.logger.debug("Successfully replaced null with none")
            
            # Update paths based on mapping
            if mapping_dict:
                self.logger.debug(f"Updating paths in {resource_type} yaml file")
                self.logger.debug(f"Path mappings: {mapping_dict}")
                yaml_data = self._update_paths_recursively(yaml_data, mapping_dict, resource_type, resource_name)
                self.logger.debug("Successfully updated paths")
            
            # Apply configuration transformations if available
            if config_manager and resource_type == 'job':
                self.logger.debug("Updating cluster conf in yaml file")
                transformations = config_manager.get_spark_conf_transformations()
                yaml_data = self._update_spark_conf_recursively(yaml_data, transformations, resource_name)
                self.logger.debug("Successfully updated cluster conf")
            
            # Write updated YAML
            self.logger.debug(f"Writing yaml file to {modified_yml_file}")
            with open(modified_yml_file, 'w', encoding='utf-8') as file:
                yaml.dump(yaml_data, file, default_flow_style=False, sort_keys=False)
            
            self.logger.debug(f"Successfully processed {resource_type} YAML file: {modified_yml_file}")
            return "0", "success"
            
        except Exception as e:
            error_msg = f"Error processing {resource_type} YAML file: {str(e)}"
            self.logger.error(error_msg)
            return error_msg, "failed"
    
    def _update_paths_recursively(self, obj: Any, mapping_dict: Dict[str, str], 
                                 resource_type: str, resource_name: str) -> Any:
        """
        Recursively update paths in YAML data structure.
        
        Args:
            obj: YAML object (dict, list, or value)
            mapping_dict: Dictionary mapping old paths to new paths
            resource_type: Type of resource ('job' or 'pipeline')
            resource_name: Resource name for logging
            
        Returns:
            Updated YAML object
        """
        if isinstance(obj, dict):
            updated_dict = {}
            for key, value in obj.items():
                if key == 'path' and isinstance(value, str) and value in mapping_dict:
                    updated_dict[key] = mapping_dict[value]
                    self.logger.debug(f"Updated {resource_type} path: {value} -> {mapping_dict[value]}")
                else:
                    updated_dict[key] = self._update_paths_recursively(value, mapping_dict, resource_type, resource_name)
            return updated_dict
        elif isinstance(obj, list):
            return [self._update_paths_recursively(item, mapping_dict, resource_type, resource_name) for item in obj]
        else:
            # Check if this is a path value that needs updating
            if isinstance(obj, str) and obj in mapping_dict:
                self.logger.debug(f"Updated {resource_type} path: {obj} -> {mapping_dict[obj]}")
                return mapping_dict[obj]
            return obj 