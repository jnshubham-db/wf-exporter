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
    
    def replace_keyword_in_values(self, data: Any, old_value: str, new_value: str) -> Any:
        """
        Recursively replaces occurrences of old_value with new_value in a nested dictionary or list.
        
        Args:
            data: The data structure to process
            old_value: The value to replace
            new_value: The replacement value
            
        Returns:
            The processed data structure with replacements made
        """
        if isinstance(data, dict):
            return {key: self.replace_keyword_in_values(value, old_value, new_value) for key, value in data.items()}
        elif isinstance(data, list):
            return [self.replace_keyword_in_values(item, old_value, new_value) for item in data]
        elif isinstance(data, str):
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
            
            self.logger.info(f"Successfully processed YAML file: {modified_yml_file}")
            return "0", "success"
            
        except Exception as e:
            self.logger.error(f"Error updating the YAML file: {str(e)}")
            return str(e), "failed" 