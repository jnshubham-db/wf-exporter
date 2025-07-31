"""
Export processing core module for Databricks Workflow Exporter.

This module contains the main DatabricksExporter class that orchestrates the entire
workflow export process using the Facade pattern. Supports both job and DLT pipeline exports.
"""

import os
import shutil
import logging
from typing import Optional, Tuple, List, Dict, Any

import pandas as pd
import yaml

from ..logging.log_manager import LogManager
from ..cli.cli_manager import DatabricksCliManager
from ..workflow.workflow_extractor import WorkflowExtractor
from ..processing.yaml_serializer import YamlSerializer
from ..processing.export_file_handler import ExportFileHandler
from ..config.config_manager import ConfigManager


class DatabricksExporter:
    """
    A class to manage the end-to-end Databricks resource export workflow.
    
    This class implements the Facade pattern, providing a simplified interface
    to coordinate the complex export process for Databricks jobs and DLT pipelines
    across multiple subsystems.
    
    Handles:
    - CLI setup and authentication
    - Job processing orchestration
    - File operations coordination
    - YAML processing coordination
    - Error handling and logging
    """
    
    def __init__(self, config_path: Optional[str] = None, 
                 databricks_host: Optional[str] = None, 
                 databricks_token: Optional[str] = None,
                 log_level: Optional[str] = None):
        """
        Initialize the DatabricksExporter with required components.
        
        Args:
            config_path: Path to configuration file (optional)
            databricks_host: Databricks workspace URL (optional)
            databricks_token: Databricks access token (optional)
            log_level: Override log level from CLI (optional)
        """
        # Create temporary logger without file handler for config loading
        temp_logger = LogManager(create_file_handler=False, override_log_level=log_level)
        
        # Load configuration with temporary logger
        self.config_manager = ConfigManager(logger=temp_logger, config_path=config_path)
        
        # Set up proper logging with config data (this is the main logger)
        self.logger = LogManager(config_data=self.config_manager.config_data, 
                                override_log_level=log_level)
        
        # Update config manager to use the proper logger
        self.config_manager.logger = self.logger
        
        # Get CLI and profile configuration
        _, _, _, cli_path, config_profile = self.config_manager.get_initial_paths()
        
        # Initialize other components with logger and configuration
        self.cli_manager = DatabricksCliManager(cli_path, config_profile, self.logger)
        self.workflow_manager = WorkflowExtractor(config_profile, self.logger)
        self.yaml_processor = YamlSerializer(self.logger)
        self.file_manager = ExportFileHandler(self.logger, self.config_manager)
        
        # Store credentials for later use
        self.databricks_host = databricks_host
        self.databricks_token = databricks_token
        
        # Set Pandas display options
        self.logger.debug("DatabricksExporter initialized successfully")
        
    def setup(self) -> None:
        """
        Set up Databricks CLI and authentication.
        
        Raises:
            RuntimeError: If any setup step fails
        """
        self.logger.debug("Setting up Databricks CLI configuration...")
        
        # Install or locate CLI based on environment
        if not self.cli_manager.install_cli():
            raise RuntimeError("Failed to install or locate Databricks CLI")
        
        # Verify CLI installation
        if not self.cli_manager.verify_installation():
            raise RuntimeError("Failed to verify Databricks CLI installation")
        
        # Set up authentication
        if not self.cli_manager.setup_authentication(self.databricks_host, self.databricks_token):
            raise RuntimeError("Failed to set up Databricks authentication")
        
        # Test authentication
        if not self.cli_manager.test_authentication():
            raise RuntimeError("Failed to authenticate with Databricks")
        
        self.logger.debug("Databricks CLI setup completed successfully")
    
    
    def _discover_generated_files(self, start_path: str, asset_name: str, asset_type: str) -> List[str]:
        """
        Discover generated YAML and source files from the filesystem.
        
        Args:
            start_path: The base path for file operations
            asset_name: The name of the asset (job or pipeline) for file naming
            asset_type: The type of asset ('job' or 'pipeline')
            
        Returns:
            List of absolute paths to discovered files
        """
        discovered_files = []
        
        # Check for YAML file in resources directory
        resources_path = os.path.join(start_path, 'resources')
        if os.path.exists(resources_path):
            # Look for YAML file associated with the asset
            expected_yaml_file = os.path.join(resources_path, f"{self.file_manager.convert_string(asset_name)}.{asset_type}.yml")
            if os.path.exists(expected_yaml_file):
                discovered_files.append(expected_yaml_file)
                self.logger.debug(f"Found expected YAML file: {expected_yaml_file}")
            else:
                self.logger.warning(f"Could not find expected YAML file: {expected_yaml_file}")
                # Fallback: find any matching .yml file in the resources directory
                for f in os.listdir(resources_path):
                    if f.endswith(f'.{asset_type}.yml'):
                        discovered_files.append(os.path.join(resources_path, f))
                        self.logger.debug(f"Found fallback YAML file: {f}")
        
        # Check for notebooks in src directory
        src_path = os.path.join(start_path, 'src')
        if os.path.exists(src_path):
            for root, _, files in os.walk(src_path):
                for f in files:
                    if f.endswith(('.py', '.sql', '.ipynb')):
                        discovered_files.append(os.path.join(root, f))
        
        self.logger.debug(f"Discovered {len(discovered_files)} generated files for {asset_type}: {asset_name}")
        return discovered_files

    def _clean_existing_files(self, start_path: str, job_name: str, backup: bool = True) -> Optional[str]:
        """
        Clean up existing job files before generating new ones.
        
        Args:
            start_path: The base path for file operations
            job_name: The name of the job (for file naming)
            backup: If True, renames existing YAML to .bak. If False, deletes the backup.
            
        Returns:
            The path to the backup file if created, otherwise None.
        """
        try:
            yaml_file = os.path.join(start_path, 'resources', f"{self.file_manager.convert_string(job_name)}.job.yml")
            backup_file = f"{yaml_file}.bak"
            
            if backup:
                # Rename the existing YAML to a backup file
                if os.path.exists(yaml_file):
                    os.rename(yaml_file, backup_file)
                    self.logger.debug(f"Backed up existing job YAML file to: {backup_file}")
                    return backup_file
            else:
                # Delete the backup file after a successful run
                if os.path.exists(backup_file):
                    os.remove(backup_file)
                    self.logger.debug(f"Removed job YAML backup file: {backup_file}")
            
        except Exception as e:
            self.logger.error(f"Error during job file cleanup/backup: {e}")
        return None

    def _clean_existing_pipeline_files(self, start_path: str, pipeline_name: str, backup: bool = True) -> Optional[str]:
        """
        Clean up existing pipeline files before generating new ones.
        
        Args:
            start_path: The base path for file operations
            pipeline_name: The name of the pipeline (for file naming)
            backup: If True, renames existing YAML to .bak. If False, deletes the backup.
            
        Returns:
            The path to the backup file if created, otherwise None.
        """
        try:
            yaml_file = os.path.join(start_path, 'resources', f"{self.file_manager.convert_string(pipeline_name)}.pipeline.yml")
            backup_file = f"{yaml_file}.bak"
            
            if backup:
                # Rename the existing YAML to a backup file
                if os.path.exists(yaml_file):
                    os.rename(yaml_file, backup_file)
                    self.logger.debug(f"Backed up existing pipeline YAML file to: {backup_file}")
                
                # Also clean up any existing src/ files related to this pipeline
                src_directory = os.path.join(start_path, 'src')
                if os.path.exists(src_directory):
                    self.logger.debug(f"Cleaning up existing src/ files in: {src_directory}")
                    try:
                        # Remove all files in src/ directory to avoid conflicts
                        import shutil
                        for filename in os.listdir(src_directory):
                            file_path = os.path.join(src_directory, filename)
                            if os.path.isfile(file_path):
                                os.remove(file_path)
                                self.logger.debug(f"Removed existing src file: {file_path}")
                            elif os.path.isdir(file_path):
                                shutil.rmtree(file_path)
                                self.logger.debug(f"Removed existing src directory: {file_path}")
                    except Exception as e:
                        self.logger.warning(f"Error cleaning src/ directory: {e}")
                
                return backup_file
            else:
                # Delete the backup file after a successful run
                if os.path.exists(backup_file):
                    os.remove(backup_file)
                    self.logger.debug(f"Removed pipeline YAML backup file: {backup_file}")
            
        except Exception as e:
            self.logger.error(f"Error during pipeline file cleanup/backup: {e}")
        return None

    def _restore_backup_file(self, backup_file: Optional[str]):
        """Restores a backup file if it exists."""
        if backup_file and os.path.exists(backup_file):
            original_file = backup_file.replace('.bak', '')
            os.rename(backup_file, original_file)
            self.logger.warning(f"Restored backup YAML file to: {original_file}")
    
    def _extract_src_paths_from_yaml(self, obj, src_paths: List[str]):
        """
        Recursively extract all paths starting with ../src/ from YAML data structure.
        
        Args:
            obj: YAML object (dict, list, or value)
            src_paths: List to collect found src paths
        """
        if isinstance(obj, dict):
            for key, value in obj.items():
                if key == 'path' and isinstance(value, str) and value.startswith('../src/'):
                    src_paths.append(value)
                elif isinstance(value, (dict, list)):
                    self._extract_src_paths_from_yaml(value, src_paths)
        elif isinstance(obj, list):
            for item in obj:
                self._extract_src_paths_from_yaml(item, src_paths)
    
    def _process_notebook_tasks(self, tasks_data: List[dict], start_path: str) -> List[dict]:
        """
        Process notebook tasks and return artifact information for download.
        
        Args:
            tasks_data: List of task dictionaries
            start_path: Base path for file operations
            
        Returns:
            List of artifacts to download with path mappings
        """
        artifacts = []
        
        for task in tasks_data:
            if task.get('Task_Type') == 'notebook_task' and task.get('Notebook_Path'):
                # Notebooks are handled by bundle generate, so we don't need to download them manually
                # But we still track them for path mapping
                self.logger.debug(f"Found notebook task: {task['Task_Key']} with path {task['Notebook_Path']}")
                
        return artifacts
    
    def _process_spark_python_tasks(self, tasks_data: List[dict], start_path: str) -> List[dict]:
        """
        Process spark python tasks and return artifact information for download.
        
        Args:
            tasks_data: List of task dictionaries
            start_path: Base path for file operations
            
        Returns:
            List of artifacts to download with path mappings
        """
        artifacts = []
        
        for task in tasks_data:
            try:
                if task.get('Task_Type') == 'spark_python_task' and task.get('Python_File'):
                    python_file = task['Python_File']
                    
                    # Ensure python_file is a string
                    if not isinstance(python_file, str) or not python_file.strip():
                        self.logger.warning(f"Skipping invalid python file path: {python_file} (type: {type(python_file)})")
                        continue
                    
                    # Apply path transformations using the same logic as notebook tasks
                    transformed_path = self.file_manager.transform_notebook_path(python_file, {})
                    
                    # Create destination directory based on transformed path
                    dest_subdir = os.path.dirname(transformed_path.replace('../', ''))
                    
                    artifacts.append({
                        'path': python_file,
                        'type': 'py',
                        'destination_subdir': dest_subdir,
                        'task_key': task['Task_Key'],
                        'original_yaml_path': python_file,
                        'relative_yaml_path': transformed_path
                    })
                    
                    self.logger.debug(f"Found spark python task: {task['Task_Key']} with file {python_file} -> {transformed_path}")
            except Exception as e:
                self.logger.error(f"Error processing spark python task {task.get('Task_Key', 'unknown')}: {e}")
                continue
        
        return artifacts
    
    def _process_python_wheel_tasks(self, tasks_data: List[dict], start_path: str) -> List[dict]:
        """
        Process python wheel tasks and return artifact information for download.
        
        Args:
            tasks_data: List of task dictionaries  
            start_path: Base path for file operations
            
        Returns:
            List of artifacts to download with path mappings
        """
        artifacts = []
        
        for task in tasks_data:
            if task.get('Task_Type') == 'python_wheel_task':
                self.logger.debug(f"Found python wheel task: {task['Task_Key']}")
                
                # Process libraries for this task
                for library in task.get('Libraries', []):
                    try:
                        if library.get('type') == 'whl' and library.get('path'):
                            whl_path = library['path']
                            
                            # Ensure whl_path is a string
                            if not isinstance(whl_path, str) or not whl_path.strip():
                                self.logger.warning(f"Skipping invalid whl path: {whl_path} (type: {type(whl_path)})")
                                continue
                            
                            # Apply path transformations using the same logic as notebook tasks
                            transformed_path = self.file_manager.transform_notebook_path(whl_path, {})
                            
                            # Create destination directory based on transformed path
                            dest_subdir = os.path.dirname(transformed_path.replace('../', ''))
                            
                            artifacts.append({
                                'path': whl_path,
                                'type': 'whl',
                                'destination_subdir': dest_subdir,
                                'task_key': task['Task_Key'],
                                'original_yaml_path': whl_path,
                                'relative_yaml_path': transformed_path
                            })
                            
                            self.logger.debug(f"Found wheel library: {whl_path} -> {transformed_path}")
                    except Exception as e:
                        self.logger.error(f"Error processing library for task {task['Task_Key']}: {e}")
                        continue
        
        return artifacts
    
    def _process_sql_tasks(self, tasks_data: List[dict], start_path: str) -> List[dict]:
        """
        Process SQL tasks and return artifact information for download.
        
        Args:
            tasks_data: List of task dictionaries
            start_path: Base path for file operations
            
        Returns:
            List of artifacts to download with path mappings
        """
        artifacts = []
        
        for task in tasks_data:
            try:
                if task.get('Task_Type') == 'sql_task' and task.get('SQL_File'):
                    sql_file = task['SQL_File']
                    
                    # Ensure sql_file is a string
                    if not isinstance(sql_file, str) or not sql_file.strip():
                        self.logger.warning(f"Skipping invalid SQL file path: {sql_file} (type: {type(sql_file)})")
                        continue
                    
                    # Apply path transformations using the same logic as notebook tasks
                    transformed_path = self.file_manager.transform_notebook_path(sql_file, {})
                    
                    # Create destination directory based on transformed path
                    dest_subdir = os.path.dirname(transformed_path.replace('../', ''))
                    
                    artifacts.append({
                        'path': sql_file,
                        'type': 'sql',
                        'destination_subdir': dest_subdir,
                        'task_key': task['Task_Key'],
                        'original_yaml_path': sql_file,
                        'relative_yaml_path': transformed_path
                    })
                    
                    self.logger.debug(f"Found SQL task: {task['Task_Key']} with file {sql_file} -> {transformed_path}")
            except Exception as e:
                self.logger.error(f"Error processing SQL task {task.get('Task_Key', 'unknown')}: {e}")
                continue
        
        return artifacts
    
    def _process_job_environments(self, tasks_data: List[dict], start_path: str) -> List[dict]:
        """
        Process job-level environments (for serverless) and return artifact information for download.
        
        Args:
            tasks_data: List of task dictionaries
            start_path: Base path for file operations
            
        Returns:
            List of artifacts to download with path mappings
        """
        artifacts = []
        
        for task in tasks_data:
            if task.get('Task_Type') == 'job_environment':
                self.logger.debug(f"Found job environment: {task['Environment_Key']}")
                
                # Process libraries for this environment
                for library in task.get('Libraries', []):
                    try:
                        if library.get('type') == 'whl' and library.get('path'):
                            whl_path = library['path']
                            
                            # Ensure whl_path is a string
                            if not isinstance(whl_path, str) or not whl_path.strip():
                                self.logger.warning(f"Skipping invalid environment whl path: {whl_path} (type: {type(whl_path)})")
                                continue
                            
                            # Apply path transformations using the same logic as notebook tasks
                            transformed_path = self.file_manager.transform_notebook_path(whl_path, {})
                            
                            # Create destination directory based on transformed path
                            dest_subdir = os.path.dirname(transformed_path.replace('../', ''))
                            
                            artifacts.append({
                                'path': whl_path,
                                'type': 'whl',
                                'destination_subdir': dest_subdir,
                                'task_key': task['Task_Key'],
                                'original_yaml_path': whl_path,
                                'relative_yaml_path': transformed_path,
                                'environment_key': task['Environment_Key']
                            })
                            
                            self.logger.debug(f"Found environment wheel library: {whl_path} -> {transformed_path}")
                    except Exception as e:
                        self.logger.error(f"Error processing environment library for {task['Environment_Key']}: {e}")
                        continue
        
        return artifacts
    
    def _process_task_libraries(self, tasks_data: List[dict], start_path: str) -> List[dict]:
        """
        Process task-level libraries (job cluster libraries) and return artifact information for download.
        
        Args:
            tasks_data: List of task dictionaries
            start_path: Base path for file operations
            
        Returns:
            List of artifacts to download with path mappings
        """
        artifacts = []
        
        for task in tasks_data:
            # Process libraries for tasks that are not python_wheel_task (those are handled separately)
            if task.get('Task_Type') != 'python_wheel_task' and task.get('Task_Type') != 'job_environment':
                for library in task.get('Libraries', []):
                    try:
                        if library.get('type') == 'whl' and library.get('path'):
                            whl_path = library['path']
                            
                            # Ensure whl_path is a string
                            if not isinstance(whl_path, str) or not whl_path.strip():
                                self.logger.warning(f"Skipping invalid task library whl path: {whl_path} (type: {type(whl_path)})")
                                continue
                            
                            # Apply path transformations using the same logic as notebook tasks
                            transformed_path = self.file_manager.transform_notebook_path(whl_path, {})
                            
                            # Create destination directory based on transformed path
                            dest_subdir = os.path.dirname(transformed_path.replace('../', ''))
                            
                            artifacts.append({
                                'path': whl_path,
                                'type': 'whl',
                                'destination_subdir': dest_subdir,
                                'task_key': task['Task_Key'],
                                'original_yaml_path': whl_path,
                                'relative_yaml_path': transformed_path
                            })
                            
                            self.logger.debug(f"Found task library: {whl_path} -> {transformed_path} for task {task['Task_Key']}")
                    except Exception as e:
                        self.logger.error(f"Error processing task library for {task['Task_Key']}: {e}")
                        continue
        
        return artifacts
    
    def _prepare_file_mapping(self, df: pd.DataFrame, job_id: str, file_paths: List[str], start_path: str) -> pd.DataFrame:
        """
        Prepare file mapping DataFrame.
        
        Args:
            df: DataFrame containing workflow task information
            job_id: The job ID
            file_paths: List of generated file paths
            start_path: The base path for file operations
            start_path: The base path for file operations
            
        Returns:
            DataFrame with file mapping information
        """
        try:
            self.logger.debug(f"_prepare_file_mapping called with job_id: {job_id}")
            self.logger.debug(f"Input DataFrame shape: {df.shape}")
            self.logger.debug(f"file_paths: {file_paths}")
            
            notebook_files = [f for f in file_paths if not f.endswith('.yml')]
            self.logger.debug(f"notebook_files: {notebook_files}")
            
            file_map = {os.path.splitext(os.path.basename(f))[0]: f for f in notebook_files}
            self.logger.debug(f"file_map: {file_map}")

            filtered_df = df[df['JobId'] == int(job_id)].copy()
            self.logger.debug(f"Filtered DataFrame shape: {filtered_df.shape}")

            # Helper function to find the absolute path of the exported file
            def get_exported_path(notebook_path):
                if notebook_path is None:
                    return None
                base_name = os.path.splitext(os.path.basename(notebook_path))[0]
                result = file_map.get(base_name)
                self.logger.debug(f"get_exported_path({notebook_path}) -> base_name: {base_name}, result: {result}")
                return result

            self.logger.debug("Applying get_exported_path function...")
            filtered_df['exported_file_path'] = filtered_df['Notebook_Path'].apply(get_exported_path)
            
            # Drop rows where no matching file was found
            self.logger.debug("Dropping rows with null exported_file_path...")
            filtered_df.dropna(subset=['exported_file_path'], inplace=True)
            self.logger.debug(f"After dropping nulls, DataFrame shape: {filtered_df.shape}")

            # src_directory: The relative path that `bundle generate` writes to the YAML file.
            # This is the key for our replacement map.
            self.logger.debug("Creating src_directory column...")
            filtered_df['src_directory'] = filtered_df['exported_file_path'].apply(
                lambda x: f"../src/{os.path.basename(x)}" if x else None
            )
            
            # dest_directory: The final, transformed path where the file should be moved.
            # This is the value for our replacement map.
            self.logger.debug("Creating dest_directory column...")
            def create_dest_directory(row):
                try:
                    if row['Notebook_Path'] is None or row['exported_file_path'] is None:
                        return None
                    
                    file_dict = {os.path.splitext(os.path.basename(row['Notebook_Path']))[0]: os.path.basename(row['exported_file_path'])}
                    result = self.file_manager.transform_notebook_path(row['Notebook_Path'], file_dict)
                    self.logger.debug(f"transform_notebook_path({row['Notebook_Path']}, {file_dict}) -> {result}")
                    return result
                except Exception as e:
                    self.logger.error(f"Error in create_dest_directory for row: {row}, error: {e}")
                    return None
            
            filtered_df['dest_directory'] = filtered_df.apply(create_dest_directory, axis=1)

            # Drop any rows where dest_directory couldn't be created
            initial_count = len(filtered_df)
            filtered_df.dropna(subset=['dest_directory'], inplace=True)
            final_count = len(filtered_df)
            
            if initial_count != final_count:
                self.logger.warning(f"Dropped {initial_count - final_count} rows due to null dest_directory")

            self.logger.debug(f"Prepared file mapping for {len(filtered_df)} files")
            self.logger.debug(f"Final DataFrame columns: {filtered_df.columns.tolist()}")
            
            # Check for any list values in key columns
            for col in ['src_directory', 'dest_directory']:
                if col in filtered_df.columns:
                    for idx, val in enumerate(filtered_df[col]):
                        if isinstance(val, list):
                            self.logger.error(f"Found list value in {col} at index {idx}: {val}")
                        elif not isinstance(val, str):
                            self.logger.warning(f"Non-string value in {col} at index {idx}: {val} (type: {type(val)})")
            
            return filtered_df
            
        except Exception as e:
            self.logger.error(f"Error in _prepare_file_mapping: {e}")
            self.logger.debug(f"Input df:\n{df}")
            raise
    
    def _validate_folder_structure(self, start_path: str, asset_name: str, asset_type: str) -> bool:
        """
        Validate the existence of essential directories and the final YAML file.
        
        Args:
            start_path: The base path for file operations
            asset_name: The name of the asset (job or pipeline)
            asset_type: The type of asset ('job' or 'pipeline')
            
        Returns:
            True if the structure is valid, False otherwise
        """
        self.logger.debug(f"Validating folder structure for {asset_type}: {asset_name}")
        
        # Check for resources directory
        resources_path = os.path.join(start_path, 'resources')
        if not os.path.exists(resources_path):
            self.logger.error(f"Validation failed: 'resources' directory not found at {start_path}")
            return False
        
        # Check for the final YAML file
        asset_resource_name = self.file_manager.convert_string(asset_name)
        yml_file_path = os.path.join(resources_path, f"{asset_resource_name}.{asset_type}.yml")
        
        if not os.path.exists(yml_file_path):
            self.logger.error(f"Validation failed: Final YAML file not found at {yml_file_path}")
            return False
        
        self.logger.debug(f"✅ Folder structure for {asset_name} is valid.")
        return True
    
    def _log_processing_summary(self, workflow_definition: List[dict], all_artifacts: List[dict], 
                               download_results: List[dict]) -> None:
        """
        Log a comprehensive summary of the processing results.
        
        Args:
            workflow_definition: Original workflow definition
            all_artifacts: All artifacts identified for processing
            download_results: Results of artifact downloads
        """
        try:
            self.logger.debug("=" * 60)
            self.logger.debug("PROCESSING SUMMARY")
            self.logger.debug("=" * 60)
            
            # Task type summary
            task_types = {}
            for task in workflow_definition:
                task_type = task.get('Task_Type', 'unknown')
                task_types[task_type] = task_types.get(task_type, 0) + 1
            
            self.logger.debug("Task Types Processed:")
            for task_type, count in task_types.items():
                self.logger.debug(f"  - {task_type}: {count} task(s)")
            
            # Artifact summary
            artifact_types = {}
            for artifact in all_artifacts:
                artifact_type = artifact.get('type', 'unknown')
                artifact_types[artifact_type] = artifact_types.get(artifact_type, 0) + 1
            
            self.logger.debug("Artifacts Identified:")
            for artifact_type, count in artifact_types.items():
                self.logger.debug(f"  - {artifact_type}: {count} file(s)")
            
            # Download results summary
            if download_results:
                successful = len([r for r in download_results if r['success']])
                failed = len([r for r in download_results if not r['success']])
                self.logger.debug(f"Download Results: {successful} successful, {failed} failed")
                
                if failed > 0:
                    self.logger.warning("Failed downloads:")
                    for result in download_results:
                        if not result['success']:
                            self.logger.warning(f"  - {result['original_path']}: {result['error_message']}")
            
            self.logger.debug("=" * 60)
            
        except Exception as e:
            self.logger.error(f"Error generating processing summary: {str(e)}")
    
    def process_job(self, job_id: str, start_path: str, backup_yaml_path: str,
                   job_status: str) -> Tuple[bool, Optional[Tuple[str, str]]]:
        """
        Process a single job - generate YAML, move files, update YAML.
        
        Args:
            job_id: The Databricks job ID
            start_path: The base path for file operations
            backup_yaml_path: Path for YAML backups
            job_status: Status of the job ('Existing' or 'New')
            
        Returns:
            Tuple of (success, resource_mapping) where resource_mapping is optional
        """
        try:
            self.logger.info(f"Starting job processing for job ID: {job_id}")

            # Get workflow definition and job details (now includes all task types)
            # Get workflow definition and job details (now includes all task types)
            workflow_definition = self.workflow_manager.get_job_workflow_tasks(job_id)
            
            if not workflow_definition:
                self.logger.error(f"No workflow definition found for job ID: {job_id}")
                return False, None

            job_name = workflow_definition[0]['Job_Name']
            
            # Log job details
            self.logger.debug(f"Processing job id: {job_id}, job name: {job_name}")
            self.logger.debug(f"Workflow definition contains {len(workflow_definition)} tasks")
            
            # Backup existing files
            backup_file = self._clean_existing_files(start_path, job_name, backup=True)
            
            # Generate YAML and source files using bundle generate (for notebooks only)
            # Generate YAML and source files using bundle generate (for notebooks only)
            self.logger.debug("Generating YAML and source files...")
            databricks_yml_path = self.config_manager.get_databricks_yml_path()
            output, outcome = self.cli_manager.generate_yaml_src_files_from_job_id(job_id, start_path, databricks_yml_path)
            if outcome == 'failed':
                self.logger.error(f"Error in generating YAML and source files for job id: {job_id}: {output}")
                self._restore_backup_file(backup_file)  # Restore on failure
                return False, None
            
            # Clean up the backup file after a successful run
            self._clean_existing_files(start_path, job_name, backup=False)
            
            # Discover generated files from the filesystem (notebooks from bundle generate)
            file_paths = self._discover_generated_files(start_path, job_name, 'job')
            if not file_paths:
                self.logger.error(f"No files were discovered for job {job_id} after generation.")
                return False, None

            self.logger.debug(f"Discovered {len(file_paths)} files from bundle generate")
            
            # Check export_libraries flag for this workflow
            export_libraries = self.config_manager.get_workflow_export_libraries_flag(job_id)
            self.logger.debug(f"Export libraries flag for job {job_id}: {export_libraries}")
            
            # Process different task types to identify additional artifacts to download
            self.logger.debug("Processing tasks by type to identify artifacts...")
            
            # Process notebook tasks (already handled by bundle generate)
            notebook_artifacts = self._process_notebook_tasks(workflow_definition, start_path)
            
            # Process spark python tasks (.py files)
            python_artifacts = self._process_spark_python_tasks(workflow_definition, start_path)
            
            # Process SQL tasks (.sql files)
            sql_artifacts = self._process_sql_tasks(workflow_definition, start_path)
            
            # Conditionally process library artifacts based on export_libraries flag
            wheel_artifacts = []
            env_artifacts = []
            task_lib_artifacts = []
            
            if export_libraries:
                self.logger.debug("Processing library artifacts (export_libraries is enabled)")
                # Process python wheel tasks (libraries)
                wheel_artifacts = self._process_python_wheel_tasks(workflow_definition, start_path)
                
                # Process job environments (serverless libraries)
                env_artifacts = self._process_job_environments(workflow_definition, start_path)
                
                # Process task-level libraries
                task_lib_artifacts = self._process_task_libraries(workflow_definition, start_path)
            else:
                self.logger.debug("Skipping library artifacts processing (export_libraries is disabled)")
            
            # Combine all artifacts that need to be downloaded
            all_artifacts = (notebook_artifacts + python_artifacts + sql_artifacts + 
                           wheel_artifacts + env_artifacts + task_lib_artifacts)
            
            # Download additional artifacts (non-notebook files)
            download_artifacts = [a for a in all_artifacts if a.get('type') != 'notebook']
            if download_artifacts:
                self.logger.debug(f"Downloading {len(download_artifacts)} additional artifacts...")
                download_results = self.workflow_manager.export_multiple_artifacts(download_artifacts, start_path)
                
                # Log download results
                successful_downloads = [r for r in download_results if r['success']]
                failed_downloads = [r for r in download_results if not r['success']]
                
                self.logger.debug(f"Successfully downloaded {len(successful_downloads)} artifacts")
                if failed_downloads:
                    self.logger.warning(f"Failed to download {len(failed_downloads)} artifacts:")
                    for failed in failed_downloads:
                        self.logger.warning(f"  - {failed['original_path']}: {failed['error_message']}")
            else:
                self.logger.debug("No additional artifacts to download")
                download_results = []
            
            # Convert workflow to DataFrame for notebook processing (backward compatibility)
            try:
                self.logger.debug("Converting workflow definition to DataFrame...")
                self.logger.debug(f"workflow_definition sample: {workflow_definition[:2] if len(workflow_definition) > 0 else 'empty'}")
                
                # Convert lists to strings to avoid "unhashable type: 'list'" error in drop_duplicates()
                workflow_definition_processed = []
                for task in workflow_definition:
                    task_copy = task.copy()
                    # Convert Libraries list to string representation for pandas operations
                    if 'Libraries' in task_copy and isinstance(task_copy['Libraries'], list):
                        task_copy['Libraries'] = str(task_copy['Libraries'])
                    workflow_definition_processed.append(task_copy)
                
                df = pd.DataFrame(workflow_definition_processed)
                self.logger.debug(f"DataFrame columns: {df.columns.tolist()}")
                self.logger.debug(f"DataFrame shape: {df.shape}")
                
                df['JobId'] = df['JobId'].astype('int64')
                df = df.drop_duplicates()
                
                # Filter for notebook tasks only for the existing file mapping logic
                notebook_df = df[df['Notebook_Path'].notnull()]
                self.logger.debug(f"Filtered DataFrame has {len(notebook_df)} rows with valid notebook paths")
                
                # Debug the notebook_df content
                if len(notebook_df) > 0:
                    self.logger.debug(f"Notebook DataFrame sample:\n{notebook_df.head()}")
                    
            except Exception as e:
                self.logger.error(f"Error creating DataFrame from workflow definition: {e}")
                self.logger.debug(f"workflow_definition content: {workflow_definition}")
                return False, None
            
            # Prepare file mapping for notebooks (existing logic)
            if len(notebook_df) > 0:
                self.logger.debug("Preparing file mapping for notebooks...")
                try:
                    filtered_df = self._prepare_file_mapping(notebook_df, job_id, file_paths, start_path)
                    self.logger.debug(f"_prepare_file_mapping returned DataFrame with shape: {filtered_df.shape}")
                    if len(filtered_df) > 0:
                        self.logger.debug(f"Prepared file mapping columns: {filtered_df.columns.tolist()}")
                        self.logger.debug(f"Sample file mapping:\n{filtered_df.head()}")
                except Exception as e:
                    self.logger.error(f"Error in _prepare_file_mapping: {e}")
                    return False, None
                
                # Move notebook files
                self.logger.debug("Moving notebook files to destination directories...")
                output, outcome = self.file_manager.move_files_to_directory(filtered_df, job_id, start_path)
                if outcome == 'failed':
                    self.logger.error(f"Failed to move notebook files for job id: {job_id}: {output}")
                    return False, None
                
                # Create notebook path mapping for YAML update
                try:
                    self.logger.debug("Creating notebook path mapping...")
                    self.logger.debug(f"filtered_df columns: {filtered_df.columns.tolist()}")
                    self.logger.debug(f"filtered_df shape: {filtered_df.shape}")
                    
                    # Debug the data types in the columns
                    src_dirs = filtered_df['src_directory'].tolist()
                    dest_dirs = filtered_df['dest_directory'].tolist()
                    
                    self.logger.debug(f"src_directory sample: {src_dirs[:3] if src_dirs else 'empty'}")
                    self.logger.debug(f"dest_directory sample: {dest_dirs[:3] if dest_dirs else 'empty'}")
                    
                    # Check for any non-string values
                    for i, (src, dest) in enumerate(zip(src_dirs, dest_dirs)):
                        if not isinstance(src, str):
                            self.logger.error(f"Non-string src_directory at index {i}: {src} (type: {type(src)})")
                        if not isinstance(dest, str):
                            self.logger.error(f"Non-string dest_directory at index {i}: {dest} (type: {type(dest)})")
                    
                    src_dest_mapping = dict(zip(filtered_df['src_directory'], filtered_df['dest_directory']))
                    self.logger.debug(f"Successfully created notebook path mapping with {len(src_dest_mapping)} entries")
                    
                except Exception as e:
                    self.logger.error(f"Error creating notebook path mapping: {e}")
                    self.logger.debug(f"filtered_df content:\n{filtered_df}")
                    return False, None
            else:
                self.logger.debug("No notebook files to move")
                src_dest_mapping = {}
            
            # Add mappings for additional artifacts
            self.logger.debug(f"Processing {len(all_artifacts)} additional artifacts for path mapping...")
            for i, artifact in enumerate(all_artifacts):
                try:
                    self.logger.debug(f"Processing artifact {i+1}/{len(all_artifacts)}: {artifact}")
                    original_path = artifact.get('original_yaml_path')
                    relative_path = artifact.get('relative_yaml_path')
                    
                    # Ensure both paths are strings and not None
                    if original_path and relative_path:
                        if isinstance(original_path, str) and isinstance(relative_path, str):
                            src_dest_mapping[original_path] = relative_path
                            self.logger.debug(f"Added artifact mapping: {original_path} -> {relative_path}")
                        else:
                            self.logger.warning(f"Skipping invalid artifact mapping - original_path type: {type(original_path)}, relative_path type: {type(relative_path)}")
                            self.logger.debug(f"  original_path value: {original_path}")
                            self.logger.debug(f"  relative_path value: {relative_path}")
                    else:
                        self.logger.debug(f"Skipping artifact with missing paths - original: {original_path}, relative: {relative_path}")
                except Exception as e:
                    self.logger.error(f"Error processing artifact mapping {i+1}: {e}")
                    self.logger.debug(f"  artifact data: {artifact}")
                    continue
            
            self.logger.debug(f"Final path mapping contains {len(src_dest_mapping)} entries")
            
            job_resource_name = self.file_manager.convert_string(job_name)
            
            # Get YAML file from discovered paths
            yml_files = [file for file in file_paths if file.endswith('.yml')]
            if not yml_files:
                self.logger.error(f"No YAML files found for job id: {job_id}")
                return False, None
            
            yml_file_abs = yml_files[0] # Path is already absolute from discovery method
            self.logger.debug(f"Using YAML file: {yml_file_abs}")
            
            # Check if the YAML file exists
            if not os.path.exists(yml_file_abs):
                self.logger.error(f"YAML file not found: {yml_file_abs}")
                return False, None
            
            # Ensure backup directory exists
            if not os.path.exists(backup_yaml_path):
                os.makedirs(backup_yaml_path)
                self.logger.debug(f"Created backup directory: {backup_yaml_path}")

            # Copy the file to yaml backup directory
            try:
                shutil.copyfile(yml_file_abs, os.path.join(backup_yaml_path, os.path.basename(yml_file_abs)))
                shutil.copyfile(yml_file_abs, os.path.join(backup_yaml_path, os.path.basename(yml_file_abs)))
                self.logger.debug(f"Copied YAML file to backup directory: {yml_file_abs}")
            except Exception as e:
                self.logger.error(f"Failed to copy YAML file: {e}")
                self.logger.debug(f"Source: {yml_file_abs}")
                self.logger.debug(f"Destination: {os.path.join(backup_yaml_path, os.path.basename(yml_file_abs))}")
                self.logger.debug(f"Destination: {os.path.join(backup_yaml_path, os.path.basename(yml_file_abs))}")
                return False, None
            
            # Get replacements and update YAML
            self.logger.debug("Updating YAML file with replacements...")
            replacements = self.config_manager.get_replacements()
            
            # The YAML file is updated in place.
            output, outcome = self.yaml_processor.load_update_dump_yaml(
                self.workflow_manager, yml_file_abs, yml_file_abs, job_id, job_resource_name, 
                src_dest_mapping, replacements, self.config_manager)
            
            if outcome == 'failed':
                self.logger.error(f"Error in updating YAML file for job id: {job_id}: {output}")
                return False, None
            
            self.logger.debug("Successfully updated the YAML file.")

            # Validate folder structure after processing
            if not self._validate_folder_structure(start_path, job_name, 'job'):
                self.logger.error(f"Folder structure validation failed for job {job_id}. Exiting.")
                return False, None

            # Log processing summary
            self._log_processing_summary(workflow_definition, all_artifacts, download_results)

            # Clean up temporary src/ folder files
            self._cleanup_src_folder(start_path)

            # No need for cleanup as we modify in-place
            if job_status == 'Existing':
                self.logger.debug(f"Job {job_id} is existing, returning resource mapping")
                return True, (job_resource_name, job_id)
                
            self.logger.debug(f"Job {job_id} processing completed successfully")
            return True, None
                
        except Exception as e:
            self.logger.error(f"Error processing job {job_id}: {str(e)}")
            return False, None
    
    def save_bind_mappings(self, v_resource_key_job_id_mapping_csv_file_path: str, 
                          list_resource_key_job_id_mapping: List[List[str]]) -> None:
        """
        Save resource key to job ID mappings to CSV file.
        
        Args:
            v_resource_key_job_id_mapping_csv_file_path: Path to save the CSV file
            list_resource_key_job_id_mapping: List of resource mappings
        """
        # Write the mapping of resource key to job id to a csv file
        header = not os.path.exists(v_resource_key_job_id_mapping_csv_file_path)
        if not os.path.exists(os.path.dirname(v_resource_key_job_id_mapping_csv_file_path)):
            os.makedirs(os.path.dirname(v_resource_key_job_id_mapping_csv_file_path))
        df_resource_key_job_id_mapping = pd.DataFrame(list_resource_key_job_id_mapping, columns=['job_key', 'job_id'])
        df_resource_key_job_id_mapping.to_csv(v_resource_key_job_id_mapping_csv_file_path, index=False, mode='a', header=header)
        self.logger.debug(f"Resource key to job ID mappings saved to {v_resource_key_job_id_mapping_csv_file_path}")

    def run_workflow_export(self) -> None:
        """Run the end-to-end workflow processing."""
        # Set up the CLI and authentication
        self.setup()
        
        # Get job arguments from config manager
        active_jobs = self.config_manager.get_active_jobs()
        self.logger.debug(f"List of Job IDs: {active_jobs}")
        
        # Initialize variables from config manager
        start_path, resource_key_job_id_mapping_csv_file_path, backup_yaml_path, _, _ = self.config_manager.get_initial_paths()
        
        self.logger.debug(f"Environment: {self.cli_manager.environment_type}")
        self.logger.debug(f"Using CLI path: {self.cli_manager.cli_path}, Config profile: {self.cli_manager.config_profile}")
        self.logger.debug(f"Start path: {start_path}, Backup YAML path: {backup_yaml_path}")
        
        # Track jobs and resource mappings for logging
        failed_jobs = []
        successful_jobs = []
        resource_mappings = []
        
        # Process each job
        for job_id, job_status in active_jobs:
            try:
                self.logger.debug(f"Starting to process job: {job_id} with status: {job_status}")
                success, resource_mapping = self.process_job(
                    job_id, start_path, backup_yaml_path, job_status)
                
                if success:
                    successful_jobs.append(job_id)
                    if resource_mapping:
                        resource_mappings.append(resource_mapping)
                        self.logger.debug(f"Resource mapping for job {job_id}: {resource_mapping}")
                else:
                    failed_jobs.append(job_id)
            except Exception as e:
                self.logger.error(f"Failed to process job {job_id}: {str(e)}")
                failed_jobs.append(job_id)
        
        # Log final summary
        self.logger.debug(f"Job processing completed. Successful jobs: {successful_jobs}")
        if failed_jobs:
            self.logger.warning(f"Failed jobs: {failed_jobs}")
        if resource_mappings:
            self.save_bind_mappings(resource_key_job_id_mapping_csv_file_path, resource_mappings)
            self.logger.debug(f"Resource mappings: {resource_mappings}")
        
        self.logger.debug(f"Workflow export process completed. {len(successful_jobs)} jobs processed successfully.")
    
    def run_pipeline_export(self) -> None:
        """Run the end-to-end pipeline processing."""
        # Set up the CLI and authentication
        self.setup()
        
        # Get pipeline arguments from config manager
        active_pipelines = self.config_manager.get_active_pipelines()
        self.logger.debug(f"List of Pipeline IDs: {active_pipelines}")
        
        # Initialize variables from config manager
        start_path, resource_key_job_id_mapping_csv_file_path, backup_yaml_path, _, _ = self.config_manager.get_initial_paths()
        
        self.logger.debug(f"Environment: {self.cli_manager.environment_type}")
        self.logger.debug(f"Using CLI path: {self.cli_manager.cli_path}, Config profile: {self.cli_manager.config_profile}")
        self.logger.debug(f"Start path: {start_path}, Backup YAML path: {backup_yaml_path}")
        
        # Track pipelines and resource mappings for logging
        failed_pipelines = []
        successful_pipelines = []
        resource_mappings = []
        
        # Process each pipeline
        for pipeline_id, pipeline_status in active_pipelines:
            try:
                self.logger.debug(f"Starting to process pipeline: {pipeline_id} with status: {pipeline_status}")
                success, resource_mapping = self.process_pipeline(
                    pipeline_id, start_path, backup_yaml_path, pipeline_status)
                
                if success:
                    successful_pipelines.append(pipeline_id)
                    if resource_mapping:
                        resource_mappings.append(resource_mapping)
                        self.logger.debug(f"Resource mapping for pipeline {pipeline_id}: {resource_mapping}")
                else:
                    failed_pipelines.append(pipeline_id)
            except Exception as e:
                self.logger.error(f"Failed to process pipeline {pipeline_id}: {str(e)}")
                failed_pipelines.append(pipeline_id)
        
        # Log final summary
        self.logger.debug(f"Pipeline processing completed. Successful pipelines: {successful_pipelines}")
        if failed_pipelines:
            self.logger.warning(f"Failed pipelines: {failed_pipelines}")
        if resource_mappings:
            # Reuse the same bind mappings function for pipelines
            self.save_bind_mappings(resource_key_job_id_mapping_csv_file_path, resource_mappings)
            self.logger.debug(f"Resource mappings: {resource_mappings}")
        
        self.logger.debug(f"Pipeline export process completed. {len(successful_pipelines)} pipelines processed successfully.")
    
    def process_pipeline(self, pipeline_id: str, start_path: str, backup_yaml_path: str,
                       pipeline_status: str) -> Tuple[bool, Optional[Tuple[str, str]]]:
        """
        Process a single pipeline - generate YAML, move files, update YAML.
        
        Args:
            pipeline_id: The Databricks pipeline ID
            start_path: The base path for file operations
            backup_yaml_path: Path for YAML backups
            pipeline_status: Status of the pipeline ('Existing' or 'New')
            
        Returns:
            Tuple of (success, resource_mapping) where resource_mapping is optional
        """
        try:
            self.logger.info(f"Starting pipeline processing for pipeline ID: {pipeline_id}")

            # Get pipeline details to extract the name (similar to workflow processing)
            pipeline_details = self.workflow_manager.get_pipeline_details(pipeline_id)
            if not pipeline_details:
                self.logger.error(f"No pipeline details found for pipeline ID: {pipeline_id}")
                return False, None
            
            pipeline_name = getattr(pipeline_details.spec, 'name', f"pipeline_{pipeline_id}")
            self.logger.debug(f"Processing pipeline id: {pipeline_id}, pipeline name: {pipeline_name}")

            # Backup existing pipeline files
            backup_file = self._clean_existing_pipeline_files(start_path, pipeline_name, backup=True)

            # Generate YAML and source files using bundle generate
            self.logger.debug("Generating YAML and source files for pipeline...")
            databricks_yml_path = self.config_manager.get_databricks_yml_path()
            output, outcome = self.cli_manager.generate_yaml_src_files_from_pipeline_id(pipeline_id, start_path, databricks_yml_path)
            if outcome == 'failed':
                self.logger.error(f"Error in generating YAML and source files for pipeline '{pipeline_name}' (ID: {pipeline_id}): {output}")
                self._restore_backup_file(backup_file)  # Restore on failure
                return False, None
            
            # Clean up the backup file after a successful run
            self._clean_existing_pipeline_files(start_path, pipeline_name, backup=False)

            self.logger.debug(f"YAML and source files generated successfully for pipeline '{pipeline_name}' (ID: {pipeline_id})")

            # Discover generated files from the filesystem
            file_paths = self._discover_generated_files(start_path, pipeline_name, 'pipeline')
            if not file_paths:
                self.logger.error(f"No files were discovered for pipeline '{pipeline_name}' (ID: {pipeline_id}) after generation.")
                return False, None

            self.logger.debug(f"Discovered {len(file_paths)} files from bundle generate")
            
            # Check export_libraries flag for this pipeline
            export_libraries = self.config_manager.get_pipeline_export_libraries_flag(pipeline_id)
            self.logger.debug(f"Export libraries flag for pipeline '{pipeline_name}' (ID: {pipeline_id}): {export_libraries}")
            
            # Get pipeline library details using the new function (similar to get_job_workflow_tasks)
            self.logger.debug("Retrieving pipeline library definitions...")
            pipeline_libraries = self.workflow_manager.get_pipeline_workflow_tasks(pipeline_id)
            self.logger.debug(f"Retrieved {len(pipeline_libraries)} library definitions for pipeline '{pipeline_name}' (ID: {pipeline_id})")
            
            # Process pipeline libraries for artifacts
            all_artifacts = []
            src_dest_mapping = {}
            
            # Find the pipeline YAML file
            pipeline_yaml_files = [f for f in file_paths if f.endswith('.yml') and 'pipeline' in f.lower()]
            if not pipeline_yaml_files:
                self.logger.error(f"No pipeline YAML file found for pipeline '{pipeline_name}' (ID: {pipeline_id})")
                return False, None
            
            yml_file_abs = pipeline_yaml_files[0]
            self.logger.debug(f"Found pipeline YAML file: {yml_file_abs}")
            
            # Process different library types from pipeline definition based on pipeline type
            # Use root folder-based approach to differentiate lakeflow vs legacy pipelines
            pipeline_artifacts = []
            
            # Count different library types for logging
            notebook_count = 0
            root_folder_count = 0
            file_count = 0
            external_notebook_count = 0
            
            for lib in pipeline_libraries:
                lib_type = lib.get('Library_Type')
                self.logger.debug(f"Processing library type: {lib_type}")
                
                # Process root folder (lakeflow pipelines) - ALWAYS download notebooks in root folder
                if lib_type == 'root_path':
                    root_path = lib.get('Root_Path')
                    if root_path:
                        root_folder_count += 1
                        self.logger.debug(f"Processing lakeflow pipeline with root path: {root_path}")
                        
                        # Apply path transformations to root path (same as other artifacts)
                        transformed_root_path = self.file_manager.transform_notebook_path(root_path, {})
                        # Remove the ../ prefix to get the local directory structure
                        local_root_subdir = transformed_root_path.replace('../', '')
                        local_root_dir = os.path.join(start_path, local_root_subdir)
                        
                        self.logger.debug(f"Root path transformation: {root_path} -> {local_root_subdir}")
                        
                        # Download entire root folder
                        try:
                            # Download all files in root folder to the transformed path
                            root_folder_files = self.workflow_manager.download_root_folder(root_path, local_root_dir)
                            
                            # Create artifacts for each downloaded file to enable path mapping
                            for downloaded_file in root_folder_files:
                                if downloaded_file.get('success'):
                                    original_path = downloaded_file['original_path']
                                    local_path = downloaded_file['local_path']
                                    
                                    # Create relative path for YAML mapping
                                    # The relative path should be relative to start_path
                                    try:
                                        relative_to_start = os.path.relpath(local_path, start_path)
                                        # Convert to ../src/ format for YAML
                                        yaml_relative_path = f"../src/{relative_to_start}"
                                        
                                        # For notebooks without extension, ensure the YAML path also has .ipynb
                                        if (downloaded_file.get('artifact_type') == 'notebook' and 
                                            not original_path.endswith(('.py', '.sql', '.ipynb')) and
                                            local_path.endswith('.ipynb')):
                                            # The YAML should reference the .ipynb file
                                            pass  # yaml_relative_path already correct with .ipynb
                                        
                                        pipeline_artifacts.append({
                                            'original_path': original_path,
                                            'local_path': local_path,
                                            'relative_yaml_path': yaml_relative_path,
                                            'success': True,
                                            'category': 'root_path'
                                        })
                                        self.logger.debug(f"Root folder file mapped: {original_path} -> {yaml_relative_path}")
                                        
                                    except Exception as e:
                                        self.logger.warning(f"Error creating path mapping for {original_path}: {e}")
                            
                            self.logger.debug(f"Root path processed: {len([f for f in root_folder_files if f.get('success')])}/{len(root_folder_files)} files downloaded to {local_root_dir}")
                            
                        except Exception as e:
                            self.logger.error(f"Error processing root path {root_path}: {e}")
                
                # Process external notebooks (outside root folder in lakeflow pipelines) - ALWAYS download
                elif lib_type == 'external_notebook':
                    notebook_path = lib.get('Notebook_Path')
                    if notebook_path:
                        external_notebook_count += 1
                        # Transform the path using existing logic
                        transformed_path = self.file_manager.transform_notebook_path(notebook_path, {})
                        dest_subdir = os.path.dirname(transformed_path.replace('../', ''))
                        
                        pipeline_artifacts.append({
                            'path': notebook_path,
                            'type': 'py' if notebook_path.endswith('.py') else 'sql' if notebook_path.endswith('.sql') else 'notebook',
                            'destination_subdir': dest_subdir,
                            'relative_yaml_path': transformed_path,
                            'category': 'external_notebook'
                        })
                        self.logger.debug(f"Added external notebook: {notebook_path} -> {dest_subdir}")
                
                # Process notebook libraries (legacy pipelines) - ALWAYS download
                elif lib_type == 'notebook_library':
                    notebook_path = lib.get('Notebook_Path')
                    if notebook_path:
                        notebook_count += 1
                        # Transform the path using existing logic
                        transformed_path = self.file_manager.transform_notebook_path(notebook_path, {})
                        dest_subdir = os.path.dirname(transformed_path.replace('../', ''))
                        
                        pipeline_artifacts.append({
                            'path': notebook_path,
                            'type': 'py' if notebook_path.endswith('.py') else 'sql' if notebook_path.endswith('.sql') else 'notebook',
                            'destination_subdir': dest_subdir,
                            'relative_yaml_path': transformed_path,
                            'category': 'notebook_library'
                        })
                        self.logger.debug(f"Added notebook library: {notebook_path} -> {dest_subdir}")
                
                # Process glob libraries (legacy pipelines) - ALWAYS download notebooks, conditionally download libraries
                elif lib_type == 'glob_library':
                    glob_pattern = lib.get('Glob_Pattern')
                    glob_files = lib.get('Glob_Files', [])
                    self.logger.debug(f"Processing glob pattern '{glob_pattern}' with {len(glob_files)} files")
                    
                    for file_path in glob_files:
                        # Check if this is a notebook file or library file
                        is_notebook = file_path.endswith(('.py', '.sql', '.ipynb'))
                        is_library = file_path.endswith(('.whl', '.jar'))
                        
                        # Always process notebook files, only process library files if export_libraries is enabled
                        if is_notebook or (is_library and export_libraries):
                            # Transform the path using existing logic
                            transformed_path = self.file_manager.transform_notebook_path(file_path, {})
                            dest_subdir = os.path.dirname(transformed_path.replace('../', ''))
                            
                            pipeline_artifacts.append({
                                'path': file_path,
                                'type': 'py' if file_path.endswith('.py') else 'sql' if file_path.endswith('.sql') else 'whl' if file_path.endswith('.whl') else 'jar' if file_path.endswith('.jar') else 'notebook',
                                'destination_subdir': dest_subdir,
                                'relative_yaml_path': transformed_path,
                                'category': 'glob_library'
                            })
                            self.logger.debug(f"Added glob file: {file_path} -> {dest_subdir}")
                        elif is_library and not export_libraries:
                            self.logger.debug(f"Skipping library file (export_libraries disabled): {file_path}")
                
                # Process file libraries (wheels, jars, etc.) - ONLY if export_libraries is enabled
                elif lib_type in ['file_library', 'whl_library', 'jar_library', 'environment_dependencies']:
                    if export_libraries:
                        libraries = lib.get('Libraries', [])
                        for library in libraries:
                            lib_path = library.get('path')
                            lib_artifact_type = library.get('type')
                            if lib_path and lib_artifact_type:
                                file_count += 1
                                # Transform the path using existing logic
                                transformed_path = self.file_manager.transform_notebook_path(lib_path, {})
                                dest_subdir = os.path.dirname(transformed_path.replace('../', ''))
                                
                                pipeline_artifacts.append({
                                    'path': lib_path,
                                    'type': lib_artifact_type,
                                    'destination_subdir': dest_subdir,
                                    'relative_yaml_path': transformed_path,
                                    'category': f'{lib_artifact_type}_library'
                                })
                                self.logger.debug(f"Added {lib_artifact_type} library: {lib_path} -> {dest_subdir}")
                    else:
                        libraries = lib.get('Libraries', [])
                        skipped_count = len(libraries)
                        if skipped_count > 0:
                            self.logger.debug(f"Skipping {skipped_count} library files (export_libraries disabled)")
            
            # Log summary of what was found
            if root_folder_count > 0:
                # Lakeflow pipeline
                self.logger.debug(f"Lakeflow pipeline summary: {root_folder_count} root folders, {external_notebook_count} external notebooks, {file_count} library files")
            else:
                # Legacy pipeline
                if export_libraries:
                    self.logger.debug(f"Legacy pipeline summary: {notebook_count} notebooks, {file_count} library files (export_libraries enabled)")
                else:
                    self.logger.debug(f"Legacy pipeline summary: {notebook_count} notebooks, 0 library files (export_libraries disabled - notebooks only)")
            
            # For non-root folder artifacts, use the existing export_multiple_artifacts method
            non_root_artifacts = [a for a in pipeline_artifacts if a.get('category') != 'root_path' and not a.get('success', False)]
            root_artifacts = [a for a in pipeline_artifacts if a.get('category') == 'root_path']
            
            if non_root_artifacts:
                self.logger.debug(f"Downloading {len(non_root_artifacts)} individual pipeline artifacts...")
                downloaded_artifacts = self.workflow_manager.export_multiple_artifacts(non_root_artifacts, start_path)
                
                # Combine root folder artifacts with downloaded artifacts
                all_artifacts = root_artifacts + downloaded_artifacts
            else:
                all_artifacts = root_artifacts
            
            if all_artifacts:
                # Log download results
                successful_artifacts = [a for a in all_artifacts if a.get('success', False)]
                self.logger.debug(f"Pipeline artifact download summary: {len(successful_artifacts)}/{len(all_artifacts)} artifacts processed successfully")
                
                # Log failed artifacts for troubleshooting
                failed_artifacts = [a for a in all_artifacts if not a.get('success', False)]
                if failed_artifacts:
                    self.logger.warning(f"Failed to download {len(failed_artifacts)} artifacts:")
                    for artifact in failed_artifacts[:5]:  # Log first 5 failed artifacts
                        self.logger.warning(f"  - {artifact.get('original_path', 'unknown')}: {artifact.get('error_message', 'unknown error')}")
            else:
                all_artifacts = []
                self.logger.debug("No pipeline artifacts found to download")
            
            # Create path mappings for downloaded artifacts
            # For pipelines, we need to map from the generated YAML paths (like ../src/file.sql) 
            # to the actual downloaded paths (like ../Users/user/folder/file.sql)
            
            # Check if this is a Lakeflow pipeline (has root_path)
            is_lakeflow_pipeline = len(root_artifacts) > 0
            if is_lakeflow_pipeline:
                self.logger.debug("Detected Lakeflow pipeline with root_path - skipping individual path mapping")
            
            # First, read the generated pipeline YAML to get the src paths
            pipeline_yaml_files = [f for f in file_paths if f.endswith('.yml') and 'pipeline' in f.lower()]
            if pipeline_yaml_files:
                try:
                    yml_file_abs = pipeline_yaml_files[0]
                    self.logger.debug(f"Reading generated pipeline YAML to extract src paths: {yml_file_abs}")
                    
                    import yaml
                    with open(yml_file_abs, 'r') as file:
                        yaml_data = yaml.safe_load(file)
                    
                    # Extract all paths from the YAML that start with ../src/
                    src_paths = []
                    self._extract_src_paths_from_yaml(yaml_data, src_paths)
                    self.logger.debug(f"Found {len(src_paths)} src paths in YAML: {src_paths}")
                    
                    # Create mapping from src paths to downloaded artifact paths
                    # Only for legacy pipelines - Lakeflow pipelines don't need path mapping
                    if not is_lakeflow_pipeline:
                        for src_path in src_paths:
                            # src_path looks like: ../src/amtrak_pipeline_code.sql
                            # We need to find the corresponding downloaded artifact
                            
                            # Extract filename with extension from src path
                            src_filename = os.path.basename(src_path)  # amtrak_pipeline_code.sql
                            src_name_without_ext = os.path.splitext(src_filename)[0]  # amtrak_pipeline_code
                            src_extension = os.path.splitext(src_filename)[1]  # .sql
                            
                            self.logger.debug(f"Processing src path: {src_path} -> filename: {src_filename}, extension: {src_extension}")
                            
                            # Find matching downloaded artifact by matching the original workspace path
                            # (since the original path in workspace might not have extension)
                            for artifact in all_artifacts:
                                if artifact.get('success') and artifact.get('local_path'):
                                    original_path = artifact.get('original_path', '')
                                    local_path = artifact.get('local_path', '')
                                    
                                    # Check if this artifact matches the src file
                                    # Match by filename (with or without extension)
                                    original_basename = os.path.basename(original_path)
                                    original_name_without_ext = os.path.splitext(original_basename)[0]
                                    
                                    if (src_name_without_ext == original_name_without_ext or 
                                        src_filename == original_basename):
                                        
                                        # Create the correct downloaded path with proper extension
                                        relative_to_start = os.path.relpath(local_path, start_path)
                                        downloaded_yaml_path = f"../{relative_to_start}"
                                        
                                        # If the YAML expects a specific extension, ensure the downloaded file has it
                                        if src_extension and not downloaded_yaml_path.endswith(src_extension):
                                            # Update the downloaded path to match the YAML extension
                                            downloaded_yaml_path = os.path.splitext(downloaded_yaml_path)[0] + src_extension
                                            
                                            # Also rename the actual downloaded file if needed
                                            new_local_path = os.path.splitext(local_path)[0] + src_extension
                                            if local_path != new_local_path and os.path.exists(local_path):
                                                try:
                                                    os.rename(local_path, new_local_path)
                                                    self.logger.debug(f"Renamed downloaded file to match YAML extension: {local_path} -> {new_local_path}")
                                                    artifact['local_path'] = new_local_path
                                                except Exception as e:
                                                    self.logger.warning(f"Failed to rename file {local_path} to {new_local_path}: {e}")
                                        
                                        src_dest_mapping[src_path] = downloaded_yaml_path
                                        self.logger.debug(f"Mapped YAML path: {src_path} -> {downloaded_yaml_path}")
                                        break
                            
                            if src_path not in src_dest_mapping:
                                self.logger.warning(f"No downloaded artifact found for YAML src path: {src_path}")
                    else:
                        self.logger.debug("Lakeflow pipeline: Skipping individual file path mapping - root folder structure preserved")
                
                except Exception as e:
                    self.logger.error(f"Error reading pipeline YAML for path mapping: {e}")
            
            # Fallback: if no YAML-based mapping was created, use the original artifact mapping
            if not src_dest_mapping:
                self.logger.debug("No YAML-based mapping created, using fallback artifact mapping")
                for artifact in all_artifacts:
                    if artifact.get('success') and artifact.get('original_path') and artifact.get('relative_yaml_path'):
                        src_dest_mapping[artifact['original_path']] = artifact['relative_yaml_path']
            
            # Update YAML file with path mappings if needed
            if src_dest_mapping or is_lakeflow_pipeline:
                # Determine what type of update to perform
                if is_lakeflow_pipeline:
                    self.logger.debug("Updating Lakeflow pipeline YAML file with value replacements only (no path mapping)")
                    # For Lakeflow pipelines, use empty path mapping but still apply value replacements
                    path_mapping = {}
                else:
                    self.logger.debug(f"Updating legacy pipeline YAML file with {len(src_dest_mapping)} path mappings...")
                    path_mapping = src_dest_mapping
                
                # Ensure backup directory exists
                if not os.path.exists(backup_yaml_path):
                    os.makedirs(backup_yaml_path)
                    self.logger.debug(f"Created backup directory: {backup_yaml_path}")

                # Copy the file to yaml backup directory (same as workflows)
                try:
                    shutil.copyfile(yml_file_abs, os.path.join(backup_yaml_path, os.path.basename(yml_file_abs)))
                    self.logger.debug(f"Copied pipeline YAML file to backup directory: {yml_file_abs}")
                except Exception as e:
                    self.logger.error(f"Failed to copy pipeline YAML file: {e}")
                    self.logger.debug(f"Source: {yml_file_abs}")
                    self.logger.debug(f"Destination: {os.path.join(backup_yaml_path, os.path.basename(yml_file_abs))}")
                    return False, None
                
                # Get replacements from config
                replacements = self.config_manager.get_replacements()
                
                # Use the actual pipeline name for resource naming (similar to job processing)
                pipeline_resource_name = self.file_manager.convert_string(pipeline_name)
                
                # Update YAML file
                output, outcome = self.yaml_processor.load_update_dump_yaml_generic(
                    self.workflow_manager, yml_file_abs, yml_file_abs, pipeline_id, 
                    pipeline_resource_name, "pipeline", path_mapping, replacements, self.config_manager, backup_yaml_path)
                
                if outcome == 'failed':
                    self.logger.error(f"Error updating pipeline YAML file: {output}")
                    return False, None
                
                self.logger.debug("Successfully updated pipeline YAML file.")
            
            # Log artifact processing summary
            if all_artifacts:
                successful_artifacts = [a for a in all_artifacts if a.get('success', False)]
                self.logger.debug(f"Pipeline artifact processing summary: {len(successful_artifacts)}/{len(all_artifacts)} artifacts processed successfully")
                
                # Log failed artifacts for troubleshooting
                failed_artifacts = [a for a in all_artifacts if not a.get('success', False)]
                if failed_artifacts:
                    self.logger.warning(f"Failed to download {len(failed_artifacts)} artifacts:")
                    for artifact in failed_artifacts[:5]:  # Log first 5 failed artifacts
                        self.logger.warning(f"  - {artifact.get('path', 'unknown')}: {artifact.get('error', 'unknown error')}")
            
            # Validate folder structure
            if not self._validate_folder_structure(start_path, pipeline_name, 'pipeline'):
                self.logger.error(f"Folder structure validation failed for pipeline '{pipeline_name}' (ID: {pipeline_id}). Exiting.")
                return False, None

            # Clean up temporary src/ folder files
            self._cleanup_src_folder(start_path)

            self.logger.debug(f"Pipeline '{pipeline_name}' (ID: {pipeline_id}) processing completed successfully")
            
            # Return pipeline resource mapping if needed
            pipeline_resource_name = self.file_manager.convert_string(pipeline_name)
            return True, (pipeline_resource_name, pipeline_id)

        except Exception as e:
            self.logger.error(f"Error processing pipeline {pipeline_id}: {str(e)}")
            return False, None

    def _analyze_pipeline_type(self, yml_file_path: str) -> str:
        """
        Analyze pipeline YAML to determine if it's legacy (extracted notebooks) or glob-based.
        
        Args:
            yml_file_path: Path to the pipeline YAML file
            
        Returns:
            str: "legacy", "glob", or "unknown"
        """
        try:
            import yaml
            with open(yml_file_path, 'r', encoding='utf-8') as f:
                yaml_content = yaml.safe_load(f)
            
            # Look for glob patterns in libraries
            if self._contains_glob_patterns(yaml_content):
                return "glob"
            
            # Look for extracted notebook references (../src/ paths)
            if self._contains_extracted_notebooks(yaml_content):
                return "legacy"
            
            # Default to unknown if no clear pattern is found
            return "unknown"
            
        except Exception as e:
            self.logger.error(f"Error analyzing pipeline type: {e}")
            return "unknown"
    
    def _contains_glob_patterns(self, yaml_content: dict) -> bool:
        """Check if YAML contains glob patterns."""
        def search_for_glob(obj):
            if isinstance(obj, dict):
                if 'glob' in obj:
                    return True
                for value in obj.values():
                    if search_for_glob(value):
                        return True
            elif isinstance(obj, list):
                for item in obj:
                    if search_for_glob(item):
                        return True
            return False
        
        return search_for_glob(yaml_content)
    
    def _contains_extracted_notebooks(self, yaml_content: dict) -> bool:
        """Check if YAML contains extracted notebook references (../src/ paths)."""
        def search_for_extracted(obj):
            if isinstance(obj, dict):
                for key, value in obj.items():
                    if key == 'path' and isinstance(value, str) and '../src/' in value:
                        return True
                    if search_for_extracted(value):
                        return True
            elif isinstance(obj, list):
                for item in obj:
                    if search_for_extracted(item):
                        return True
            return False
        
        return search_for_extracted(yaml_content)
    
    def _process_legacy_pipeline(self, pipeline_id: str, file_paths: List[str], start_path: str, export_libraries: bool) -> Tuple[bool, Dict[str, str]]:
        """
        Process legacy pipeline with extracted notebooks.
        
        Args:
            pipeline_id: Pipeline ID
            file_paths: List of generated file paths
            start_path: Base path for operations
            export_libraries: Whether to process library artifacts
            
        Returns:
            Tuple of (success, path_mappings)
        """
        try:
            path_mappings = {}
            
            # Find notebook files in src directory
            src_files = [f for f in file_paths if '/src/' in f and (f.endswith('.py') or f.endswith('.sql'))]
            self.logger.debug(f"Found {len(src_files)} extracted notebooks in src directory")
            
            for src_file in src_files:
                try:
                    # Get the base filename
                    filename = os.path.basename(src_file)
                    base_name = os.path.splitext(filename)[0]
                    
                    # Use SDK to find the original workspace path for this notebook
                    original_workspace_path = self._find_original_notebook_path(pipeline_id, base_name)
                    
                    if original_workspace_path:
                        # Transform the workspace path using the same logic as workflows
                        transformed_path = self.file_manager.transform_notebook_path(original_workspace_path, {base_name: filename})
                        
                        # Create destination directory based on transformed path
                        dest_dir = os.path.dirname(transformed_path.replace('../', ''))
                        dest_path = os.path.join(start_path, dest_dir)
                        
                        # Ensure destination directory exists
                        os.makedirs(dest_path, exist_ok=True)
                        
                        # Move the file from src to the correct location
                        dest_file_path = os.path.join(dest_path, filename)
                        if os.path.exists(src_file):
                            import shutil
                            shutil.move(src_file, dest_file_path)
                            self.logger.debug(f"Moved {src_file} to {dest_file_path}")
                        
                        # Add to path mappings
                        src_relative_path = f"../src/{filename}"
                        path_mappings[src_relative_path] = transformed_path
                        self.logger.debug(f"Added legacy mapping: {src_relative_path} -> {transformed_path}")
                    else:
                        self.logger.warning(f"Could not find original workspace path for notebook: {base_name}")
                        
                except Exception as e:
                    self.logger.error(f"Error processing legacy notebook {src_file}: {e}")
                    continue
            
            # Process library artifacts if enabled
            if export_libraries:
                library_mappings = self._process_pipeline_libraries(pipeline_id, start_path)
                path_mappings.update(library_mappings)
            
            self.logger.debug(f"Legacy pipeline processing completed with {len(path_mappings)} path mappings")
            return True, path_mappings
            
        except Exception as e:
            self.logger.error(f"Error processing legacy pipeline: {e}")
            return False, {}
    
    def _find_original_notebook_path(self, pipeline_id: str, notebook_name: str) -> Optional[str]:
        """
        Use Databricks SDK to find the original workspace path for a notebook.
        
        Args:
            pipeline_id: Pipeline ID
            notebook_name: Base name of the notebook
            
        Returns:
            Original workspace path or None if not found
        """
        try:
            # Get pipeline details to find notebook references
            pipeline_details = self.workflow_manager.client.pipelines.get(pipeline_id=pipeline_id)
            
            # Search through pipeline libraries for notebook references
            if hasattr(pipeline_details, 'spec') and hasattr(pipeline_details.spec, 'libraries'):
                for library in pipeline_details.spec.libraries:
                    if hasattr(library, 'notebook') and hasattr(library.notebook, 'path'):
                        notebook_path = library.notebook.path
                        if notebook_name in os.path.basename(notebook_path):
                            self.logger.debug(f"Found original path for {notebook_name}: {notebook_path}")
                            return notebook_path
            
            self.logger.debug(f"Could not find original path for notebook: {notebook_name}")
            return None
            
        except Exception as e:
            self.logger.error(f"Error finding original notebook path: {e}")
            return None
    
    def _process_pipeline_libraries(self, pipeline_id: str, start_path: str) -> Dict[str, str]:
        """
        Process pipeline library dependencies.
        
        Args:
            pipeline_id: Pipeline ID
            start_path: Base path for operations
            
        Returns:
            Dictionary of path mappings for libraries
        """
        try:
            path_mappings = {}
            
            # Get pipeline details
            pipeline_details = self.workflow_manager.client.pipelines.get(pipeline_id=pipeline_id)
            
            # Process environment dependencies if available
            if (hasattr(pipeline_details, 'spec') and 
                hasattr(pipeline_details.spec, 'environment') and 
                hasattr(pipeline_details.spec.environment, 'dependencies')):
                
                for dependency in pipeline_details.spec.environment.dependencies:
                    if isinstance(dependency, str) and dependency.endswith('.whl'):
                        try:
                            # Download the library file
                            success, local_path, error_msg = self.workflow_manager.export_artifact(
                                dependency, start_path, 'whl')
                            
                            if success:
                                # Transform the path
                                transformed_path = self.file_manager.transform_notebook_path(dependency, {})
                                path_mappings[dependency] = transformed_path
                                self.logger.debug(f"Added library mapping: {dependency} -> {transformed_path}")
                            else:
                                self.logger.warning(f"Failed to download library {dependency}: {error_msg}")
                                
                        except Exception as e:
                            self.logger.error(f"Error processing library {dependency}: {e}")
                            continue
            
            return path_mappings
            
        except Exception as e:
            self.logger.error(f"Error processing pipeline libraries: {e}")
            return {}
    
    def _process_pipeline_notebook_libraries(self, pipeline_libraries: List[Dict[str, Any]], start_path: str) -> List[Dict[str, Any]]:
        """
        Process notebook libraries from pipeline definition.
        
        Args:
            pipeline_libraries: List of pipeline library definitions
            start_path: Base path for operations
            
        Returns:
            List of artifact processing results
        """
        artifacts = []
        
        for lib in pipeline_libraries:
            if lib.get('Library_Type') == 'notebook_library':
                notebook_path = lib.get('Notebook_Path')
                if notebook_path:
                    try:
                        # Determine artifact type
                        artifact_type = 'py' if notebook_path.endswith('.py') else 'sql' if notebook_path.endswith('.sql') else 'notebook'
                        
                        # Transform the path using existing logic
                        transformed_path = self.file_manager.transform_notebook_path(notebook_path, {})
                        dest_subdir = os.path.dirname(transformed_path.replace('../', ''))
                        local_directory = os.path.join(start_path, dest_subdir) if dest_subdir else start_path
                        
                        # Download the notebook
                        success, local_path, error_msg = self.workflow_manager.export_artifact(
                            notebook_path, local_directory, artifact_type)
                        
                        artifacts.append({
                            'original_path': notebook_path,
                            'local_path': local_path if success else '',
                            'relative_yaml_path': transformed_path,
                            'type': artifact_type,
                            'success': success,
                            'error': error_msg if not success else '',
                            'category': 'notebook_library'
                        })
                        
                        if success:
                            self.logger.debug(f"Downloaded notebook library: {notebook_path} -> {local_path}")
                        else:
                            self.logger.warning(f"Failed to download notebook library {notebook_path}: {error_msg}")
                            
                    except Exception as e:
                        self.logger.error(f"Error processing notebook library {notebook_path}: {e}")
                        artifacts.append({
                            'original_path': notebook_path,
                            'success': False,
                            'error': str(e),
                            'category': 'notebook_library'
                        })
        
        return artifacts
    
    def _process_pipeline_glob_libraries(self, pipeline_libraries: List[Dict[str, Any]], start_path: str) -> List[Dict[str, Any]]:
        """
        Process glob libraries from pipeline definition.
        
        Args:
            pipeline_libraries: List of pipeline library definitions
            start_path: Base path for operations
            
        Returns:
            List of artifact processing results
        """
        artifacts = []
        
        for lib in pipeline_libraries:
            if lib.get('Library_Type') == 'glob_library':
                glob_pattern = lib.get('Glob_Pattern')
                glob_files = lib.get('Glob_Files', [])
                
                if glob_pattern and glob_files:
                    self.logger.debug(f"Processing glob pattern '{glob_pattern}' with {len(glob_files)} files")
                    
                    for file_path in glob_files:
                        try:
                            # Determine artifact type
                            artifact_type = 'py' if file_path.endswith('.py') else 'sql' if file_path.endswith('.sql') else 'notebook'
                            
                            # Create destination directory preserving workspace structure
                            if '/Workspace/' in file_path:
                                relative_path = file_path.replace('/Workspace/', '')
                            else:
                                relative_path = file_path.lstrip('/')
                            
                            dest_subdir = os.path.dirname(relative_path)
                            local_directory = os.path.join(start_path, dest_subdir) if dest_subdir else start_path
                            
                            # Download the file
                            success, local_path, error_msg = self.workflow_manager.export_artifact(
                                file_path, local_directory, artifact_type)
                            
                            # For glob patterns, we typically preserve original paths in YAML
                            # So we don't create path mappings for these
                            artifacts.append({
                                'original_path': file_path,
                                'local_path': local_path if success else '',
                                'type': artifact_type,
                                'success': success,
                                'error': error_msg if not success else '',
                                'category': 'glob_library',
                                'glob_pattern': glob_pattern
                            })
                            
                            if success:
                                self.logger.debug(f"Downloaded glob file: {file_path} -> {local_path}")
                            else:
                                self.logger.warning(f"Failed to download glob file {file_path}: {error_msg}")
                                
                        except Exception as e:
                            self.logger.error(f"Error processing glob file {file_path}: {e}")
                            artifacts.append({
                                'original_path': file_path,
                                'success': False,
                                'error': str(e),
                                'category': 'glob_library',
                                'glob_pattern': glob_pattern
                            })
        
        return artifacts
    
    def _process_pipeline_file_libraries(self, pipeline_libraries: List[Dict[str, Any]], start_path: str) -> List[Dict[str, Any]]:
        """
        Process file libraries (wheels, jars) from pipeline definition.
        
        Args:
            pipeline_libraries: List of pipeline library definitions
            start_path: Base path for operations
            
        Returns:
            List of artifact processing results
        """
        artifacts = []
        
        for lib in pipeline_libraries:
            if lib.get('Library_Type') in ['file_library', 'whl_library', 'jar_library']:
                libraries = lib.get('Libraries', [])
                
                for library in libraries:
                    lib_path = library.get('path')
                    lib_type = library.get('type')
                    
                    if lib_path and lib_type:
                        try:
                            # Transform the path using existing logic
                            transformed_path = self.file_manager.transform_notebook_path(lib_path, {})
                            dest_subdir = os.path.dirname(transformed_path.replace('../', ''))
                            local_directory = os.path.join(start_path, dest_subdir) if dest_subdir else start_path
                            
                            # Download the library file
                            success, local_path, error_msg = self.workflow_manager.export_artifact(
                                lib_path, local_directory, lib_type)
                            
                            artifacts.append({
                                'original_path': lib_path,
                                'local_path': local_path if success else '',
                                'relative_yaml_path': transformed_path,
                                'type': lib_type,
                                'success': success,
                                'error': error_msg if not success else '',
                                'category': f'{lib_type}_library'
                            })
                            
                            if success:
                                self.logger.debug(f"Downloaded {lib_type} library: {lib_path} -> {local_path}")
                            else:
                                self.logger.warning(f"Failed to download {lib_type} library {lib_path}: {error_msg}")
                                
                        except Exception as e:
                            self.logger.error(f"Error processing {lib_type} library {lib_path}: {e}")
                            artifacts.append({
                                'original_path': lib_path,
                                'success': False,
                                'error': str(e),
                                'category': f'{lib_type}_library'
                            })
        
        return artifacts
    
    def _process_pipeline_environment_libraries(self, pipeline_libraries: List[Dict[str, Any]], start_path: str) -> List[Dict[str, Any]]:
        """
        Process environment dependencies from pipeline definition.
        
        Args:
            pipeline_libraries: List of pipeline library definitions
            start_path: Base path for operations
            
        Returns:
            List of artifact processing results
        """
        artifacts = []
        
        for lib in pipeline_libraries:
            if lib.get('Library_Type') == 'environment_dependencies':
                libraries = lib.get('Libraries', [])
                
                for library in libraries:
                    lib_path = library.get('path')
                    lib_type = library.get('type')
                    
                    if lib_path and lib_type:
                        try:
                            # Transform the path using existing logic
                            transformed_path = self.file_manager.transform_notebook_path(lib_path, {})
                            dest_subdir = os.path.dirname(transformed_path.replace('../', ''))
                            local_directory = os.path.join(start_path, dest_subdir) if dest_subdir else start_path
                            
                            # Download the environment dependency
                            success, local_path, error_msg = self.workflow_manager.export_artifact(
                                lib_path, local_directory, lib_type)
                            
                            artifacts.append({
                                'original_path': lib_path,
                                'local_path': local_path if success else '',
                                'relative_yaml_path': transformed_path,
                                'type': lib_type,
                                'success': success,
                                'error': error_msg if not success else '',
                                'category': 'environment_dependency'
                            })
                            
                            if success:
                                self.logger.debug(f"Downloaded environment dependency: {lib_path} -> {local_path}")
                            else:
                                self.logger.warning(f"Failed to download environment dependency {lib_path}: {error_msg}")
                                
                        except Exception as e:
                            self.logger.error(f"Error processing environment dependency {lib_path}: {e}")
                            artifacts.append({
                                'original_path': lib_path,
                                'success': False,
                                'error': str(e),
                                'category': 'environment_dependency'
                            })
        
        return artifacts
    
    def _list_workspace_files_by_pattern(self, pattern: str) -> List[str]:
        """
        List workspace files matching a glob pattern.
        
        Args:
            pattern: Glob pattern (e.g., "/Workspace/Users/user/folder/*")
            
        Returns:
            List of matching file paths
        """
        try:
            import fnmatch
            
            # Extract the base directory from the pattern
            if '*' in pattern:
                base_path = pattern.split('*')[0].rstrip('/')
            else:
                base_path = os.path.dirname(pattern)
            
            self.logger.debug(f"Listing files in workspace path: {base_path}")
            
            matching_files = []
            
            # List workspace contents recursively
            try:
                workspace_objects = self.workflow_manager.client.workspace.list(base_path, recursive=True)
                
                for obj in workspace_objects:
                    if hasattr(obj, 'path') and hasattr(obj, 'object_type'):
                        obj_path = obj.path
                        obj_type = str(obj.object_type)
                        
                        # Only include files (not directories)
                        if obj_type in ['FILE', 'NOTEBOOK']:
                            # Check if the path matches the pattern
                            if fnmatch.fnmatch(obj_path, pattern):
                                matching_files.append(obj_path)
                                self.logger.debug(f"Pattern match: {obj_path}")
                
            except Exception as e:
                self.logger.error(f"Error listing workspace contents for pattern {pattern}: {e}")
                return []
            
            self.logger.debug(f"Found {len(matching_files)} files matching pattern: {pattern}")
            return matching_files
            
        except Exception as e:
            self.logger.error(f"Error processing glob pattern {pattern}: {e}")
            return []
    
    def _determine_artifact_type_from_path(self, file_path: str) -> str:
        """Determine artifact type from file path."""
        if file_path.endswith('.py'):
            return 'py'
        elif file_path.endswith('.sql'):
            return 'sql'
        elif file_path.endswith('.whl'):
            return 'whl'
        elif file_path.endswith('.jar'):
            return 'jar'
        elif file_path.endswith('.ipynb'):
            return 'notebook'
        else:
            return 'auto'
    
    def _create_dest_subdir_from_pattern(self, pattern: str, file_path: str) -> str:
        """Create destination subdirectory based on glob pattern and file path."""
        try:
            # Extract the relative path structure from the pattern
            if '/Workspace/' in pattern:
                base_pattern = pattern.replace('/Workspace/', '')
            else:
                base_pattern = pattern.lstrip('/')
            
            if '/Workspace/' in file_path:
                relative_path = file_path.replace('/Workspace/', '')
            else:
                relative_path = file_path.lstrip('/')
            
            # Return the directory portion of the relative path
            return os.path.dirname(relative_path)
            
        except Exception as e:
            self.logger.error(f"Error creating destination subdirectory: {e}")
            return ""
    
    def _create_dest_subdir_from_dependency(self, dependency: str) -> str:
        """Create destination subdirectory for environment dependencies."""
        try:
            # Transform the dependency path
            transformed_path = self.file_manager.transform_notebook_path(dependency, {})
            return os.path.dirname(transformed_path.replace('../', ''))
            
        except Exception as e:
            self.logger.error(f"Error creating destination subdirectory for dependency: {e}")
            return "libs"  # Default fallback
    
    def _cleanup_src_folder(self, start_path: str):
        """
        Clean up the src/ folder after processing is complete.
        
        Args:
            start_path: The base path containing the src folder
        """
        try:
            src_directory = os.path.join(start_path, 'src')
            if os.path.exists(src_directory):
                self.logger.debug(f"Cleaning up src/ folder: {src_directory}")
                
                import shutil
                # Count files before cleanup
                file_count = 0
                for root, dirs, files in os.walk(src_directory):
                    file_count += len(files)
                
                # Remove the entire src directory
                shutil.rmtree(src_directory)
                self.logger.debug(f"Cleaned up src/ folder: removed {file_count} temporary files from {src_directory}")
            else:
                self.logger.debug("No src/ folder found to clean up")
                
        except Exception as e:
            self.logger.warning(f"Error cleaning up src/ folder: {e}")
            # Don't fail the process if cleanup fails
    
    def run(self) -> None:
        """Legacy method for backward compatibility - defaults to workflow export."""
        self.logger.warning("Using deprecated run() method. Please use run_workflow_export() or run_pipeline_export() instead.")
        self.run_workflow_export() 