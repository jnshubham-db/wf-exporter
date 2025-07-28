"""
Export processing core module for Databricks Workflow Exporter.

This module contains the main DatabricksExporter class that orchestrates the entire
workflow export process using the Facade pattern. Supports both job and DLT pipeline exports.
"""

import os
import shutil
import logging
from typing import Optional, Tuple, List

import pandas as pd

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
                 databricks_token: Optional[str] = None):
        """
        Initialize the DatabricksExporter with required components.
        
        Args:
            config_path: Path to configuration file (optional)
            databricks_host: Databricks workspace URL (optional)
            databricks_token: Databricks access token (optional)
        """
        # Create temporary logger without file handler for config loading
        temp_logger = LogManager(create_file_handler=False)
        
        # Load configuration with temporary logger
        self.config_manager = ConfigManager(logger=temp_logger, config_path=config_path)
        
        # Set up proper logging with config data (this is the main logger)
        self.logger = LogManager(config_data=self.config_manager.config_data)
        
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
        
        self.logger.info("Databricks CLI setup completed successfully")
    
    
    def _discover_generated_files(self, start_path: str, job_name: str) -> List[str]:
        """
        Discover generated YAML and source files by scanning the filesystem.
        
        Args:
            start_path: The base path for file operations.
            job_name: The name of the job (for file naming).
            
        Returns:
            A list of absolute paths to the generated files.
        """
        self.logger.debug("Discovering generated files from filesystem...")
        discovered_files = []
        
        # Discover files in src directory
        src_path = os.path.join(start_path, 'src')
        if os.path.isdir(src_path):
            for root, _, files in os.walk(src_path):
                for name in files:
                    discovered_files.append(os.path.join(root, name))
        
        # Discover YAML file in resources directory
        job_resource_name = self.file_manager.convert_string(job_name)
        yml_file_path = os.path.join(start_path, 'resources', f"{job_resource_name}.job.yml")
        
        if os.path.exists(yml_file_path):
            discovered_files.append(yml_file_path)
            self.logger.debug(f"Discovered YAML file: {yml_file_path}")
        else:
            self.logger.warning(f"Could not find expected YAML file: {yml_file_path}")
            
        self.logger.debug(f"Discovered {len(discovered_files)} files.")
        return discovered_files

    def _clean_existing_files(self, start_path: str, job_name: str) -> None:
        """
        Clean up existing files before generating new ones.
        
        Args:
            start_path: The base path for file operations
            job_name: The name of the job (for file naming)
        """
        try:
            # Remove src directory if it exists
            src_path = os.path.join(start_path, 'src')
            if os.path.exists(src_path):
                shutil.rmtree(src_path)
                self.logger.debug("Removed existing src directory")
                
            # Remove yaml file in resources directory if it exists
            yaml_file = os.path.join(start_path, 'resources', f"{self.file_manager.convert_string(job_name)}.job.yml")
            if os.path.exists(yaml_file):
                os.remove(yaml_file)
                self.logger.debug("Removed existing YAML file")
                
            self.logger.debug("Files cleaned up before running bundle generate job.")
        except Exception as e:
            self.logger.debug(f"No files to remove before running bundle generate job: {e}")
    
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
    
    def _validate_folder_structure(self, start_path: str, job_name: str) -> bool:
        """
        Validate that the final folder structure is correct for all artifact types.
        
        Expected structure:
        exports/
        ├── src/                          # Generated source files (notebooks, .py, .sql)
        │   ├── Users/                    # User workspace files
        │   │   └── username/             
        │   └── other_workspace_paths/    
        ├── libs/                         # Library files (.whl, .jar)
        ├── resources/                    # Generated YAML files
        │   └── job_name.job.yml
        └── backup_jobs_yaml/             # Backup of original YAML
        
        Args:
            start_path: Base path for file operations
            job_name: Name of the job
            
        Returns:
            True if structure is valid, False otherwise
        """
        try:
            self.logger.debug("Validating folder structure...")
            
            job_resource_name = self.file_manager.convert_string(job_name)
            
            # Check for required directories
            required_dirs = ['src', 'resources']
            optional_dirs = ['libs', 'backup_jobs_yaml']
            
            for dir_name in required_dirs:
                dir_path = os.path.join(start_path, dir_name)
                if not os.path.exists(dir_path):
                    self.logger.error(f"Required directory missing: {dir_path}")
                    return False
                else:
                    self.logger.debug(f"Found required directory: {dir_path}")
            
            # Check for YAML file in resources
            yaml_file = os.path.join(start_path, 'resources', f"{job_resource_name}.job.yml")
            if not os.path.exists(yaml_file):
                self.logger.error(f"YAML file missing: {yaml_file}")
                return False
            else:
                self.logger.debug(f"Found YAML file: {yaml_file}")
            
            # Check and report on optional directories
            for dir_name in optional_dirs:
                dir_path = os.path.join(start_path, dir_name)
                if os.path.exists(dir_path):
                    file_count = len([f for f in os.listdir(dir_path) if os.path.isfile(os.path.join(dir_path, f))])
                    self.logger.debug(f"Found optional directory: {dir_path} with {file_count} files")
                else:
                    self.logger.debug(f"Optional directory not found: {dir_path}")
            
            # Report on src directory contents
            src_path = os.path.join(start_path, 'src')
            if os.path.exists(src_path):
                src_files = []
                for root, dirs, files in os.walk(src_path):
                    for file in files:
                        rel_path = os.path.relpath(os.path.join(root, file), src_path)
                        src_files.append(rel_path)
                
                self.logger.info(f"Source files structure validated. Found {len(src_files)} files:")
                for file_path in sorted(src_files):
                    self.logger.info(f"  src/{file_path}")
            
            # Report on libs directory contents if it exists
            libs_path = os.path.join(start_path, 'libs')
            if os.path.exists(libs_path):
                lib_files = [f for f in os.listdir(libs_path) if os.path.isfile(os.path.join(libs_path, f))]
                self.logger.info(f"Library files found: {lib_files}")
            
            self.logger.info("Folder structure validation completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error validating folder structure: {str(e)}")
            return False
    
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
            self.logger.info("=" * 60)
            self.logger.info("PROCESSING SUMMARY")
            self.logger.info("=" * 60)
            
            # Task type summary
            task_types = {}
            for task in workflow_definition:
                task_type = task.get('Task_Type', 'unknown')
                task_types[task_type] = task_types.get(task_type, 0) + 1
            
            self.logger.info("Task Types Processed:")
            for task_type, count in task_types.items():
                self.logger.info(f"  - {task_type}: {count} task(s)")
            
            # Artifact summary
            artifact_types = {}
            for artifact in all_artifacts:
                artifact_type = artifact.get('type', 'unknown')
                artifact_types[artifact_type] = artifact_types.get(artifact_type, 0) + 1
            
            self.logger.info("Artifacts Identified:")
            for artifact_type, count in artifact_types.items():
                self.logger.info(f"  - {artifact_type}: {count} file(s)")
            
            # Download results summary
            if download_results:
                successful = len([r for r in download_results if r['success']])
                failed = len([r for r in download_results if not r['success']])
                self.logger.info(f"Download Results: {successful} successful, {failed} failed")
                
                if failed > 0:
                    self.logger.warning("Failed downloads:")
                    for result in download_results:
                        if not result['success']:
                            self.logger.warning(f"  - {result['original_path']}: {result['error_message']}")
            
            self.logger.info("=" * 60)
            
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
            self.logger.debug(f"Starting job processing for job ID: {job_id}")

            # Get workflow definition and job details (now includes all task types)
            workflow_definition = self.workflow_manager.get_job_workflow_tasks(job_id)
            
            if not workflow_definition:
                self.logger.error(f"No workflow definition found for job ID: {job_id}")
                return False, None

            job_name = workflow_definition[0]['Job_Name']
            
            # Log job details
            self.logger.info(f"Processing job id: {job_id}, job name: {job_name}")
            self.logger.debug(f"Workflow definition contains {len(workflow_definition)} tasks")
            
            # Clean existing files
            self._clean_existing_files(start_path, job_name)
            
            # Generate YAML and source files using bundle generate (for notebooks only)
            self.logger.debug("Generating YAML and source files...")
            databricks_yml_path = self.config_manager.get_databricks_yml_path()
            output, outcome = self.cli_manager.generate_yaml_src_files_from_job_id(job_id, start_path, databricks_yml_path)
            if outcome == 'failed':
                self.logger.error(f"Error in generating YAML and source files for job id: {job_id}: {output}")
                return False, None
            
            self.logger.info(f"YAML and source files generated successfully for job id: {job_id}")

            # Discover generated files from the filesystem (notebooks from bundle generate)
            file_paths = self._discover_generated_files(start_path, job_name)
            if not file_paths:
                self.logger.error(f"No files were discovered for job {job_id} after generation.")
                return False, None

            self.logger.debug(f"Discovered {len(file_paths)} files from bundle generate")
            
            # Process different task types to identify additional artifacts to download
            self.logger.info("Processing tasks by type to identify artifacts...")
            
            # Process notebook tasks (already handled by bundle generate)
            notebook_artifacts = self._process_notebook_tasks(workflow_definition, start_path)
            
            # Process spark python tasks (.py files)
            python_artifacts = self._process_spark_python_tasks(workflow_definition, start_path)
            
            # Process SQL tasks (.sql files)
            sql_artifacts = self._process_sql_tasks(workflow_definition, start_path)
            
            # Process python wheel tasks (libraries)
            wheel_artifacts = self._process_python_wheel_tasks(workflow_definition, start_path)
            
            # Process job environments (serverless libraries)
            env_artifacts = self._process_job_environments(workflow_definition, start_path)
            
            # Process task-level libraries
            task_lib_artifacts = self._process_task_libraries(workflow_definition, start_path)
            
            # Combine all artifacts that need to be downloaded
            all_artifacts = (notebook_artifacts + python_artifacts + sql_artifacts + 
                           wheel_artifacts + env_artifacts + task_lib_artifacts)
            
            # Download additional artifacts (non-notebook files)
            download_artifacts = [a for a in all_artifacts if a.get('type') != 'notebook']
            if download_artifacts:
                self.logger.info(f"Downloading {len(download_artifacts)} additional artifacts...")
                download_results = self.workflow_manager.export_multiple_artifacts(download_artifacts, start_path)
                
                # Log download results
                successful_downloads = [r for r in download_results if r['success']]
                failed_downloads = [r for r in download_results if not r['success']]
                
                self.logger.info(f"Successfully downloaded {len(successful_downloads)} artifacts")
                if failed_downloads:
                    self.logger.warning(f"Failed to download {len(failed_downloads)} artifacts:")
                    for failed in failed_downloads:
                        self.logger.warning(f"  - {failed['original_path']}: {failed['error_message']}")
            else:
                self.logger.info("No additional artifacts to download")
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
                self.logger.info("No notebook files to move")
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
                self.logger.debug(f"Copied YAML file to backup directory: {yml_file_abs}")
            except Exception as e:
                self.logger.error(f"Failed to copy YAML file: {e}")
                self.logger.debug(f"Source: {yml_file_abs}")
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
            
            self.logger.info("Successfully updated the YAML file.")

            # Validate folder structure after processing
            if not self._validate_folder_structure(start_path, job_name):
                self.logger.error(f"Folder structure validation failed for job {job_id}. Exiting.")
                return False, None

            # Log processing summary
            self._log_processing_summary(workflow_definition, all_artifacts, download_results)

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
        if not os.path.exists(os.path.dirname(v_resource_key_job_id_mapping_csv_file_path)):
            os.makedirs(os.path.dirname(v_resource_key_job_id_mapping_csv_file_path))
        df_resource_key_job_id_mapping = pd.DataFrame(list_resource_key_job_id_mapping, columns=['job_key', 'job_id'])
        df_resource_key_job_id_mapping.to_csv(v_resource_key_job_id_mapping_csv_file_path, index=False)
        self.logger.info(f"Resource key to job ID mappings saved to {v_resource_key_job_id_mapping_csv_file_path}")

    def run(self) -> None:
        """Run the end-to-end job processing workflow."""
        # Set up the CLI and authentication
        self.setup()
        
        # Get job arguments from config manager
        active_jobs = self.config_manager.get_active_jobs()
        self.logger.info(f"List of Job IDs: {active_jobs}")
        
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
                        self.logger.info(f"Resource mapping for job {job_id}: {resource_mapping}")
                else:
                    failed_jobs.append(job_id)
            except Exception as e:
                self.logger.error(f"Failed to process job {job_id}: {str(e)}")
                failed_jobs.append(job_id)
        
        # Log final summary
        self.logger.info(f"Job processing completed. Successful jobs: {successful_jobs}")
        if failed_jobs:
            self.logger.warning(f"Failed jobs: {failed_jobs}")
        if resource_mappings:
            self.save_bind_mappings(resource_key_job_id_mapping_csv_file_path, resource_mappings)
            self.logger.info(f"Resource mappings: {resource_mappings}")
        
        self.logger.info(f"Workflow export process completed. {len(successful_jobs)} jobs processed successfully.") 