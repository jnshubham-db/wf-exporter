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
    
    def _prepare_file_mapping(self, df: pd.DataFrame, job_id: str, file_paths: List[str]) -> pd.DataFrame:
        """
        Prepare file mapping DataFrame.
        
        Args:
            df: DataFrame containing workflow task information
            job_id: The job ID
            file_paths: List of generated file paths
            
        Returns:
            DataFrame with file mapping information
        """
        # Filter files not ending with .yml
        list_files_except_yml = [os.path.basename(file) for file in file_paths if not file.endswith('.yml')]
        
        # Create a dictionary mapping base names to full filenames
        dict_file_map = {os.path.splitext(name)[0]: name for name in list_files_except_yml}
        
        # src and dest directories for moving files
        filtered_df = df[df['JobId'] == int(job_id)].copy()
        
        # Apply transformations to create source and destination paths
        filtered_df['src_directory'] = filtered_df['Notebook_Path'].apply(
            lambda x: self.file_manager.map_src_file_name(x, dict_file_map))
        filtered_df['dest_directory'] = filtered_df['Notebook_Path'].apply(
            lambda x: self.file_manager.transform_notebook_path(x, dict_file_map))
            
        self.logger.debug(f"Prepared file mapping for {len(filtered_df)} files")
        return filtered_df
    
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

            # Get workflow definition and job details
            workflow_definition = self.workflow_manager.get_job_workflow_tasks(job_id)
            
            if not workflow_definition:
                self.logger.error(f"No workflow definition found for job ID: {job_id}")
                return False, None

            job_name = workflow_definition[0]['Job_Name']
            
            # Log job details
            self.logger.info(f"Processing job id: {job_id}, job name: {job_name}")
            self.logger.debug(f"Workflow definition contains {len(workflow_definition)} tasks")
            
            # Convert workflow to DataFrame and filter for non-null notebook paths
            df = pd.DataFrame(workflow_definition)
            df['JobId'] = df['JobId'].astype('int64')
            df = df.drop_duplicates()
            
            df = df[df['Notebook_Path'].notnull()]
            self.logger.debug(f"Filtered DataFrame has {len(df)} rows with valid notebook paths")
            
            # Clean existing files
            self._clean_existing_files(start_path, job_name)
            
            # Generate YAML and source files
            self.logger.debug("Generating YAML and source files...")
            databricks_yml_path = self.config_manager.get_databricks_yml_path()
            output, outcome = self.cli_manager.generate_yaml_src_files_from_job_id(job_id, start_path, databricks_yml_path)
            if outcome == 'failed':
                self.logger.error(f"Error in generating YAML and source files for job id: {job_id}: {output}")
                return False, None
            
            file_paths = output
            self.logger.info(f"YAML and source files generated successfully for job id: {job_id}")
            self.logger.debug(f"Generated {len(file_paths)} files")
            
            # Prepare file mapping
            self.logger.debug("Preparing file mapping...")
            filtered_df = self._prepare_file_mapping(df, job_id, file_paths)
            
            # Move files
            self.logger.debug("Moving files to destination directories...")
            output, outcome = self.file_manager.move_files_to_directory(filtered_df, job_id, start_path)
            if outcome == 'failed':
                self.logger.error(f"Failed to move files for job id: {job_id}: {output}")
                return False, None
                
            # Create notebook path mapping
            src_dest_mapping = dict(zip(filtered_df['src_directory'], filtered_df['dest_directory']))
            job_resource_name = self.file_manager.convert_string(job_name)
            
            # Get YAML files
            yml_files = [file for file in file_paths if file.endswith('.yml')]
            if not yml_files:
                self.logger.error(f"No YAML files generated for job id: {job_id}")
                return False, None
                
            yml_file = yml_files[0]
            self.logger.debug(f"Using YAML file: {yml_file}")
            
            # Convert relative path to absolute path
            if not os.path.isabs(yml_file):
                yml_file_abs = os.path.join(start_path, yml_file)
            else:
                yml_file_abs = yml_file
            
            self.logger.debug(f"Absolute YAML file path: {yml_file_abs}")
            
            # Check if the YAML file exists
            if not os.path.exists(yml_file_abs):
                self.logger.error(f"YAML file not found: {yml_file_abs}")
                self.logger.debug(f"Current working directory: {os.getcwd()}")
                self.logger.debug(f"Start path: {start_path}")
                self.logger.debug(f"Original yml_file: {yml_file}")
                return False, None
            
            # Ensure backup directory exists
            if not os.path.exists(backup_yaml_path):
                os.makedirs(backup_yaml_path)
                self.logger.debug(f"Created backup directory: {backup_yaml_path}")

            modified_yml_file = yml_file_abs
            # Copy the file to yaml backup directory
            try:
                shutil.copyfile(yml_file_abs, os.path.join(backup_yaml_path, os.path.basename(yml_file)))
                self.logger.debug(f"Copied YAML file to backup directory: {yml_file_abs}")
            except Exception as e:
                self.logger.error(f"Failed to copy YAML file: {e}")
                self.logger.debug(f"Source: {yml_file_abs}")
                self.logger.debug(f"Destination: {os.path.join(backup_yaml_path, os.path.basename(yml_file))}")
                return False, None
            
            # Get replacements and update YAML
            self.logger.debug("Updating YAML file with replacements...")
            replacements = self.config_manager.get_replacements()
            
            output, outcome = self.yaml_processor.load_update_dump_yaml(
                self.workflow_manager, yml_file_abs, modified_yml_file, job_id, job_resource_name, 
                src_dest_mapping, replacements, self.config_manager)
            
            if outcome == 'failed':
                self.logger.error(f"Error in updating YAML file for job id: {job_id}: {output}")
                return False, None
            
            self.logger.info("Successfully updated the YAML file.")

            # Clean up temporary files if modified file is different from original
            if yml_file_abs != modified_yml_file:
                try:
                    os.remove(yml_file_abs)
                    self.logger.debug(f"Removed temporary file: {yml_file_abs}")
                except Exception as e:
                    self.logger.warning(f"Could not remove temporary file {yml_file_abs}: {e}")

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