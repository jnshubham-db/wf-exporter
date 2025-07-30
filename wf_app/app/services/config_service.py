"""Configuration service for managing config.yml files."""

import yaml
from typing import Dict, Any, List
import logging
from .databricks_service import DatabricksService


class ConfigService:
    """Service class for configuration file operations."""
    
    def __init__(self):
        """Initialize config service."""
        self.logger = logging.getLogger(__name__)

    def load_config(self, config_path: str) -> str:
        """Load configuration content from Databricks workspace."""
        try:
            # Create fresh DatabricksService instance to get current authentication
            databricks_service = DatabricksService()
            content = databricks_service.read_workspace_file(config_path)
            self.logger.info(f"Configuration loaded from: {config_path}")
            return content
        except Exception as e:
            self.logger.error(f"Error loading config from {config_path}: {e}")
            raise
    
    def update_config_with_jobs(self, config_content: str, selected_jobs: List[Dict[str, Any]]) -> str:
        """Update configuration content with selected jobs."""
        try:
            updated_config = self.update_workflows_section(config_content, selected_jobs)
            self.logger.info(f"Configuration updated with {len(selected_jobs)} jobs")
            return updated_config
        except Exception as e:
            self.logger.error(f"Error updating config with jobs: {e}")
            raise
    
    def update_config_with_pipelines(self, config_content: str, selected_pipelines: List[Dict[str, Any]]) -> str:
        """Update configuration content with selected pipelines."""
        try:
            updated_config = self.update_pipelines_section(config_content, selected_pipelines)
            self.logger.info(f"Configuration updated with {len(selected_pipelines)} pipelines")
            return updated_config
        except Exception as e:
            self.logger.error(f"Error updating config with pipelines: {e}")
            raise

    def validate_yaml(self, content: str) -> Dict[str, Any]:
        """Validate YAML content and return parsed data."""
        try:
            data = yaml.safe_load(content)
            self.logger.info("YAML validation successful")
            return data
        except yaml.YAMLError as e:
            self.logger.error(f"YAML validation error: {e}")
            raise ValueError(f"Invalid YAML format: {e}")

    def update_workflows_section(self, config_content: str, jobs: List[Dict[str, Any]]) -> str:
        """Update the workflows section in config with selected jobs."""
        try:
            config_data = self.validate_yaml(config_content)
            
            # Ensure config_data is a dictionary
            if not isinstance(config_data, dict):
                config_data = {}
            
            # Update workflows section
            config_data['workflows'] = []
            for job in jobs:
                workflow_entry = {
                    'job_name': job['name'],
                    'job_id': job['job_id'],
                    'is_existing': job.get('is_existing', True),
                    'is_active': job.get('is_active', True),
                    'export_libraries': job.get('export_libraries', False)
                }
                
                # Add description if available from job details
                if 'description' in job and job['description']:
                    workflow_entry['description'] = job['description']
                
                config_data['workflows'].append(workflow_entry)
            
            # Convert back to YAML with proper formatting
            updated_content = yaml.dump(
                config_data, 
                default_flow_style=False, 
                sort_keys=False,
                indent=2,
                allow_unicode=True
            )
            
            self.logger.info(f"Successfully updated workflows section with {len(jobs)} jobs")
            return updated_content
        except Exception as e:
            self.logger.error(f"Error updating workflows section: {e}")
            raise

    def update_pipelines_section(self, config_content: str, pipelines: List[Dict[str, Any]]) -> str:
        """Update the pipelines section in config with selected pipelines."""
        try:
            config_data = self.validate_yaml(config_content)
            
            # Ensure config_data is a dictionary
            if not isinstance(config_data, dict):
                config_data = {}
            
            # Update pipelines section
            config_data['pipelines'] = []
            for pipeline in pipelines:
                pipeline_entry = {
                    'pipeline_name': pipeline.get('pipeline_name', pipeline.get('name')),  # Handle both field names
                    'pipeline_id': pipeline['pipeline_id'],
                    'is_existing': pipeline.get('is_existing', True),
                    'is_active': pipeline.get('is_active', True),
                    'export_libraries': pipeline.get('export_libraries', False)
                }
                
                # Add description if available from job details
                if 'description' in pipeline and pipeline['description']:
                    pipeline_entry['description'] = pipeline['description']
                
                config_data['pipelines'].append(pipeline_entry)
            
            # Convert back to YAML with proper formatting
            updated_content = yaml.dump(
                config_data, 
                default_flow_style=False, 
                sort_keys=False,
                indent=2,
                allow_unicode=True
            )
            
            self.logger.info(f"Successfully updated pipelines section with {len(pipelines)} pipelines")
            return updated_content
        except Exception as e:
            self.logger.error(f"Error updating pipelines section: {e}")
            raise
    
    def get_workflows_from_config(self, config_content: str) -> List[Dict[str, Any]]:
        """Extract workflows from configuration content."""
        try:
            config_data = self.validate_yaml(config_content)
            workflows = config_data.get('workflows', [])
            
            self.logger.info(f"Extracted {len(workflows)} workflows from config")
            return workflows
        except Exception as e:
            self.logger.error(f"Error extracting workflows from config: {e}")
            return []

    def get_pipelines_from_config(self, config_content: str) -> List[Dict[str, Any]]:
        """Extract pipelines from configuration content."""
        try:
            config_data = self.validate_yaml(config_content)
            pipelines = config_data.get('pipelines', [])
            
            self.logger.info(f"Extracted {len(pipelines)} pipelines from config")
            return pipelines
        except Exception as e:
            self.logger.error(f"Error extracting pipelines from config: {e}")
            return []
    
    def validate_workflow_structure(self, workflows: List[Dict[str, Any]]) -> bool:
        """Validate the structure of workflow entries."""
        try:
            required_fields = ['job_name', 'job_id', 'is_existing', 'is_active', 'export_libraries']
            
            for workflow in workflows:
                if not all(field in workflow for field in required_fields):
                    missing_fields = [field for field in required_fields if field not in workflow]
                    self.logger.error(f"Workflow missing required fields: {missing_fields}")
                    return False
                
                # Validate data types
                if not isinstance(workflow['job_id'], int):
                    self.logger.error(f"job_id must be an integer, got {type(workflow['job_id'])}")
                    return False
                
                if not isinstance(workflow['is_existing'], bool):
                    self.logger.error(f"is_existing must be a boolean, got {type(workflow['is_existing'])}")
                    return False
                
                if not isinstance(workflow['is_active'], bool):
                    self.logger.error(f"is_active must be a boolean, got {type(workflow['is_active'])}")
                    return False

                if not isinstance(workflow['export_libraries'], bool):
                    self.logger.error(f"export_libraries must be a boolean, got {type(workflow['export_libraries'])}")
                    return False
            
            self.logger.info(f"Successfully validated {len(workflows)} workflow entries")
            return True
        except Exception as e:
            self.logger.error(f"Error validating workflow structure: {e}")
            return False 

    def validate_pipeline_structure(self, pipelines: List[Dict[str, Any]]) -> bool:
        """Validate the structure of pipeline entries."""
        try:
            required_fields = ['pipeline_name', 'pipeline_id', 'is_existing', 'is_active', 'export_libraries']
            
            for pipeline in pipelines:
                if not all(field in pipeline for field in required_fields):
                    missing_fields = [field for field in required_fields if field not in pipeline]
                    self.logger.error(f"Pipeline missing required fields: {missing_fields}")
                    return False
                
                # Validate data types
                if not isinstance(pipeline['pipeline_id'], int):
                    self.logger.error(f"pipeline_id must be an integer, got {type(pipeline['pipeline_id'])}")
                    return False
                
                if not isinstance(pipeline['is_existing'], bool):
                    self.logger.error(f"is_existing must be a boolean, got {type(pipeline['is_existing'])}")
                    return False
                
                if not isinstance(pipeline['is_active'], bool):
                    self.logger.error(f"is_active must be a boolean, got {type(pipeline['is_active'])}")
                    return False
                
                if not isinstance(pipeline['export_libraries'], bool):
                    self.logger.error(f"export_libraries must be a boolean, got {type(pipeline['export_libraries'])}")
                    return False
            
            self.logger.info(f"Successfully validated {len(pipelines)} pipeline entries")
            return True
        except Exception as e:
            self.logger.error(f"Error validating pipeline structure: {e}")
            return False
    
    def load_app_config(self, config_path: str) -> str:
        """Load app_config.yml content from local filesystem."""
        try:
            import os
            
            # If path is relative, make it relative to the application root
            if not os.path.isabs(config_path):
                # Get the application root directory (where main.py is located)
                app_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
                config_path = os.path.join(app_root, config_path)
            
            # Read the local file
            with open(config_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            self.logger.info(f"App configuration loaded from local file: {config_path}")
            return content
        except FileNotFoundError:
            self.logger.error(f"App config file not found: {config_path}")
            raise FileNotFoundError(f"App config file not found: {config_path}")
        except Exception as e:
            self.logger.error(f"Error loading app config from {config_path}: {e}")
            raise
    
    def validate_app_config(self, content: str) -> Dict[str, Any]:
        """Validate app_config.yml content and return parsed data."""
        try:
            data = yaml.safe_load(content)
            
            if not isinstance(data, dict):
                raise ValueError("App config must be a dictionary")
            
            # Check for required export-job section
            if 'export-job' not in data:
                raise ValueError("App config must contain 'export-job' section")
            
            export_job = data['export-job']
            if not isinstance(export_job, dict):
                raise ValueError("'export-job' must be a dictionary")
            
            # Check required fields in export-job
            required_fields = ['job_name', 'job_id']
            for field in required_fields:
                if field not in export_job:
                    raise ValueError(f"'export-job' must contain '{field}' field")
            
            # Validate job_id is an integer
            if not isinstance(export_job['job_id'], int):
                raise ValueError("'job_id' must be an integer")
            
            # Validate job_name is a string
            if not isinstance(export_job['job_name'], str):
                raise ValueError("'job_name' must be a string")
            
            self.logger.info("App config validation successful")
            return data
            
        except yaml.YAMLError as e:
            self.logger.error(f"App config YAML validation error: {e}")
            raise ValueError(f"Invalid YAML format in app config: {e}")
        except Exception as e:
            self.logger.error(f"App config validation error: {e}")
            raise ValueError(f"App config validation failed: {e}")
    
    def get_export_job_from_app_config(self, content: str) -> Dict[str, Any]:
        """Extract export job configuration from app_config.yml."""
        try:
            config_data = self.validate_app_config(content)
            export_job = config_data.get('export-job', {})
            
            self.logger.info(f"Extracted export job: {export_job.get('job_name', 'Unknown')} (ID: {export_job.get('job_id', 'Unknown')})")
            return export_job
        except Exception as e:
            self.logger.error(f"Error extracting export job from app config: {e}")
            return {}
    
    def create_default_app_config(self, job_name: str, job_id: int) -> str:
        """Create a default app_config.yml content with provided job details."""
        try:
            config_data = {
                'export-job': {
                    'job_name': job_name,
                    'job_id': job_id
                }
            }
            
            # Convert to YAML with proper formatting
            content = yaml.dump(
                config_data, 
                default_flow_style=False, 
                sort_keys=False,
                indent=2,
                allow_unicode=True
            )
            
            self.logger.info(f"Created default app config with job: {job_name} (ID: {job_id})")
            return content
        except Exception as e:
            self.logger.error(f"Error creating default app config: {e}")
            raise 