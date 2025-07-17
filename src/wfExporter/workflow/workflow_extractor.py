"""
Workflow management module for Databricks Workflow Exporter.

This module handles interactions with the Databricks SDK for retrieving
workflow definitions and permissions.
"""

from typing import Dict, List, Any, Optional
from databricks.sdk import WorkspaceClient

from ..logging.log_manager import LogManager


class WorkflowExtractor:
    """
    A class to manage and retrieve information about Databricks workflows and permissions.
    
    Handles:
    - Workflow task retrieval
    - Job permissions management
    - Databricks SDK interactions
    """
    
    def __init__(self, config_profile: str = None, logger: Optional[LogManager] = None):
        """
        Initialize the WorkflowExtractor with a WorkspaceClient.
        
        Args:
            config_profile: Databricks config profile to use (None for environment-based auth)
            logger: Logger instance for logging operations
        """
        self.config_profile = config_profile
        self.logger = logger or LogManager()
        
        # Initialize WorkspaceClient based on authentication method
        if config_profile:
            self.client = WorkspaceClient(profile=config_profile)
            self.logger.debug(f"Initialized WorkspaceClient with profile: {config_profile}")
        else:
            self.client = WorkspaceClient()
            self.logger.debug("Initialized WorkspaceClient with environment variables")
    
    def get_job_workflow_tasks(self, job_id: str) -> List[Dict[str, Any]]:
        """
        Retrieves the workflow task definitions for a given job ID.
        
        Args:
            job_id: The Databricks job ID to retrieve tasks for
            
        Returns:
            List of dictionaries containing job task information
        """
        self.logger.debug(f"Retrieving workflow tasks for job ID: {job_id}")
        
        # Get job details using the SDK
        job_details = self.client.jobs.get(job_id=int(job_id))
        
        all_tasks = []
        task_value = {
            'Job_Name': job_details.settings.name,
            'JobId': job_id
        }

        if job_details.settings.tasks:
            for task in job_details.settings.tasks:
                if hasattr(task, 'notebook_task'):
                    task_value['Notebook_Path'] = getattr(task.notebook_task, 'notebook_path', None)
                    source = getattr(task.notebook_task, 'source', None)
                    # Convert enum to string if it has a value attribute
                    if source and hasattr(source, 'value'):
                        source = source.value
                    task_value['Notebook_Source'] = source
                else:
                    task_value['Notebook_Path'] = None
                    task_value['Notebook_Source'] = None
                all_tasks.append(task_value.copy())
                task_value = {
                    'Job_Name': job_details.settings.name,
                    'JobId': job_id
                }
        
        self.logger.debug(f"Retrieved {len(all_tasks)} tasks for job {job_id}")
        return all_tasks
    
    def get_job_acls(self, job_id: str) -> List[Dict[str, str]]:
        """
        Retrieves and transforms job permissions.
        
        Args:
            job_id: The Databricks job ID to retrieve permissions for
            
        Returns:
            List of dictionaries containing permission information
        """
        self.logger.debug(f"Retrieving job permissions for job ID: {job_id}")
        
        # Get job permissions using the SDK
        permissions = self.client.permissions.get(
            request_object_type="jobs", 
            request_object_id=int(job_id)
        )
        
        output = []
        for acl in permissions.access_control_list:
            # Skip admins group 
            if acl.group_name == 'admins':
                continue
                
            # Extract permission level as a string
            permission_level = acl.all_permissions[0].permission_level
            # Convert enum to string
            if hasattr(permission_level, 'value'):
                permission_level = permission_level.value
            
            # Add entry based on type (user, group, or service principal)
            if acl.user_name:
                output.append({
                    'user_name': acl.user_name,
                    'level': permission_level
                })
            elif acl.group_name:
                output.append({
                    'group_name': acl.group_name,
                    'level': permission_level
                })
            elif acl.service_principal_name:
                output.append({
                    'service_principal_name': acl.service_principal_name,
                    'level': permission_level
                })
        
        self.logger.debug(f"Retrieved {len(output)} permissions for job {job_id}")
        return output 