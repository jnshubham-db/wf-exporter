"""
Workflow management module for Databricks Workflow Exporter.

This module handles interactions with the Databricks SDK for retrieving
workflow definitions and permissions.
"""

import os
import shutil
from typing import Dict, List, Any, Optional, Tuple
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
            List of dictionaries containing job task information for all task types
        """
        self.logger.debug(f"Retrieving workflow tasks for job ID: {job_id}")
        
        # Get job details using the SDK
        job_details = self.client.jobs.get(job_id=int(job_id))
        
        all_tasks = []
        
        if job_details.settings.tasks:
            for task in job_details.settings.tasks:
                task_info = {
                    'Job_Name': job_details.settings.name,
                    'JobId': job_id,
                    'Task_Key': task.task_key,
                    'Task_Type': None,
                    'Notebook_Path': None,
                    'Notebook_Source': None,
                    'Python_File': None,
                    'SQL_File': None,
                    'Python_Wheel_Entry_Point': None,
                    'Python_Wheel_Package_Name': None,
                    'Libraries': [],
                    'Environment_Key': getattr(task, 'environment_key', None)
                }
                
                # Extract notebook task information
                if hasattr(task, 'notebook_task') and task.notebook_task:
                    task_info['Task_Type'] = 'notebook_task'
                    task_info['Notebook_Path'] = getattr(task.notebook_task, 'notebook_path', None)
                    source = getattr(task.notebook_task, 'source', None)
                    # Convert enum to string if it has a value attribute
                    if source and hasattr(source, 'value'):
                        source = source.value
                    task_info['Notebook_Source'] = source
                
                # Extract spark python task information
                elif hasattr(task, 'spark_python_task') and task.spark_python_task:
                    task_info['Task_Type'] = 'spark_python_task'
                    task_info['Python_File'] = getattr(task.spark_python_task, 'python_file', None)
                
                # Extract python wheel task information
                elif hasattr(task, 'python_wheel_task') and task.python_wheel_task:
                    task_info['Task_Type'] = 'python_wheel_task'
                    task_info['Python_Wheel_Entry_Point'] = getattr(task.python_wheel_task, 'entry_point', None)
                    task_info['Python_Wheel_Package_Name'] = getattr(task.python_wheel_task, 'package_name', None)
                
                # Extract SQL task information
                elif hasattr(task, 'sql_task') and task.sql_task:
                    task_info['Task_Type'] = 'sql_task'
                    if hasattr(task.sql_task, 'file') and task.sql_task.file:
                        task_info['SQL_File'] = getattr(task.sql_task.file, 'path', None)
                
                # Extract libraries information (for .whl files)
                if hasattr(task, 'libraries') and task.libraries:
                    for lib in task.libraries:
                        try:
                            if hasattr(lib, 'whl') and lib.whl:
                                # Ensure whl path is a string
                                whl_path = lib.whl
                                if isinstance(whl_path, str) and whl_path.strip():
                                    task_info['Libraries'].append({
                                        'type': 'whl',
                                        'path': whl_path
                                    })
                                else:
                                    self.logger.warning(f"Skipping invalid whl library path: {whl_path} (type: {type(whl_path)})")
                            # Add other library types if needed (jar, pypi, etc.)
                            elif hasattr(lib, 'jar') and lib.jar:
                                jar_path = lib.jar
                                if isinstance(jar_path, str) and jar_path.strip():
                                    task_info['Libraries'].append({
                                        'type': 'jar',
                                        'path': jar_path
                                    })
                                else:
                                    self.logger.warning(f"Skipping invalid jar library path: {jar_path} (type: {type(jar_path)})")
                        except Exception as e:
                            self.logger.error(f"Error processing library for task {task.task_key}: {e}")
                            continue
                
                all_tasks.append(task_info)
        
        # Also check for job-level environments (for serverless jobs)
        if hasattr(job_details.settings, 'environments') and job_details.settings.environments:
            for env in job_details.settings.environments:
                env_info = {
                    'Job_Name': job_details.settings.name,
                    'JobId': job_id,
                    'Task_Key': f"environment_{env.environment_key}",
                    'Task_Type': 'job_environment',
                    'Environment_Key': env.environment_key,
                    'Libraries': [],
                    'Notebook_Path': None,
                    'Notebook_Source': None,
                    'Python_File': None,
                    'SQL_File': None,
                    'Python_Wheel_Entry_Point': None,
                    'Python_Wheel_Package_Name': None
                }
                
                # Extract dependencies from environment spec
                if hasattr(env, 'spec') and env.spec and hasattr(env.spec, 'dependencies'):
                    for dep in env.spec.dependencies:
                        try:
                            # Dependencies in environments are typically file paths or package names
                            if isinstance(dep, str) and dep.strip() and (dep.startswith('/') or dep.endswith('.whl')):
                                env_info['Libraries'].append({
                                    'type': 'whl',
                                    'path': dep
                                })
                            else:
                                self.logger.debug(f"Skipping environment dependency: {dep} (type: {type(dep)})")
                        except Exception as e:
                            self.logger.error(f"Error processing environment dependency: {e}")
                            continue
                
                if env_info['Libraries']:  # Only add if there are libraries
                    all_tasks.append(env_info)
        
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
    
    def export_artifact(self, artifact_path: str, local_directory: str, artifact_type: str = 'auto') -> Tuple[bool, str, str]:
        """
        Export an artifact from Databricks to local filesystem.
        
        Args:
            artifact_path: The path to the artifact in Databricks (workspace or volume)
            local_directory: The local directory to save the artifact
            artifact_type: Type of artifact ('py', 'sql', 'whl', 'notebook', 'auto')
            
        Returns:
            Tuple of (success, local_file_path, error_message)
        """
        try:
            self.logger.debug(f"Exporting artifact: {artifact_path} to {local_directory}")
            
            # Determine artifact type if auto
            if artifact_type == 'auto':
                if artifact_path.endswith('.py'):
                    artifact_type = 'py'
                elif artifact_path.endswith('.sql'):
                    artifact_type = 'sql'
                elif artifact_path.endswith('.whl'):
                    artifact_type = 'whl'
                elif artifact_path.endswith('.ipynb'):
                    artifact_type = 'notebook'
                else:
                    # Try to determine from path structure
                    if artifact_path.startswith('/Workspace'):
                        artifact_type = 'py'  # Default for workspace files
                    elif artifact_path.startswith('/Volume'):
                        artifact_type = 'whl'  # Default for volume files
                    else:
                        artifact_type = 'py'  # Default fallback
            
            # Ensure local directory exists
            os.makedirs(local_directory, exist_ok=True)
            
            # Get the filename from the path
            filename = os.path.basename(artifact_path)
            local_file_path = os.path.join(local_directory, filename)
            
            # Debug logging for local path construction
            self.logger.debug(f"Local file path: {local_file_path}")
            
            # Download based on artifact type and location
            if artifact_path.startswith('/Workspace'):
                # Try workspace first, but if it fails for wheel files, try as volume
                success, error_msg = self._download_workspace_file(artifact_path, local_file_path, artifact_type)
                
            elif artifact_path.startswith('/Volume'):
                success, error_msg = self._download_volume_file(artifact_path, local_file_path)
            
            if success:
                self.logger.info(f"Successfully exported {artifact_path} to {local_file_path}")
                return True, local_file_path, ""
            else:
                self.logger.error(f"Failed to export {artifact_path}: {error_msg}")
                return False, "", error_msg
                
        except Exception as e:
            error_msg = f"Error exporting artifact {artifact_path}: {str(e)}"
            self.logger.error(error_msg)
            return False, "", error_msg
    
    def _download_workspace_file(self, workspace_path: str, local_file_path: str, artifact_type: str) -> Tuple[bool, str]:
        """
        Download a file from Databricks workspace.
        
        Args:
            workspace_path: Path to file in workspace
            local_file_path: Local path to save the file
            artifact_type: Type of artifact (py, sql, notebook)
            
        Returns:
            Tuple of (success, error_message)
        """
        try:
            self.logger.debug(f"Downloading workspace file: {workspace_path}")
            
            # Import the format enum from databricks SDK
            from databricks.sdk.service.workspace import ExportFormat            
            if(artifact_type != 'whl'):
                with self.client.workspace.download(path=workspace_path) as content:
                    self.logger.debug(f"Writing binary content to {local_file_path}")
                    with open(local_file_path, 'w') as f:
                        f.write(content.read().decode('utf-8'))
            else:
                with self.client.workspace.download(path=workspace_path, format=ExportFormat.AUTO) as content:
                    self.logger.debug(f"Writing binary content to {local_file_path}")
                    with open(local_file_path, 'wb') as f:
                        f.write(content.read())

            self.logger.debug(f"Successfully downloaded workspace file to {local_file_path}")
            return True, ""
            
        except Exception as e:
            error_msg = f"Error downloading workspace file {workspace_path}: {str(e)}"
            self.logger.error(error_msg)
            return False, error_msg
    
    def _download_volume_file(self, volume_path: str, local_file_path: str) -> Tuple[bool, str]:
        """
        Download a file from Databricks volumes (Unity Catalog).
        
        Args:
            volume_path: Path to file in volume
            local_file_path: Local path to save the file
            
        Returns:
            Tuple of (success, error_message)
        """
        try:
            self.logger.debug(f"Downloading volume file: {volume_path}")
            
            # Use files API to download from volume
            with open(local_file_path, 'wb') as f:
                # Download file from volume
                content = self.client.files.download(volume_path)
                if hasattr(content, 'contents'):
                    shutil.copyfileobj(content.contents, f)
                elif hasattr(content, 'content'):
                    # Some versions might use content instead of contents
                    if isinstance(content.content, bytes):
                        f.write(content.content)
                    else:
                        f.write(content.content.encode('utf-8'))
                else:
                    # Try to write the content directly
                    f.write(content)
            
            self.logger.debug(f"Successfully downloaded volume file to {local_file_path}")
            return True, ""
            
        except Exception as e:
            error_msg = f"Error downloading volume file {volume_path}: {str(e)}"
            self.logger.debug(error_msg)  # Use debug instead of error for fallback attempts
            return False, error_msg
    
    def export_multiple_artifacts(self, artifacts: List[Dict[str, str]], base_local_directory: str) -> List[Dict[str, Any]]:
        """
        Export multiple artifacts and return their local mappings.
        
        Args:
            artifacts: List of artifact dictionaries with 'path', 'type', and 'destination_subdir' keys
            base_local_directory: Base local directory for exports
            
        Returns:
            List of dictionaries with export results including success status and local paths
        """
        self.logger.debug(f"Starting export_multiple_artifacts with base_local_directory: {base_local_directory}")
        self.logger.debug(f"Number of artifacts to process: {len(artifacts)}")
        
        results = []
        
        for artifact in artifacts:
            artifact_path = artifact.get('path', '')
            artifact_type = artifact.get('type', 'auto')
            subdir = artifact.get('destination_subdir', '')
            
            if not artifact_path:
                continue
                
            # Create subdirectory if specified
            local_directory = os.path.join(base_local_directory, subdir) if subdir else base_local_directory
            
            self.logger.debug(f"Processing artifact: {artifact_path}")
            self.logger.debug(f"  - artifact_type: {artifact_type}")
            self.logger.debug(f"  - subdir: {subdir}")
            self.logger.debug(f"  - local_directory: {local_directory}")
            
            success, local_path, error_msg = self.export_artifact(artifact_path, local_directory, artifact_type)
            
            result = {
                'original_path': artifact_path,
                'local_path': local_path if success else '',
                'success': success,
                'error_message': error_msg,
                'artifact_type': artifact_type
            }
            results.append(result)
        
        self.logger.info(f"Exported {len([r for r in results if r['success']])} out of {len(artifacts)} artifacts successfully")
        return results 