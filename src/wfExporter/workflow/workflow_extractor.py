"""
Workflow management module for Databricks Workflow Exporter.

This module handles interactions with the Databricks SDK for retrieving
workflow definitions and permissions.
"""

import os
import shutil
from typing import Dict, List, Any, Optional, Tuple
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
    
    def get_pipeline_acls(self, pipeline_id: str) -> List[Dict[str, str]]:
        """
        Retrieves and transforms pipeline permissions.
        
        Args:
            pipeline_id: The Databricks pipeline ID to retrieve permissions for
            
        Returns:
            List of dictionaries containing permission information
        """
        self.logger.debug(f"Retrieving pipeline permissions for pipeline ID: {pipeline_id}")
        
        try:
            # Get pipeline permissions using the SDK
            permissions = self.client.permissions.get(
                request_object_type="pipelines", 
                request_object_id=pipeline_id  # Pipeline ID is already a string
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
            
            self.logger.debug(f"Retrieved {len(output)} permissions for pipeline {pipeline_id}")
            return output
            
        except Exception as e:
            self.logger.warning(f"Error retrieving pipeline permissions for {pipeline_id}: {e}")
            return []
    
    def get_pipeline_workflow_tasks(self, pipeline_id: str) -> List[Dict[str, Any]]:
        """
        Retrieves the pipeline library definitions for a given pipeline ID.
        Handles both legacy pipelines and lakeflow pipelines with root folders.
        
        Args:
            pipeline_id: The Databricks pipeline ID to retrieve libraries for
            
        Returns:
            List of dictionaries containing pipeline library information
        """
        self.logger.debug(f"Retrieving pipeline libraries for pipeline ID: {pipeline_id}")
        
        # Get pipeline details using the SDK
        pipeline_details = self.client.pipelines.get(pipeline_id=pipeline_id)
        
        # Debug: Log pipeline spec structure
        if hasattr(pipeline_details, 'spec'):
            self.logger.debug(f"Pipeline spec attributes: {dir(pipeline_details.spec)}")
            if hasattr(pipeline_details.spec, 'name'):
                self.logger.debug(f"Pipeline name: {pipeline_details.spec.name}")
            if hasattr(pipeline_details.spec, 'libraries'):
                self.logger.debug(f"Pipeline has {len(pipeline_details.spec.libraries)} libraries")
        
        all_libraries = []
        
        # Check for root folder to determine pipeline type
        root_folder = None
        if hasattr(pipeline_details, 'spec'):
            # Check for root_path in pipeline spec
            if hasattr(pipeline_details.spec, 'configuration') and pipeline_details.spec.configuration:
                # Root path might be in configuration
                self.logger.debug(f"Pipeline configuration keys: {list(pipeline_details.spec.configuration.keys())}")
                for key, value in pipeline_details.spec.configuration.items():
                    self.logger.debug(f"Configuration key: {key} = {value}")
                    if 'root' in key.lower() and 'path' in key.lower():
                        root_folder = value
                        self.logger.debug(f"Found root_path in configuration: {key} = {value}")
                        break
            
            # Also check if there's a direct root_path attribute
            if hasattr(pipeline_details.spec, 'root_path'):
                root_folder = pipeline_details.spec.root_path
                self.logger.debug(f"Found direct root_path attribute: {root_folder}")
        
        pipeline_type = "lakeflow" if root_folder else "legacy"
        self.logger.info(f"Detected pipeline type: {pipeline_type}" + (f" with root path: {root_folder}" if root_folder else ""))
        
        if pipeline_type == "lakeflow":
            # For lakeflow pipelines with root folder
            all_libraries.append({
                'Pipeline_Name': getattr(pipeline_details.spec, 'name', f"pipeline_{pipeline_id}"),
                'PipelineId': pipeline_id,
                'Library_Key': 'root_path',
                'Library_Type': 'root_path',
                'Root_Path': root_folder,
                'Notebook_Path': None,
                'Notebook_Source': 'WORKSPACE',
                'Python_File': None,
                'SQL_File': None,
                'Glob_Pattern': None,
                'Glob_Files': [],
                'Libraries': [],
                'Environment_Key': None
            })
            
            # Check individual notebooks - only add if they're outside the root folder
            if hasattr(pipeline_details.spec, 'libraries'):
                self.logger.debug(f"Processing {len(pipeline_details.spec.libraries)} libraries for lakeflow pipeline")
                for i, lib in enumerate(pipeline_details.spec.libraries):
                    self.logger.debug(f"Library {i}: {dir(lib)}")
                    if hasattr(lib, 'notebook') and lib.notebook:
                        notebook_path = getattr(lib.notebook, 'path', None)
                        self.logger.debug(f"Found notebook library: {notebook_path}")
                        if notebook_path and not notebook_path.startswith(root_folder):
                            # Notebook is outside root folder, add it for individual download
                            lib_info = {
                                'Pipeline_Name': getattr(pipeline_details.spec, 'name', f"pipeline_{pipeline_id}"),
                                'PipelineId': pipeline_id,
                                'Library_Key': f"external_notebook_{len(all_libraries)}",
                                'Library_Type': 'external_notebook',
                                'Root_Path': None,
                                'Notebook_Path': notebook_path,
                                'Notebook_Source': 'WORKSPACE',
                                'Python_File': notebook_path if notebook_path.endswith('.py') else None,
                                'SQL_File': notebook_path if notebook_path.endswith('.sql') else None,
                                'Glob_Pattern': None,
                                'Glob_Files': [],
                                'Libraries': [],
                                'Environment_Key': None
                            }
                            all_libraries.append(lib_info)
                            self.logger.debug(f"Added external notebook (outside root folder): {notebook_path}")
                        else:
                            self.logger.debug(f"Skipping notebook (inside root folder): {notebook_path}")
                    else:
                        self.logger.debug(f"Library {i} is not a notebook library: {type(lib)}")
            else:
                self.logger.debug("No libraries found in lakeflow pipeline spec")
        
        else:
            # For legacy pipelines without root folder - process all libraries individually
            if hasattr(pipeline_details, 'spec') and hasattr(pipeline_details.spec, 'libraries'):
                for lib in pipeline_details.spec.libraries:
                    lib_info = {
                        'Pipeline_Name': getattr(pipeline_details.spec, 'name', f"pipeline_{pipeline_id}"),
                        'PipelineId': pipeline_id,
                        'Library_Key': f"library_{len(all_libraries)}",
                        'Library_Type': None,
                        'Root_Path': None,
                        'Notebook_Path': None,
                        'Notebook_Source': 'WORKSPACE',
                        'Python_File': None,
                        'SQL_File': None,
                        'Glob_Pattern': None,
                        'Glob_Files': [],
                        'Libraries': [],
                        'Environment_Key': None
                    }
                    
                    # Extract notebook library information
                    if hasattr(lib, 'notebook') and lib.notebook:
                        lib_info['Library_Type'] = 'notebook_library'
                        lib_info['Notebook_Path'] = getattr(lib.notebook, 'path', None)
                        
                        # Determine file type from path
                        if lib_info['Notebook_Path']:
                            if lib_info['Notebook_Path'].endswith('.py'):
                                lib_info['Python_File'] = lib_info['Notebook_Path']
                            elif lib_info['Notebook_Path'].endswith('.sql'):
                                lib_info['SQL_File'] = lib_info['Notebook_Path']
                    
                    # Extract glob library information
                    elif hasattr(lib, 'glob') and lib.glob:
                        lib_info['Library_Type'] = 'glob_library'
                        
                        # Get the glob pattern
                        if hasattr(lib.glob, 'include'):
                            if isinstance(lib.glob.include, str):
                                lib_info['Glob_Pattern'] = lib.glob.include
                            elif isinstance(lib.glob.include, list):
                                # If multiple patterns, create separate entries
                                for pattern in lib.glob.include:
                                    pattern_lib_info = lib_info.copy()
                                    pattern_lib_info['Library_Key'] = f"library_{len(all_libraries)}_{pattern}"
                                    pattern_lib_info['Glob_Pattern'] = pattern
                                    
                                    # Expand glob pattern to actual files
                                    try:
                                        matching_files = self._expand_glob_pattern(pattern)
                                        pattern_lib_info['Glob_Files'] = matching_files
                                        self.logger.debug(f"Glob pattern '{pattern}' expanded to {len(matching_files)} files")
                                    except Exception as e:
                                        self.logger.error(f"Error expanding glob pattern '{pattern}': {e}")
                                        pattern_lib_info['Glob_Files'] = []
                                    
                                    all_libraries.append(pattern_lib_info)
                                continue  # Skip the main append since we added individual patterns
                        
                        # Expand glob pattern to actual files for single pattern
                        if lib_info['Glob_Pattern']:
                            try:
                                matching_files = self._expand_glob_pattern(lib_info['Glob_Pattern'])
                                lib_info['Glob_Files'] = matching_files
                                self.logger.debug(f"Glob pattern '{lib_info['Glob_Pattern']}' expanded to {len(matching_files)} files")
                            except Exception as e:
                                self.logger.error(f"Error expanding glob pattern '{lib_info['Glob_Pattern']}': {e}")
                                lib_info['Glob_Files'] = []
                    
                    # Extract file library information (for wheel files, etc.)
                    elif hasattr(lib, 'file') and lib.file:
                        lib_info['Library_Type'] = 'file_library'
                        file_path = getattr(lib.file, 'path', None)
                        if file_path:
                            if file_path.endswith('.whl'):
                                lib_info['Libraries'].append({
                                    'type': 'whl',
                                    'path': file_path
                                })
                            elif file_path.endswith('.jar'):
                                lib_info['Libraries'].append({
                                    'type': 'jar',
                                    'path': file_path
                                })
                            elif file_path.endswith('.py'):
                                lib_info['Python_File'] = file_path
                            elif file_path.endswith('.sql'):
                                lib_info['SQL_File'] = file_path
                    
                    # Extract jar library information
                    elif hasattr(lib, 'jar') and lib.jar:
                        lib_info['Library_Type'] = 'jar_library'
                        jar_path = lib.jar
                        if isinstance(jar_path, str) and jar_path.strip():
                            lib_info['Libraries'].append({
                                'type': 'jar',
                                'path': jar_path
                            })
                    
                    # Extract whl library information
                    elif hasattr(lib, 'whl') and lib.whl:
                        lib_info['Library_Type'] = 'whl_library'
                        whl_path = lib.whl
                        if isinstance(whl_path, str) and whl_path.strip():
                            lib_info['Libraries'].append({
                                'type': 'whl',
                                'path': whl_path
                            })
                    
                    all_libraries.append(lib_info)
        
        # Also check for pipeline-level environment dependencies (for both types)
        if (hasattr(pipeline_details, 'spec') and 
            hasattr(pipeline_details.spec, 'environment') and 
            hasattr(pipeline_details.spec.environment, 'dependencies')):
            
            env_info = {
                'Pipeline_Name': getattr(pipeline_details.spec, 'name', f"pipeline_{pipeline_id}"),
                'PipelineId': pipeline_id,
                'Library_Key': f"environment_dependencies",
                'Library_Type': 'environment_dependencies',
                'Root_Path': None,
                'Notebook_Path': None,
                'Notebook_Source': None,
                'Python_File': None,
                'SQL_File': None,
                'Glob_Pattern': None,
                'Glob_Files': [],
                'Libraries': [],
                'Environment_Key': 'pipeline_environment'
            }
            
            for dep in pipeline_details.spec.environment.dependencies:
                if isinstance(dep, str) and dep.strip():
                    if dep.endswith('.whl'):
                        env_info['Libraries'].append({
                            'type': 'whl',
                            'path': dep
                        })
                    elif dep.endswith('.jar'):
                        env_info['Libraries'].append({
                            'type': 'jar', 
                            'path': dep
                        })
            
            if env_info['Libraries']:  # Only add if there are libraries
                all_libraries.append(env_info)
        
        self.logger.debug(f"Retrieved {len(all_libraries)} libraries for pipeline {pipeline_id}")
        return all_libraries
    
    def _expand_glob_pattern(self, pattern: str) -> List[str]:
        """
        Expand a glob pattern to actual file paths using workspace.list API.
        
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
                # If no wildcard, return the pattern itself if it's a valid file
                return [pattern]
            
            self.logger.debug(f"Expanding glob pattern '{pattern}' from base path: {base_path}")
            
            matching_files = []
            
            try:
                # List workspace contents recursively
                workspace_objects = self.client.workspace.list(base_path, recursive=True)
                
                for obj in workspace_objects:
                    if hasattr(obj, 'path') and hasattr(obj, 'object_type'):
                        obj_path = obj.path
                        obj_type = obj.object_type
                        
                        # Only include files (not directories) - handle both enum and string values
                        if (hasattr(obj_type, 'value') and obj_type.value in ['FILE', 'NOTEBOOK']) or str(obj_type) in ['FILE', 'NOTEBOOK']:
                            # Check if the path matches the pattern
                            if fnmatch.fnmatch(obj_path, pattern):
                                matching_files.append(obj_path)
                                self.logger.debug(f"Glob match: {obj_path}")
                
            except Exception as e:
                self.logger.error(f"Error listing workspace contents for pattern {pattern}: {e}")
                return []
            
            self.logger.debug(f"Glob pattern '{pattern}' expanded to {len(matching_files)} files")
            return matching_files
            
        except Exception as e:
            self.logger.error(f"Error expanding glob pattern {pattern}: {e}")
            return []
    
    def download_root_folder(self, root_folder_path: str, local_directory: str) -> List[Dict[str, Any]]:
        """
        Download entire root folder recursively from Databricks workspace.
        
        Args:
            root_folder_path: Path to the root folder in workspace
            local_directory: Local directory to save the folder structure
            
        Returns:
            List of download results for each file in the folder
        """
        try:
            self.logger.debug(f"Downloading root folder: {root_folder_path} to {local_directory}")
            
            downloaded_files = []
            
            # First, check if the root folder exists and get its status
            try:
                folder_status = self.client.workspace.get_status(root_folder_path)
                self.logger.debug(f"Root folder status: {folder_status}")
                if folder_status.object_type.value != 'DIRECTORY':
                    self.logger.warning(f"Root path {root_folder_path} is not a directory: {folder_status.object_type}")
                    return []
            except Exception as e:
                self.logger.error(f"Root folder does not exist or cannot be accessed: {root_folder_path} - {e}")
                return []
            
            # List all files in the root folder recursively
            try:
                self.logger.debug(f"Listing workspace objects in {root_folder_path} recursively...")
                workspace_objects = list(self.client.workspace.list(root_folder_path, recursive=True))
                self.logger.debug(f"Found {len(workspace_objects)} objects in root folder")
                
                # Debug: log first few objects
                for i, obj in enumerate(workspace_objects[:5]):  # Log first 5 objects
                    self.logger.debug(f"Object {i}: path={getattr(obj, 'path', 'N/A')}, type={getattr(obj, 'object_type', 'N/A')}")
                
                if len(workspace_objects) > 5:
                    self.logger.debug(f"... and {len(workspace_objects) - 5} more objects")
                
                for obj in workspace_objects:
                    if hasattr(obj, 'path') and hasattr(obj, 'object_type'):
                        obj_path = obj.path
                        obj_type = obj.object_type
                        
                        self.logger.debug(f"Processing object: {obj_path} (type: {obj_type})")
                        
                        # Only process files (not directories) - handle both enum and string values
                        if (hasattr(obj_type, 'value') and obj_type.value in ['FILE', 'NOTEBOOK']) or str(obj_type) in ['FILE', 'NOTEBOOK']:
                            try:
                                # Calculate relative path within the root folder
                                relative_path = os.path.relpath(obj_path, root_folder_path)
                                self.logger.debug(f"Relative path: {relative_path}")
                                
                                # Create local directory structure
                                local_file_dir = os.path.join(local_directory, os.path.dirname(relative_path))
                                os.makedirs(local_file_dir, exist_ok=True)
                                self.logger.debug(f"Created local directory: {local_file_dir}")
                                
                                # Determine artifact type
                                artifact_type = 'py' if obj_path.endswith('.py') else 'sql' if obj_path.endswith('.sql') else 'notebook'
                                
                                # Download the file
                                local_file_path = os.path.join(local_file_dir, os.path.basename(obj_path))
                                
                                self.logger.debug(f"Downloading {obj_path} to {local_file_path}")
                                success, error_msg = self._download_workspace_file(obj_path, local_file_path, artifact_type)
                                
                                downloaded_files.append({
                                    'original_path': obj_path,
                                    'local_path': local_file_path if success else '',
                                    'relative_path': relative_path,
                                    'success': success,
                                    'error_message': error_msg if not success else '',
                                    'artifact_type': artifact_type
                                })
                                
                                if success:
                                    self.logger.debug(f"Downloaded root folder file: {obj_path} -> {local_file_path}")
                                else:
                                    self.logger.warning(f"Failed to download root folder file {obj_path}: {error_msg}")
                                    
                            except Exception as e:
                                self.logger.error(f"Error processing root folder file {obj_path}: {e}")
                                downloaded_files.append({
                                    'original_path': obj_path,
                                    'success': False,
                                    'error_message': str(e),
                                    'artifact_type': 'unknown'
                                })
                
            except Exception as e:
                self.logger.error(f"Error listing root folder contents {root_folder_path}: {e}")
                return []
            
            successful_downloads = [f for f in downloaded_files if f.get('success', False)]
            self.logger.info(f"Root folder download completed: {len(successful_downloads)}/{len(downloaded_files)} files downloaded successfully")
            
            return downloaded_files
            
        except Exception as e:
            self.logger.error(f"Error downloading root folder {root_folder_path}: {e}")
            return []
    
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
            
            else:
                # Default fallback: try workspace download for paths that don't start with /Workspace or /Volume
                self.logger.debug(f"Path doesn't start with /Workspace or /Volume, trying workspace download: {artifact_path}")
                success, error_msg = self._download_workspace_file(artifact_path, local_file_path, artifact_type)
            
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

    def get_pipeline_details(self, pipeline_id: str) -> Optional[Any]:
        """
        Get pipeline details using the Databricks SDK.
        
        Args:
            pipeline_id: The Databricks pipeline ID
            
        Returns:
            Pipeline details object or None if not found
        """
        try:
            self.logger.debug(f"Retrieving pipeline details for pipeline ID: {pipeline_id}")
            pipeline_details = self.client.pipelines.get(pipeline_id=pipeline_id)
            self.logger.debug(f"Successfully retrieved pipeline details for {pipeline_id}")
            return pipeline_details
        except Exception as e:
            self.logger.error(f"Error retrieving pipeline details for {pipeline_id}: {e}")
            return None
    
    def list_workspace_objects(self, path: str, recursive: bool = True) -> List[Any]:
        """
        List workspace objects using the Databricks SDK.
        
        Args:
            path: Workspace path to list
            recursive: Whether to list recursively
            
        Returns:
            List of workspace objects
        """
        try:
            self.logger.debug(f"Listing workspace objects at path: {path}")
            objects = list(self.client.workspace.list(path, recursive=recursive))
            self.logger.debug(f"Found {len(objects)} objects at {path}")
            return objects
        except Exception as e:
            self.logger.error(f"Error listing workspace objects at {path}: {e}")
            return []
    
    def export_artifacts_batch(self, artifacts: List[Dict[str, Any]], base_local_directory: str, 
                              filter_by_type: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Export multiple artifacts with optional type filtering (generalized version).
        
        Args:
            artifacts: List of artifact dictionaries
            base_local_directory: Base local directory for exports
            filter_by_type: Optional list of artifact types to include (e.g., ['py', 'sql', 'whl'])
            
        Returns:
            List of dictionaries with export results
        """
        if filter_by_type:
            filtered_artifacts = [a for a in artifacts if a.get('type') in filter_by_type]
            self.logger.debug(f"Filtered {len(artifacts)} artifacts to {len(filtered_artifacts)} by type: {filter_by_type}")
        else:
            filtered_artifacts = artifacts
        
        return self.export_multiple_artifacts(filtered_artifacts, base_local_directory) 