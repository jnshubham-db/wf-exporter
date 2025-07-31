"""
App installer for WF Exporter.

This module handles installation of the WF Exporter web application to Databricks
with integration to the installer framework.
"""

import os
import io
import time
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat
from databricks.sdk.service.apps import AppDeployment

from .installer_core import InstallerCore

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ActiveDeploymentError(Exception):
    """Exception raised when there's an active deployment in progress."""
    def __init__(self, app_name: str, message: str = None):
        self.app_name = app_name
        if message is None:
            message = f"App {app_name} has active deployments in progress. Manual intervention required."
        super().__init__(message)


class AppInstaller:
    """Handles app installation to Databricks."""
    
    def __init__(self, profile: Optional[str] = None):
        """
        Initialize the app installer.
        
        Args:
            profile: Databricks profile to use
        """
        self.profile = profile
        self.core = InstallerCore(profile=profile)
        self.client = self.core.client
        
        # App configuration
        self.app_name = "wf-exporter-app"
        self.workspace_app_path = "/Workspace/Applications/wf_exporter/app_config"
        self.workspace_base_path = "/Workspace/Applications/wf_exporter"
        
        # Get the wf_app source directory using multiple fallback strategies
        self.wf_app_dir = self._find_wf_app_directory()
        
        if not self.client:
            self.core._initialize_client()
    
    def _find_wf_app_directory(self) -> Path:
        """
        Find the wf_app directory using multiple strategies.
        
        Returns:
            Path to wf_app directory
            
        Raises:
            FileNotFoundError: If wf_app directory cannot be found
        """
        possible_paths = []
        
        # 1. Try to find wf_app by importing it and getting its path
        try:
            import wf_app
            if hasattr(wf_app, '__file__') and wf_app.__file__:
                wf_app_path = Path(wf_app.__file__).parent
                possible_paths.append(wf_app_path)
        except ImportError:
            pass
        
        # 2. Try using pkg_resources to find the installed wf_app package
        try:
            import pkg_resources
            dist = pkg_resources.get_distribution('wfexporter')
            site_packages = Path(dist.location)
            possible_paths.append(site_packages / "wf_app")
        except (ImportError, pkg_resources.DistributionNotFound, FileNotFoundError):
            pass
        
        # 3. Try relative to this file's location (development structure)
        current_dir = Path(__file__).parent.parent.parent.parent
        possible_paths.append(current_dir / "wf_app")
        
        # 4. Try in the same directory as the package root (development)
        wf_exporter_dir = Path(__file__).parent.parent.parent
        possible_paths.append(wf_exporter_dir.parent / "wf_app")
        
        # Find the first existing wf_app directory
        for path in possible_paths:
            if path.exists() and path.is_dir() and (path / "main.py").exists():
                logger.debug(f"Found wf_app directory: {path}")
                return path
        
        # If we can't find it, raise an error with helpful information
        raise FileNotFoundError(
            f"wf_app directory not found in any of the following locations:\n" +
            "\n".join(f"  • {path}" for path in possible_paths) +
            "\n\nMake sure the wf_app package is properly installed or you're running from the project root."
        )
    
    def install(self, progress=None) -> Dict[str, Any]:
        """
        Install the app to Databricks with wait-and-retry for active deployments.
        
        Args:
            progress: Optional progress indicator for CLI updates
        
        Returns:
            Dictionary containing installation results including app_id
            
        Raises:
            ActiveDeploymentError: If active deployments persist after all retries
        """
        logger.info(f"Starting app installation: {self.app_name}")
        
        if not self.client:
            self.core._initialize_client()

        # Create workspace directory
        if progress:
            progress.start_step("Creating app workspace directory...")
        logger.debug("Creating workspace directory...")
        self._create_workspace_directory()
        if progress:
            progress.complete_step("App workspace directory created")
        
        # Upload app files
        if progress:
            progress.start_step("Uploading app files...")
        logger.debug("Uploading app files...")
        uploaded_files = self._upload_app_files()
        if progress:
            progress.complete_step("App files uploaded")
        
        # Create app.yaml configuration
        if progress:
            progress.start_step("Creating app configuration...")
        logger.debug("Creating app.yaml configuration...")
        self._create_app_yaml()
        if progress:
            progress.complete_step("App configuration created")
        
        # Create or update the app
        if progress:
            progress.start_step("Compute is starting. Please wait for it to be ready before deploying the app. This process may take 2 to 3 minutes....")
        logger.debug("Creating or updating app...")
        app = self._create_or_update_app()
        
        # Extract app_id from the app object
        logger.debug("Extracting app_id from app object...")
        app_id = self._extract_app_id(app)
        if not app_id:
            logger.warning("Could not extract app_id from app object")
        else:
            logger.info(f"Successfully extracted app_id: {app_id}")
        if progress:
            progress.complete_step("Databricks app created")

        # Deploy the app with retry logic for active deployments
        if progress:
            progress.start_step("Deploying app...")
        max_retries = 3
        wait_time_minutes = 2
        deployment = None
        
        for attempt in range(max_retries + 1):  # 0, 1, 2, 3 (4 total attempts)
            try:
                if progress and attempt > 0:
                    progress.update_step(f"Deploying app (attempt {attempt + 1})...")
                logger.debug(f"Deploying app (attempt {attempt + 1}/{max_retries + 1})...")
                deployment = self._deploy_app()
                break  # Deployment successful, exit retry loop
                
            except ActiveDeploymentError as e:
                if attempt < max_retries:
                    logger.warning(f"Active deployment in progress for {self.app_name}")
                    logger.info(f"Waiting {wait_time_minutes} minutes before retry (attempt {attempt + 1}/{max_retries + 1})...")
                    
                    if progress:
                        progress.update_step(f"Waiting for active deployment to complete ({wait_time_minutes}min)...")
                    
                    import time
                    time.sleep(wait_time_minutes * 60)  # Convert minutes to seconds
                    logger.info(f"Retrying deployment...")
                    continue
                else:
                    # Final attempt failed
                    logger.error(f"Failed to deploy after {max_retries + 1} attempts")
                    logger.info("💡 The app has active deployments that persist after waiting.")
                    logger.info("   You may need to manually delete the app in the Databricks UI and try again.")
                    if progress:
                        progress.fail_step("App deployment failed after retries")
                    raise e
        if progress:
            progress.complete_step("App deployed successfully")

        # Get app URL
        if progress:
            progress.start_step("Getting app URL...")
        logger.debug("Getting app URL...")
        app_url = self._get_app_url()
        if progress:
            progress.complete_step("App URL retrieved")
        
        # Set permissions if app_id is available
        if app_id:
            if progress:
                progress.start_step("Setting app permissions...")
            logger.debug("Setting permissions...")
            folder_success, workflow_success = self._set_permissions(app_id)
            self._summarize_permission_status(app_id, folder_success, workflow_success)
            if progress:
                progress.complete_step("App permissions configured")
        
        # Prepare result
        result = {
            "app_name": self.app_name,
            "app_url": app_url,
            "uploaded_files": uploaded_files,
            "app_id": app_id,
            "deployment_id": deployment.deployment_id if deployment else None
        }
        
        if progress:
            progress.finish()
        
        logger.info(f"App installation completed successfully!")
        return result
    
    def install_with_force_delete(self, progress=None) -> Dict[str, Any]:
        """
        Install the app by forcefully deleting any existing app first.
        This method bypasses active deployment checks by deleting the app completely.
        
        Args:
            progress: Optional progress indicator for CLI updates
        
        Returns:
            Dictionary containing installation results including app_id
        """
        logger.info(f"Starting forced app installation: {self.app_name}")
        logger.warning("This will delete any existing app and all its deployments!")
        
        # Force delete the app if it exists
        if progress:
            progress.start_step("Deleting existing app...")
        self._delete_app_if_exists()
        if progress:
            progress.complete_step("Existing app deleted")
        
        # Wait a bit more for complete deletion
        if progress:
            progress.start_step("Waiting for cleanup completion...")
        import time
        time.sleep(10)
        if progress:
            progress.complete_step("Cleanup completed")
        
        # Create workspace directory
        if progress:
            progress.start_step("Creating app workspace directory...")
        self._create_workspace_directory()
        if progress:
            progress.complete_step("App workspace directory created")
        
        # Upload app files
        if progress:
            progress.start_step("Uploading app files...")
        uploaded_files = self._upload_app_files()
        if progress:
            progress.complete_step("App files uploaded")
        
        # Create app.yaml configuration
        if progress:
            progress.start_step("Creating app configuration...")
        self._create_app_yaml()
        if progress:
            progress.complete_step("App configuration created")
        
        # Create the app (should be fresh since we deleted it)
        if progress:
            progress.start_step("Creating Databricks app...")
        app = self._create_or_update_app()
        
        # Extract app_id from the app object
        app_id = self._extract_app_id(app)
        if not app_id:
            logger.warning("Could not extract app_id from app object")
        else:
            logger.info(f"Successfully extracted app_id: {app_id}")
        if progress:
            progress.complete_step("Databricks app created")
        
        # Deploy the app
        if progress:
            progress.start_step("Deploying app...")
        deployment = self._deploy_app()
        if progress:
            progress.complete_step("App deployed successfully")
        
        # Get app URL
        if progress:
            progress.start_step("Getting app URL...")
        app_url = self._get_app_url()
        if progress:
            progress.complete_step("App URL retrieved")
        
        # Set permissions if app_id is available
        if app_id:
            if progress:
                progress.start_step("Setting app permissions...")
            try:
                logger.info(f"Setting permissions for app_id: {app_id}")
                folder_success = self._set_folder_permissions(app_id)
                workflow_success = self._set_workflow_permissions(app_id)
                self._summarize_permission_status(app_id, folder_success, workflow_success)
                if progress:
                    progress.complete_step("App permissions configured")
            except Exception as e:
                logger.warning(f"Failed to set permissions for app_id {app_id}: {e}")
                logger.info("Permission setting is optional - installation can continue without it")
                if progress:
                    progress.complete_step("App permissions partially configured")
        else:
            logger.warning("No app_id available - skipping permission setup")
            logger.info("You may need to set permissions manually in the Databricks UI")
        
        if progress:
            progress.finish()
        
        logger.info(f"Forced app installation completed successfully!")
        
        return {
            'app_name': self.app_name,
            'app_id': app_id,
            'app_url': app_url,
            'workspace_path': self.workspace_app_path,
            'uploaded_files': len(uploaded_files),
            'deployment_id': deployment.deployment_id if deployment else None
        }
    
    def _set_folder_permissions(self, app_id: str) -> bool:
        """
        Set can_manage permissions on the workspace folder for the app using direct API call.
        
        Args:
            app_id: The application ID to grant permissions to
            
        Returns:
            True if permissions were set successfully, False otherwise
        """
        logger.info(f"Setting folder permissions for app_id: {app_id} on folder: {self.workspace_base_path}")
        logger.debug(f"Using app_id as service principal: {app_id}")
        
        try:
            # First, try to get the workspace object ID for the folder
            folder_path = self.workspace_base_path
            logger.debug(f"Getting workspace object ID for folder: {folder_path}")
            
            # Get folder info to get the object ID
            try:
                folder_info = self.client.workspace.get_status(folder_path)
                folder_object_id = folder_info.object_id
                logger.info(f"Found folder object ID: {folder_object_id}")
            except Exception as e:
                logger.error(f"Could not get folder object ID for {folder_path}: {e}")
                return False
            
            # Prepare the API request payload
            permissions_payload = {
                "access_control_list": [
                    {
                        "service_principal_name": app_id,
                        "permission_level": "CAN_MANAGE"
                    }
                ]
            }
            
            # Use direct API call to set permissions
            api_endpoint = f"permissions/directories/{folder_object_id}"
            logger.debug(f"Calling API endpoint: {api_endpoint}")
            logger.debug(f"API payload: {permissions_payload}")
            
            # Make the API call using the client's do method with proper JSON serialization
            import json
            response = self.client.api_client.do(
                method="PATCH",
                path=f"/api/2.0/{api_endpoint}",
                data=json.dumps(permissions_payload),
                headers={"Content-Type": "application/json"}
            )
            
            logger.info(f"Successfully set folder permissions for app_id: {app_id}")
            logger.debug(f"API response: {response}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to set folder permissions for app_id {app_id}: {e}")
            logger.debug(f"Permission error details: {type(e).__name__}: {str(e)}")
            logger.warning("Could not set workspace folder permissions using API")
            logger.info("Please set permissions on the folder manually in the Databricks UI")
            logger.info(f"Folder path: {self.workspace_base_path}")
            logger.info(f"Grant CAN_MANAGE access to service principal: {app_id}")
            return False
    
    def _set_workflow_permissions(self, app_id: str) -> bool:
        """
        Set can_manage permissions on the workflow for the app.
        
        Args:
            app_id: The application ID to grant permissions to
            
        Returns:
            True if permissions were set successfully, False otherwise
        """
        try:
            # Get the workflow_id from the app config or core
            workflow_id = self._get_workflow_id()
            if not workflow_id:
                logger.warning("No workflow_id found, skipping workflow permissions")
                return False
            
            logger.info(f"Setting workflow permissions for app_id: {app_id} on workflow: {workflow_id}")
            logger.debug(f"Using app_id as service principal name: {app_id}")
            
            # Import the correct classes for job permissions
            from databricks.sdk.service.iam import AccessControlRequest, PermissionLevel
            
            # Create access control request for the job
            access_control_request = AccessControlRequest(
                service_principal_name=app_id,
                permission_level=PermissionLevel.CAN_MANAGE
            )
            
            logger.debug(f"Created access control request: {access_control_request}")
            
            # Use the permissions API for jobs - this is the correct approach
            logger.debug(f"Calling permissions.update for job: {workflow_id}")
            self.client.permissions.update(
                request_object_type="jobs",
                request_object_id=str(workflow_id),
                access_control_list=[access_control_request]
            )
            
            logger.info(f"Successfully set workflow permissions for app_id: {app_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to set workflow permissions for app_id {app_id}: {e}")
            logger.debug(f"Permission error details: {type(e).__name__}: {str(e)}")
            return False
    
    def _get_workflow_id(self) -> Optional[str]:
        """
        Get the workflow ID from the app configuration or core installer.
        
        Returns:
            The workflow ID if found, None otherwise
        """
        try:
            # Try to get workflow ID from app config file
            app_config_path = self.wf_app_dir / "app_config.yml"
            if app_config_path.exists():
                import yaml
                with open(app_config_path, 'r') as f:
                    app_config = yaml.safe_load(f)
                    # Check the correct structure: workflow_config.job_id
                    workflow_config = app_config.get('export-job', {})
                    workflow_id = workflow_config.get('job_id')
                    if workflow_id:
                        logger.debug(f"Found workflow_id in app_config.yml: {workflow_id}")
                        return str(workflow_id)
            
            # Could also try to get from environment or other sources
            # For now, return None if not found
            logger.debug("No workflow_id found in app_config.yml")
            return None
            
        except Exception as e:
            logger.warning(f"Error getting workflow_id: {e}")
            return None

    def _create_workspace_directory(self) -> None:
        """Create the app workspace directory."""
        self.core.create_workspace_directories([self.workspace_app_path])
    
    def _upload_app_files(self) -> List[str]:
        """
        Upload app files to workspace, excluding files listed in .appignore.
        
        Returns:
            List of uploaded file paths
        """
        if not self.wf_app_dir.exists():
            raise FileNotFoundError(f"wf_app directory not found: {self.wf_app_dir}")
        
        # Read .appignore patterns
        ignore_patterns = self._read_appignore()
        
        uploaded_files = []
        
        # Upload all files recursively, excluding ignored patterns
        for file_path in self.wf_app_dir.rglob('*'):
            if file_path.is_file():
                # Calculate relative path from wf_app_dir
                relative_path = file_path.relative_to(self.wf_app_dir)
                
                # Check if file should be ignored
                if self._should_ignore_file(relative_path, ignore_patterns):
                    continue
                
                # Upload file
                workspace_file_path = f"{self.workspace_app_path}/{relative_path.as_posix()}"
                self._upload_file(file_path, workspace_file_path)
                uploaded_files.append(workspace_file_path)
        
        logger.info(f"Uploaded {len(uploaded_files)} files to workspace")
        return uploaded_files
    
    def _read_appignore(self) -> List[str]:
        """
        Read .appignore file and return list of patterns to ignore.
        
        Returns:
            List of ignore patterns
        """
        appignore_path = self.wf_app_dir / ".appignore"
        patterns = []
        
        if appignore_path.exists():
            with open(appignore_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        patterns.append(line)
        
        # Always ignore .appignore itself
        patterns.append('.appignore')
        
        return patterns
    
    def _should_ignore_file(self, file_path: Path, ignore_patterns: List[str]) -> bool:
        """
        Check if a file should be ignored based on patterns.
        
        Args:
            file_path: Relative path of the file
            ignore_patterns: List of ignore patterns
            
        Returns:
            True if file should be ignored
        """
        file_str = file_path.as_posix()
        
        for pattern in ignore_patterns:
            # Simple pattern matching
            if pattern in file_str or file_path.name == pattern:
                return True
            
            # Handle directory patterns
            if pattern.endswith('/') and file_str.startswith(pattern):
                return True
            
            # Handle glob-like patterns (basic)
            if '*' in pattern:
                import fnmatch
                if fnmatch.fnmatch(file_str, pattern) or fnmatch.fnmatch(file_path.name, pattern):
                    return True
        
        return False
    
    def _upload_file(self, source_path: Path, target_path: str) -> None:
        """
        Upload a file to the workspace, always replacing existing files.
        
        Args:
            source_path: Local file path
            target_path: Workspace target path
        """
        if not source_path.exists():
            raise FileNotFoundError(f"Source file not found: {source_path}")
        
        logger.debug(f"Reading file: {source_path} (size: {source_path.stat().st_size} bytes)")
        
        # Create directory structure in workspace
        target_dir = "/".join(target_path.split("/")[:-1])
        try:
            self.client.workspace.mkdirs(target_dir)
        except Exception:
            pass  # Directory might already exist
        
        # Read file content
        try:
            with open(source_path, 'rb') as f:
                content = f.read()
        except Exception as e:
            logger.error(f"Failed to read file {source_path}: {e}")
            raise
        
        logger.debug(f"Read {len(content)} bytes from {source_path}")
        
        # Use AUTO format for all files to avoid format-related issues
        file_format = ImportFormat.AUTO
        
        logger.debug(f"Uploading file with format: {file_format} (overwrite=True)")
        
        try:
            # Always replace existing files
            self.client.workspace.upload(
                path=target_path,
                content=io.BytesIO(content),
                format=file_format,
                overwrite=True
            )
            
            logger.info(f"Successfully uploaded: {source_path.name} -> {target_path}")
            
        except Exception as e:
            logger.error(f"Failed to upload {source_path} to {target_path}: {e}")
            # Log additional debug information
            logger.debug(f"File details - Path: {source_path}, Size: {len(content)} bytes, Target: {target_path}")
            raise
    
    def _create_app_yaml(self) -> None:
        """Create app.yaml configuration file in the workspace."""
        app_yaml_content = """command: ["flask", "--app", "main.py", "run"]"""
        
        app_yaml_path = f"{self.workspace_app_path}/app.yaml"
        
        logger.debug(f"Creating app.yaml with AUTO format at {app_yaml_path}")
        
        try:
            # Use AUTO format to avoid format-related issues
            self.client.workspace.upload(
                path=app_yaml_path,
                content=io.BytesIO(app_yaml_content.encode('utf-8')),
                format=ImportFormat.AUTO,
                overwrite=True
            )
            
            logger.info(f"Successfully created app.yaml at {app_yaml_path}")
            
        except Exception as e:
            logger.error(f"Failed to create app.yaml at {app_yaml_path}: {e}")
            logger.debug(f"App YAML content length: {len(app_yaml_content)} chars")
            raise
    
    def _create_or_update_app(self) -> Any:
        """
        Create a new app or get existing app.
        
        Returns:
            App object
        """
        logger.info(f"Creating/updating app: {self.app_name}")
        
        # Check if app already exists
        try:
            existing_app = self.client.apps.get(self.app_name)
            logger.info(f"App {self.app_name} already exists")
            return existing_app
                
        except Exception:
            logger.info(f"App {self.app_name} does not exist, creating new app")
        
        # Create new app with proper App object
        try:
            from databricks.sdk.service.apps import App
            
            # Create App object with required and optional parameters
            app_obj = App(
                name=self.app_name,
                description="Databricks Workflow Exporter Web Application"
            )
            
            logger.debug(f"Created App object: name={app_obj.name}, description={app_obj.description}")
            
            # Create the app using the App object and wait for it to complete
            new_app = self.client.apps.create_and_wait(app=app_obj)
            logger.info(f"Successfully created app {self.app_name}")
            return new_app
            
        except Exception as e:
            logger.error(f"Failed to create app {self.app_name}: {e}")
            raise
    
    def _check_active_deployments(self) -> bool:
        """
        Check if there are any active deployments for the app.
        
        Returns:
            True if there are active deployments, False otherwise
        """
        try:
            # List all deployments for the app
            deployments = self.client.apps.list_deployments(app_name=self.app_name)
            
            from databricks.sdk.service.apps import AppDeploymentState
            active_states = [
                AppDeploymentState.IN_PROGRESS,
                AppDeploymentState.CANCELLING
            ]
            
            for deployment in deployments:
                if deployment.status and deployment.status.state in active_states:
                    logger.info(f"Found active deployment: {deployment.deployment_id} in state: {deployment.status.state}")
                    return True
            
            return False
            
        except Exception as e:
            logger.debug(f"Could not check deployments (app might not exist): {e}")
            return False
    
    def _delete_app_if_exists(self) -> None:
        """
        Delete the app if it exists.
        """
        try:
            # Check if app exists
            existing_app = self.client.apps.get(self.app_name)
            logger.info(f"App {self.app_name} exists, deleting it...")
            
            # Delete the app
            self.client.apps.delete(self.app_name)
            logger.info(f"Successfully deleted app: {self.app_name}")
            
            # Wait a bit for deletion to complete
            import time
            time.sleep(5)
            
        except Exception as e:
            if "does not exist" in str(e).lower() or "not found" in str(e).lower():
                logger.debug(f"App {self.app_name} does not exist, nothing to delete")
            else:
                logger.warning(f"Error deleting app {self.app_name}: {e}")
    
    def _deploy_app(self) -> Optional[AppDeployment]:
        """
        Deploy the app with source code from workspace path.
        
        Returns:
            Deployment object or None if failed
            
        Raises:
            ActiveDeploymentError: If there are active deployments blocking the new deployment
        """
        logger.info(f"Deploying app {self.app_name} from {self.workspace_app_path}")
        
        try:
            # Create AppDeployment object with proper SDK types
            from databricks.sdk.service.apps import AppDeployment, AppDeploymentMode
            
            # Try AUTO_SYNC first, fallback to SNAPSHOT if not available
            deployment_mode = AppDeploymentMode.AUTO_SYNC
            
            try:
                # Create deployment configuration with AUTO_SYNC
                app_deployment = AppDeployment(
                    source_code_path=self.workspace_app_path,
                    mode=deployment_mode
                )
                
                logger.debug(f"Attempting deployment with mode: {deployment_mode}")
                
                # Deploy the app using the AppDeployment object and wait for completion
                deployment = self.client.apps.deploy_and_wait(
                    app_name=self.app_name,
                    app_deployment=app_deployment
                )
                
                logger.info(f"Deployment initiated with {deployment_mode}: {deployment.deployment_id}")
                
            except Exception as auto_sync_error:
                if "AUTO_SYNC is not enabled" in str(auto_sync_error):
                    logger.warning(f"AUTO_SYNC mode not available, falling back to SNAPSHOT mode")
                    
                    # Fallback to SNAPSHOT mode
                    deployment_mode = AppDeploymentMode.SNAPSHOT
                    app_deployment = AppDeployment(
                        source_code_path=self.workspace_app_path,
                        mode=deployment_mode
                    )
                    
                    logger.debug(f"Retrying deployment with mode: {deployment_mode}")
                    
                    deployment = self.client.apps.deploy_and_wait(
                        app_name=self.app_name,
                        app_deployment=app_deployment
                    )
                    
                    logger.info(f"Deployment initiated with {deployment_mode}: {deployment.deployment_id}")
                else:
                    # Re-raise if it's a different error
                    raise auto_sync_error
            
            # No need to wait manually since we used deploy_and_wait
            return deployment
            
        except Exception as e:
            # Check if it's an active deployment error
            if "active deployment in progress" in str(e).lower():
                raise ActiveDeploymentError(
                    self.app_name,
                    f"Cannot deploy app {self.app_name} - active deployment in progress. "
                    "Delete and recreate the app to continue."
                )
            else:
                logger.error(f"Deployment failed: {e}")
                logger.debug(f"App name: {self.app_name}, workspace path: {self.workspace_app_path}")
                raise
    
    def _wait_for_deployment(self, deployment_id: str, max_wait_time: int = 600) -> None:
        """
        Wait for deployment to complete.
        
        Args:
            deployment_id: Deployment ID to monitor
            max_wait_time: Maximum time to wait in seconds
        """
        logger.info("Waiting for deployment to complete...")
        
        start_time = time.time()
        check_interval = 10  # seconds
        
        while time.time() - start_time < max_wait_time:
            try:
                deployment_status = self.client.apps.get_deployment(
                    app_name=self.app_name,
                    deployment_id=deployment_id
                )
                
                status = deployment_status.status
                state = status.state  # Get the actual state enum
                message = status.message
                
                logger.info(f"Deployment status: {state} - {message}")
                
                from databricks.sdk.service.apps import AppDeploymentState
                
                if state == AppDeploymentState.SUCCEEDED:
                    logger.info("✅ Deployment completed successfully!")
                    return
                elif state == AppDeploymentState.FAILED:
                    logger.error(f"❌ Deployment failed: {message}")
                    raise RuntimeError(f"App deployment failed: {message}")
                elif state in [AppDeploymentState.IN_PROGRESS]:
                    logger.debug(f"⏳ Deployment in progress: {message}")
                    time.sleep(check_interval)
                elif state == AppDeploymentState.CANCELLED:
                    logger.error(f"❌ Deployment cancelled: {message}")
                    raise RuntimeError(f"App deployment cancelled: {message}")
                else:
                    logger.warning(f"⚠️ Unknown deployment state: {state} - {message}")
                    time.sleep(check_interval)
                    
            except Exception as e:
                logger.warning(f"Error checking deployment status: {e}")
                time.sleep(check_interval)
        
        logger.warning("Deployment timeout reached")
    
    def _get_app_url(self) -> str:
        """
        Get the URL for the deployed app.
        
        Returns:
            App URL
        """
        try:
            workspace_url = self.client.config.host
            app_url = f"{workspace_url}/apps/{self.app_name}"
            return app_url
        except Exception as e:
            logger.error(f"Could not retrieve app URL: {e}")
            return f"https://your-workspace/apps/{self.app_name}"
    
    def uninstall(self) -> None:
        """Uninstall the app."""
        self.core.uninstall_app() 

    def _set_permissions(self, app_id: str) -> tuple[bool, bool]:
        """
        Set permissions for the app on both folders and workflow.
        
        Args:
            app_id: The application ID to grant permissions to
            
        Returns:
            Tuple of (folder_success, workflow_success) indicating success status
        """
        folder_success = False
        workflow_success = False
        
        try:
            logger.info(f"Setting permissions for app_id: {app_id}")
            
            # Set folder permissions
            try:
                folder_success = self._set_folder_permissions(app_id)
            except Exception as e:
                logger.warning(f"Failed to set folder permissions for app_id {app_id}: {e}")
            
            # Set workflow permissions
            try:
                workflow_success = self._set_workflow_permissions(app_id)
            except Exception as e:
                logger.warning(f"Failed to set workflow permissions for app_id {app_id}: {e}")
            
            if not folder_success and not workflow_success:
                logger.warning("Failed to set any permissions - you may need to set them manually in the Databricks UI")
            
        except Exception as e:
            logger.warning(f"Failed to set permissions for app_id {app_id}: {e}")
            logger.info("Permission setting is optional - installation can continue without it")
        
        return folder_success, workflow_success

    def _extract_app_id(self, app) -> Optional[str]:
        """
        Extract app_id from the app object.
        
        Args:
            app: The app object returned from Databricks API
            
        Returns:
            The app_id if found, None otherwise
        """
        try:
            # According to Databricks SDK documentation, apps are identified by their 'name' attribute
            if hasattr(app, 'id') and app.name:
                logger.debug(f"Successfully extracted app_id: {app.id}")
                return app.id
            else:
                logger.error("App object does not have a 'id' attribute or it is None")
                return None
                
        except Exception as e:
            logger.error(f"Error extracting app_id from app object: {e}")
            return None

    def _summarize_permission_status(self, app_id: str, folder_success: bool, workflow_success: bool) -> None:
        """
        Summarize the permission setting status and provide guidance.
        
        Args:
            app_id: The application ID
            folder_success: True if folder permissions were set successfully, False otherwise
            workflow_success: True if workflow permissions were set successfully, False otherwise
        """
        logger.info(f"Permission Summary for App ID: {app_id}")
        logger.info("----------------------------------------")
        
        if folder_success:
            logger.info("✅ Folder Permissions: CAN_MANAGE access granted to service principal.")
        else:
            logger.warning("⚠️ Folder Permissions: Could not set CAN_MANAGE access. Please set manually in Databricks UI.")
            logger.info(f"Folder Path: {self.workspace_base_path}")
            logger.info(f"Grant CAN_MANAGE access to service principal: {app_id}")
        
        if workflow_success:
            logger.info("✅ Workflow Permissions: CAN_MANAGE access granted to service principal.")
        else:
            logger.warning("⚠️ Workflow Permissions: Could not set CAN_MANAGE access. Please set manually in Databricks UI.")
            logger.info(f"Workflow ID: {self._get_workflow_id()}")
            logger.info(f"Grant CAN_MANAGE access to service principal: {app_id}")
        
        logger.info("----------------------------------------") 