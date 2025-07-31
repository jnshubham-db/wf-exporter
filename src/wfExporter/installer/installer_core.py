"""
Core installer functionality for WF Exporter.

This module provides the base installer functionality, profile management,
and workspace validation.
"""

import os
import sys
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import DatabricksError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class InstallerCore:
    """Core installer functionality for WF Exporter components."""
    
    def __init__(self, profile: Optional[str] = None):
        """
        Initialize the installer core.
        
        Args:
            profile: Databricks profile to use
        """
        self.profile = profile
        self.client = None
        self.current_user = None
        
        if profile:
            self._initialize_client()
    
    def _initialize_client(self) -> None:
        """Initialize the Databricks client with the specified profile."""
        try:
            if self.profile:
                self.client = WorkspaceClient(profile=self.profile)
            else:
                self.client = WorkspaceClient()
            
            # Validate connection
            self.current_user = self.client.current_user.me()
            
        except Exception as e:
            raise RuntimeError(f"Failed to initialize Databricks client: {e}")
    
    def validate_workspace(self) -> bool:
        """
        Validate workspace connectivity and permissions.
        
        Returns:
            True if workspace is valid and accessible
        """
        try:
            if not self.client:
                self._initialize_client()
            
            # Test basic operations
            user = self.client.current_user.me()
            workspace_info = self.client.workspace.get_status("/")
            
            return True
        except Exception:
            return False
    
    def create_workspace_directories(self, directories: List[str]) -> None:
        """
        Create workspace directories if they don't exist.
        
        Args:
            directories: List of workspace paths to create
        """
        if not self.client:
            self._initialize_client()
        
        for directory in directories:
            try:
                logger.info(f"Creating workspace directory: {directory}")
                self.client.workspace.mkdirs(directory)
                logger.debug(f"Successfully created directory: {directory}")
            except DatabricksError as e:
                # Directory might already exist
                if "already exists" not in str(e).lower():
                    logger.error(f"Failed to create directory {directory}: {e}")
                    raise
                else:
                    logger.debug(f"Directory already exists: {directory}")
    
    def get_installation_status(self) -> Dict[str, Any]:
        """
        Get installation status of WF Exporter components.
        
        Returns:
            Dictionary containing installation status
        """
        status = {
            'workflow': {'installed': False},
            'app': {'installed': False},
            'configs': {'present': False}
        }
        
        try:
            if not self.client:
                self._initialize_client()
            
            # Check workflow installation
            workflow_status = self._check_workflow_status()
            status['workflow'].update(workflow_status)
            
            # Check app installation
            app_status = self._check_app_status()
            status['app'].update(app_status)
            
            # Check local configuration files
            config_status = self._check_config_status()
            status['configs'].update(config_status)
            
        except Exception:
            # If we can't check status, assume not installed
            pass
        
        return status
    
    def _check_workflow_status(self) -> Dict[str, Any]:
        """Check if workflow is installed."""
        try:
            # Look for the WF Exporter job
            jobs = list(self.client.jobs.list())
            for job in jobs:
                if job.settings and job.settings.name == "[WF] Exporter":
                    return {
                        'installed': True,
                        'job_id': job.job_id,
                        'job_name': job.settings.name
                    }
        except Exception:
            pass
        
        return {'installed': False}
    
    def _check_app_status(self) -> Dict[str, Any]:
        """Check if app is installed."""
        try:
            # Check if the app exists
            app = self.client.apps.get("wf-exporter-app")
            if app:
                workspace_url = self.client.config.host
                app_url = f"{workspace_url}/apps/wf-exporter-app"
                return {
                    'installed': True,
                    'app_name': "wf-exporter-app",
                    'app_url': app_url
                }
        except Exception:
            pass
        
        return {'installed': False}
    
    def _check_config_status(self) -> Dict[str, Any]:
        """Check if configuration files are present."""
        config_files = ['config.yml', 'databricks.yml', 'sample_export.py']
        present_files = []
        
        for file_name in config_files:
            if Path(file_name).exists():
                present_files.append(file_name)
        
        return {
            'present': len(present_files) > 0,
            'files': present_files
        }
    
    def uninstall_workflow(self) -> None:
        """Uninstall the workflow component."""
        if not self.client:
            self._initialize_client()
        
        try:
            # Find and delete the WF Exporter job
            user = self.client.current_user.me()['userName']
            jobs = list(self.client.jobs.list(name="[WF] Exporter"))
            for job in jobs:
                if job.settings and job.settings.name == "[WF] Exporter" and job.settings.creator_user_name == user:
                    self.client.jobs.delete(job.job_id)
                    break
            
            # Clean up workspace files
            workspace_paths = [
                "/Workspace/Applications/wf_exporter/wf_config"
            ]
            
            for path in workspace_paths:
                try:
                    self.client.workspace.delete(path, recursive=True)
                except Exception as e:
                    logger.error(f"Failed to delete workspace path: {e}")
                    raise
        
        except Exception as e:
            raise RuntimeError(f"Failed to uninstall workflow: {e}")
    
    def uninstall_app(self) -> None:
        """Uninstall the app component."""
        if not self.client:
            self._initialize_client()
        
        try:
            # Delete the app
            try:
                user = self.client.current_user.me()['userName']
                app = self.client.apps.get("wf-exporter-app")
                if app and app.creator_user_name == user:
                    self.client.apps.delete("wf-exporter-app")
            except Exception as e:
                logger.error(f"Failed to uninstall app: {e}")
                raise
            
            # Clean up workspace files
            try:
                self.client.workspace.delete(
                    "/Workspace/Applications/wf_exporter/app_config",
                    recursive=True
                )
            except Exception as e:
                logger.error(f"Failed to delete app config: {e}")
                raise
        
        except Exception as e:
            raise RuntimeError(f"Failed to uninstall app: {e}")


class Installer:
    """
    High-level installer interface for programmatic usage.
    
    This class provides a simple interface for installing WF Exporter
    components programmatically.
    """
    
    def __init__(self, include_app: bool = True, profile: Optional[str] = None):
        """
        Initialize the installer.
        
        Args:
            include_app: Whether to include app installation
            profile: Databricks profile to use
        """
        self.include_app = include_app
        self.profile = profile
        self.core = InstallerCore(profile=profile)
    
    def install(
        self,
        profile: Optional[str] = None,
        serverless: bool = True,
        generate_samples: bool = True
    ) -> Dict[str, Any]:
        """
        Install WF Exporter components.
        
        Args:
            profile: Databricks profile to use (overrides init profile)
            serverless: Use serverless configuration
            generate_samples: Generate sample configuration files
            
        Returns:
            Dictionary containing installation results
        """
        if profile:
            self.profile = profile
            self.core = InstallerCore(profile=profile)
        
        results = {}
        
        # Install workflow
        from .workflow_installer import WorkflowInstaller
        workflow_installer = WorkflowInstaller(profile=self.profile)
        workflow_result = workflow_installer.install(serverless=serverless)
        results['workflow'] = workflow_result
        
        # Install app if requested
        if self.include_app:
            from .app_installer import AppInstaller
            app_installer = AppInstaller(profile=self.profile)
            app_result = app_installer.install()
            results['app'] = app_result
        
        # Generate samples if requested
        if generate_samples:
            from .config_generator import ConfigGenerator
            generator = ConfigGenerator()
            sample_files = generator.generate_samples(Path.cwd())
            results['samples'] = {'files': sample_files}
        
        return results 