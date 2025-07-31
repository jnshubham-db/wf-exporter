"""
Workflow installer for WF Exporter.

This module handles installation of the WF Exporter workflow to Databricks
with support for both serverless and job cluster configurations.
"""

import os
import io
import logging
from pathlib import Path
from typing import Dict, Any, Optional
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import JobSettings
from databricks.sdk.service.workspace import ImportFormat

from .installer_core import InstallerCore
from .github_utils import get_whl_file_for_installation

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class WorkflowInstaller:
    """Handles workflow installation to Databricks."""
    
    def __init__(self, profile: Optional[str] = None, interactive: bool = True):
        """
        Initialize the workflow installer.
        
        Args:
            profile: Databricks profile to use
            interactive: Whether to prompt for user input
        """
        self.profile = profile
        self.interactive = interactive
        self.core = InstallerCore(profile=profile)
        self.client = self.core.client
        
        # Workspace paths
        self.workspace_config_path = "/Workspace/Applications/wf_exporter/wf_config"
        self.workspace_exports_path = "/Workspace/Applications/wf_exporter/exports"
        
        # Import config generator for template content
        from .config_generator import ConfigGenerator
        self.config_generator = ConfigGenerator()
        
        logger.debug("WorkflowInstaller initialized successfully")
    
    def install(self, serverless: bool = True, progress=None) -> Dict[str, Any]:
        """
        Install the workflow to Databricks.
        
        Args:
            serverless: Whether to use serverless configuration
            progress: Optional progress indicator for CLI updates
            
        Returns:
            Dictionary containing installation results
        """
        logger.info(f"Starting workflow installation (serverless: {serverless})")
        
        if not self.client:
            self.core._initialize_client()
        
        try:
            # Create workspace directories
            if progress:
                progress.start_step("Creating workspace directories...")
            logger.info("Creating workspace directories...")
            self._create_workspace_directories()
            if progress:
                progress.complete_step("Workspace directories created")
            
            # Upload configuration files
            if progress:
                progress.start_step("Uploading configuration files...")
            logger.info("Uploading configuration files...")
            whl_file_path = self._upload_config_files(progress)
            if progress:
                progress.complete_step("Configuration files uploaded")
            
            # Create the workflow with timing
            if progress:
                progress.start_step("Creating workflow job...")
            logger.info("Creating workflow...")
            import time
            start_time = time.time()
            job_info = self._create_workflow(serverless=serverless, whl_file_path=whl_file_path)
            end_time = time.time()
            logger.info(f"Workflow creation completed in {end_time - start_time:.2f} seconds")
            if progress:
                progress.complete_step("Workflow job created")
            
            # Update app config with job information
            if progress:
                progress.start_step("Updating app configuration...")
            logger.info("Updating app configuration...")
            self._update_app_config(job_info)
            if progress:
                progress.complete_step("App configuration updated")
                progress.finish()
            
            logger.info("Workflow installation completed successfully!")
            return job_info
            
        except Exception as e:
            if progress:
                progress.fail_step(f"Installation failed: {str(e)}")
            logger.error(f"Workflow installation failed: {e}")
            raise
    
    def _create_workspace_directories(self) -> None:
        """Create necessary workspace directories."""
        directories = [
            self.workspace_config_path,
            self.workspace_exports_path
        ]
        self.core.create_workspace_directories(directories)
    
    def _upload_config_files(self, progress=None) -> str:
        """
        Upload configuration files to the workspace using generated content.
        
        Args:
            progress: Optional progress indicator for CLI updates
        
        Returns:
            Workspace path to the uploaded WHL file
        """
        logger.info("Uploading configuration files to workspace...")
        
        # Upload config.yml using generated content
        if progress:
            progress.update_step("Uploading config.yml...")
        config_content = self.config_generator._get_config_yml_content()
        config_target = f"{self.workspace_config_path}/config.yml"
        logger.info(f"Uploading generated config.yml to {config_target}")
        self._upload_content(config_content, config_target)
        
        # Upload databricks.yml using generated content
        if progress:
            progress.update_step("Uploading databricks.yml...")
        databricks_content = self.config_generator._get_databricks_yml_content()
        databricks_target = f"{self.workspace_config_path}/databricks.yml"
        logger.info(f"Uploading generated databricks.yml to {databricks_target}")
        self._upload_content(databricks_content, databricks_target)
        
        # Upload run.py using generated content
        if progress:
            progress.update_step("Uploading run.py...")
        run_content = self.config_generator._get_run_py_content()
        run_target = f"{self.workspace_config_path}/run.py"
        logger.info(f"Uploading generated run.py to {run_target}")
        self._upload_content(run_content, run_target)
        
        # Handle WHL file
        if progress:
            progress.update_step("Uploading WHL package...")
        logger.info("Handling WHL file upload...")
        whl_file_path = self._upload_whl_file()
        
        logger.info("All configuration files uploaded successfully")
        return whl_file_path
    
    def _upload_file(self, source_path: Path, target_path: str) -> None:
        """
        Upload a file from local path to Databricks workspace.
        
        Args:
            source_path: Local file path
            target_path: Target workspace path
        """
        try:
            with open(source_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Upload file with AUTO format, always replacing existing
            self.client.workspace.upload(
                path=target_path,
                content=io.BytesIO(content.encode('utf-8')),
                format=ImportFormat.AUTO,
                overwrite=True  # Always replace existing files
            )
            
            logger.info(f"Successfully uploaded file: {source_path} -> {target_path}")
            
        except Exception as e:
            logger.error(f"Failed to upload file {source_path} to {target_path}: {e}")
            raise

    def _upload_content(self, content: str, target_path: str) -> None:
        """
        Upload string content directly to Databricks workspace.
        
        Args:
            content: String content to upload
            target_path: Target workspace path
        """
        try:
            # Upload content with AUTO format, always replacing existing
            self.client.workspace.upload(
                path=target_path,
                content=io.BytesIO(content.encode('utf-8')),
                format=ImportFormat.AUTO,
                overwrite=True  # Always replace existing files
            )
            
            logger.info(f"Successfully uploaded content to: {target_path}")
            
        except Exception as e:
            logger.error(f"Failed to upload content to {target_path}: {e}")
            raise
    
    def _upload_whl_file(self) -> str:
        """
        Get and upload the WHL file to the workspace.
        
        Returns:
            Workspace path to the uploaded WHL file
        """
        logger.info("Getting WHL file for installation...")
        
        # Get WHL file (local or download)
        whl_file = get_whl_file_for_installation()
        if not whl_file:
            raise RuntimeError("Could not find or download WHL file")
        
        logger.info(f"Found WHL file: {whl_file}")
        
        # Upload to workspace
        whl_filename = whl_file.name
        workspace_whl_path = f"{self.workspace_config_path}/{whl_filename}"
        
        logger.info(f"Uploading WHL file to: {workspace_whl_path}")
        
        try:
            with open(whl_file, 'rb') as f:
                content = f.read()
            
            # Upload WHL file with AUTO format, always replacing existing
            self.client.workspace.upload(
                path=workspace_whl_path,
                content=io.BytesIO(content),
                format=ImportFormat.AUTO,
                overwrite=True  # Always replace existing files
            )
            
            logger.info(f"Successfully uploaded WHL file: {workspace_whl_path}")
            return workspace_whl_path
            
        except Exception as e:
            logger.error(f"Failed to upload WHL file {whl_file} to {workspace_whl_path}: {e}")
            raise
    
    def _create_workflow(self, serverless: bool, whl_file_path: str) -> Dict[str, Any]:
        """
        Create the WF Exporter workflow.
        
        Args:
            serverless: Whether to use serverless configuration
            whl_file_path: Workspace path to the WHL file
            
        Returns:
            Dictionary containing job information
        """
        if serverless:
            job_config = self._create_serverless_job_config(whl_file_path)
        else:
            job_config = self._create_job_cluster_config(whl_file_path)
        
        # Check if job already exists
        existing_job = self._find_existing_job()
        
        job_name = "[WF] Exporter"
        
        if existing_job:
            # Ask user what they want to do with existing job
            if self.interactive:
                # Import here to avoid circular imports
                import sys
                # Clear any progress indicator output before prompting
                sys.stdout.write('\r' + ' ' * 80 + '\r')
                sys.stdout.flush()
                action = self._prompt_existing_job_action(existing_job)
            else:
                # Non-interactive mode: default to update
                action = "update"
                logger.info(f"Non-interactive mode: updating existing job {existing_job.job_id}")
            
            if action == "update":
                # Update existing job
                logger.info(f"Updating existing job: {existing_job.job_id}")
                try:
                    # For job reset, we need a JobSettings object (from dict, not SDK objects)
                    logger.debug("Creating JobSettings object for job update")
                    from databricks.sdk.service.jobs import JobSettings
                    
                    # JobSettings.from_dict expects plain dictionaries, not SDK objects
                    job_settings = JobSettings.from_dict(job_config)
                    logger.debug(f"Created JobSettings object: {type(job_settings)}")
                    
                    logger.info("Updating job configuration...")
                    self.client.jobs.reset(job_id=existing_job.job_id, new_settings=job_settings)
                    job_id = existing_job.job_id
                    logger.info("✅ Job updated successfully")
                except Exception as e:
                    logger.error(f"Failed to update existing job: {e}")
                    logger.debug(f"Job config that failed: {job_config}")
                    raise RuntimeError(f"Could not update existing job: {e}")
            else:  # action == "redeploy"
                # Delete existing job and create new one
                logger.info(f"Deleting existing job: {existing_job.job_id}")
                self.client.jobs.delete(existing_job.job_id)
                logger.info("Creating new job...")
                logger.debug(f"Job config type: {type(job_config)}")
                logger.debug(f"Job config keys: {list(job_config.keys()) if isinstance(job_config, dict) else 'Not a dict'}")
                
                try:
                    # ROOT CAUSE FIX: Convert dictionaries to proper SDK objects
                    logger.debug("Converting config dictionaries to SDK objects...")
                    sdk_config = self._convert_config_to_sdk_objects(job_config)
                    
                    created_job = self.client.jobs.create(**sdk_config)
                    job_id = created_job.job_id
                    logger.info("✅ Job redeployed successfully")
                except Exception as e:
                    logger.error(f"Failed to create job with SDK objects: {e}")
                    logger.debug(f"Config content: {job_config}")
                    raise
        else:
            # Create new job - use dictionary directly
            logger.info("Creating new job...")
            logger.debug(f"Job config type: {type(job_config)}")
            logger.debug(f"Job config keys: {list(job_config.keys()) if isinstance(job_config, dict) else 'Not a dict'}")
            
            try:
                # ROOT CAUSE FIX: Convert dictionaries to proper SDK objects
                logger.debug("Converting config dictionaries to SDK objects...")
                sdk_config = self._convert_config_to_sdk_objects(job_config)
                
                created_job = self.client.jobs.create(**sdk_config)
                job_id = created_job.job_id
                logger.info("✅ Job created successfully")
            except Exception as e:
                logger.error(f"Failed to create job with SDK objects: {e}")
                logger.debug(f"Config content: {job_config}")
                raise
        
        logger.info(f"Job created/updated successfully with ID: {job_id}")
        
        return {
            'job_id': job_id,
            'job_name': job_name,
            'serverless': serverless,
            'whl_file_path': whl_file_path
        }
    
    def _create_serverless_job_config(self, whl_file_path: str) -> Dict[str, Any]:
        """Create serverless job configuration."""
        return {
            "name": "[WF] Exporter",
            "tasks": [
                {
                    "task_key": "wfExporter",
                    "spark_python_task": {
                        "python_file": f"{self.workspace_config_path}/run.py",
                        "parameters": [
                            "--config_path",
                            "{{job.parameters.config_path}}",
                        ],
                    },
                    "min_retry_interval_millis": 900000,
                    "environment_key": "Default",
                }
            ],
            "queue": {
                "enabled": True,
            },
            "parameters": [
                {
                    "name": "config_path",
                    "default": f"{self.workspace_config_path}/config.yml",
                }
            ],
            "environments": [
                {
                    "environment_key": "Default",
                    "spec": {
                        "client": "2",
                        "dependencies": [
                            whl_file_path,
                        ],
                    },
                }
            ],
            "performance_target": "PERFORMANCE_OPTIMIZED",
        }
    
    def _create_job_cluster_config(self, whl_file_path: str) -> Dict[str, Any]:
        """Create job cluster configuration."""
        return {
            "name": "[WF] Exporter",
            "tasks": [
                {
                    "task_key": "wfExporter",
                    "spark_python_task": {
                        "python_file": f"{self.workspace_config_path}/run.py",
                        "parameters": [
                            "--config_path",
                            "{{job.parameters.config_path}}",
                        ],
                    },
                    "job_cluster_key": "Job_cluster",
                    "libraries": [
                        {
                            "whl": whl_file_path,
                        },
                    ],
                    "min_retry_interval_millis": 900000,
                }
            ],
            "job_clusters": [
                {
                    "job_cluster_key": "Job_cluster",
                    "new_cluster": {
                        "spark_version": "16.4.x-scala2.12",
                        "azure_attributes": {
                            "first_on_demand": 1,
                            "availability": "SPOT_WITH_FALLBACK_AZURE",
                            "spot_bid_max_price": -1,
                        },
                        "node_type_id": "Standard_D4ds_v5",
                        "spark_env_vars": {
                            "PYSPARK_PYTHON": "/databricks/python3/bin/python3",
                        },
                        "enable_elastic_disk": True,
                        "data_security_mode": "SINGLE_USER",
                        "runtime_engine": "PHOTON",
                        "num_workers": 8,
                    },
                }
            ],
            "queue": {
                "enabled": True,
            },
            "parameters": [
                {
                    "name": "config_path",
                    "default": f"{self.workspace_config_path}/config.yml",
                }
            ],
        }
    
    def _find_existing_job(self) -> Optional[Any]:
        """
        Find existing WF Exporter job.
        
        Returns:
            Existing job object or None if not found
        """
        try:
            jobs = list(self.client.jobs.list(name="[WF] Exporter"))
            for job in jobs:
                if job.settings and job.settings.name == "[WF] Exporter":
                    return job
        except Exception:
            pass
        
        return None
    
    def _update_app_config(self, job_info: Dict[str, Any]) -> None:
        """
        Update app configuration with job information in simple format.
        
        Args:
            job_info: Dictionary containing job information
        """
        try:
            # Create simple app_config.yml content with only export-job section
            app_config_content = f"""export-job:
  job_id: {job_info['job_id']}
  job_name: "{job_info['job_name']}"
"""
            
            # Use the robust wf_app directory finding method from app_installer
            wf_app_dir = self._find_wf_app_directory()
            app_config_path = wf_app_dir / "app_config.yml"
            
            # Write the config file
            with open(app_config_path, 'w') as f:
                f.write(app_config_content)
            
            logger.info(f"Updated app_config.yml with job_id: {job_info['job_id']}")
            
        except Exception as e:
            # Don't fail the whole installation if app config update fails
            logger.warning(f"Failed to update app config: {e}")
    
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
            if path.exists() and path.is_dir():
                logger.debug(f"Found wf_app directory: {path}")
                return path
        
        # If we can't find it, use the fallback method
        logger.warning("Could not find wf_app directory, using fallback path")
        return Path(__file__).parent.parent.parent.parent / "wf_app"
    
    def _get_current_timestamp(self) -> str:
        """Get current timestamp as string."""
        from datetime import datetime
        return datetime.now().isoformat()
    
    def uninstall(self) -> None:
        """Uninstall the workflow."""
        self.core.uninstall_workflow() 

    def _prompt_existing_job_action(self, existing_job) -> str:
        """
        Prompt user to choose action for existing job.
        
        Args:
            existing_job: Existing job object
            
        Returns:
            "update" or "redeploy"
        """
        import click
        import sys
        
        # Ensure clean output for the prompt
        sys.stdout.write('\n')  # Add newline for clean separation
        sys.stdout.flush()
        
        click.echo(f"⚠️  Existing workflow found:")
        click.echo(f"   Job ID: {existing_job.job_id}")
        click.echo(f"   Job Name: {existing_job.settings.name if existing_job.settings else '[Unknown]'}")
        click.echo()
        click.echo("Choose an action:")
        click.echo("  1. Update existing workflow (preserves job ID and history)")
        click.echo("  2. Redeploy workflow (deletes existing and creates new)")
        click.echo()
        
        while True:
            try:
                choice = click.prompt(
                    "Select option (1-2)",
                    type=int,
                    default=1
                )
                
                if choice == 1:
                    return "update"
                elif choice == 2:
                    # Confirm redeploy action
                    if click.confirm("⚠️  This will permanently delete the existing workflow. Continue?"):
                        return "redeploy"
                    else:
                        click.echo("Please choose a different option.")
                        continue
                else:
                    click.echo("Invalid selection. Please enter 1 or 2.")
                    continue
            except (ValueError, click.Abort):
                click.echo("Invalid input. Please enter 1 or 2.")
                continue 

    def _convert_config_to_sdk_objects(self, job_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert plain dictionary configuration to proper Databricks SDK objects.
        
        ROOT CAUSE FIX: The Databricks SDK expects specific object types (Task, JobEnvironment, etc.)
        but we were passing plain dictionaries, causing 'dict' object has no attribute 'as_dict' errors.
        
        Args:
            job_config: Dictionary configuration
            
        Returns:
            Dictionary with SDK objects instead of plain dicts
        """
        from databricks.sdk.service.jobs import (
            Task, JobEnvironment, QueueSettings, 
            JobParameterDefinition, JobCluster, PerformanceTarget
        )
        
        logger.debug("Converting job config dictionaries to SDK objects...")
        sdk_config = job_config.copy()
        
        # Convert tasks from list of dicts to list of Task objects
        if "tasks" in job_config and job_config["tasks"]:
            logger.debug(f"Converting {len(job_config['tasks'])} tasks to Task objects")
            sdk_config["tasks"] = [
                Task.from_dict(task_dict) for task_dict in job_config["tasks"]
            ]
        
        # Convert environments from list of dicts to list of JobEnvironment objects
        if "environments" in job_config and job_config["environments"]:
            logger.debug(f"Converting {len(job_config['environments'])} environments to JobEnvironment objects")
            sdk_config["environments"] = [
                JobEnvironment.from_dict(env_dict) for env_dict in job_config["environments"]
            ]
        
        # Convert queue from dict to QueueSettings object
        if "queue" in job_config and job_config["queue"]:
            logger.debug("Converting queue to QueueSettings object")
            sdk_config["queue"] = QueueSettings.from_dict(job_config["queue"])
        
        # Convert parameters from list of dicts to list of JobParameterDefinition objects
        if "parameters" in job_config and job_config["parameters"]:
            logger.debug(f"Converting {len(job_config['parameters'])} parameters to JobParameterDefinition objects")
            sdk_config["parameters"] = [
                JobParameterDefinition.from_dict(param_dict) for param_dict in job_config["parameters"]
            ]
        
        # Convert job_clusters from list of dicts to list of JobCluster objects
        if "job_clusters" in job_config and job_config["job_clusters"]:
            logger.debug(f"Converting {len(job_config['job_clusters'])} job_clusters to JobCluster objects")
            sdk_config["job_clusters"] = [
                JobCluster.from_dict(cluster_dict) for cluster_dict in job_config["job_clusters"]
            ]
        
        # Convert performance_target from string to enum object
        if "performance_target" in job_config and isinstance(job_config["performance_target"], str):
            logger.debug(f"Converting performance_target string '{job_config['performance_target']}' to PerformanceTarget enum")
            try:
                sdk_config["performance_target"] = getattr(PerformanceTarget, job_config["performance_target"])
                logger.debug(f"Successfully converted to: {sdk_config['performance_target']}")
            except AttributeError:
                logger.warning(f"Unknown performance_target value: {job_config['performance_target']}, using default")
                sdk_config["performance_target"] = PerformanceTarget.STANDARD
        
        logger.debug("Successfully converted all config objects to SDK types")
        return sdk_config 