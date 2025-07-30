"""Databricks service for API interactions."""

from typing import List, Dict, Any, Optional
import logging
import os
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from databricks.sdk.errors.base import DatabricksError
from databricks.sdk.service.workspace import ImportFormat
import base64
from flask import session, has_request_context
from databricks.sdk.service.jobs import PerformanceTarget

class DatabricksService:
    """Service class for Databricks API operations."""
    
    def __init__(self, host: str | None = None, token: str | None = None):
        """Initialize Databricks service."""
        self.host = host or os.environ.get('DATABRICKS_HOST')
        self.token = token or os.environ.get('DATABRICKS_TOKEN')
        self.logger = logging.getLogger(__name__)
        self._client = None
        self._connection_tested = False
        self._current_auth_source = None  # Track current authentication source
        
    def _get_session_auth_config(self) -> Dict[str, Any] | None:
        """Get authentication configuration from session if available."""
        if not has_request_context():
            return None
            
        try:
            auth_config = session.get('databricks_auth_config')
            auth_status = session.get('databricks_auth_status', {})
            
            if auth_config and auth_status.get('connected', False):
                return auth_config
        except Exception as e:
            self.logger.debug(f"Could not get session auth config: {e}")
            
        return None
        
    @property
    def client(self) -> WorkspaceClient:
        """Get or create Databricks client."""
        # Always check for session authentication first
        session_config = self._get_session_auth_config()
        current_auth_source = 'session' if session_config else ('env' if self.host and self.token else 'default')
        
        # Recreate client if authentication source has changed or client doesn't exist
        if self._client is None or self._current_auth_source != current_auth_source:
            try:
                if session_config:
                    self._client = self._create_client_from_session_config(session_config)
                    self._current_auth_source = 'session'
                    self.logger.info("Databricks client initialized from session authentication")
                elif self.host and self.token:
                    # Use explicit host and token (legacy)
                    config = Config(
                        host=self.host,
                        token=self.token
                    )
                    self._client = WorkspaceClient(config=config)
                    self._current_auth_source = 'env'
                    self.logger.info("Databricks client initialized from environment variables")
                else:
                    # Use default authentication (env vars, profiles, etc.)
                    self._client = WorkspaceClient()
                    self._current_auth_source = 'default'
                    self.logger.info("Databricks client initialized with default authentication")
                
            except Exception as e:
                self.logger.error(f"Failed to initialize Databricks client: {e}")
                raise DatabricksError(f"Failed to connect to Databricks: {e}")
        
        return self._client
        
    def _get_secret_from_keyvault(self, keyvault_name: str, secret_key: str) -> str:
        """Get secret from Azure Key Vault using default Databricks client"""
        try:
            # Use default workspace client without parameters
            w = WorkspaceClient()
            secret_response = w.secrets.get_secret(scope=keyvault_name, key=secret_key)
            if not secret_response.value:
                raise Exception(f"Secret '{secret_key}' not found or empty in Key Vault '{keyvault_name}'")
            decoded_secret = base64.b64decode(secret_response.value).decode('utf-8')
            return decoded_secret
        except Exception as e:
            self.logger.error(f"Failed to get secret '{secret_key}' from Key Vault '{keyvault_name}': {str(e)}")
            raise DatabricksError(f"Failed to get secret from Key Vault: {str(e)}")
        
    def _create_client_from_session_config(self, config: Dict[str, Any]) -> WorkspaceClient:
        """Create Databricks client from session configuration."""
        auth_type = config.get('auth_type')
        
        # Use Config objects to create isolated configurations
        if auth_type == 'pat':
            # Fetch token from Key Vault
            token = self._get_secret_from_keyvault(config['keyvault_name'], config['secret_name'])
            
            # Create isolated config for PAT authentication
            databricks_config = Config(
                host=config['host'],
                token=token,
                auth_type='pat'
            )
            return WorkspaceClient(config=databricks_config)
        
        elif auth_type == 'azure-client-secret':
            # Fetch Azure credentials from Key Vault
            tenant_id = self._get_secret_from_keyvault(config['keyvault_name'], config['azure_tenant_id_key'])
            client_id = self._get_secret_from_keyvault(config['keyvault_name'], config['azure_client_id_key'])
            client_secret = self._get_secret_from_keyvault(config['keyvault_name'], config['azure_client_secret_key'])
            
            # Create isolated config for Azure Service Principal authentication
            databricks_config = Config(
                host=config['host'],
                azure_tenant_id=tenant_id,
                azure_client_id=client_id,
                azure_client_secret=client_secret,
                auth_type='azure-client-secret'
            )
            return WorkspaceClient(config=databricks_config)
        
        else:
            raise DatabricksError(f"Unsupported authentication type: {auth_type}")
    
    def test_connection(self) -> Dict[str, Any]:
        """Test connection to Databricks workspace."""
        try:
            # Try to get current user info as a connection test
            current_user = self.client.current_user.me()
            workspace_info = self.client.workspace.get_status("/")
            
            self._connection_tested = True
            
            connection_info = {
                'connected': True,
                'user': current_user.user_name if current_user else 'Unknown',
                'workspace_url': self.client.config.host,
                'workspace_id': getattr(workspace_info, 'workspace_id', 'Unknown')
            }
            
            self.logger.info(f"Connection test successful: {connection_info}")
            return connection_info
            
        except Exception as e:
            self.logger.error(f"Connection test failed: {e}")
            return {
                'connected': False,
                'error': str(e)
            }
    
    def get_jobs(self) -> List[Dict[str, Any]]:
        """Fetch all jobs from Databricks workspace."""
        try:
            self.logger.info("Fetching jobs from Databricks")
            
            jobs_list = []
            
            # Use the jobs API to list all jobs
            for job in self.client.jobs.list():
                job_info = {
                    'job_id': job.job_id,
                    'name': job.settings.name if job.settings else f"Job {job.job_id}",
                    'created_time': job.created_time,
                    'creator_user_name': job.creator_user_name,
                    'job_type': getattr(job.settings, 'job_type', 'Unknown') if job.settings else 'Unknown'
                }
                jobs_list.append(job_info)
            
            self.logger.info(f"Successfully fetched {len(jobs_list)} jobs")
            return jobs_list
        except Exception as e:
            self.logger.error(f"Error fetching jobs: {e}")
            raise DatabricksError(f"Failed to fetch jobs: {e}")

    def get_lakeflow_pipelines(self) -> List[Dict[str, Any]]:
        """Fetch all lakeflow pipelines from Databricks workspace."""
        try:
            self.logger.info("Fetching lakeflow pipelines from Databricks")
            
            pipelines_list = []
            
            # Use the jobs API to list all jobs
            for pipeline in self.client.pipelines.list_pipelines():
                pipeline_info = {
                    'pipeline_id': pipeline.pipeline_id,
                    'name': pipeline.name if pipeline.name else f"Pipeline {pipeline.pipeline_id}",
                    'creator_user_name': pipeline.creator_user_name
                }
                pipelines_list.append(pipeline_info)
            
            self.logger.info(f"Successfully fetched {len(pipelines_list)} pipelines")
            return pipelines_list
            
        except Exception as e:
            self.logger.error(f"Error fetching pipelines: {e}")
            raise DatabricksError(f"Failed to fetch pipelines: {e}")
    
    def get_pipeline_details(self, pipeline_id: str) -> Dict[str, Any]:
        """Get detailed information about a specific pipeline."""
        try:
            pipeline = self.client.pipelines.get(pipeline_id)
            
            pipeline_details = {
                'pipeline_id': pipeline.pipeline_id,
                'pipeline_name': pipeline.name if pipeline.name else f"Pipeline {pipeline.pipeline_id}",
                'creator_user_name': pipeline.creator_user_name,
                'state': pipeline.state.value if pipeline.state else 'Unknown',
                'cluster_id': pipeline.cluster_id if hasattr(pipeline, 'cluster_id') else None,
                'library_updates': getattr(pipeline, 'library_updates', []),
                'creation_time': getattr(pipeline, 'creation_time', None)
            }
            
            self.logger.info(f"Retrieved details for pipeline {pipeline_id}")
            return pipeline_details
            
        except Exception as e:
            self.logger.error(f"Error getting pipeline details for {pipeline_id}: {e}")
            raise DatabricksError(f"Failed to get pipeline details: {e}")
    
    def get_job_details(self, job_id: int) -> Dict[str, Any]:
        """Get detailed information about a specific job."""
        try:
            job = self.client.jobs.get(job_id)
            
            # Enhanced cluster type detection for Databricks jobs
            cluster_type = 'job_cluster'  # Default assumption
            cluster_info = {'type': 'job_cluster'}
            
            try:
                if(job.settings.job_clusters is not None and len(job.settings.job_clusters) > 0):
                    cluster_type = 'job_cluster'
                    cluster_info = {'type': 'job_cluster'}
                elif(job.settings.performance_target == PerformanceTarget.PERFORMANCE_OPTIMIZED):
                    cluster_type = 'serverless_performance'
                    cluster_info = {'type': 'serverless_performance'}
                else:
                    cluster_type = 'serverless'
                    cluster_info = {'type': 'serverless'}
            except Exception as e:
                self.logger.warning(f"Could not determine cluster type for job {job_id}: {e}")
                cluster_type = 'job_cluster'  # Safe default
                cluster_info = {'type': 'job_cluster', 'error': str(e)}
            
            job_details = {
                'job_id': job.job_id,
                'name': job.settings.name if job.settings else f"Job {job.job_id}",
                'description': getattr(job.settings, 'description', '') if job.settings else '',
                'created_time': job.created_time,
                'creator_user_name': job.creator_user_name,
                'job_type': getattr(job.settings, 'job_type', 'Unknown') if job.settings else 'Unknown',
                'tags': getattr(job.settings, 'tags', {}) if job.settings else {},
                'timeout_seconds': getattr(job.settings, 'timeout_seconds', None) if job.settings else None,
                'cluster_type': cluster_type,
                'cluster_info': cluster_info
            }
            
            self.logger.info(f"Job {job_id} details: cluster_type={cluster_type}")
            return job_details
            
        except Exception as e:
            self.logger.error(f"Error getting job details for {job_id}: {e}")
            raise DatabricksError(f"Failed to get job details: {e}")
    
    def trigger_workflow(self, config_path: str, job_id: Optional[int] = None) -> str:
        """Trigger a Databricks workflow with config path."""
        try:
            if not job_id:
                # Use a default job ID or find a workflow job
                self.logger.warning("No job ID provided for workflow trigger")
                return "placeholder_run_id"
            
            # Trigger the job run
            run = self.client.jobs.run_now(
                job_id=job_id,
                job_parameters={
                    'config_path': config_path
                }
            )
            
            run_id = str(run.run_id)
            self.logger.info(f"Workflow triggered successfully: {run_id}")
            return run_id
            
        except Exception as e:
            self.logger.error(f"Error triggering workflow: {e}")
            raise DatabricksError(f"Failed to trigger workflow: {e}")
    
    def get_workflow_status(self, run_id: str) -> Dict[str, Any]:
        """Get workflow run status."""
        try:
            run = self.client.jobs.get_run(int(run_id))
            
            # Simple extraction of status info
            lifecycle_state = 'UNKNOWN'
            result_state = None
            state_message = None
            
            if run.state:
                # Convert enum values to strings safely
                if run.state.life_cycle_state:
                    lifecycle_state = str(run.state.life_cycle_state)
                if run.state.result_state:
                    result_state = str(run.state.result_state)
                if hasattr(run.state, 'state_message'):
                    state_message = run.state.state_message
            
            # Log for debugging
            self.logger.info(f"Databricks status for run {run_id}: lifecycle={lifecycle_state}, result={result_state}")
            
            status_info = {
                'run_id': run_id,
                'status': lifecycle_state,
                'result_state': result_state,
                'state_message': state_message,
                'start_time': run.start_time,
                'end_time': run.end_time,
                'run_duration': run.run_duration,
                'setup_duration': run.setup_duration,
                'execution_duration': run.execution_duration,
                'cleanup_duration': run.cleanup_duration
            }
            
            return status_info
            
        except Exception as e:
            self.logger.error(f"Error getting workflow status for {run_id}: {e}")
            raise DatabricksError(f"Failed to get workflow status: {e}")
    
    def read_workspace_file(self, path: str) -> str:
        """Read file from Databricks workspace."""
        try:
            # Ensure path starts with /
            if not path.startswith('/'):
                path = '/' + path
            
            # Use workspace API to read file
            response = self.client.workspace.download(path)
            
            # Read the content from the BinaryIO response
            content_bytes = response.read()
            
            # Decode to string
            content = content_bytes.decode('utf-8')
            
            self.logger.info(f"Successfully read file from workspace: {path}")
            return content
            
        except Exception as e:
            self.logger.error(f"Error reading workspace file {path}: {e}")
            raise DatabricksError(f"Failed to read workspace file: {e}")
    
    def write_workspace_file(self, path: str, content: str) -> bool:
        """Write file to Databricks workspace."""
        try:
            # Ensure path starts with /
            if not path.startswith('/'):
                path = '/' + path
            
            # Convert content to bytes
            content_bytes = content.encode('utf-8')
            
            # Use workspace API to upload file
            self.client.workspace.upload(
                path=path,
                content=content_bytes,
                overwrite=True,
                format=ImportFormat.AUTO
            )
            
            self.logger.info(f"Successfully wrote file to workspace: {path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error writing workspace file {path}: {e}")
            raise DatabricksError(f"Failed to write workspace file: {e}")
    
    def list_workspace_files(self, path: str = '/') -> List[Dict[str, Any]]:
        """List files in a workspace directory."""
        try:
            files = []
            
            for item in self.client.workspace.list(path):
                file_info = {
                    'path': item.path,
                    'object_type': item.object_type.value,
                    'language': item.language.value if item.language else None,
                    'object_id': item.object_id
                }
                files.append(file_info)
            
            self.logger.info(f"Listed {len(files)} items in workspace path: {path}")
            return files
            
        except Exception as e:
            self.logger.error(f"Error listing workspace files in {path}: {e}")
            raise DatabricksError(f"Failed to list workspace files: {e}")
    
    def get_workflow_run_status(self, run_id: str) -> Dict[str, Any]:
        """Get workflow run status - alias for get_workflow_status."""
        return self.get_workflow_status(run_id)
    
    def build_workspace_url(self, path: str) -> str:
        """Build workspace URL for a given path."""
        try:
            # Ensure path starts with /
            if not path.startswith('/'):
                path = '/' + path
            
            # Get base URL from client config
            base_url = self.client.config.host.rstrip('/')
            
            # Build the full workspace URL
            workspace_url = f"{base_url}/#workspace{path}"
            
            self.logger.info(f"Built workspace URL: {workspace_url}")
            return workspace_url
            
        except Exception as e:
            self.logger.error(f"Error building workspace URL for {path}: {e}")
            # Return a fallback URL
            return f"https://databricks.com/#workspace{path}"
    
    def build_workflow_run_url(self, job_id: int, run_id: str) -> str:
        """Build URL for a specific workflow run."""
        try:
            # Get base URL from client config
            base_url = self.client.config.host.rstrip('/')
            
            # Build the workflow run URL
            run_url = f"{base_url}/#job/{job_id}/run/{run_id}"
            
            self.logger.info(f"Built workflow run URL: {run_url}")
            return run_url
            
        except Exception as e:
            self.logger.error(f"Error building workflow run URL for job {job_id}, run {run_id}: {e}")
            # Return a fallback URL
            return f"https://databricks.com/#job/{job_id}/run/{run_id}"
    
    def get_workspace_info(self) -> Dict[str, Any]:
        """Get workspace information."""
        try:
            # Get workspace status
            workspace_status = self.client.workspace.get_status('/')
            
            # Get current user
            current_user = self.client.current_user.me()
            
            workspace_info = {
                'host': self.client.config.host,
                'workspace_id': getattr(workspace_status, 'workspace_id', 'Unknown'),
                'current_user': current_user.user_name if current_user else 'Unknown',
                'object_type': workspace_status.object_type.value if workspace_status.object_type else 'Unknown'
            }
            
            return workspace_info
            
        except Exception as e:
            self.logger.error(f"Error getting workspace info: {e}")
            raise DatabricksError(f"Failed to get workspace info: {e}") 