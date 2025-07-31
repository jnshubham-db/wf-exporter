"""
CLI management module for Databricks Workflow Exporter.

This module handles Databricks CLI operations including installation,
authentication, and command execution for both local and Databricks environments.
"""

import os
import re
import subprocess
from typing import Optional, Tuple, Union, List

from ..logging.log_manager import LogManager


class DatabricksCliManager:
    """
    Class to manage Databricks CLI operations for both local and Databricks environments.
    
    Handles:
    - Environment detection (local vs Databricks)
    - CLI installation and setup
    - Authentication (profile-based, token-based, runtime-based)
    - CLI command execution
    """
    
    def __init__(self, cli_path: str = None, config_profile: str = None, logger: Optional[LogManager] = None):
        """
        Initialize the Databricks CLI Manager.
        
        Args:
            cli_path: Path to Databricks CLI executable (None for auto-detection)
            config_profile: Databricks config profile to use (None for environment-based auth)
            logger: Logger instance for logging operations
        """
        self.cli_path = cli_path
        self.config_profile = config_profile
        self.is_installed = False
        self.is_authenticated = False
        self.environment_type = self._detect_environment()
        self.logger = logger or LogManager()
        
        self.logger.debug(f"Detected environment: {self.environment_type}")
        self.logger.debug(f"CLI path: {self.cli_path}, Config profile: {self.config_profile}")
    
    def _detect_environment(self) -> str:
        """
        Detect if running in Databricks environment or local environment.
        
        Returns:
            str: 'databricks' if running in Databricks, 'local' otherwise
        """
        try:
            # Check for Databricks-specific modules and variables
            import sys
            if 'databricks' in sys.modules or 'pyspark' in sys.modules:
                # Try to access Databricks-specific utilities
                try:
                    import pyspark
                    spark = pyspark.sql.SparkSession.getActiveSession()
                    if spark and hasattr(spark, 'conf'):
                        spark.conf.get('spark.databricks.workspaceUrl')
                        return 'databricks'
                except:
                    pass
            return 'local'
        except:
            return 'local'
    
    def install_cli(self) -> bool:
        """
        Install or locate Databricks CLI based on environment.
        
        Returns:
            bool: True if successful, False otherwise
        """
        if self.environment_type == 'local':
            return self._setup_local_cli()
        else:
            return self._install_databricks_cli()
    
    def _setup_local_cli(self) -> bool:
        """
        Set up CLI for local environment (assumes pre-installed CLI).
        
        Returns:
            bool: True if CLI is available and working, False otherwise
        """
        try:
            # Use provided CLI path or default to "databricks"
            self.cli_path = self.cli_path or "databricks"
            
            # Verify CLI is available
            result = subprocess.run(
                f"{self.cli_path} --version",
                shell=True,
                capture_output=True,
                text=True
            )
            
            if result.returncode == 0:
                self.is_installed = True
                self.logger.debug(f"Using local Databricks CLI at: {self.cli_path}")
                self.logger.debug(f"CLI version: {result.stdout.strip()}")
                return True
            else:
                self.logger.error(f"Databricks CLI not found at: {self.cli_path}")
                self.logger.error("Please install Databricks CLI: https://docs.databricks.com/dev-tools/cli/databricks-cli.html")
                return False
                
        except Exception as e:
            self.logger.error(f"Error setting up local CLI: {str(e)}")
            return False
    
    def _install_databricks_cli(self) -> bool:
        """
        Install Databricks CLI in Databricks environment.
        
        Returns:
            bool: True if installation successful, False otherwise
        """
        try:
            command = "curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh"
            self.logger.debug(f"Installing Databricks CLI with command: {command}")
            
            result = subprocess.run(
                command,
                shell=True,
                capture_output=True,
                text=True
            )
            self.logger.debug(result.stdout)
            
            shell_output = result.stdout
            
            # Match1 works when CLI is installed first time
            match1 = re.search(r"(/[\w\-./]+)(?=\.)", shell_output)
            
            # Match2 works when CLI is already installed
            match2 = re.search(r"'([^']+)'", shell_output)
            
            if match1:
                self.cli_path = match1.group(1)
                self.is_installed = True
                self.logger.debug(f"Databricks CLI installed to path: {self.cli_path}")
            elif match2:
                self.cli_path = match2.group(1)
                self.is_installed = True
                self.logger.debug(f"Databricks CLI installed to path: {self.cli_path}")
            else:
                self.logger.error("Failed to determine CLI installation path.")
                self.is_installed = False
            
            return self.is_installed
        except Exception as e:
            self.logger.error(f"Error installing Databricks CLI: {str(e)}")
            self.is_installed = False
            return False
    
    def verify_installation(self) -> bool:
        """
        Verify that Databricks CLI is properly installed and functional.
        
        Returns:
            bool: True if CLI is installed and working, False otherwise
        """
        try:
            # Check if CLI path is set and executable exists
            if not self.cli_path:
                self.logger.error("CLI path not set")
                return False
            
            # Test basic CLI functionality with version command
            result = subprocess.run(
                [self.cli_path, "--version"],
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                self.logger.debug(f"CLI verification successful: {result.stdout.strip()}")
                return True
            else:
                self.logger.error(f"CLI verification failed: {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            self.logger.error("CLI verification timed out")
            return False
        except FileNotFoundError:
            self.logger.error(f"CLI executable not found at: {self.cli_path}")
            return False
        except Exception as e:
            self.logger.error(f"Error verifying CLI installation: {str(e)}")
            return False
    
    def setup_authentication(self, databricks_host: str = None, databricks_token: str = None) -> bool:
        """
        Set up authentication based on environment and available credentials.
        
        Args:
            databricks_host: Databricks workspace URL (optional)
            databricks_token: Databricks access token (optional)
            
        Returns:
            bool: True if authentication setup successful, False otherwise
        """
        if self.environment_type == 'local':
            return self._setup_local_authentication(databricks_host, databricks_token)
        else:
            return self._setup_databricks_authentication(databricks_host, databricks_token)
    
    def _setup_local_authentication(self, databricks_host: str = None, databricks_token: str = None) -> bool:
        """
        Set up authentication for local environment.
        
        Args:
            databricks_host: Databricks workspace URL (optional)
            databricks_token: Databricks access token (optional)
            
        Returns:
            bool: True if authentication setup successful, False otherwise
        """
        try:
            if self.config_profile and self.config_profile != "default":
                # Use profile-based authentication
                os.environ['DATABRICKS_CONFIG_PROFILE'] = self.config_profile
                self.logger.debug(f"Using Databricks config profile: {self.config_profile}")
                return True
            elif databricks_host and databricks_token:
                # Use token-based authentication
                os.environ["DATABRICKS_HOST"] = databricks_host
                os.environ["DATABRICKS_TOKEN"] = databricks_token
                self.logger.debug(f"Authentication configured with host: {databricks_host}")
                return True
            elif os.getenv("DATABRICKS_HOST") and os.getenv("DATABRICKS_TOKEN"):
                # Use existing environment variables
                self.logger.debug("Using existing DATABRICKS_HOST and DATABRICKS_TOKEN environment variables")
                return True
            else:
                self.logger.error("Authentication required. Either:")
                self.logger.error("1. Set config_profile parameter to use profile-based auth, or")
                self.logger.error("2. Provide databricks_host and databricks_token parameters, or")
                self.logger.error("3. Set DATABRICKS_HOST and DATABRICKS_TOKEN environment variables")
                return False
                
        except Exception as e:
            self.logger.error(f"Error setting up local authentication: {str(e)}")
            return False
    
    def _setup_databricks_authentication(self, databricks_host: str = None, databricks_token: str = None) -> bool:
        """
        Set up authentication for Databricks environment using runtime context or provided credentials.
        
        Args:
            databricks_host: Databricks workspace URL (fallback if Spark not available)
            databricks_token: Databricks access token (fallback if Spark not available)
        
        Returns:
            bool: True if authentication setup successful, False otherwise
        """
        try:
            # Try to get Spark session and use runtime authentication
            try:
                if spark and hasattr(spark, 'conf'):
                    # Get the Databricks workspace URL
                    workspace_url = spark.conf.get('spark.databricks.workspaceUrl')
                    url = f"https://{workspace_url}"
                    
                    # Get the Databricks API token using dbutils
                    try:
                        token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
                        
                        # Set Databricks credentials using environment variables
                        os.environ["DATABRICKS_HOST"] = url
                        os.environ["DATABRICKS_TOKEN"] = token
                        
                        self.logger.debug(f"Authentication configured with Databricks workspace: {url}")
                        return True
                        
                    except Exception as token_error:
                        self.logger.warning(f"Failed to get API token from Databricks context: {str(token_error)}")
                        # Fall through to use provided credentials
                        
            except Exception as spark_error:
                self.logger.warning(f"No active Spark session found: {str(spark_error)}")
                # Fall through to use provided credentials
            
            # Fallback: Use provided host and token if Spark authentication failed
            if databricks_host and databricks_token:
                os.environ["DATABRICKS_HOST"] = databricks_host
                os.environ["DATABRICKS_TOKEN"] = databricks_token
                self.logger.debug(f"Authentication configured with provided credentials: {databricks_host}")
                return True
            else:
                self.logger.error("No active Spark session found and no databricks_host/databricks_token provided")
                self.logger.error("Please provide databricks_host and databricks_token parameters")
                return False
            
        except Exception as e:
            self.logger.error(f"Error setting up Databricks authentication: {str(e)}")
            return False
    
    def test_authentication(self) -> bool:
        """
        Test if authentication is working.
        
        Returns:
            bool: True if authentication test successful, False otherwise
        """
        if not self.cli_path:
            self.logger.error("CLI path not set. Install CLI first.")
            return False
            
        try:
            result = subprocess.run(
                f"{self.cli_path} current-user me",
                shell=True,
                capture_output=True,
                text=True
            )
            
            if result.returncode == 0:
                self.logger.debug(f"Authentication test successful: {result.stdout.strip()}")
                self.logger.debug("Authentication test successful")
                self.is_authenticated = True
                return True
            else:
                self.logger.error(f"Authentication test failed: {result.stderr}")
                self.is_authenticated = False
                return False
                
        except Exception as e:
            self.logger.error(f"Error testing authentication: {str(e)}")
            self.is_authenticated = False
            return False

    def generate_yaml_src_files_from_pipeline_id(self, pipeline_id: str, start_path: str, databricks_yml_path: str) -> Tuple[Union[List[str], str], str]:
        """
        Generates YAML and source files for a given Databricks pipeline ID.
        
        Args:
            pipeline_id: The Databricks pipeline ID
            start_path: Starting path for file generation (where files should be created)
            databricks_yml_path: Path to the databricks.yml file from config
            
        Returns:
            Tuple containing either list of file paths or error message, and status
        """
        self.logger.debug(f"Generating YAML and source files for pipeline ID: {pipeline_id}")
        
        # Verify databricks.yml exists at the configured path
        if not os.path.exists(databricks_yml_path):
            self.logger.error(f"databricks.yml not found at configured path: {databricks_yml_path}")
            self.logger.error("Please ensure the databricks.yml file exists at the path specified in config: v_databricks_yml_path")
            return f"databricks.yml not found at: {databricks_yml_path}", "failed"
        
        self.logger.debug(f"Using databricks.yml from: {databricks_yml_path}")
        
        # Ensure the start_path directory exists
        if not os.path.exists(start_path):
            os.makedirs(start_path, exist_ok=True)
            self.logger.debug(f"Created start_path directory: {start_path}")
        
        # Handle databricks.yml file placement
        target_databricks_yml = os.path.join(start_path, "databricks.yml")
        
        try:
            # Check if config path and start path are different
            if os.path.abspath(os.path.dirname(databricks_yml_path)) != os.path.abspath(start_path):
                # Copy/replace databricks.yml from config path to start path
                import shutil
                shutil.copy2(databricks_yml_path, target_databricks_yml)
                if os.path.exists(target_databricks_yml):
                    self.logger.debug(f"Replaced existing databricks.yml in target directory: {target_databricks_yml}")
                else:
                    self.logger.debug(f"Copied databricks.yml to target directory: {target_databricks_yml}")
            else:
                self.logger.debug(f"databricks.yml already in target directory, no copy needed: {target_databricks_yml}")
            
            # Generate command for pipeline
            command = [
                self.cli_path,
                "bundle", "generate", "pipeline",
                "--existing-pipeline-id", str(pipeline_id),
                "--force"
            ]
            
            self.logger.debug(f"Executing command: {' '.join(command)}")
            self.logger.debug(f"Working directory: {start_path}")
            
            # Execute the bundle generate pipeline command
            result = subprocess.run(
                command,
                cwd=start_path,
                capture_output=True,
                text=True
            )
            
            self.logger.debug(f"Command output: {result.stdout}")
            self.logger.debug(f"Command stderr: {result.stderr}")
            
            if result.returncode == 0:
                self.logger.debug(f"Successfully generated pipeline bundle for pipeline ID: {pipeline_id}")
                
                # Find generated files in the start_path
                generated_files = []
                for root, dirs, files in os.walk(start_path):
                    for file in files:
                        if file.endswith(('.yml', '.yaml', '.py', '.sql', '.json')):
                            generated_files.append(os.path.join(root, file))
                
                self.logger.debug(f"Found {len(generated_files)} generated files")
                return generated_files, "success"
            else:
                error_msg = f"Pipeline bundle generation failed: {result.stderr}"
                self.logger.error(error_msg)
                return error_msg, "failed"
                
        except Exception as e:
            error_msg = f"Error during pipeline bundle generation: {str(e)}"
            self.logger.error(error_msg)
            return error_msg, "failed"

    def generate_yaml_src_files_from_job_id(self, job_id: str, start_path: str, databricks_yml_path: str) -> Tuple[Union[List[str], str], str]:
        """
        Generates YAML and source files for a given Databricks job ID.
        
        Args:
            job_id: The Databricks job ID
            start_path: Starting path for file generation (where files should be created)
            databricks_yml_path: Path to the databricks.yml file from config
            
        Returns:
            Tuple containing either list of file paths or error message, and status
        """
        self.logger.debug(f"Generating YAML and source files for job ID: {job_id}")
        
        # Verify databricks.yml exists at the configured path
        if not os.path.exists(databricks_yml_path):
            self.logger.error(f"databricks.yml not found at configured path: {databricks_yml_path}")
            self.logger.error("Please ensure the databricks.yml file exists at the path specified in config: v_databricks_yml_path")
            return f"databricks.yml not found at: {databricks_yml_path}", "failed"
        
        self.logger.debug(f"Using databricks.yml from: {databricks_yml_path}")
        
        # Ensure the start_path directory exists
        if not os.path.exists(start_path):
            os.makedirs(start_path, exist_ok=True)
            self.logger.debug(f"Created start_path directory: {start_path}")
        
        # Handle databricks.yml file placement
        target_databricks_yml = os.path.join(start_path, "databricks.yml")
        
        try:
            # Check if config path and start path are different
            if os.path.abspath(os.path.dirname(databricks_yml_path)) != os.path.abspath(start_path):
                # Copy/replace databricks.yml from config path to start path
                import shutil
                shutil.copy2(databricks_yml_path, target_databricks_yml)
                if os.path.exists(target_databricks_yml):
                    self.logger.debug(f"Replaced existing databricks.yml in target directory: {target_databricks_yml}")
                else:
                    self.logger.debug(f"Copied databricks.yml to target directory: {target_databricks_yml}")
            else:
                self.logger.debug(f"databricks.yml already in target directory, no copy needed: {target_databricks_yml}")
            
            # Define the shell command - run from start_path directory
            command = f'''
                cd {start_path}; 
                {self.cli_path} bundle generate job --existing-job-id {job_id}'''

            # Run the command
            result = subprocess.run(
                command,
                shell=True,
                capture_output=True, 
                text=True,
                executable='/bin/bash'
            )
            
            # Check if the command succeeded
            if result.returncode == 0:
                status_message = f"Command 'bundle generate job' executed successfully for job id: {job_id}"
                self.logger.debug(status_message)
                file_paths = [
                    re.search(r'(src|resources)/[^\n\r]+', file).group(0) if re.search(r'(src|resources)/[^\n\r]+', file) else ''
                    for file in result.stderr.split('\n')
                ]
                self.logger.debug(f"Generated files: {file_paths}")
                return file_paths, "success"
            else:
                self.logger.error(f"Command failed with return code {result.returncode}.")
                self.logger.error(f"Command error output: {result.stderr}")
                return str(result.stderr), "failed"
                
        except Exception as e:
            self.logger.error(f"An error occurred while generating files: {e}")
            return str(e), "failed"