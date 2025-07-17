"""
Configuration management module for Databricks Workflow Exporter.

This module provides centralized configuration loading and access,
implementing the singleton pattern to ensure config is loaded only once.
"""

import os
import yaml
from typing import Dict, List, Tuple, Any, Optional

try:
    import pkg_resources
except ImportError:
    pkg_resources = None

from ..logging.log_manager import LogManager


class ConfigManager:
    """
    A centralized configuration manager that loads and provides access to configuration.
    This ensures the config file is only read once using the singleton pattern.
    
    Handles:
    - Configuration file loading from multiple locations
    - Singleton pattern implementation
    - Active job configuration
    - Path and replacement settings
    - Spark configuration transformations
    """
    
    _instance = None  # Singleton instance
    
    def __new__(cls, logger: Optional['LogManager'] = None, config_path: Optional[str] = None):
        """Implements singleton pattern for configuration management."""
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, logger: Optional['LogManager'] = None, config_path: Optional[str] = None):
        """Initialize the ConfigManager and load configuration."""
        if getattr(self, '_initialized', False):
            return
            
        self.logger = logger or LogManager()
        self.config_path = config_path
        self.config_data = self._load_config()
        self._initialized = True
    
    def _find_config_file(self) -> str:
        """
        Find the config.yml file, checking multiple possible locations.
        
        Returns:
            Path to the configuration file
        """
        # If config_path is explicitly provided, use it
        if self.config_path and os.path.exists(self.config_path):
            return self.config_path
        
        # # List of possible config file locations
        # possible_paths = [
        #     # Current working directory
        #     os.path.join(os.getcwd(), "config.yml"),
        #     # Same directory as this script
        #     os.path.join(os.path.dirname(__file__), "config.yml"),
        #     # Parent directory (for module structure)
        #     os.path.join(os.path.dirname(os.path.dirname(__file__)), "config.yml"),
        # ]
        
        # # Check file system paths first
        # for path in possible_paths:
        #     if path and os.path.exists(path):
        #         self.logger.debug(f"Found config file at: {path}")
        #         return path
        
        # # Try to load from package resources (for installed wheel)
        # if pkg_resources:
        #     try:
        #         resource_path = pkg_resources.resource_filename('wfexporter', 'config.yml')
        #         if os.path.exists(resource_path):
        #             self.logger.debug(f"Found config file in package resources: {resource_path}")
        #             return resource_path
        #     except Exception:
        #         pass
        
        # # If none found, use default path
        # default_path = os.path.join(os.getcwd(), "config.yml")
        # self.logger.warning(f"No config file found. Will attempt to use: {default_path}")
        # return default_path
    
    def _load_config(self) -> Dict[str, Any]:
        """
        Load configuration from config.yml file.
        
        Returns:
            Dictionary containing configuration data
        """
        try:
            config_file_path = self._find_config_file()
            
            if os.path.exists(config_file_path):
                with open(config_file_path, "r") as config_file:
                    config_data = yaml.safe_load(config_file)
                    self.logger.info(f"Configuration loaded successfully from: {config_file_path}")
                    return config_data
            else:
                self.logger.error(f"Config file not found: {config_file_path}")
                return {}
                
        except Exception as e:
            self.logger.error(f"Error loading configuration: {str(e)}")
            return {}
    
    def get_active_jobs(self) -> List[Tuple[str, str]]:
        """
        Get active job configurations from config.
        
        Returns:
            List of tuples containing (job_id, status) for active jobs
        """
        workflows_details = self.config_data.get("workflows", [])
        
        active_jobs = []
        for wf in workflows_details:
            if wf.get('is_active', '') == True:
                is_existing = "Existing" if wf.get('is_existing', '') == True else "New"
                active_jobs.append((wf.get('job_id', ''), is_existing))
        
        self.logger.debug(f"Found {len(active_jobs)} active jobs in configuration")
        return active_jobs
    
    def get_initial_paths(self) -> Tuple[str, str, str, str, str]:
        """
        Get initial path variables and CLI configuration from config.
        
        Returns:
            Tuple containing start_path, resource_key_job_id_mapping_csv_file_path, 
            backup_jobs_yaml_path, cli_path, and config_profile
        """
        initial_variables = self.config_data.get("initial_variables", {})
        
        v_start_path = initial_variables.get("v_start_path", "../Project_ATT_Databricks_Workflows")
        v_resource_key_job_id_mapping_csv_file_path = initial_variables.get(
            "v_resource_key_job_id_mapping_csv_file_path", 
            "{v_start_path}/bind_scripts/resource_key_job_id_mapping.csv"
        ).format(v_start_path=v_start_path)
        v_backup_jobs_yaml_path = initial_variables.get(
            "v_backup_jobs_yaml_path", 
            "{v_start_path}/backup_jobs_yaml/"
        ).format(v_start_path=v_start_path)
        v_databricks_cli_path = initial_variables.get("v_databricks_cli_path", None)  # None for auto-detection
        v_databricks_config_profile = initial_variables.get("v_databricks_config_profile", None)  # None for env-based auth
        
        return v_start_path, v_resource_key_job_id_mapping_csv_file_path, v_backup_jobs_yaml_path, v_databricks_cli_path, v_databricks_config_profile
    
    def get_log_directory_path(self) -> str:
        """
        Get the log directory path from configuration.
        
        Returns:
            str: Path to the log directory
        """
        initial_variables = self.config_data.get("initial_variables", {})
        v_start_path = initial_variables.get("v_start_path", os.getcwd())
        
        v_log_directory_path = initial_variables.get(
            "v_log_directory_path", 
            "run_logs"  # Default to root run_logs for backward compatibility
        )
        
        # If the path contains placeholders, format it
        if "{v_start_path}" in v_log_directory_path:
            v_log_directory_path = v_log_directory_path.format(v_start_path=v_start_path)
        
        return v_log_directory_path
    
    def get_databricks_yml_path(self) -> str:
        """
        Get the databricks.yml file path from configuration.
        
        Returns:
            str: Path to the databricks.yml file
        """
        initial_variables = self.config_data.get("initial_variables", {})
        v_start_path = initial_variables.get("v_start_path", os.getcwd())
        
        v_databricks_yml_path = initial_variables.get(
            "v_databricks_yml_path", 
            "{v_start_path}/databricks.yml"
        ).format(v_start_path=v_start_path)
        
        return v_databricks_yml_path
    
    def get_replacements(self) -> Dict[str, str]:
        """
        Get YAML replacement mappings from config.
        
        Returns:
            Dictionary of value replacements to apply to YAML files
        """
        replacements = self.config_data.get("value_replacements", {})
        self.logger.debug(f"Loaded {len(replacements)} value replacements from configuration")
        return replacements
    
    def get_spark_conf_transformations(self) -> List[Dict[str, Any]]:
        """
        Get spark configuration transformation rules from config.
        
        Returns:
            List of dictionaries containing spark configuration transformation rules
        """
        transformations = self.config_data.get("spark_conf_key_replacements", [])
        self.logger.debug(f"Loaded {len(transformations)} spark configuration transformations from configuration")
        return transformations
    
    def get_path_replacements(self) -> Dict[str, str]:
        """
        Get path replacement patterns from config.
        
        Returns:
            Dictionary of path replacement patterns
        """
        replacements = self.config_data.get("path_replacement", {})
        self.logger.debug(f"Loaded {len(replacements)} path replacement patterns from configuration")
        return replacements 