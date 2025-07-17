"""
Main entry point for the Databricks Workflow Exporter.

This module provides the main function that serves as the programmatic
entry point, separate from the CLI interface.
"""

import logging
from typing import Optional
from .core.databricks_exporter import DatabricksExporter


def main(config_path: Optional[str] = None, databricks_host: Optional[str] = None, databricks_token: Optional[str] = None):
    """
    Main entry point for the Databricks Workflow Exporter.
    
    This function automatically detects the execution environment:
    - Local Environment: Uses CLI path and config profile from config.yml or provided credentials
    - Databricks Environment: Auto-installs CLI and uses runtime authentication
    
    Args:
        config_path: Path to config.yml file (optional)
        databricks_host: Databricks workspace URL (for manual authentication)
        databricks_token: Databricks access token (for manual authentication)
    """
    processor = DatabricksExporter(config_path, databricks_host, databricks_token)
    processor.run()
    logging.shutdown()


if __name__ == "__main__":
    # For direct script execution
    main() 