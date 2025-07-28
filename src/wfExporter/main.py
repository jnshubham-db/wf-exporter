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
    
    This function automatically detects the execution environment and asset type:
    - Local Environment: Uses CLI path and config profile from config.yml or provided credentials
    - Databricks Environment: Auto-installs CLI and uses runtime authentication
    - Asset Type: Automatically detects whether to export workflows or pipelines based on config.yml
    - Processing: Can handle both workflows and pipelines in the same run
    
    Args:
        config_path: Path to config.yml file (optional)
        databricks_host: Databricks workspace URL (for manual authentication)
        databricks_token: Databricks access token (for manual authentication)
    """
    processor = DatabricksExporter(config_path, databricks_host, databricks_token)
    
    # Determine asset types based on configuration
    active_jobs = processor.config_manager.get_active_jobs()
    active_pipelines = processor.config_manager.get_active_pipelines()
    
    # Track processing status
    workflows_processed = False
    pipelines_processed = False
    
    # Process workflows if any are active
    if active_jobs:
        processor.logger.info(f"Detected {len(active_jobs)} active workflows. Starting workflow export process...")
        try:
            processor.run_workflow_export()
            workflows_processed = True
            processor.logger.info("✅ Workflow export process completed successfully.")
        except Exception as e:
            processor.logger.error(f"❌ Workflow export process failed: {str(e)}")
    
    # Process pipelines if any are active
    if active_pipelines:
        processor.logger.info(f"Detected {len(active_pipelines)} active pipelines. Starting pipeline export process...")
        try:
            processor.run_pipeline_export()
            pipelines_processed = True
            processor.logger.info("✅ Pipeline export process completed successfully.")
        except Exception as e:
            processor.logger.error(f"❌ Pipeline export process failed: {str(e)}")
    
    # Summary and validation
    if not active_jobs and not active_pipelines:
        processor.logger.error("No active workflows or pipelines found in configuration. Please set is_active: true for at least one item.")
    else:
        processed_types = []
        if workflows_processed:
            processed_types.append(f"{len(active_jobs)} workflows")
        if pipelines_processed:
            processed_types.append(f"{len(active_pipelines)} pipelines")
        
        if processed_types:
            processor.logger.info(f"🎉 Export process completed successfully! Processed: {', '.join(processed_types)}")
        else:
            processor.logger.error("Export process completed with errors. Check the logs above for details.")
    
    logging.shutdown()


if __name__ == "__main__":
    # For direct script execution
    main() 