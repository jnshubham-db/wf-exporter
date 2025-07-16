"""
Databricks Workflow Exporter

A Python package for exporting Databricks workflows as YAML files with support for both
local development environments and Databricks notebook environments.

Features:
- Automatic environment detection
- Multiple authentication methods
- Modular architecture
- Comprehensive logging
"""

# Main interface
from .core.databricks_exporter import main, cli_main, DatabricksExporter

# Individual modules for advanced usage
from .logging import LogManager
from .cli import DatabricksCliManager
from .workflow import WorkflowExtractor
from .processing import YamlSerializer, ExportFileHandler
from .config import ConfigManager

# Version information
__version__ = "1.0.0"
__author__ = "Databricks Workflow Exporter Team"

# Public API
__all__ = [
    # Main interface
    'main',
    'cli_main',
    'DatabricksExporter',
    
    # Core components
    'LogManager',
    'DatabricksCliManager', 
    'WorkflowExtractor',
    'YamlSerializer',
    'ExportFileHandler',
    'ConfigManager',
]
