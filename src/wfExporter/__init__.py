"""
Databricks Workflow Exporter

A Python package for exporting Databricks workflows as YAML files with support for both
local development environments and Databricks notebook environments. Includes installation
functionality for deploying WF Exporter to Databricks workspaces.

Features:
- Automatic environment detection
- Multiple authentication methods
- Modular architecture
- Comprehensive logging
- Interactive installation wizard
- Programmatic installation interface
"""

# Main interface
from .main import main
from .core.databricks_exporter import DatabricksExporter
from .cli_entry import cli_main

# Individual modules for advanced usage
from .logging import LogManager
from .cli import DatabricksCliManager
from .workflow import WorkflowExtractor
from .processing import YamlSerializer, ExportFileHandler
from .config import ConfigManager

# Import installer classes for programmatic usage
try:
    from .installer import Installer, InstallerCore
    _installer_available = True
except ImportError:
    # Gracefully handle missing installer dependencies
    _installer_available = False
    Installer = None
    InstallerCore = None

# Version information
__version__ = "0.4.0"
__author__ = "Shubham Jain"

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
    
    # Metadata
    '__version__',
]

# Add installer classes if available
if _installer_available:
    __all__.extend(['Installer', 'InstallerCore'])
