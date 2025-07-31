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

# Version information - dynamically read from package metadata
try:
    from importlib.metadata import version
    __version__ = version("wfexporter")
except ImportError:
    # Fallback for Python < 3.8
    try:
        from importlib_metadata import version
        __version__ = version("wfexporter")
    except ImportError:
        # Final fallback if metadata is not available
        __version__ = "unknown"
except Exception:
    # Fallback if package is not installed - read from pyproject.toml
    try:
        import os
        import re
        
        # Get the path to pyproject.toml (go up from src/wfExporter to project root)
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(os.path.dirname(current_dir))
        pyproject_path = os.path.join(project_root, "pyproject.toml")
        
        if os.path.exists(pyproject_path):
            with open(pyproject_path, 'r') as f:
                content = f.read()
                # Use regex to find version = "x.y.z" in the [tool.poetry] section
                version_match = re.search(r'version\s*=\s*["\']([^"\']+)["\']', content)
                if version_match:
                    __version__ = version_match.group(1)
                else:
                    __version__ = "unknown"
        else:
            __version__ = "unknown"
    except Exception:
        __version__ = "unknown"

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
