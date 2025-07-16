# Databricks to Python Script Conversion
# This script exports Databricks workflows as YAML files
# 
# BACKWARD COMPATIBILITY MODULE
# This file maintains the original interface while importing from the new modular structure.
# All classes have been extracted to separate modules for better maintainability.

"""
DEPRECATED - Use modular imports instead

This module maintains backward compatibility but is deprecated.
Use the new modular structure for better maintainability:

from wfexporter.logging import LogManager
from wfexporter.cli import DatabricksCliManager  
from wfexporter.workflow import WorkflowExtractor
from wfexporter.processing import YamlSerializer, ExportFileHandler
from wfexporter.config import ConfigManager
from wfexporter.core import DatabricksExporter, main, cli_main

Or use the main interface:
from wfexporter import main, DatabricksExporter
"""

# Import from modular structure for backward compatibility
from .logging import LogManager
from .cli import DatabricksCliManager  
from .workflow import WorkflowExtractor
from .processing import YamlSerializer, ExportFileHandler
from .config import ConfigManager
from .core import DatabricksExporter, main, cli_main

# For backward compatibility, re-export all classes
__all__ = [
    'LogManager',
    'DatabricksCliManager',
    'WorkflowExtractor', 
    'YamlSerializer',
    'ExportFileHandler',
    'ConfigManager',
    'DatabricksExporter',
    'main',
    'cli_main'
]

# Legacy support - these imports work exactly as before
# But new code should use the modular imports above

# Execute the main function when the script is run directly
if __name__ == "__main__":
    cli_main()

