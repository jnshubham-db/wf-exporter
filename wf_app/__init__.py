"""
WF Exporter Web Application

Flask-based web application for managing Databricks workflow exports.
"""

# Import the main function for the script entry point
from .main import main

# Import the app factory for direct usage
from .app import create_app

__version__ = "0.1.0"
__all__ = ['main', 'create_app'] 