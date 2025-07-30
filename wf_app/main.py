#!/usr/bin/env python3
"""Main entry point for the Databricks Workflow Manager Web App."""

import os
import sys

# Handle both relative imports (when run as package) and absolute imports (when run directly)
try:
    # Try relative import first (when run as part of wf_app package)
    from .app import create_app
except ImportError:
    # Fall back to absolute import (when run directly as script)
    # Add current directory to path to find the app module
    current_dir = os.path.dirname(os.path.abspath(__file__))
    if current_dir not in sys.path:
        sys.path.insert(0, current_dir)
    from app import create_app


def main():
    """Create and run the Flask application."""
    # Get configuration from environment
    config_name = os.environ.get('FLASK_ENV', 'development')

    
    # Create Flask app using factory pattern
    app = create_app(config_name)
    
    # Run the application
    host = os.environ.get('FLASK_HOST', '127.0.0.1')
    port = int(os.environ.get('FLASK_PORT', 5000))
    debug = config_name == 'development'
    
    print(f"Starting Databricks Workflow Manager Web App...")
    print(f"Environment: {config_name}")
    print(f"Running on: http://{host}:{port}")
    
    app.run(host=host, port=port, debug=debug)


if __name__ == "__main__":
    main()
