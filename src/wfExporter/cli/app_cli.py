"""
App CLI module for WF Exporter.

This module handles running the Flask web application locally.
"""

import click
import os
import sys
from pathlib import Path


def run_local_app(host: str = '127.0.0.1', port: int = 5000, debug: bool = False) -> None:
    """
    Run the WF Exporter web application locally.
    
    Args:
        host: Host to bind to
        port: Port to bind to
        debug: Enable debug mode
    """
    click.echo("🌐 Starting WF Exporter Web Application")
    click.echo("=" * 40)
    
    # First, try to import wf_app directly and run it
    try:
        # Set environment variables before importing
        os.environ['FLASK_ENV'] = 'development' if debug else 'production'
        os.environ['FLASK_HOST'] = host
        os.environ['FLASK_PORT'] = str(port)
        
        # Try to import and run wf_app directly
        import wf_app
        
        click.echo(f"🚀 Starting server on http://{host}:{port}")
        click.echo(f"📦 Using installed wf_app package: {wf_app.__file__}")
        if debug:
            click.echo("🐛 Debug mode enabled")
        click.echo("Press Ctrl+C to stop the server")
        click.echo("")
        
        # Run the Flask app using the imported module
        wf_app.main()
        return
        
    except ImportError as import_error:
        click.echo(f"⚠️  Could not import wf_app package: {import_error}")
        click.echo("📁 Falling back to directory-based approach...")
        click.echo("")
    
    # Fallback: Try multiple possible locations for wf_app directory
    possible_paths = []
    
    # 1. Try to find wf_app by importing it and getting its path
    try:
        import wf_app
        if hasattr(wf_app, '__file__') and wf_app.__file__:
            wf_app_path = Path(wf_app.__file__).parent
            possible_paths.append(wf_app_path)
    except ImportError:
        pass
    
    # 2. Try using pkg_resources to find the installed wf_app package
    try:
        import pkg_resources
        dist = pkg_resources.get_distribution('wfexporter')
        site_packages = Path(dist.location)
        possible_paths.append(site_packages / "wf_app")
    except (ImportError, pkg_resources.DistributionNotFound, FileNotFoundError):
        pass
    
    # 3. Try using importlib.resources (Python 3.9+)
    try:
        try:
            from importlib.resources import files
        except ImportError:
            from importlib_resources import files
        
        try:
            wf_app_ref = files('wf_app')
            if hasattr(wf_app_ref, '__fspath__'):
                possible_paths.append(Path(wf_app_ref.__fspath__()))
        except (ModuleNotFoundError, AttributeError):
            pass
    except ImportError:
        pass
    
    # 4. Try relative to current working directory (development mode)
    possible_paths.append(Path.cwd() / "wf_app")
    
    # 5. Try relative to this file's location (development structure)
    current_dir = Path(__file__).parent.parent.parent.parent
    possible_paths.append(current_dir / "wf_app")
    
    # 6. Try in the same directory as the package root (development)
    wf_exporter_dir = Path(__file__).parent.parent.parent
    possible_paths.append(wf_exporter_dir.parent / "wf_app")
    
    # Find the first existing wf_app directory
    wf_app_dir = None
    for path in possible_paths:
        if path.exists() and path.is_dir() and (path / "main.py").exists():
            wf_app_dir = path
            break
    
    if not wf_app_dir:
        click.echo("❌ Error: wf_app directory not found in any of the following locations:")
        for i, path in enumerate(possible_paths, 1):
            click.echo(f"  {i}. {path}")
        click.echo("")
        click.echo("🔍 Debugging information:")
        
        # Try to import wf_app and show what happens
        try:
            import wf_app
            click.echo(f"✅ wf_app module found at: {wf_app.__file__}")
            click.echo("ℹ️  The package is installed but may have an issue with the directory structure.")
        except ImportError as e:
            click.echo(f"❌ wf_app module not found: {e}")
            click.echo("ℹ️  The wf_app package may not be properly installed.")
        
        click.echo("")
        click.echo("💡 Solutions:")
        click.echo("   • Reinstall the package: pip install --force-reinstall wfexporter")
        click.echo("   • Or run from project root directory")
        sys.exit(1)
    
    # Add wf_app directory to Python path
    sys.path.insert(0, str(wf_app_dir))
    
    # Set environment variables
    os.environ['FLASK_ENV'] = 'development' if debug else 'production'
    os.environ['FLASK_HOST'] = host
    os.environ['FLASK_PORT'] = str(port)
    
    try:
        # Import and run the Flask app
        from main import main as flask_main
        
        click.echo(f"🚀 Starting server on http://{host}:{port}")
        click.echo(f"📂 Using wf_app directory: {wf_app_dir}")
        if debug:
            click.echo("🐛 Debug mode enabled")
        click.echo("Press Ctrl+C to stop the server")
        click.echo("")
        
        # Run the Flask app
        flask_main()
        
    except ImportError as e:
        click.echo(f"❌ Failed to import Flask app: {e}")
        click.echo("Make sure all dependencies are installed: pip install -r requirements.txt")
        sys.exit(1)
    except Exception as e:
        click.echo(f"❌ Failed to start app: {e}")
        sys.exit(1) 