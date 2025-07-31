"""
CLI entry point for wfExporter.

This module provides the command-line interface entry point using Click library,
separating CLI concerns from core business logic.
"""

import click
import sys
import os
from .main import main

# Import version dynamically
try:
    from . import __version__
except ImportError:
    __version__ = "unknown"


def _configure_logging(log_level):
    """Configure logging level for the current command."""
    import logging
    numeric_level = getattr(logging, log_level.upper(), None)
    if isinstance(numeric_level, int):
        logging.getLogger().setLevel(numeric_level)
        
        # Also configure specific loggers for our modules
        logging.getLogger('wfExporter').setLevel(numeric_level)
        logging.getLogger('wfExporter.installer').setLevel(numeric_level)
        logging.getLogger('wfExporter.installer.workflow_installer').setLevel(numeric_level)
        logging.getLogger('wfExporter.installer.github_utils').setLevel(numeric_level)
        logging.getLogger('wfExporter.installer.installer_core').setLevel(numeric_level)
        logging.getLogger('wfExporter.installer.app_installer').setLevel(numeric_level)
        
        # Configure logging format for better readability
        if log_level.upper() == 'DEBUG':
            logging.basicConfig(
                level=numeric_level,
                format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                force=True
            )
        else:
            logging.basicConfig(
                level=numeric_level,
                format='%(levelname)s - %(message)s',
                force=True
            )


@click.group(name="wf-export")
@click.version_option(version=__version__, prog_name="wf-export")
@click.option('--log-level', 
              type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR'], case_sensitive=False),
              default='INFO',
              help='Set the logging level')
@click.pass_context
def cli(ctx, log_level):
    """Databricks Workflow Exporter - Export workflows and pipelines as YAML files."""
    ctx.ensure_object(dict)
    
    # Configure logging level
    import logging
    numeric_level = getattr(logging, log_level.upper(), None)
    if isinstance(numeric_level, int):
        logging.getLogger().setLevel(numeric_level)
        
        # Also configure specific loggers for our modules
        logging.getLogger('wfExporter').setLevel(numeric_level)
        logging.getLogger('wfExporter.installer').setLevel(numeric_level)
        logging.getLogger('wfExporter.installer.workflow_installer').setLevel(numeric_level)
        logging.getLogger('wfExporter.installer.github_utils').setLevel(numeric_level)
        logging.getLogger('wfExporter.installer.installer_core').setLevel(numeric_level)
        logging.getLogger('wfExporter.installer.app_installer').setLevel(numeric_level)
        
        # Configure logging format for better readability
        if log_level.upper() == 'DEBUG':
            logging.basicConfig(
                level=numeric_level,
                format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                force=True
            )
        else:
            logging.basicConfig(
                level=numeric_level,
                format='%(levelname)s - %(message)s',
                force=True
            )


@cli.command()
@click.option('--config', '-c', help='Path to config.yml file')
@click.option('--host', help='Databricks workspace URL (optional if using profile or running in Databricks)')
@click.option('--token', help='Databricks access token (optional if using profile or running in Databricks)')
@click.option('--log-level', 
              type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR'], case_sensitive=False),
              default='INFO',
              help='Set the logging level for this command')
@click.pass_context
def run(ctx, config, host, token, log_level):
    """Run the Databricks workflow and pipeline export process."""
    # Override the global log level with command-specific log level
    _configure_logging(log_level)
    
    try:
        # Delegate to the main function with log level
        main(config, host, token, log_level)
    except KeyboardInterrupt:
        click.echo("\nOperation cancelled by user.")
        sys.exit(1)
    except RuntimeError as e:
        if "authentication" in str(e).lower():
            click.echo("Error: Databricks authentication failed.", err=True)
            click.echo("\nAuthentication Options:")
            click.echo("1. Use config profile: Set 'v_databricks_config_profile' in config.yml")
            click.echo("2. Environment variables: Set DATABRICKS_HOST and DATABRICKS_TOKEN")
            click.echo("3. Command line: Use --host and --token arguments")
            click.echo("4. Run in Databricks notebook (auto-authenticated)")
            sys.exit(1)
        else:
            click.echo(f"Error: {e}", err=True)
            sys.exit(1)
    except Exception as e:
        click.echo(f"Unexpected error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option('--config', '-c', help='Path to config.yml file')
@click.option('--host', help='Databricks workspace URL (optional if using profile or running in Databricks)')
@click.option('--token', help='Databricks access token (optional if using profile or running in Databricks)')
def export(config, host, token):
    """Export Databricks workflows and pipelines (legacy command - use 'run' instead)."""
    try:
        # Delegate to the main function
        main(config, host, token)
    except KeyboardInterrupt:
        click.echo("\nOperation cancelled by user.")
        sys.exit(1)
    except RuntimeError as e:
        if "authentication" in str(e).lower():
            click.echo("Error: Databricks authentication failed.", err=True)
            click.echo("\nAuthentication Options:")
            click.echo("1. Use config profile: Set 'v_databricks_config_profile' in config.yml")
            click.echo("2. Environment variables: Set DATABRICKS_HOST and DATABRICKS_TOKEN")
            click.echo("3. Command line: Use --host and --token arguments")
            click.echo("4. Run in Databricks notebook (auto-authenticated)")
            sys.exit(1)
        else:
            click.echo(f"Error: {e}", err=True)
            sys.exit(1)
    except Exception as e:
        click.echo(f"Unexpected error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option('--workflow/--no-workflow', default=None, help='Install workflow component')
@click.option('--app/--no-app', default=None, help='Install app component')
@click.option('--profile', help='Databricks profile to use')
@click.option('--serverless/--job-cluster', default=None, help='Use serverless or job cluster configuration')
@click.option('--generate-samples/--no-samples', default=None, help='Generate sample configuration files')
@click.option('--interactive/--non-interactive', default=True, help='Run in interactive mode')
@click.option('--log-level', 
              type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR'], case_sensitive=False),
              default='WARNING',
              help='Set the logging level for the installation process')
def install(workflow, app, profile, serverless, generate_samples, interactive, log_level):
    """Install WF Exporter workflow and/or app to Databricks workspace."""
    # Import here to avoid circular imports and ensure CLI loads quickly
    from .cli.install_cli import run_install
    
    try:
        run_install(
            install_workflow=workflow,
            install_app=app,
            profile=profile,
            serverless=serverless,
            generate_samples=generate_samples,
            interactive=interactive,
            log_level=log_level
        )
    except KeyboardInterrupt:
        click.echo("\nInstallation cancelled by user.")
        sys.exit(1)
    except Exception as e:
        click.echo(f"Installation failed: {e}", err=True)
        sys.exit(1)


@cli.group(name="app")
def app_group():
    """App-related commands."""
    pass


@app_group.command(name="run")
@click.option('--host', default='127.0.0.1', help='Host to bind to')
@click.option('--port', default=5000, help='Port to bind to')
@click.option('--debug/--no-debug', default=False, help='Enable debug mode')
def app_run(host, port, debug):
    """Run the WF Exporter web application locally."""
    # Import here to avoid loading Flask unless needed
    from .cli.app_cli import run_local_app
    
    try:
        run_local_app(host=host, port=port, debug=debug)
    except KeyboardInterrupt:
        click.echo("\nApp stopped by user.")
        sys.exit(1)
    except Exception as e:
        click.echo(f"Failed to start app: {e}", err=True)
        sys.exit(1)


@cli.command()
def status():
    """Show installation status of WF Exporter components."""
    from .cli.install_cli import show_status
    
    try:
        show_status()
    except Exception as e:
        click.echo(f"Failed to check status: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option('--workflow', is_flag=True, help='Uninstall workflow only')
@click.option('--app', is_flag=True, help='Uninstall app only')
@click.option('--all', 'uninstall_all', is_flag=True, help='Uninstall all components')
@click.confirmation_option(prompt='Are you sure you want to uninstall WF Exporter components?')
def uninstall(workflow, app, uninstall_all):
    """Uninstall WF Exporter components from Databricks workspace."""
    from .cli.install_cli import run_uninstall
    
    # If no specific flags are provided, default to uninstalling all components
    if not (workflow or app or uninstall_all):
        uninstall_all = True
        workflow = True
        app = True
    
    try:
        run_uninstall(
            uninstall_workflow=workflow or uninstall_all,
            uninstall_app=app or uninstall_all,
            uninstall_all=uninstall_all
        )
    except Exception as e:
        click.echo(f"Uninstallation failed: {e}", err=True)
        sys.exit(1)


def cli_main():
    """
    Main CLI entry point for backward compatibility.
    
    This function maintains compatibility with the existing argparse-based interface
    while routing to the new Click-based CLI when appropriate.
    """
    # Check for help or version flags first (should be handled by Click directly)
    if len(sys.argv) > 1 and sys.argv[1] in ['--help', '-h', '--version']:
        cli()
        return
    
    # Check if this is being called with old-style arguments (no subcommands)
    if len(sys.argv) == 1 or (len(sys.argv) > 1 and sys.argv[1].startswith('-')):
        # This looks like the old CLI format, route to run command (primary command)
        from click.testing import CliRunner
        runner = CliRunner()
        
        # Convert old-style args to new format
        args = ['run'] + sys.argv[1:]
        result = runner.invoke(cli, args, catch_exceptions=False)
        sys.exit(result.exit_code)
    else:
        # This is new-style CLI with subcommands
        cli()


if __name__ == "__main__":
    cli_main() 