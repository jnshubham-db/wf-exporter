"""
Installation CLI module for WF Exporter.

This module handles interactive installation of Databricks workflows and apps.
"""

import logging
import click
import os
import sys
from pathlib import Path
from typing import Optional, List, Dict, Any

# Add the src directory to Python path for imports
src_path = Path(__file__).parent.parent
sys.path.insert(0, str(src_path))

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    from ..installer.workflow_installer import WorkflowInstaller
    from ..installer.app_installer import AppInstaller, ActiveDeploymentError
    from ..installer.config_generator import ConfigGenerator
    _installer_available = True
except ImportError as e:
    logger.error(f"Installer modules not available: {e}")
    _installer_available = False
    WorkflowInstaller = None
    AppInstaller = None
    ActiveDeploymentError = None
    ConfigGenerator = None


def get_available_profiles() -> List[str]:
    """Get list of available Databricks profiles from .databrickscfg."""
    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.core import Config
        
        # Try to get profiles from the config
        config_file = Path.home() / '.databrickscfg'
        if not config_file.exists():
            return []
        
        profiles = []
        current_section = None
        
        with open(config_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line.startswith('[') and line.endswith(']'):
                    profile_name = line[1:-1]
                    if profile_name:
                        # Include all profiles including DEFAULT
                        profiles.append(profile_name)
        
        return profiles
    except Exception:
        return []


def validate_profile(profile: str) -> bool:
    """Validate that a Databricks profile is working."""
    try:
        from databricks.sdk import WorkspaceClient
        
        client = WorkspaceClient(profile=profile)
        # Try to get current user to validate connection
        user = client.current_user.me()
        click.echo(f"✅ Authenticated as: {user.display_name}")
        return True
    except Exception as e:
        click.echo(f"❌ Failed to validate profile '{profile}': {e}", err=True)
        return False


def prompt_profile_selection(available_profiles: List[str]) -> Optional[str]:
    """Prompt user to select a Databricks profile."""
    if not available_profiles:
        click.echo("No Databricks profiles found in ~/.databrickscfg")
        click.echo("Please configure a profile first using: databricks configure")
        return None
    
    click.echo("\nAvailable Databricks profiles:")
    for i, profile in enumerate(available_profiles, 1):
        click.echo(f"  {i}. {profile}")
    
    while True:
        try:
            choice = click.prompt(
                f"\nSelect profile (1-{len(available_profiles)})",
                type=int
            )
            if 1 <= choice <= len(available_profiles):
                selected_profile = available_profiles[choice - 1]
                
                # Validate the selected profile
                click.echo(f"Validating profile '{selected_profile}'...")
                if validate_profile(selected_profile):
                    click.echo(f"✓ Profile '{selected_profile}' validated successfully")
                    return selected_profile
                else:
                    click.echo("Profile validation failed. Please select another profile.")
                    continue
            else:
                click.echo("Invalid selection. Please try again.")
        except (ValueError, click.Abort):
            click.echo("Invalid input. Please enter a number.")
        except KeyboardInterrupt:
            return None


def show_folders_to_create(component: str) -> None:
    """Show which workspace folders will be created for a component."""
    if component == "workflow":
        folders = [
            "/Workspace/Applications/wf_exporter/wf_config/",
            "/Workspace/Applications/wf_exporter/exports/"
        ]
    elif component == "app":
        folders = [
            "/Workspace/Applications/wf_exporter/app_config/"
        ]
    else:
        return
    
    click.echo(f"\nThe following workspace folders will be created for {component}:")
    for folder in folders:
        click.echo(f"  • {folder}")


def run_install(
    install_workflow: Optional[bool] = None,
    install_app: Optional[bool] = None,
    profile: Optional[str] = None,
    serverless: Optional[bool] = None,
    generate_samples: Optional[bool] = None,
    interactive: bool = True,
    log_level: str = 'WARNING'
) -> None:
    """
    Run the installation process for WF Exporter components.
    
    Args:
        install_workflow: Whether to install workflow component
        install_app: Whether to install app component  
        profile: Databricks profile to use
        serverless: Whether to use serverless configuration
        generate_samples: Whether to generate sample files
        interactive: Whether to run in interactive mode
        log_level: Logging level for the installation process
    """
    # Configure logging for installation
    from ..cli_entry import _configure_logging
    _configure_logging(log_level)
    
    # Check if installer modules are available
    if not _installer_available:
        click.echo("❌ Error: Installer modules are not available.")
        click.echo("💡 Make sure all dependencies are installed: pip install wfexporter")
        return
    
    click.echo("🚀 WF Exporter Installation Wizard")
    click.echo("=" * 40)
    
    # Profile selection
    if not profile and interactive:
        available_profiles = get_available_profiles()
        profile = prompt_profile_selection(available_profiles)
        if not profile:
            click.echo("Installation cancelled: No valid profile selected.")
            return
    elif profile:
        # Validate provided profile
        if not validate_profile(profile):
            click.echo(f"Installation cancelled: Profile '{profile}' is invalid.")
            return
    
    # Workflow installation
    if install_workflow is None and interactive:
        install_workflow = click.confirm("\n📋 Install workflow component?", default=True)
    
    if install_workflow:
        show_folders_to_create("workflow")
        
        if serverless is None and interactive:
            serverless = click.confirm("\n⚡ Is your workspace serverless enabled?", default=True)
        
        try:
            from .progress_indicator import InstallationProgress
            
            workflow_installer = WorkflowInstaller(profile=profile, interactive=interactive)
            progress = InstallationProgress()
            
            click.echo(f"\n🔧 Installing workflow with {'serverless' if serverless else 'job cluster'} configuration...")
            
            job_info = workflow_installer.install(serverless=serverless, progress=progress)
            
            click.echo(f"✅ Workflow installed successfully!")
            click.echo(f"   Job ID: {job_info.get('job_id')}")
            click.echo(f"   Job Name: {job_info.get('job_name')}")
            
        except Exception as e:
            click.echo(f"❌ Workflow installation failed: {e}", err=True)
            return
    
    # App installation
    if install_app is None and interactive:
        install_app = click.confirm("\n🌐 Install web application?", default=True)
    
    if install_app:
        if not install_workflow:
            click.echo("⚠️  Warning: App installation requires workflow to be installed first.")
            if interactive and not click.confirm("Continue with app installation anyway?"):
                install_app = False
        
        if install_app:
            show_folders_to_create("app")
            
            try:
                app_installer = AppInstaller(profile=profile)
                app_progress = InstallationProgress()
                
                click.echo(f"\n🔧 Installing web application...")
                
                app_info = app_installer.install(progress=app_progress)
                
                click.echo(f"✅ App installed successfully!")
                click.echo(f"   App Name: {app_info.get('app_name')}")
                if app_info.get('app_id'):
                    click.echo(f"   App ID: {app_info.get('app_id')}")
                click.echo(f"   App URL: {app_info.get('app_url')}")
                
                # Display permission status if app_id is available
                if app_info.get('app_id'):
                    click.echo(f"   🔐 Permissions set for app_id: {app_info.get('app_id')}")
                
                # Display deployment info
                if app_info.get('deployment_id'):
                    click.echo(f"   🚀 Deployment ID: {app_info.get('deployment_id')}")
                
            except ActiveDeploymentError as e:
                click.echo(f"❌ App installation failed: {e}", err=True)
                click.echo()
                click.echo("💡 The app has active deployments in progress.")
                click.echo("   This usually means a previous deployment is still running or stuck.")
                click.echo()
                
                if interactive and click.confirm("Do you want to delete the existing app and recreate it? This will remove all active deployments."):
                    try:
                        click.echo("⚠️  Deleting existing app and recreating...")
                        app_progress = InstallationProgress()
                        app_info = app_installer.install_with_force_delete(progress=app_progress)
                        click.echo(f"✅ App installed successfully!")
                        click.echo(f"   App Name: {app_info.get('app_name')}")
                        if app_info.get('app_id'):
                            click.echo(f"   App ID: {app_info.get('app_id')}")
                        click.echo(f"   App URL: {app_info.get('app_url')}")
                        if app_info.get('app_id'):
                            click.echo(f"   🔐 Permissions set for app_id: {app_info.get('app_id')}")
                        if app_info.get('deployment_id'):
                            click.echo(f"   🚀 Deployment ID: {app_info.get('deployment_id')}")
                    except Exception as delete_e:
                        click.echo(f"❌ Failed to delete and recreate app: {delete_e}", err=True)
                        click.echo("💡 You may need to manually delete the app in the Databricks UI and try again.")
                        return
                else:
                    click.echo("Installation cancelled.")
                    click.echo("💡 You can use the --force flag to automatically delete and recreate the app:")
                    click.echo("   python -m wfExporter.cli install --component app --force")
                    return
                
            except Exception as e:
                click.echo(f"❌ App installation failed: {e}", err=True)
                return
    
    # Sample configuration generation
    if generate_samples is None and interactive:
        generate_samples = click.confirm("\n📄 Generate sample configs in current directory for local run?", default=True)
    
    if generate_samples:
        try:
            generator = ConfigGenerator()
            
            click.echo("\n🔧 Generating sample configuration files...")
            
            files_created = generator.generate_samples(Path.cwd())
            
            click.echo("✅ Sample files created:")
            for file_path in files_created:
                click.echo(f"   • {file_path}")
                
        except Exception as e:
            click.echo(f"❌ Failed to generate sample configs: {e}", err=True)
            return
    
    click.echo("\n🎉 Installation completed successfully!")
    click.echo("\nNext steps:")
    if install_workflow:
        click.echo("1. Update the generated config.yml with your workflow/pipeline IDs")
        click.echo("2. Run: wf-export run -c config.yml")
    if install_app:
        click.echo("3. Access your web app at the URL shown above")


def show_status() -> None:
    """Show installation status of WF Exporter components."""
    click.echo("🔍 WF Exporter Installation Status")
    click.echo("=" * 35)
    
    try:
        from ..installer.installer_core import InstallerCore
        core = InstallerCore()
        
        status = core.get_installation_status()
        
        # Workflow status
        workflow_status = status.get('workflow', {})
        if workflow_status.get('installed'):
            click.echo("📋 Workflow: ✅ Installed")
            click.echo(f"   Job ID: {workflow_status.get('job_id', 'Unknown')}")
            click.echo(f"   Job Name: {workflow_status.get('job_name', 'Unknown')}")
        else:
            click.echo("📋 Workflow: ❌ Not installed")
        
        # App status
        app_status = status.get('app', {})
        if app_status.get('installed'):
            click.echo("🌐 App: ✅ Installed")
            click.echo(f"   App Name: {app_status.get('app_name', 'Unknown')}")
            click.echo(f"   App URL: {app_status.get('app_url', 'Unknown')}")
        else:
            click.echo("🌐 App: ❌ Not installed")
        
        # Configuration files
        config_status = status.get('configs', {})
        if config_status.get('present'):
            click.echo("📄 Sample Configs: ✅ Present in current directory")
        else:
            click.echo("📄 Sample Configs: ❌ Not found in current directory")
            
    except Exception as e:
        click.echo(f"❌ Failed to check status: {e}", err=True)


def run_uninstall(
    uninstall_workflow: bool = False,
    uninstall_app: bool = False,
    uninstall_all: bool = False
) -> None:
    """
    Run the uninstallation process.
    
    Args:
        uninstall_workflow: Uninstall workflow component
        uninstall_app: Uninstall app component
        uninstall_all: Uninstall all components
    """
    click.echo("🗑️  WF Exporter Uninstallation")
    click.echo("=" * 35)

    
    if not (uninstall_workflow or uninstall_app or uninstall_all):
        click.echo("No components specified for uninstallation.")
        return
    
    try:
        from ..installer.installer_core import InstallerCore
        core = InstallerCore()
        
        if uninstall_workflow or uninstall_all:
            click.echo("🔧 Uninstalling workflow component...")
            core.uninstall_workflow()
            click.echo("✅ Workflow uninstalled successfully")
        
        if uninstall_app or uninstall_all:
            click.echo("🔧 Uninstalling app component...")
            core.uninstall_app()
            click.echo("✅ App uninstalled successfully")
        
        click.echo("\n🎉 Uninstallation completed successfully!")
        
    except Exception as e:
        click.echo(f"❌ Uninstallation failed: {e}", err=True) 