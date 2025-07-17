"""
CLI entry point for wfExporter.

This module provides the command-line interface entry point,
separating CLI concerns from core business logic.
"""

import argparse
import sys
from .main import main


def cli_main():
    """
    Command line interface entry point.
    
    Supports multiple authentication methods:
    1. Profile-based authentication (set in config.yml)
    2. Environment variables (DATABRICKS_HOST, DATABRICKS_TOKEN)
    3. Command line arguments (--host, --token)
    4. Auto-detection in Databricks environment
    """
    parser = argparse.ArgumentParser(
        description='Export Databricks workflows as YAML files',
        epilog="""
Authentication Options:
  1. Use config profile (set v_databricks_config_profile in config.yml)
  2. Environment variables: DATABRICKS_HOST and DATABRICKS_TOKEN
  3. Command line: --host and --token arguments
  4. Auto-detect in Databricks notebooks (no setup required)
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--config', '-c', help='Path to config.yml file')
    parser.add_argument('--host', help='Databricks workspace URL (optional if using profile or running in Databricks)')
    parser.add_argument('--token', help='Databricks access token (optional if using profile or running in Databricks)')
    
    args = parser.parse_args()
    
    try:
        # Delegate to the main function
        main(args.config, args.host, args.token)
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
        sys.exit(1)
    except RuntimeError as e:
        if "authentication" in str(e).lower():
            print("Error: Databricks authentication failed.")
            print("\nAuthentication Options:")
            print("1. Use config profile: Set 'v_databricks_config_profile' in config.yml")
            print("2. Environment variables: Set DATABRICKS_HOST and DATABRICKS_TOKEN")
            print("3. Command line: Use --host and --token arguments")
            print("4. Run in Databricks notebook (auto-authenticated)")
            sys.exit(1)
        else:
            print(f"Error: {e}")
            sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    cli_main() 