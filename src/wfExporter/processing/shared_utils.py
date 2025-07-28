"""
Shared utilities for Databricks Workflow and Pipeline Exporter.

This module contains common functions that are used across both workflow 
and pipeline export processes to minimize code duplication.
"""

import os
import fnmatch
from typing import Dict, List, Any, Optional, Tuple
from ..logging.log_manager import LogManager


class SharedUtils:
    """
    Utility class containing common functions used by both workflow and pipeline exporters.
    """
    
    def __init__(self, logger: Optional[LogManager] = None):
        """
        Initialize SharedUtils.
        
        Args:
            logger: Logger instance for logging operations
        """
        self.logger = logger or LogManager()
    
    @staticmethod
    def determine_artifact_type_from_path(file_path: str) -> str:
        """
        Determine artifact type from file path.
        
        Args:
            file_path: Path to the file
            
        Returns:
            Artifact type string
        """
        if file_path.endswith('.py'):
            return 'py'
        elif file_path.endswith('.sql'):
            return 'sql'
        elif file_path.endswith('.whl'):
            return 'whl'
        elif file_path.endswith('.jar'):
            return 'jar'
        elif file_path.endswith('.ipynb'):
            return 'notebook'
        else:
            return 'auto'
    
    @staticmethod
    def create_dest_subdir_from_workspace_path(workspace_path: str) -> str:
        """
        Create destination subdirectory from workspace path.
        
        Args:
            workspace_path: Original workspace path
            
        Returns:
            Destination subdirectory path
        """
        try:
            if '/Workspace/' in workspace_path:
                relative_path = workspace_path.replace('/Workspace/', '')
            else:
                relative_path = workspace_path.lstrip('/')
            
            return os.path.dirname(relative_path)
            
        except Exception:
            return ""
    
    @staticmethod
    def match_files_by_pattern(file_paths: List[str], pattern: str) -> List[str]:
        """
        Match file paths against a glob pattern.
        
        Args:
            file_paths: List of file paths to check
            pattern: Glob pattern to match against
            
        Returns:
            List of matching file paths
        """
        return [path for path in file_paths if fnmatch.fnmatch(path, pattern)]
    
    def validate_path_mappings(self, mappings: Dict[str, str]) -> Tuple[bool, List[str]]:
        """
        Validate path mappings for consistency.
        
        Args:
            mappings: Dictionary of path mappings
            
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []
        
        for original, transformed in mappings.items():
            if not isinstance(original, str) or not isinstance(transformed, str):
                errors.append(f"Invalid mapping types: {type(original)} -> {type(transformed)}")
            elif not original.strip() or not transformed.strip():
                errors.append(f"Empty paths in mapping: '{original}' -> '{transformed}'")
        
        is_valid = len(errors) == 0
        if not is_valid:
            self.logger.warning(f"Path mapping validation failed with {len(errors)} errors")
        else:
            self.logger.debug(f"Path mapping validation passed for {len(mappings)} mappings")
        
        return is_valid, errors
    
    def extract_patterns_from_yaml(self, yaml_content: dict, pattern_key: str) -> List[str]:
        """
        Extract specific patterns from YAML content.
        
        Args:
            yaml_content: Parsed YAML content
            pattern_key: Key to search for (e.g., 'glob', 'include')
            
        Returns:
            List of found patterns
        """
        patterns = []
        
        def search_recursive(obj):
            if isinstance(obj, dict):
                if pattern_key in obj:
                    value = obj[pattern_key]
                    if isinstance(value, str):
                        patterns.append(value)
                    elif isinstance(value, list):
                        patterns.extend(value)
                
                for val in obj.values():
                    search_recursive(val)
            elif isinstance(obj, list):
                for item in obj:
                    search_recursive(item)
        
        search_recursive(yaml_content)
        self.logger.debug(f"Found {len(patterns)} {pattern_key} patterns")
        return patterns
    
    def create_artifact_summary(self, artifacts: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Create a summary of artifacts for logging and reporting.
        
        Args:
            artifacts: List of artifact dictionaries
            
        Returns:
            Summary dictionary with counts and statistics
        """
        total = len(artifacts)
        successful = len([a for a in artifacts if a.get('success', False)])
        failed = total - successful
        
        # Count by type
        by_type = {}
        for artifact in artifacts:
            artifact_type = artifact.get('type', 'unknown')
            by_type[artifact_type] = by_type.get(artifact_type, 0) + 1
        
        summary = {
            'total_artifacts': total,
            'successful': successful,
            'failed': failed,
            'success_rate': (successful / total * 100) if total > 0 else 0,
            'by_type': by_type,
            'failed_artifacts': [a for a in artifacts if not a.get('success', False)]
        }
        
        self.logger.info(f"Artifact summary: {successful}/{total} successful ({summary['success_rate']:.1f}%)")
        return summary 