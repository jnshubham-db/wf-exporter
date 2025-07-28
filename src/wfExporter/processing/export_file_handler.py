"""
File management module for Databricks Workflow Exporter.

This module handles file operations such as moving, mapping, and transforming
file paths for the workflow export process.
"""

import os
import re
import shutil
from typing import Dict, List, Tuple, Optional

import pandas as pd

from ..logging.log_manager import LogManager


class ExportFileHandler:
    """
    A class to handle file operations such as moving, mapping, and transforming file paths.
    
    Handles:
    - File path transformations
    - Directory operations
    - File mapping and moving
    - Path pattern replacements
    """
    
    def __init__(self, logger: Optional[LogManager] = None, config_manager: Optional['ConfigManager'] = None):
        """
        Initialize the FileManager.
        
        Args:
            logger: Logger instance for logging operations
            config_manager: ConfigManager instance for accessing configuration
        """
        self.logger = logger or LogManager()
        self.config_manager = config_manager
        
    def _get_path_prefixes(self) -> List[Tuple[str, str]]:
        """
        Get path replacement patterns from config as list of tuples.
        
        Returns:
            List of (pattern, replacement) tuples for path transformation
        """
        if self.config_manager:
            path_replacements = self.config_manager.get_path_replacements()
            # Convert dict to list of tuples to maintain compatibility with existing logic
            path_prefixes = [(pattern, replacement) for pattern, replacement in path_replacements.items()]
            self.logger.debug(f"Loaded {len(path_prefixes)} path replacement patterns from config")
            return path_prefixes
        else:
            self.logger.debug("No config manager provided, using default path replacements")
            # Default path replacements if no config manager
            return [
                (r"^/Workspace/Repos/[^/]+/", "../"),
                (r"^/Repos/[^/]+/", "../"),
                (r"^/Workspace/", "../"),
                (r"^/Shared/", "../"),
                (r"^/", "../")
            ]
    
    def convert_string(self, input_string: str) -> str:
        """
        Converts a string to a standardized format by replacing special characters and spaces 
        with underscores, converting to lowercase, and cleaning up extra underscores.
        
        Args:
            input_string: The string to convert
            
        Returns:
            The converted string in standardized format
        """
        # Replace special chars and spaces with underscores, convert to lowercase
        result = re.sub(r'[^\w\s]|\s', '_', input_string).lower()
        # Clean up multiple underscores and remove leading/trailing ones
        return re.sub(r'_+', '_', result).strip('_')
    
    def map_src_file_name(self, file_path: str, file_dict: Dict[str, str]) -> str:
        """
        Maps a file path to its corresponding source file name based on a dictionary mapping.
        
        Args:
            file_path: The original file path
            file_dict: Dictionary mapping base names to full filenames
            
        Returns:
            The mapped source file path
        """
        # Extract the base name of the file from the path
        base_name = os.path.basename(file_path)
        # Map the base name using the dictionary
        base_name_with_extension = file_dict.get(base_name, '') 
        # Construct and return the mapped source file path
        return '../src/' + base_name_with_extension
    
    def transform_notebook_path(self, path: str, file_dict: Dict[str, str]) -> str:
        """
        Transforms a notebook path by replacing path prefixes and mapping the file name.
        
        Args:
            path: The original notebook path
            file_dict: Dictionary mapping base names to full filenames
            
        Returns:
            The transformed notebook path
        """
        # Replace path prefixes
        for pattern, replacement in self._get_path_prefixes():
            path = re.sub(pattern, replacement, path)
        
        # Map the file name only if file_dict is provided and has a mapping
        if file_dict:
            base_name = os.path.basename(path)
            base_name_with_extension = file_dict.get(base_name, '')
            if base_name_with_extension:
                # Only replace the filename at the end, not anywhere in the path
                path_dir = os.path.dirname(path)
                path = os.path.join(path_dir, base_name_with_extension)
        
        return path
    
    def move_files_to_directory(self, df: pd.DataFrame, job_id: str, start_path: str) -> Tuple[str, str]:
        """
        Moves files from a source directory to a destination directory based on a DataFrame.
        
        Args:
            df: DataFrame containing file mapping information with src_directory and dest_directory columns
            job_id: The job ID for logging purposes
            start_path: The base path for file operations
            
        Returns:
            Tuple of (error_message, status) - ("0", "success") if successful
        """
        try:
            files_not_moved = 0
            total_files = len(df)
            
            self.logger.debug(f"Moving {total_files} files for job {job_id}")
            
            # Iterate over each row in the filtered DataFrame
            for index, row in df.iterrows():
                # exported_file_path is the absolute path to the file after export
                src_file_path = row['exported_file_path']
                dest_file_path = (row['dest_directory']).replace('..', start_path)

                self.logger.debug(f"Moving file {index+1}/{total_files}: {src_file_path} -> {dest_file_path}")

                # Ensure the destination directory exists
                dest_dir = os.path.dirname(dest_file_path)
                if not os.path.exists(dest_dir):
                    os.makedirs(dest_dir)
                    self.logger.debug(f"Created directory: {dest_dir}")
                
                # Move the file if it exists
                if src_file_path and os.path.exists(src_file_path):
                    shutil.move(src_file_path, dest_file_path)
                    self.logger.debug(f"Successfully moved: {os.path.basename(src_file_path)}")
                else:
                    self.logger.warning(f"Source file not found or path is empty for notebook: {row['Notebook_Path']}. Skipping move.")
                    files_not_moved += 1
            
            # Log summary
            files_moved = total_files - files_not_moved
            if files_not_moved == 0:
                self.logger.info(f"All {files_moved} files moved successfully for job id: {job_id}")
            else:
                self.logger.warning(f"{files_moved}/{total_files} files moved successfully for job id: {job_id}. {files_not_moved} files not moved.")
            
            return "0", "success"
            
        except Exception as e:
            self.logger.error(f"Error moving files for job {job_id}: {str(e)}")
            return str(e), "failed" 