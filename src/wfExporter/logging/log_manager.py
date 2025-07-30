"""
Logging management module for Databricks Workflow Exporter.

This module provides centralized logging functionality with configurable levels,
colored console output, and file logging capabilities.
"""

import os
import logging
import datetime
from typing import Dict, Any, Optional


class LogManager:
    """
    A class to handle logging operations with different log levels.
    Supports colored console output and file logging.
    """
    
    # Class variable to track instances and prevent duplicate files
    _instances = {}
    
    def __init__(self, name: str = "WF_Exporter", config_data: Optional[Dict[str, Any]] = None, 
                 create_file_handler: bool = True, override_log_level: Optional[str] = None):
        """
        Initialize the logger with the specified name and level from config.
        
        Args:
            name: The name of the logger
            config_data: Configuration data containing log level
            create_file_handler: Whether to create file handler (False for temporary loggers)
            override_log_level: Override log level (from CLI --log-level flag)
        """
        # Store config_data for later use
        self.config_data = config_data
        
        # Load log level from override first, then config, then default
        if override_log_level:
            log_level_str = override_log_level
        elif config_data:
            log_level_str = config_data.get("initial_variables", {}).get("v_log_level", "INFO")
        else:
            log_level_str = "INFO"
        
        # Convert string log level to logging constant
        level_map = {
            "DEBUG": logging.DEBUG,
            "INFO": logging.INFO,
            "WARNING": logging.WARNING,
            "ERROR": logging.ERROR,
            "CRITICAL": logging.CRITICAL
        }
        level = level_map.get(log_level_str.upper(), logging.INFO)
        
        # Store the level for use in file handler
        self.log_level = level
        
        # Check if logger with this name already exists
        if name in LogManager._instances:
            # Remove the existing instance to ensure a new log file is created
            del LogManager._instances[name]
            
        # Configure the logger
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)
        
        # Prevent propagation to root logger (stops duplicate logs)
        self.logger.propagate = False
        
        # Clear existing handlers to prevent duplicates
        for handler in list(self.logger.handlers):
            self.logger.removeHandler(handler)
            handler.close()
        
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        
        # Console handler setup with colors
        console_handler = self._create_colored_console_handler(level, formatter)
        self.logger.addHandler(console_handler)
        
        # File handler setup (only if requested)
        if create_file_handler:
            self._setup_file_handler(formatter)
        
        # Store instance in class dictionary
        LogManager._instances[name] = self
    
    def _create_colored_console_handler(self, level: int, formatter: logging.Formatter) -> logging.StreamHandler:
        """Create a console handler with colored output."""
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)
        
        # Define color codes for console output
        COLOR_CODES = {
            logging.INFO: "\033[94m",    # Blue
            logging.DEBUG: "\033[92m",   # Green
            logging.WARNING: "\033[93m", # Yellow
            logging.ERROR: "\033[91m",   # Red
            logging.CRITICAL: "\033[95m", # Magenta
            "RESET": "\033[0m"           # Reset
        }

        # Custom formatter with colors for console output
        class ColoredFormatter(logging.Formatter):
            def __init__(self, fmt, color_codes):
                super().__init__(fmt)
                self.color_codes = color_codes

            def format(self, record):
                color = self.color_codes.get(record.levelno, self.color_codes["RESET"])
                reset = self.color_codes["RESET"]
                record.msg = f"{color}{record.msg}{reset}"
                return super().format(record)

        # Apply the colored formatter to the console handler
        console_handler.setFormatter(ColoredFormatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s', COLOR_CODES))
        
        return console_handler
    
    def _setup_file_handler(self, formatter: logging.Formatter) -> None:
        """Set up file logging handler."""
        try:
            current_timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            
            # Get log directory from config if available, otherwise use default
            if hasattr(self, 'config_data') and self.config_data:
                # Use ConfigManager to get the log directory path
                from ..config.config_manager import ConfigManager
                config_manager = ConfigManager(logger=None, config_path=None)
                log_dir = os.path.abspath(config_manager.get_log_directory_path())
            else:
                # Fallback to default path
                log_dir = os.path.abspath("run_logs")
            
            # Create directory if it doesn't exist
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)

            log_file = f"{log_dir}/yaml_generation_{current_timestamp}.log"
            file_handler = logging.FileHandler(log_file, mode='w')
            file_handler.setLevel(self.log_level) # Use the level stored in self.log_level
            file_handler.setFormatter(formatter)
            
            # Add the file handler to the logger
            self.logger.addHandler(file_handler)
            
            self.log_file_path = log_file
            self.logger.info(f"Log file created at: {log_file}")
        except Exception as e:
            print(f"Failed to set up file logging: {e}")

    def debug(self, message: str) -> None:
        """Log a debug message."""
        self.logger.debug(message)
    
    def info(self, message: str) -> None:
        """Log an info message."""
        self.logger.info(message)
    
    def warning(self, message: str) -> None:
        """Log a warning message."""
        self.logger.warning(message)
    
    def error(self, message: str) -> None:
        """Log an error message."""
        self.logger.error(message)
    
    def critical(self, message: str) -> None:
        """Log a critical message."""
        self.logger.critical(message) 