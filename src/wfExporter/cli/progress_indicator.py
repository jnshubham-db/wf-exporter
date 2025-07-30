"""
Progress indicator utilities for CLI operations.

This module provides utilities for showing progress indicators with spinners
that update on a single line during CLI operations.
"""

import sys
import time
import threading
from contextlib import contextmanager
from typing import Optional


class ProgressIndicator:
    """A progress indicator that shows a spinner and status message on a single line."""
    
    def __init__(self, message: str = "Processing"):
        """
        Initialize the progress indicator.
        
        Args:
            message: Initial message to display
        """
        self.message = message
        self.is_running = False
        self.thread: Optional[threading.Thread] = None
        self.spinner_chars = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏']
        self.current_char_index = 0
        
    def start(self) -> None:
        """Start the progress indicator."""
        if self.is_running:
            return
            
        self.is_running = True
        self.thread = threading.Thread(target=self._animate, daemon=True)
        self.thread.start()
        
    def stop(self, final_message: Optional[str] = None) -> None:
        """
        Stop the progress indicator.
        
        Args:
            final_message: Optional final message to display
        """
        if not self.is_running:
            return
            
        self.is_running = False
        if self.thread:
            self.thread.join()
            
        # Clear the line and show final message
        sys.stdout.write('\r' + ' ' * (len(self.message) + 10) + '\r')
        if final_message:
            sys.stdout.write(final_message + '\n')
        sys.stdout.flush()
        
    def update_message(self, message: str) -> None:
        """
        Update the status message.
        
        Args:
            message: New message to display
        """
        self.message = message
        
    def _animate(self) -> None:
        """Animate the spinner."""
        while self.is_running:
            spinner_char = self.spinner_chars[self.current_char_index]
            self.current_char_index = (self.current_char_index + 1) % len(self.spinner_chars)
            
            # Write the current state
            sys.stdout.write(f'\r{spinner_char} {self.message}')
            sys.stdout.flush()
            
            time.sleep(0.1)


@contextmanager
def progress_indicator(message: str):
    """
    Context manager for showing a progress indicator.
    
    Args:
        message: Message to display during progress
        
    Usage:
        with progress_indicator("Installing workflow..."):
            # Do some work
            pass
    """
    indicator = ProgressIndicator(message)
    try:
        indicator.start()
        yield indicator
    finally:
        indicator.stop()


class InstallationProgress:
    """Handles progress indication for the installation process."""
    
    def __init__(self):
        """Initialize the installation progress tracker."""
        self.indicator: Optional[ProgressIndicator] = None
        
    def start_step(self, message: str) -> None:
        """
        Start a new installation step.
        
        Args:
            message: Description of the current step
        """
        if self.indicator:
            self.indicator.update_message(message)
        else:
            self.indicator = ProgressIndicator(message)
            self.indicator.start()
            
    def complete_step(self, success_message: str) -> None:
        """
        Complete the current step.
        
        Args:
            success_message: Message to show upon completion
        """
        if self.indicator:
            self.indicator.stop(f"✅ {success_message}")
            self.indicator = None
            
    def fail_step(self, error_message: str) -> None:
        """
        Mark the current step as failed.
        
        Args:
            error_message: Error message to display
        """
        if self.indicator:
            self.indicator.stop(f"❌ {error_message}")
            self.indicator = None
            
    def update_step(self, message: str) -> None:
        """
        Update the current step message.
        
        Args:
            message: New message for the current step
        """
        if self.indicator:
            self.indicator.update_message(message)
            
    def finish(self) -> None:
        """Finish the installation progress."""
        if self.indicator:
            self.indicator.stop()
            self.indicator = None 