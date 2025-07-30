"""Export state persistence service for managing export session data."""

import json
import logging
import os
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, Any, Optional


class ExportStateService:
    """Service class for persisting export session state across restarts."""
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        """Singleton pattern to ensure single instance."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(ExportStateService, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Initialize export state service."""
        if not hasattr(self, 'initialized'):
            self.logger = logging.getLogger(__name__)
            self.state_file_path = os.path.join(os.getcwd(), 'export_state.json')
            self.state_lock = threading.Lock()
            self.cleanup_interval = 3600  # Clean up every hour
            self.max_state_age = timedelta(hours=24)  # Keep states for 24 hours
            self.initialized = True
            
            # Start cleanup thread
            self._start_cleanup_thread()
    
    def _start_cleanup_thread(self):
        """Start background thread for cleaning up expired states."""
        def cleanup_worker():
            while True:
                try:
                    self.cleanup_expired_states()
                    time.sleep(self.cleanup_interval)
                except Exception as e:
                    self.logger.error(f"Error in export state cleanup: {e}")
                    time.sleep(self.cleanup_interval)
        
        cleanup_thread = threading.Thread(target=cleanup_worker, daemon=True)
        cleanup_thread.start()
        self.logger.info("Started export state cleanup thread")
    
    def _load_state_file(self) -> Dict[str, Any]:
        """Load export states from file."""
        try:
            if os.path.exists(self.state_file_path):
                with open(self.state_file_path, 'r') as f:
                    data = json.load(f)
                    return data if isinstance(data, dict) else {}
            return {}
        except Exception as e:
            self.logger.error(f"Error loading export state file: {e}")
            return {}
    
    def _save_state_file(self, states: Dict[str, Any]):
        """Save export states to file."""
        try:
            with open(self.state_file_path, 'w') as f:
                json.dump(states, f, indent=2, default=str)
        except Exception as e:
            self.logger.error(f"Error saving export state file: {e}")
    
    def save_export_state(self, run_id: str, state_data: Dict[str, Any]):
        """Save export state for a specific run."""
        with self.state_lock:
            try:
                states = self._load_state_file()
                
                # Add timestamp and ensure serializable
                state_data = dict(state_data)  # Make a copy
                state_data['saved_at'] = datetime.now().isoformat()
                state_data['run_id'] = run_id
                
                # Ensure all values are JSON serializable
                for key, value in state_data.items():
                    if isinstance(value, (datetime,)):
                        state_data[key] = value.isoformat()
                    elif not isinstance(value, (str, int, float, bool, list, dict, type(None))):
                        state_data[key] = str(value)
                
                states[run_id] = state_data
                self._save_state_file(states)
                
                self.logger.info(f"Export state saved for run_id: {run_id}")
                
            except Exception as e:
                self.logger.error(f"Error saving export state for {run_id}: {e}")
    
    def get_export_state(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Get export state for a specific run."""
        with self.state_lock:
            try:
                states = self._load_state_file()
                state = states.get(run_id)
                
                if state:
                    self.logger.info(f"Export state retrieved for run_id: {run_id}")
                    return state
                else:
                    self.logger.warning(f"No export state found for run_id: {run_id}")
                    return None
                    
            except Exception as e:
                self.logger.error(f"Error getting export state for {run_id}: {e}")
                return None
    
    def update_export_status(self, run_id: str, status: str, additional_data: Dict[str, Any] = None):
        """Update the status of an export run."""
        with self.state_lock:
            try:
                states = self._load_state_file()
                
                if run_id in states:
                    states[run_id]['status'] = status
                    states[run_id]['updated_at'] = datetime.now().isoformat()
                    
                    if additional_data:
                        states[run_id].update(additional_data)
                    
                    self._save_state_file(states)
                    self.logger.info(f"Export status updated to '{status}' for run_id: {run_id}")
                else:
                    self.logger.warning(f"Cannot update status for non-existent run_id: {run_id}")
                    
            except Exception as e:
                self.logger.error(f"Error updating export status for {run_id}: {e}")
    
    def get_active_exports(self) -> Dict[str, Dict[str, Any]]:
        """Get all currently active export runs."""
        with self.state_lock:
            try:
                states = self._load_state_file()
                active_states = {}
                
                for run_id, state in states.items():
                    if state.get('status') in ['running', 'pending']:
                        active_states[run_id] = state
                
                self.logger.info(f"Found {len(active_states)} active export runs")
                return active_states
                
            except Exception as e:
                self.logger.error(f"Error getting active exports: {e}")
                return {}
    
    def cleanup_expired_states(self):
        """Clean up expired export states."""
        with self.state_lock:
            try:
                states = self._load_state_file()
                current_time = datetime.now()
                expired_count = 0
                
                # Find expired states
                expired_runs = []
                for run_id, state in states.items():
                    saved_at_str = state.get('saved_at')
                    if saved_at_str:
                        try:
                            saved_at = datetime.fromisoformat(saved_at_str)
                            if current_time - saved_at > self.max_state_age:
                                expired_runs.append(run_id)
                                expired_count += 1
                        except ValueError:
                            # Invalid date format, consider expired
                            expired_runs.append(run_id)
                            expired_count += 1
                
                # Remove expired states
                for run_id in expired_runs:
                    del states[run_id]
                
                if expired_count > 0:
                    self._save_state_file(states)
                    self.logger.info(f"Cleaned up {expired_count} expired export states")
                
            except Exception as e:
                self.logger.error(f"Error during export state cleanup: {e}")
    
    def delete_export_state(self, run_id: str):
        """Delete export state for a specific run."""
        with self.state_lock:
            try:
                states = self._load_state_file()
                
                if run_id in states:
                    del states[run_id]
                    self._save_state_file(states)
                    self.logger.info(f"Export state deleted for run_id: {run_id}")
                else:
                    self.logger.warning(f"Cannot delete non-existent export state for run_id: {run_id}")
                    
            except Exception as e:
                self.logger.error(f"Error deleting export state for {run_id}: {e}")
    
    def clear_all_states(self):
        """Clear all export states (use with caution)."""
        with self.state_lock:
            try:
                self._save_state_file({})
                self.logger.info("All export states cleared")
            except Exception as e:
                self.logger.error(f"Error clearing all export states: {e}")
    
    def get_state_summary(self) -> Dict[str, Any]:
        """Get summary of export state service."""
        with self.state_lock:
            try:
                states = self._load_state_file()
                active_count = sum(1 for state in states.values() if state.get('status') in ['running', 'pending'])
                
                return {
                    'total_states': len(states),
                    'active_exports': active_count,
                    'completed_exports': len(states) - active_count,
                    'state_file_path': self.state_file_path,
                    'file_exists': os.path.exists(self.state_file_path)
                }
            except Exception as e:
                self.logger.error(f"Error getting state summary: {e}")
                return {
                    'error': str(e)
                } 