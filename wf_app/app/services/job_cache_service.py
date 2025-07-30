"""Job cache service for managing cached Databricks jobs."""

import logging
import threading
import time
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from .databricks_service import DatabricksService


class JobCacheService:
    """Service class for caching Databricks jobs globally."""
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        """Singleton pattern to ensure single instance."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(JobCacheService, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Initialize job cache service."""
        if not hasattr(self, 'initialized'):
            self.logger = logging.getLogger(__name__)
            self._jobs_cache: List[Dict[str, Any]] = []
            self._pipelines_cache: List[Dict[str, Any]] = []
            self._cache_timestamp: Optional[datetime] = None
            self._cache_duration = timedelta(minutes=30)  # Cache for 30 minutes
            self._jobs_loading = False
            self._pipelines_loading = False
            self._load_lock = threading.Lock()
            self.initialized = True
    
    def is_cache_valid(self) -> bool:
        """Check if the current cache is still valid."""
        if not self._cache_timestamp or (not self._jobs_cache and not self._pipelines_cache):
            return False
        
        return datetime.now() - self._cache_timestamp < self._cache_duration
    
    def get_pipelines(self, force_refresh: bool = False) -> List[Dict[str, Any]]:
        """Get pipelines from cache or load them if cache is invalid."""
        if not force_refresh and self.is_cache_valid():
            self.logger.info(f"Returning {len(self._pipelines_cache)} pipelines from cache")
            return self._pipelines_cache.copy()
        
        return self._load_pipelines_sync()

    def _load_pipelines_sync(self) -> List[Dict[str, Any]]:
        """Load pipelines synchronously with thread safety."""
        with self._load_lock:
            # Double-check if cache is still invalid after acquiring lock
            if self.is_cache_valid() and self._pipelines_cache:
                return self._pipelines_cache.copy()
            
            if self._pipelines_loading:
                # Another thread is loading pipelines, wait for it
                while self._pipelines_loading:
                    time.sleep(0.1)
                return self._pipelines_cache.copy()
            
            self._pipelines_loading = True
            
            try:
                self.logger.info("Loading pipelines from Databricks API...")
                # Create a fresh DatabricksService instance to get current authentication
                databricks_service = DatabricksService()
                pipelines = databricks_service.get_lakeflow_pipelines()
        
                self._pipelines_cache = pipelines
                if not self._cache_timestamp:  # Only set timestamp if not already set by jobs
                    self._cache_timestamp = datetime.now()
                self._pipelines_loading = False
                
                self.logger.info(f"Successfully cached {len(pipelines)} pipelines")
                return self._pipelines_cache.copy()
                
            except Exception as e:
                self._pipelines_loading = False
                self.logger.error(f"Error loading pipelines for cache: {e}")
                # Return existing cache if available, even if stale
                if self._pipelines_cache:
                    self.logger.warning("Returning stale cache due to load error")
                    return self._pipelines_cache.copy()
                raise
    
    def get_jobs(self, force_refresh: bool = False) -> List[Dict[str, Any]]:
        """Get jobs from cache or load them if cache is invalid."""
        if not force_refresh and self.is_cache_valid():
            self.logger.info(f"Returning {len(self._jobs_cache)} jobs from cache")
            return self._jobs_cache.copy()
        
        return self._load_jobs_sync()
    
    def _load_jobs_sync(self) -> List[Dict[str, Any]]:
        """Load jobs synchronously with thread safety."""
        with self._load_lock:
            # Double-check if cache is still invalid after acquiring lock
            if self.is_cache_valid() and self._jobs_cache:
                return self._jobs_cache.copy()
            
            if self._jobs_loading:
                # Another thread is loading jobs, wait for it
                while self._jobs_loading:
                    time.sleep(0.1)
                return self._jobs_cache.copy()
            
            self._jobs_loading = True
            
            try:
                self.logger.info("Loading jobs from Databricks API...")
                # Create a fresh DatabricksService instance to get current authentication
                databricks_service = DatabricksService()
                jobs = databricks_service.get_jobs()
        
                self._jobs_cache = jobs
                self._cache_timestamp = datetime.now()
                self._jobs_loading = False
                
                self.logger.info(f"Successfully cached {len(jobs)} jobs")
                return self._jobs_cache.copy()
                
            except Exception as e:
                self._jobs_loading = False
                self.logger.error(f"Error loading jobs for cache: {e}")
                # Return existing cache if available, even if stale
                if self._jobs_cache:
                    self.logger.warning("Returning stale cache due to load error")
                    return self._jobs_cache.copy()
                raise
    
    def load_jobs_background(self):
        """Load jobs and pipelines in background thread."""
        def background_load():
            try:
                # Load jobs first
                if not self._jobs_loading and (not self._jobs_cache or not self.is_cache_valid()):
                    self._load_jobs_sync()
                
                # Load pipelines second 
                if not self._pipelines_loading and (not self._pipelines_cache or not self.is_cache_valid()):
                    self._load_pipelines_sync()
                    
            except Exception as e:
                self.logger.error(f"Background job/pipelines loading failed: {e}")
        
        if not self._jobs_loading and not self._pipelines_loading:
            thread = threading.Thread(target=background_load, daemon=True)
            thread.start()
            self.logger.info("Started background job/pipelines loading")
    
    def clear_cache(self):
        """Clear the job and pipeline caches."""
        with self._load_lock:
            self._jobs_cache = []
            self._pipelines_cache = []
            self._cache_timestamp = None
            self._jobs_loading = False
            self._pipelines_loading = False
            self.logger.info("Job and pipeline caches cleared")
    
    def get_cache_info(self) -> Dict[str, Any]:
        """Get information about the current cache state."""
        return {
            'job_count': len(self._jobs_cache),
            'pipeline_count': len(self._pipelines_cache),
            'cache_timestamp': self._cache_timestamp.isoformat() if self._cache_timestamp else None,
            'is_valid': self.is_cache_valid(),
            'is_loading': self._loading,
            'cache_duration_minutes': self._cache_duration.total_seconds() / 60
        } 