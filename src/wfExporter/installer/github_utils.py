"""
GitHub utilities for WF Exporter installer.

This module handles GitHub release detection, WHL file downloads,
and local file discovery.
"""

import os
import re
import requests
import logging
from pathlib import Path
from typing import Optional, Dict, Any, List
from urllib.parse import urlparse

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class GitHubReleaseManager:
    """Manages GitHub releases and WHL file downloads."""
    
    def __init__(self, repo_owner: str = "jnshubham-db", repo_name: str = "wf-exporter"):
        """
        Initialize the GitHub release manager.
        
        Args:
            repo_owner: GitHub repository owner
            repo_name: GitHub repository name
        """
        self.repo_owner = repo_owner
        self.repo_name = repo_name
        self.base_url = f"https://api.github.com/repos/{repo_owner}/{repo_name}"
    
    def get_latest_release(self) -> Optional[Dict[str, Any]]:
        """
        Get information about the latest release.
        
        Returns:
            Dictionary containing release information or None if failed
        """
        try:
            url = f"{self.base_url}/releases/latest"
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Failed to get latest release: {e}")
            return None
    
    def get_latest_version(self) -> Optional[str]:
        """
        Get the latest version string.
        
        Returns:
            Version string (e.g., "0.3.1") or None if failed
        """
        release = self.get_latest_release()
        if release and 'tag_name' in release:
            # Remove 'v' prefix if present
            version = release['tag_name'].lstrip('v')
            return version
        return None
    
    def get_whl_download_url(self, version: Optional[str] = None) -> Optional[str]:
        """
        Get the download URL for the WHL file.
        
        Args:
            version: Specific version to download (if None, uses latest)
            
        Returns:
            Download URL or None if not found
        """
        if not version:
            version = self.get_latest_version()
        
        if not version:
            return None
        
        # Construct expected WHL filename
        whl_filename = f"wfexporter-{version}-py3-none-any.whl"
        download_url = f"https://github.com/{self.repo_owner}/{self.repo_name}/releases/download/v{version}/{whl_filename}"
        
        # Verify the URL exists
        try:
            response = requests.head(download_url, timeout=10)
            if response.status_code == 200:
                return download_url
        except Exception:
            pass
        
        # If direct URL doesn't work, try to find it in release assets
        release = self.get_latest_release()
        if release and 'assets' in release:
            for asset in release['assets']:
                if asset['name'].endswith('.whl') and 'wfexporter' in asset['name']:
                    return asset['browser_download_url']
        
        return None
    
    def download_whl_file(self, download_path: Path, version: Optional[str] = None) -> Optional[Path]:
        """
        Download the WHL file to a specified path.
        
        Args:
            download_path: Directory to download the file to
            version: Specific version to download (if None, uses latest)
            
        Returns:
            Path to downloaded file or None if failed
        """
        download_url = self.get_whl_download_url(version)
        if not download_url:
            return None
        
        # Extract filename from URL
        filename = Path(urlparse(download_url).path).name
        file_path = download_path / filename
        
        try:
            # Download the file
            response = requests.get(download_url, stream=True, timeout=30)
            response.raise_for_status()
            
            # Create directory if it doesn't exist
            download_path.mkdir(parents=True, exist_ok=True)
            
            # Write file
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            return file_path
            
        except Exception as e:
            print(f"Failed to download WHL file: {e}")
            if file_path.exists():
                file_path.unlink()  # Remove partial file
            return None


class WhlFileManager:
    """Manages WHL file discovery and handling."""
    
    def __init__(self):
        """Initialize the WHL file manager."""
        self.github_manager = GitHubReleaseManager()
    
    def find_local_whl_file(self, search_path: Path = None) -> Optional[Path]:
        """
        Find a local WHL file in the specified path.
        
        Args:
            search_path: Directory to search (defaults to current working directory)
            
        Returns:
            Path to WHL file or None if not found
        """
        if search_path is None:
            search_path = Path.cwd()
        
        # Look for wfexporter WHL files
        whl_pattern = re.compile(r'wfexporter-.*\.whl$', re.IGNORECASE)
        
        for file_path in search_path.iterdir():
            if file_path.is_file() and whl_pattern.match(file_path.name):
                return file_path
        
        return None
    
    def get_whl_file(self, prefer_local: bool = True, download_path: Path = None) -> Optional[Path]:
        """
        Get a WHL file, either from local directory or by downloading.
        
        Args:
            prefer_local: Whether to prefer local files over downloading
            download_path: Path to download to if local file not found
            
        Returns:
            Path to WHL file or None if not available
        """
        if download_path is None:
            download_path = Path.cwd()
        
        logger.info(f"Searching for WHL file (prefer_local: {prefer_local})")
        
        # First, try to find local file if preferred
        if prefer_local:
            logger.info(f"Looking for local WHL file in: {download_path}")
            local_whl = self.find_local_whl_file(download_path)
            if local_whl:
                logger.info(f"Found local WHL file: {local_whl}")
                return local_whl
            else:
                logger.info("No local WHL file found")
        
        # If no local file found or not preferred, try to download
        logger.info("Attempting to download WHL file from GitHub...")
        downloaded_whl = self.github_manager.download_whl_file(download_path)
        if downloaded_whl:
            logger.info(f"Downloaded WHL file: {downloaded_whl}")
            return downloaded_whl
        else:
            logger.warning("Failed to download WHL file from GitHub")
        
        # If download failed and we haven't checked local files yet, try now
        if not prefer_local:
            logger.info("Fallback: Looking for local WHL file...")
            local_whl = self.find_local_whl_file(download_path)
            if local_whl:
                logger.info(f"Found local WHL file as fallback: {local_whl}")
                return local_whl
        
        logger.error("No WHL file found (local or downloadable)")
        return None
    
    def get_whl_version(self, whl_path: Path) -> Optional[str]:
        """
        Extract version from WHL filename.
        
        Args:
            whl_path: Path to WHL file
            
        Returns:
            Version string or None if not found
        """
        filename = whl_path.name
        
        # Match pattern: wfexporter-{version}-py3-none-any.whl
        pattern = r'wfexporter-([0-9]+\.[0-9]+\.[0-9]+[^-]*)-py3-none-any\.whl'
        match = re.match(pattern, filename, re.IGNORECASE)
        
        if match:
            return match.group(1)
        
        return None
    
    def validate_whl_file(self, whl_path: Path) -> bool:
        """
        Validate that a WHL file is valid.
        
        Args:
            whl_path: Path to WHL file to validate
            
        Returns:
            True if file is valid
        """
        try:
            # Check if file exists and has reasonable size
            if not whl_path.exists():
                return False
            
            # Check file size (should be at least a few KB)
            if whl_path.stat().st_size < 1024:
                return False
            
            # Check filename pattern
            if not self.get_whl_version(whl_path):
                return False
            
            # TODO: Could add more validation like checking ZIP structure
            
            return True
            
        except Exception:
            return False


def get_whl_file_for_installation() -> Optional[Path]:
    """
    Convenience function to get a WHL file for installation.
    
    This function implements the priority system:
    1. Check current working directory for local WHL file
    2. Download from GitHub if not found locally
    
    Returns:
        Path to WHL file or None if not available
    """
    manager = WhlFileManager()
    return manager.get_whl_file(prefer_local=True) 