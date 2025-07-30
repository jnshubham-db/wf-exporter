/**
 * Alpine.js component for Header Authentication Status Display
 */
function authStatusDisplay() {
    return {
        // Authentication status data
        authStatus: {
            connected: false,
            user_name: '',
            workspace_url: '',
            auth_type: '',
            last_tested: null
        },
        
        // Loading state
        isLoading: false,
        
        // Initialize component with event listeners
        init() {
            this.loadAuthStatusInstant(); // Use instant loading on init
            
            // Listen for auth status changes (debounced to prevent cascade)
            document.addEventListener('auth-status-changed', (event) => {
                // Only clear cache on specific actions, not all events
                const reason = event.detail?.reason;
                if (reason === 'logout' || reason === 'login' || reason === 'config-change' || reason === 'config-clear') {
                    if (window.sessionTimeout) {
                        window.sessionTimeout.clearAuthCache();
                    }
                }
                
                // Update immediately if status provided in event
                if (event.detail?.status) {
                    this.updateAuthStatus(event.detail.status);
                }
                
                // Also trigger a fresh load for consistency
                this.loadAuthStatusInstant();
            });
        },
        
        // Instant auth status loading (prioritizes cache)
        async loadAuthStatusInstant() {
            try {
                this.isLoading = true;
                
                // Try to get cached status instantly first
                if (window.sessionTimeout && window.sessionTimeout.cachedAuthStatus && 
                    Date.now() < window.sessionTimeout.cacheExpiry) {
                    const data = window.sessionTimeout.cachedAuthStatus;
                    this.updateAuthStatus(data);
                    this.isLoading = false;
                    return;
                }
                
                // Use global helper for consistent loading
                const data = await window.getAuthStatus(false, true);
                this.updateAuthStatus(data);
            } catch (error) {
                console.error('Failed to load auth status:', error);
                this.updateAuthStatus(null);
            } finally {
                this.isLoading = false;
            }
        },
        
        // Debounced auth status loading
        async loadAuthStatusDebounced() {
            if (window.sessionTimeout) {
                const data = await window.sessionTimeout.loadAuthStatusDebounced();
                this.updateAuthStatus(data);
            } else {
                await this.loadAuthStatus();
            }
        },
        
        // Update auth status data
        updateAuthStatus(data) {
            if (data) {
                this.authStatus = {
                    connected: data.connected || false,
                    user_name: data.user_name || '',
                    workspace_url: this.formatWorkspaceUrl(data.workspace_url),
                    auth_type: data.auth_type || '',
                    last_tested: data.last_tested
                };
            } else {
                this.authStatus = {
                    connected: false,
                    user_name: '',
                    workspace_url: '',
                    auth_type: '',
                    last_tested: null
                };
            }
        },
        
        // Load authentication status (use cached if available)
        async loadAuthStatus() {
            try {
                this.isLoading = true;
                
                // Try to get cached status from session timeout manager first
                let data;
                if (window.sessionTimeout && window.sessionTimeout.cachedAuthStatus && 
                    Date.now() < window.sessionTimeout.cacheExpiry) {
                    data = window.sessionTimeout.cachedAuthStatus;
                } else {
                    // Make API call and cache result
                    const response = await fetch('/api/auth/status');
                    if (response.ok) {
                        data = await response.json();
                        
                        // Cache in session timeout manager
                        if (window.sessionTimeout) {
                            window.sessionTimeout.cachedAuthStatus = data;
                            window.sessionTimeout.cacheExpiry = Date.now() + window.sessionTimeout.CACHE_DURATION;
                        }
                    }
                }
                
                if (data) {
                    this.authStatus = {
                        connected: data.connected || false,
                        user_name: data.user_name || '',
                        workspace_url: this.formatWorkspaceUrl(data.workspace_url),
                        auth_type: data.auth_type || '',
                        last_tested: data.last_tested
                    };
                } else {
                    // Default status if no data available
                    this.authStatus = {
                        connected: false,
                        user_name: '',
                        workspace_url: '',
                        auth_type: '',
                        last_tested: null
                    };
                }
            } catch (error) {
                console.error('Failed to load auth status:', error);
                // Default to not configured on error
                this.authStatus = {
                    connected: false,
                    user_name: '',
                    workspace_url: '',
                    auth_type: '',
                    last_tested: null
                };
            } finally {
                this.isLoading = false;
            }
        },
        
        // Format workspace URL for display
        formatWorkspaceUrl(url) {
            if (!url) return '';
            
            try {
                const parsedUrl = new URL(url);
                // Remove protocol and trailing slash for cleaner display
                return parsedUrl.host;
            } catch (error) {
                // If URL parsing fails, return as-is but limit length
                return url.length > 30 ? url.substring(0, 30) + '...' : url;
            }
        },
        
        // Refresh auth status (can be called from other components)
        async refreshAuthStatus() {
            await this.loadAuthStatus();
        },
        
        // Force immediate refresh (global accessible method)
        async forceRefresh() {
            await this.loadAuthStatusInstant();
        },
        
        // Logout functionality
        async logout() {
            try {
                const response = await fetch('/api/auth/logout', {
                    method: 'POST'
                });
                
                if (response.ok) {
                    // First clear the current status
                    this.authStatus = {
                        connected: false,
                        user_name: '',
                        workspace_url: '',
                        auth_type: '',
                        last_tested: null
                    };
                    
                    // Show initial logout success message
                    this.showNotification('Logged out successfully. Checking for default authentication...', 'success');
                    
                    // Small delay to allow backend session to clear
                    setTimeout(async () => {
                        // Reload auth status to check for default authentication
                        await this.loadAuthStatus();
                        
                        // Dispatch event to notify other components
                        document.dispatchEvent(new CustomEvent('auth-status-changed', {
                            detail: { 
                                status: this.authStatus,
                                reason: 'logout'
                            }
                        }));
                        
                        // Show appropriate message based on result
                        if (this.authStatus.connected) {
                            this.showNotification('Reverted to default authentication', 'info');
                        } else {
                            this.showNotification('No default authentication available', 'warning');
                            // Only redirect to auth page if no default authentication is available
                            setTimeout(() => {
                                window.location.href = '/auth';
                            }, 1500);
                        }
                    }, 800);
                    
                } else {
                    const result = await response.json();
                    this.showNotification(result.error || 'Logout failed', 'error');
                }
                
            } catch (error) {
                console.error('Logout error:', error);
                this.showNotification('Logout failed: ' + error.message, 'error');
            }
        },
        
        // Show notification helper
        showNotification(message, type = 'info') {
            // Create notification element
            const notification = document.createElement('div');
            notification.className = `fixed top-4 right-4 z-50 max-w-sm p-4 rounded-lg shadow-lg transition-all duration-300 transform translate-x-full`;
            
            // Set notification style based on type
            const styles = {
                success: 'bg-green-500 text-white',
                error: 'bg-red-500 text-white',
                warning: 'bg-yellow-500 text-black',
                info: 'bg-blue-500 text-white'
            };
            
            notification.className += ` ${styles[type] || styles.info}`;
            notification.innerHTML = `
                <div class="flex items-center justify-between">
                    <span class="text-sm font-medium">${message}</span>
                    <button class="ml-3 text-lg" onclick="this.parentNode.parentNode.remove()">&times;</button>
                </div>
            `;
            
            document.body.appendChild(notification);
            
            // Animate in
            setTimeout(() => {
                notification.classList.remove('translate-x-full');
            }, 100);
            
            // Auto remove after 3 seconds
            setTimeout(() => {
                notification.classList.add('translate-x-full');
                setTimeout(() => {
                    if (notification.parentNode) {
                        notification.parentNode.removeChild(notification);
                    }
                }, 300);
            }, 3000);
        }
    };
}

/**
 * Alpine.js component for Header functionality
 */
function headerComponent() {
    return {
        showActionsMenu: false,
        
        // Initialize header component
        init() {
            // Set up event listeners for auth status changes
            document.addEventListener('auth-status-changed', () => {
                // Find and refresh auth status display
                const authDisplay = document.querySelector('[x-data*="authStatusDisplay"]');
                if (authDisplay && authDisplay._x_dataStack) {
                    const authComponent = authDisplay._x_dataStack[0];
                    if (authComponent && authComponent.refreshAuthStatus) {
                        authComponent.refreshAuthStatus();
                    }
                }
            });
            
            console.log('Header component initialized');
        },
        
        // Show help
        showHelp() {
            this.showNotification('Help panel opened', 'info');
            // Add help logic here
        },
        
        // Show notification helper
        showNotification(message, type = 'info') {
            // Create notification element
            const notification = document.createElement('div');
            notification.className = `fixed top-4 right-4 z-50 max-w-sm p-4 rounded-lg shadow-lg transition-all duration-300 transform translate-x-full`;
            
            // Set notification style based on type
            const styles = {
                success: 'bg-green-500 text-white',
                error: 'bg-red-500 text-white',
                warning: 'bg-yellow-500 text-black',
                info: 'bg-blue-500 text-white'
            };
            
            notification.className += ` ${styles[type] || styles.info}`;
            notification.innerHTML = `
                <div class="flex items-center justify-between">
                    <span class="text-sm font-medium">${message}</span>
                    <button class="ml-3 text-lg" onclick="this.parentNode.parentNode.remove()">&times;</button>
                </div>
            `;
            
            document.body.appendChild(notification);
            
            // Animate in
            setTimeout(() => {
                notification.classList.remove('translate-x-full');
            }, 100);
            
            // Auto remove after 3 seconds
            setTimeout(() => {
                notification.classList.add('translate-x-full');
                setTimeout(() => {
                    if (notification.parentNode) {
                        notification.parentNode.removeChild(notification);
                    }
                }, 300);
            }, 3000);
        }
    };
} 