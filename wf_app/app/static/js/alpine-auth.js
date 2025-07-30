/**
 * Alpine.js component for Databricks Authentication Configuration
 */
function authManager() {
    return {
        // Authentication configuration state
        authConfig: {
            auth_type: '',
            // PAT configuration
            host: '',
            keyvault_name: '',
            secret_name: '',
            // Azure Service Principal configuration
            azure_workspace_resource_id: '',
            azure_tenant_id: '',
            azure_client_id: '',
            azure_client_secret: '',
            azure_tenant_id_key: '',
            azure_client_id_key: '',
            azure_client_secret_key: ''
        },
        
        // Authentication status
        authStatus: {
            connected: false,
            details: '',
            last_tested: null
        },
        
        // UI state
        showTokens: {
            azure_secret: false
        },
        
        isSaving: false,
        isTestingConnection: false,
        
        // Initialize component
        init() {
            console.log('Authentication Manager initialized');
            this.loadAuthStatusInstant(); // Use instant loading on page load
            this.loadSavedConfig();
            
            // Force header sync after auth component loads
            setTimeout(() => {
                if (window.forceHeaderRefresh) {
                    window.forceHeaderRefresh();
                }
            }, 200);
            
            // Listen for auth status changes (debounced to prevent cascade)
            document.addEventListener('auth-status-changed', (event) => {
                // Only clear cache on specific actions, not all events
                const reason = event.detail?.reason;
                if (reason === 'logout' || reason === 'login' || reason === 'config-change' || reason === 'config-clear') {
                    if (window.sessionTimeout) {
                        window.sessionTimeout.clearAuthCache();
                    }
                }
                
                // Use instant loading for immediate UI updates
                this.loadAuthStatusInstant();
            });
        },
        
        // Instant auth status loading (prioritizes cache)
        async loadAuthStatusInstant() {
            try {
                // Try to get cached status instantly first
                if (window.sessionTimeout && window.sessionTimeout.cachedAuthStatus && 
                    Date.now() < window.sessionTimeout.cacheExpiry) {
                    this.authStatus = window.sessionTimeout.cachedAuthStatus;
                    return;
                }
                
                // Use global helper for consistent loading
                const data = await window.getAuthStatus(false, true);
                if (data) {
                    this.authStatus = data;
                }
            } catch (error) {
                console.error('Failed to load auth status:', error);
                this.authStatus = { connected: false, details: '', last_tested: null };
            }
        },
        
        // Debounced auth status loading
        async loadAuthStatusDebounced() {
            if (window.sessionTimeout) {
                const data = await window.sessionTimeout.loadAuthStatusDebounced();
                if (data) {
                    this.authStatus = data;
                }
            } else {
                await this.loadAuthStatus();
            }
        },
        
        // Load current authentication status (use cached if available)
        async loadAuthStatus() {
            try {
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
                    this.authStatus = data;
                }
            } catch (error) {
                console.error('Failed to load auth status:', error);
            }
        },
        
        // Load saved configuration (without sensitive data)
        async loadSavedConfig() {
            try {
                const response = await fetch('/api/auth/config');
                if (response.ok) {
                    const data = await response.json();
                    // Only load non-sensitive configuration data
                    this.authConfig.auth_type = data.auth_type || '';
                    this.authConfig.host = data.host || '';
                    this.authConfig.keyvault_name = data.keyvault_name || '';
                    this.authConfig.secret_name = data.secret_name || '';
                    this.authConfig.azure_workspace_resource_id = data.azure_workspace_resource_id || '';
                    this.authConfig.azure_tenant_id = data.azure_tenant_id || '';
                    this.authConfig.azure_client_id = data.azure_client_id || '';
                    this.authConfig.azure_tenant_id_key = data.azure_tenant_id_key || '';
                    this.authConfig.azure_client_id_key = data.azure_client_id_key || '';
                    this.authConfig.azure_client_secret_key = data.azure_client_secret_key || '';
                }
            } catch (error) {
                console.error('Failed to load saved config:', error);
            }
        },
        
        // Get description for selected auth type
        getAuthTypeDescription() {
            const descriptions = {
                'pat': 'Recommended method using a Personal Access Token. Simple and secure for individual use.',
                'azure-client-secret': 'Enterprise authentication using Azure Service Principal. Best for automated systems and CI/CD.'
            };
            return descriptions[this.authConfig.auth_type] || '';
        },
        
        // Handle auth type change
        onAuthTypeChange() {
            // Clear all auth-specific fields when changing type
            this.authConfig.host = '';
            this.authConfig.keyvault_name = '';
            this.authConfig.secret_name = '';
            this.authConfig.azure_workspace_resource_id = '';
            this.authConfig.azure_tenant_id = '';
            this.authConfig.azure_client_id = '';
            this.authConfig.azure_client_secret = '';
            this.authConfig.azure_tenant_id_key = '';
            this.authConfig.azure_client_id_key = '';
            this.authConfig.azure_client_secret_key = '';
            
            console.log('Auth type changed to:', this.authConfig.auth_type);
        },
        
        // Toggle token visibility
        toggleTokenVisibility(tokenType) {
            this.showTokens[tokenType] = !this.showTokens[tokenType];
        },
        
        // Check if configuration can be saved
        canSaveConfig() {
            if (!this.authConfig.auth_type) return false;
            
            switch (this.authConfig.auth_type) {
                case 'pat':
                    return this.authConfig.host && 
                           this.authConfig.keyvault_name && 
                           this.authConfig.secret_name;
                    
                case 'azure-client-secret':
                    return this.authConfig.host && 
                           this.authConfig.keyvault_name && 
                           this.authConfig.azure_tenant_id_key && 
                           this.authConfig.azure_client_id_key && 
                           this.authConfig.azure_client_secret_key;
                           
                default:
                    return false;
            }
        },
        
        // Save configuration and test connection
        async saveConfiguration() {
            if (!this.canSaveConfig()) {
                this.showNotification('Please fill in all required fields', 'error');
                return;
            }
            
            this.isSaving = true;
            
            try {
                const response = await fetch('/api/auth/configure', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify(this.authConfig)
                });
                
                const result = await response.json();
                
                if (response.ok) {
                    this.showNotification(result.message || 'Configuration saved successfully', 'success');
                    this.authStatus = result.status;
                    
                    // Update cached auth status
                    if (window.sessionTimeout && result.status) {
                        window.sessionTimeout.cachedAuthStatus = result.status;
                        window.sessionTimeout.cacheExpiry = Date.now() + window.sessionTimeout.CACHE_DURATION;
                    }
                    
                    // Clear sensitive fields from UI for security
                    this.clearSensitiveFields();
                    
                    // Update local status immediately
                    this.authStatus = result.status;
                    
                    // Dispatch event to notify other components about auth status change
                    document.dispatchEvent(new CustomEvent('auth-status-changed', {
                        detail: { 
                            status: result.status,
                            reason: 'config-change'
                        }
                    }));
                    
                    // Force header refresh for immediate sync
                    if (window.forceHeaderRefresh) {
                        setTimeout(() => window.forceHeaderRefresh(), 100);
                    }
                    
                    // Reset session timeout activity since user just configured auth
                    if (window.sessionTimeout) {
                        window.sessionTimeout.updateLastActivity();
                    }
                    
                } else {
                    this.showNotification(result.error || 'Failed to save configuration', 'error');
                }
                
            } catch (error) {
                console.error('Save configuration error:', error);
                this.showNotification('Failed to save configuration: ' + error.message, 'error');
            } finally {
                this.isSaving = false;
            }
        },
        
        // Test existing connection
        async testConnection() {
            this.isTestingConnection = true;
            
            try {
                const response = await fetch('/api/auth/test', {
                    method: 'POST'
                });
                
                const result = await response.json();
                
                if (response.ok) {
                    this.showNotification('Connection test successful', 'success');
                    this.authStatus = result.status;
                } else {
                    this.showNotification(result.error || 'Connection test failed', 'error');
                    this.authStatus.connected = false;
                }
                
            } catch (error) {
                console.error('Test connection error:', error);
                this.showNotification('Connection test failed: ' + error.message, 'error');
                this.authStatus.connected = false;
            } finally {
                this.isTestingConnection = false;
            }
        },
        
        // Test connection by fetching jobs
        async testJobsConnection() {
            this.isTestingConnection = true;
            
            try {
                const response = await fetch('/api/auth/test-connection', {
                    method: 'POST'
                });
                
                const result = await response.json();
                
                if (response.ok) {
                    this.showNotification(result.message || 'Jobs access test successful', 'success');
                } else {
                    this.showNotification(result.error || 'Jobs access test failed', 'error');
                }
                
            } catch (error) {
                console.error('Test jobs connection error:', error);
                this.showNotification('Jobs access test failed: ' + error.message, 'error');
            } finally {
                this.isTestingConnection = false;
            }
        },
        
        // Clear configuration
        async clearConfiguration() {
            if (!confirm('Are you sure you want to clear the authentication configuration?')) {
                return;
            }
            
            try {
                const response = await fetch('/api/auth/clear', {
                    method: 'POST'
                });
                
                if (response.ok) {
                    // Reset all configuration
                    this.authConfig = {
                        auth_type: '',
                        host: '',
                        keyvault_name: '',
                        secret_name: '',
                        azure_workspace_resource_id: '',
                        azure_tenant_id: '',
                        azure_client_id: '',
                        azure_client_secret: '',
                        azure_tenant_id_key: '',
                        azure_client_id_key: '',
                        azure_client_secret_key: ''
                    };
                    
                    this.showNotification('Configuration cleared successfully. Checking for default authentication...', 'success');
                    
                    // Reload auth status to check for default authentication
                    await this.loadAuthStatus();
                    
                    // Dispatch event to notify other components about auth status change
                    document.dispatchEvent(new CustomEvent('auth-status-changed', {
                        detail: { 
                            status: this.authStatus,
                            reason: 'config-clear'
                        }
                    }));
                    
                    // Force header refresh for immediate sync
                    if (window.forceHeaderRefresh) {
                        setTimeout(() => window.forceHeaderRefresh(), 100);
                    }
                } else {
                    this.showNotification('Failed to clear configuration', 'error');
                }
                
            } catch (error) {
                console.error('Clear configuration error:', error);
                this.showNotification('Failed to clear configuration: ' + error.message, 'error');
            }
        },
        
        // Clear sensitive fields from UI for security
        clearSensitiveFields() {
            this.authConfig.azure_client_secret = '';
            
            // Reset token visibility
            this.showTokens = {
                azure_secret: false
            };
        },
        
        // Logout current user
        async logoutUser() {
            try {
                const response = await fetch('/api/auth/logout', {
                    method: 'POST'
                });
                
                if (response.ok) {
                    // Reset auth configuration
                    this.authConfig = {
                        auth_type: '',
                        host: '',
                        keyvault_name: '',
                        secret_name: '',
                        azure_workspace_resource_id: '',
                        azure_tenant_id: '',
                        azure_client_id: '',
                        azure_client_secret: '',
                        azure_tenant_id_key: '',
                        azure_client_id_key: '',
                        azure_client_secret_key: ''
                    };
                    
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
                        
                        // Force header refresh for immediate sync
                        if (window.forceHeaderRefresh) {
                            setTimeout(() => window.forceHeaderRefresh(), 100);
                        }
                        
                        // Show result message
                        if (this.authStatus.connected) {
                            this.showNotification('Reverted to default authentication', 'info');
                        } else {
                            this.showNotification('No default authentication available. Please configure authentication.', 'warning');
                        }
                    }, 500);
                    
                } else {
                    const result = await response.json();
                    this.showNotification(result.error || 'Logout failed', 'error');
                }
                
            } catch (error) {
                console.error('Logout error:', error);
                this.showNotification('Logout failed: ' + error.message, 'error');
            }
        },
        
        // Show notification
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
            
            // Auto remove after 5 seconds
            setTimeout(() => {
                notification.classList.add('translate-x-full');
                setTimeout(() => {
                    if (notification.parentNode) {
                        notification.parentNode.removeChild(notification);
                    }
                }, 300);
            }, 5000);
        }
    };
} 