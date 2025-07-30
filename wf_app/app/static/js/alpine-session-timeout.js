/**
 * Alpine.js component for Session Timeout Management
 * Automatically logs out users after 20 minutes of inactivity
 * Reverts to default workspace client authentication
 * Uses event-driven approach instead of continuous polling
 */
function sessionTimeoutManager() {
    return {
        // Configuration
        TIMEOUT_DURATION: 20 * 60 * 1000, // 20 minutes in milliseconds
        WARNING_TIME: 2 * 60 * 1000, // Show warning 2 minutes before timeout
        
        // State
        lastActivity: Date.now(),
        timeoutTimer: null,
        warningTimer: null,
        showWarning: false,
        timeLeft: 0,
        cachedAuthStatus: null,
        cacheExpiry: 0,
        CACHE_DURATION: 5 * 60 * 1000, // Cache auth status for 5 minutes
        isLoadingAuth: false, // Prevent multiple simultaneous auth checks
        authLoadDebounceTimer: null, // Debounce auth loading
        
        // Initialize the timeout manager
        async init() {
            console.log('Session Timeout Manager initialized (event-driven)');
            this.updateLastActivity();
            this.startActivityTracking();
            await this.loadLastActivity();
            
            // Pre-load auth status immediately for instant UI
            try {
                await this.checkAuthStatusIfNeeded();
                console.log('✅ Session manager auth status pre-loaded');
            } catch (error) {
                console.log('⚠️ Session manager auth preload failed (non-critical):', error);
            }
        },
        
        // Load last activity from localStorage (persists across page refreshes)
        async loadLastActivity() {
            const stored = localStorage.getItem('lastActivity');
            if (stored) {
                this.lastActivity = parseInt(stored);
                // Check for timeout on page load if user was idle
                await this.checkForTimeoutOnActivity();
            }
        },
        
        // Update last activity timestamp
        updateLastActivity() {
            this.lastActivity = Date.now();
            localStorage.setItem('lastActivity', this.lastActivity.toString());
            
            // Clear any existing warnings
            this.clearWarning();
            
            // Clear timeout timer since user is active
            if (this.timeoutTimer) {
                clearTimeout(this.timeoutTimer);
                this.timeoutTimer = null;
            }
        },
        
        // Start tracking user activity
        startActivityTracking() {
            const events = ['mousedown', 'mousemove', 'keypress', 'scroll', 'touchstart', 'click'];
            let lastActivityCheck = Date.now();
            
            events.forEach(event => {
                document.addEventListener(event, () => {
                    const now = Date.now();
                    
                    // Check for timeout only if user was idle for a while
                    if (now - lastActivityCheck > this.WARNING_TIME) {
                        this.checkForTimeoutOnActivity();
                    }
                    
                    this.updateLastActivity();
                    lastActivityCheck = now;
                }, { passive: true });
            });
            
            // Track page visibility changes
            document.addEventListener('visibilitychange', () => {
                if (!document.hidden) {
                    const now = Date.now();
                    
                    // Check for timeout when coming back to page
                    if (now - lastActivityCheck > this.WARNING_TIME) {
                        this.checkForTimeoutOnActivity();
                    }
                    
                    this.updateLastActivity();
                    lastActivityCheck = now;
                }
            });
        },
        
        // Smart authentication status checking with caching and loading prevention
        async checkAuthStatusIfNeeded(forceRefresh = false) {
            const now = Date.now();
            
            // Prevent multiple simultaneous auth checks
            if (this.isLoadingAuth && !forceRefresh) {
                console.debug('Auth check already in progress, returning cached status');
                return this.cachedAuthStatus || { connected: false };
            }
            
            // Return cached status if still valid and not forcing refresh
            if (!forceRefresh && this.cachedAuthStatus && now < this.cacheExpiry) {
                return this.cachedAuthStatus;
            }
            
            this.isLoadingAuth = true;
            
            try {
                const response = await fetch('/api/auth/status');
                if (response.ok) {
                    const data = await response.json();
                    
                    // Cache the result
                    this.cachedAuthStatus = data;
                    this.cacheExpiry = now + this.CACHE_DURATION;
                    
                    return data;
                }
            } catch (error) {
                console.debug('Could not check auth status:', error);
            } finally {
                this.isLoadingAuth = false;
            }
            
            // Return cached status even if expired, or default
            return this.cachedAuthStatus || { connected: false };
        },
        
        // Smart auth status loading with immediate cache return
        async loadAuthStatusDebounced(delay = 50) {
            const now = Date.now();
            
            // Return cached status immediately if available and valid
            if (this.cachedAuthStatus && now < this.cacheExpiry) {
                return this.cachedAuthStatus;
            }
            
            // If we have expired cache, return it first, then update in background
            if (this.cachedAuthStatus) {
                // Return cached immediately
                const cachedResult = this.cachedAuthStatus;
                
                // Update in background (debounced)
                if (this.authLoadDebounceTimer) {
                    clearTimeout(this.authLoadDebounceTimer);
                }
                
                this.authLoadDebounceTimer = setTimeout(async () => {
                    await this.checkAuthStatusIfNeeded(true);
                }, delay);
                
                return cachedResult;
            }
            
            // No cache available, load fresh (minimal debounce)
            if (this.authLoadDebounceTimer) {
                clearTimeout(this.authLoadDebounceTimer);
            }
            
            return new Promise((resolve) => {
                this.authLoadDebounceTimer = setTimeout(async () => {
                    const result = await this.checkAuthStatusIfNeeded(true);
                    resolve(result);
                }, delay);
            });
        },
        
        // Clear cached auth status
        clearAuthCache() {
            this.cachedAuthStatus = null;
            this.cacheExpiry = 0;
        },
        
        // Check if session should timeout (called on activity)
        async checkForTimeoutOnActivity() {
            const now = Date.now();
            const timeSinceActivity = now - this.lastActivity;
            
            // If user was inactive for a while, check if we need to validate session
            if (timeSinceActivity > this.WARNING_TIME) {
                const authStatus = await this.checkAuthStatusIfNeeded();
                
                // Only timeout configured sessions (PAT, Azure), not default auth
                const hasConfiguredSession = authStatus.connected && 
                                           authStatus.auth_type && 
                                           authStatus.auth_type !== 'default' && 
                                           authStatus.auth_type !== '';
                
                if (!hasConfiguredSession) {
                    this.clearWarning();
                    return;
                }
                
                // Check if we should show warning
                if (timeSinceActivity >= (this.TIMEOUT_DURATION - this.WARNING_TIME) && !this.showWarning) {
                    this.scheduleTimeoutWarning();
                }
                
                // Check if we should logout
                if (timeSinceActivity >= this.TIMEOUT_DURATION) {
                    this.autoLogout();
                }
            }
        },
        
        // Schedule timeout warning based on activity
        scheduleTimeoutWarning() {
            if (this.timeoutTimer) {
                clearTimeout(this.timeoutTimer);
            }
            
            const timeUntilWarning = (this.TIMEOUT_DURATION - this.WARNING_TIME) - (Date.now() - this.lastActivity);
            
            if (timeUntilWarning <= 0) {
                this.showTimeoutWarning();
            } else {
                this.timeoutTimer = setTimeout(() => {
                    this.showTimeoutWarning();
                }, timeUntilWarning);
            }
        },
        
        // Show timeout warning dialog
        showTimeoutWarning() {
            this.showWarning = true;
            this.timeLeft = Math.ceil((this.TIMEOUT_DURATION - (Date.now() - this.lastActivity)) / 1000);
            
            // Update countdown every second
            this.warningTimer = setInterval(() => {
                this.timeLeft = Math.ceil((this.TIMEOUT_DURATION - (Date.now() - this.lastActivity)) / 1000);
                
                if (this.timeLeft <= 0) {
                    this.autoLogout();
                }
            }, 1000);
            
            console.log('Session timeout warning shown');
        },
        
        // Clear timeout warning
        clearWarning() {
            this.showWarning = false;
            if (this.warningTimer) {
                clearInterval(this.warningTimer);
                this.warningTimer = null;
            }
        },
        
        // Extend session (user clicked "Stay Logged In")
        async extendSession() {
            try {
                // Call backend to extend session
                const response = await fetch('/api/auth/session/extend', {
                    method: 'POST'
                });
                
                if (response.ok) {
                    this.updateLastActivity();
                    this.clearWarning();
                    this.showNotification('Session extended successfully', 'success');
                } else {
                    const result = await response.json();
                    this.showNotification(result.error || 'Failed to extend session', 'error');
                }
            } catch (error) {
                console.error('Session extension error:', error);
                this.updateLastActivity();
                this.clearWarning();
                this.showNotification('Session extended locally', 'warning');
            }
        },
        
        // Perform automatic logout
        async autoLogout() {
            console.log('Auto-logout triggered due to inactivity');
            this.clearWarning();
            this.clearAuthCache(); // Clear cached auth status
            
            try {
                // Call logout API
                const response = await fetch('/api/auth/logout', {
                    method: 'POST'
                });
                
                if (response.ok) {
                    // Clear local storage
                    localStorage.removeItem('lastActivity');
                    
                    // Reset activity tracking
                    this.lastActivity = Date.now();
                    
                    // Show notification
                    this.showNotification('Session expired due to inactivity. Checking for default authentication...', 'warning');
                    
                    // Reload auth status to check for default authentication
                    setTimeout(async () => {
                        const authData = await this.checkAuthStatus();
                        
                        // Dispatch event to notify other components
                        document.dispatchEvent(new CustomEvent('auth-status-changed', {
                            detail: { 
                                status: authData || { connected: false },
                                reason: 'logout'
                            }
                        }));
                        
                        // Show appropriate message and handle redirect
                        if (authData && authData.connected) {
                            this.showNotification('Reverted to default authentication', 'info');
                        } else {
                            this.showNotification('No default authentication available', 'warning');
                            setTimeout(() => {
                                window.location.href = '/auth';
                            }, 2000);
                        }
                    }, 1000);
                    
                } else {
                    console.error('Auto-logout failed');
                    this.showNotification('Auto-logout failed', 'error');
                }
                
            } catch (error) {
                console.error('Auto-logout error:', error);
                this.showNotification('Auto-logout error: ' + error.message, 'error');
            }
        },
        
        // Check authentication status
        async checkAuthStatus() {
            try {
                const response = await fetch('/api/auth/status');
                if (response.ok) {
                    const data = await response.json();
                    return data;
                }
            } catch (error) {
                console.error('Error checking auth status:', error);
            }
            return { connected: false };
        },
        
        // Format time for display
        formatTime(seconds) {
            const minutes = Math.floor(seconds / 60);
            const secs = seconds % 60;
            return `${minutes}:${secs.toString().padStart(2, '0')}`;
        },
        
        // Show notification
        showNotification(message, type = 'info') {
            // Create notification element
            const notification = document.createElement('div');
            notification.className = `fixed top-4 right-4 z-50 max-w-sm p-4 rounded-lg shadow-lg transition-all duration-300`;
            
            // Set colors based on type
            const colors = {
                success: 'bg-green-500 text-white',
                error: 'bg-red-500 text-white',
                warning: 'bg-yellow-500 text-white',
                info: 'bg-blue-500 text-white'
            };
            
            notification.className += ` ${colors[type] || colors.info}`;
            notification.textContent = message;
            
            // Add to page
            document.body.appendChild(notification);
            
            // Auto remove after 5 seconds
            setTimeout(() => {
                notification.remove();
            }, 5000);
        },
        
        // Cleanup when component is destroyed
        destroy() {
            if (this.timeoutTimer) {
                clearTimeout(this.timeoutTimer);
            }
            if (this.warningTimer) {
                clearInterval(this.warningTimer);
            }
            if (this.authLoadDebounceTimer) {
                clearTimeout(this.authLoadDebounceTimer);
            }
            this.clearAuthCache();
        }
    }
}

// Global session timeout instance (initialized immediately)
window.sessionTimeout = sessionTimeoutManager();

// Initialize session timeout manager immediately for pre-loading
document.addEventListener('DOMContentLoaded', () => {
    if (window.sessionTimeout && typeof window.sessionTimeout.init === 'function') {
        window.sessionTimeout.init();
    }
});

// Global helper function for instant auth status checking
window.getAuthStatus = async function(forceRefresh = false, useDebounced = false) {
    if (!window.sessionTimeout) {
        // Fallback to direct API call if session timeout manager not available
        try {
            const response = await fetch('/api/auth/status');
            return response.ok ? await response.json() : { connected: false };
        } catch {
            return { connected: false };
        }
    }
    
    let authStatus;
    
    // For immediate UI updates, always try cache first
    const now = Date.now();
    if (!forceRefresh && window.sessionTimeout.cachedAuthStatus && now < window.sessionTimeout.cacheExpiry) {
        return window.sessionTimeout.cachedAuthStatus;
    }
    
    // If using debounced, use the smart loading function
    if (useDebounced) {
        authStatus = await window.sessionTimeout.loadAuthStatusDebounced();
    } else {
        authStatus = await window.sessionTimeout.checkAuthStatusIfNeeded(forceRefresh);
    }
    
    // Ensure we return a valid auth status object
    return authStatus || { connected: false, auth_type: '', user_name: '', workspace_url: '' };
};

// Global helper to validate auth before making authenticated API calls
window.validateAuthBeforeApiCall = async function() {
    const authStatus = await window.getAuthStatus();
    
    if (!authStatus.connected) {
        throw new Error('Authentication required. Please configure authentication first.');
    }
    
    return authStatus;
};

// Global function to force header refresh (for component sync)
window.forceHeaderRefresh = function() {
    // Find and refresh header component
    const headerElement = document.querySelector('[x-data*="authStatusDisplay"]');
    if (headerElement && headerElement._x_dataStack) {
        const headerComponent = headerElement._x_dataStack[0];
        if (headerComponent && typeof headerComponent.forceRefresh === 'function') {
            headerComponent.forceRefresh();
        }
    }
}; 