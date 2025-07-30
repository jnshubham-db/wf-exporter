// Alpine.js Global Store and Components
document.addEventListener('alpine:init', () => {
    // Global application store
    Alpine.store('app', {
        // Application state
        isLoading: false,
        loadingMessage: '',
        
        // Mobile navigation state
        isMobileNavOpen: false,
        
        // Message system
        message: '',
        messageType: '', // 'success', 'error', 'warning', 'info'
        
        // Progress tracking
        progress: {
            setup: false,
            config: false, 
            jobs: false,
            export: false
        },
        
        // Global config management
        currentConfigPath: '',
        configLoaded: false,
        
        // Global job management
        globalJobs: [],
        jobsLoaded: false,
        jobsLoading: false,
        jobsLoadedAt: null,
        
        // Initialize store
        init() {
            // Pre-load auth status for instant component access
            this.preloadAuthStatus();
            
            // Load config path from session
            this.loadConfigPathFromSession();
            
            // Start loading jobs immediately for faster page loads
            console.log('🚀 Starting background job loading on app startup...');
            this.loadGlobalJobs();
            
            // Update progress based on current page
            this.updateProgressBasedOnPage();
            
            // Listen for escape key to close mobile nav
            document.addEventListener('keydown', (e) => {
                if (e.key === 'Escape' && this.isMobileNavOpen) {
                    this.closeMobileNav();
                }
            });
            
            // Close mobile nav on window resize to desktop size
            window.addEventListener('resize', () => {
                if (window.innerWidth >= 1024 && this.isMobileNavOpen) {
                    this.closeMobileNav();
                }
            });
        },
        
        // Pre-load auth status for instant component access
        async preloadAuthStatus() {
            console.log('🚀 Pre-loading auth status for instant UI...');
            try {
                if (window.sessionTimeout) {
                    await window.sessionTimeout.checkAuthStatusIfNeeded();
                    console.log('✅ Auth status pre-loaded successfully');
                } else {
                    // If session timeout manager not ready, try direct call
                    const response = await fetch('/api/auth/status');
                    if (response.ok) {
                        const data = await response.json();
                        console.log('✅ Auth status loaded directly');
                        // Store for session timeout manager when it becomes available
                        setTimeout(() => {
                            if (window.sessionTimeout) {
                                window.sessionTimeout.cachedAuthStatus = data;
                                window.sessionTimeout.cacheExpiry = Date.now() + window.sessionTimeout.CACHE_DURATION;
                            }
                        }, 100);
                    }
                }
            } catch (error) {
                console.log('⚠️ Auth status preload failed (non-critical):', error);
            }
        },
        
        // Mobile navigation methods
        toggleMobileNav() {
            this.isMobileNavOpen = !this.isMobileNavOpen;
        },
        
        openMobileNav() {
            this.isMobileNavOpen = true;
        },
        
        closeMobileNav() {
            this.isMobileNavOpen = false;
        },
        
        // Computed properties
        get progressPercentage() {
            const completed = Object.values(this.progress).filter(Boolean).length;
            return Math.round((completed / 4) * 100);
        },
        
                // Message system
        showMessage(text, type = 'info') {
            this.message = text;
            this.messageType = type;
            // Auto-dismiss after 5 seconds
            setTimeout(() => this.clearMessage(), 5000);
        },
        
        clearMessage() {
            this.message = '';
            this.messageType = '';
        },
        
        // Loading system
        setLoading(loading, message = 'Loading...') {
            this.isLoading = loading;
            this.loadingMessage = message;
        },
        
        showLoading(message = 'Loading...') {
            this.isLoading = true;
            this.loadingMessage = message;
        },
        
        hideLoading() {
            this.isLoading = false;
        },
        
        // Progress tracking
        updateProgress(step, completed) {
            this.progress[step] = completed;
        },
        
        updateProgressBasedOnPage() {
            const path = window.location.pathname;
            
            // Reset progress
            this.progress = { setup: false, config: false, jobs: false, export: false };
            
            if (path === '/' || path.includes('dashboard')) {
                this.progress.setup = true;
            } else if (path.includes('config')) {
                this.progress.setup = true;
                this.progress.config = true;
            } else if (path.includes('jobs')) {
                this.progress.setup = true;
                this.progress.config = true;
                this.progress.jobs = true;
            } else if (path.includes('export')) {
                this.progress.setup = true;
                this.progress.config = true;
                this.progress.jobs = true;
                this.progress.export = true;
            }
        },
        
        // Help system
        showHelp() {
            // Trigger help modal display
            this.$dispatch('show-help');
        },
        
        // Global config management methods
        setConfigPath(path) {
            this.currentConfigPath = path;
            this.configLoaded = !!path;
            // Store in session
            if (path) {
                sessionStorage.setItem('databricks_config_path', path);
            } else {
                sessionStorage.removeItem('databricks_config_path');
            }
            
            // Broadcast config change to all components
            this.$dispatch('config-path-changed', { path: path });
        },
        
        getConfigPath() {
            return this.currentConfigPath;
        },
        
        loadConfigPathFromSession() {
            const storedPath = sessionStorage.getItem('databricks_config_path');
            if (storedPath) {
                this.currentConfigPath = storedPath;
                this.configLoaded = true;
            }
        },
        
        clearConfigPath() {
            this.setConfigPath('');
        },
        
        get configStatus() {
            if (!this.configLoaded || !this.currentConfigPath) {
                return {
                    text: 'No configuration loaded',
                    class: 'text-gray-500',
                    icon: 'warning'
                };
            }
            return {
                text: this.currentConfigPath,
                class: 'text-green-600',
                icon: 'success'
            };
        },
        
        // Global job management methods
        async loadGlobalJobs() {
            if (this.jobsLoading || this.jobsLoaded) {
                return; // Already loading or loaded
            }
            
            console.log('Starting global job loading...');
            this.jobsLoading = true;
            
            try {
                const response = await fetch('/jobs/list');
                const data = await response.json();
                
                if (data.success) {
                    this.globalJobs = data.jobs || [];
                    this.jobsLoaded = true;
                    this.jobsLoadedAt = new Date();
                    console.log(`✅ Global jobs loaded: ${this.globalJobs.length} jobs available (${data.from_cache ? 'from cache' : 'from API'})`);
                    
                    // Broadcast that jobs are loaded
                    document.dispatchEvent(new CustomEvent('global-jobs-loaded', {
                        detail: { jobs: this.globalJobs }
                    }));
                } else {
                    console.error('Failed to load global jobs:', data.message);
                }
            } catch (error) {
                console.error('Error loading global jobs:', error);
            } finally {
                this.jobsLoading = false;
            }
        },
        
        async refreshGlobalJobs() {
            console.log('Refreshing global jobs...');
            this.jobsLoaded = false;
            this.globalJobs = [];
            await this.loadGlobalJobs();
        },
        
        getGlobalJobs() {
            return this.globalJobs;
        },
        
        get jobsStatus() {
            if (this.jobsLoading) {
                return 'Loading jobs...';
            } else if (this.jobsLoaded) {
                const timeSince = this.jobsLoadedAt ? 
                    Math.floor((new Date() - this.jobsLoadedAt) / 1000) : 0;
                return `${this.globalJobs.length} jobs (${timeSince}s ago)`;
            } else {
                return 'Jobs not loaded';
            }
        },
        
        get jobsAreFresh() {
            if (!this.jobsLoadedAt) return false;
            const fiveMinutes = 5 * 60 * 1000;
            return (new Date() - this.jobsLoadedAt) < fiveMinutes;
        }
    });
    
    // Mobile navigation component
    Alpine.data('mobileNav', () => ({
        get isOpen() {
            return this.$store.app.isMobileNavOpen;
        },
        
        toggle() {
            this.$store.app.toggleMobileNav();
        },
        
        close() {
            this.$store.app.closeMobileNav();
        },
        
        // Handle click outside to close
        clickOutside(event) {
            if (this.isOpen && !event.target.closest('.sidebar-mobile')) {
                this.close();
            }
        }
    }));
    
    // Global message component
    Alpine.data('messageComponent', () => ({
        get message() {
            return this.$store.app.message;
        },
        
        get messageType() {
            return this.$store.app.messageType;
        },
        
        get hasMessage() {
            return this.message && this.message.length > 0;
        },
        
        get messageClasses() {
            const baseClasses = 'alert fixed top-4 right-4 z-50 p-4 rounded-lg shadow-lg max-w-sm';
            const typeClasses = {
                success: 'bg-green-50 text-green-800 border border-green-200',
                error: 'bg-red-50 text-red-800 border border-red-200',
                warning: 'bg-yellow-50 text-yellow-800 border border-yellow-200',
                info: 'bg-blue-50 text-blue-800 border border-blue-200'
            };
            
            return `${baseClasses} ${typeClasses[this.messageType] || typeClasses.info}`;
        },
        
        dismiss() {
            this.$store.app.clearMessage();
        }
    }));
    
    // Progress bar component
    Alpine.data('progressBar', () => ({
        get percentage() {
            return this.$store.app.progressPercentage;
        },
        
        get width() {
            return `${this.percentage}%`;
        }
    }));
    
    // Loading button component
    Alpine.data('loadingButton', (defaultText = 'Submit') => ({
        get isLoading() {
            return this.$store.app.isLoading;
        },
        
        get buttonText() {
            return this.isLoading ? 'Loading...' : defaultText;
        },
        
        get buttonClasses() {
            return this.isLoading ? 'opacity-50 cursor-not-allowed' : '';
        }
    }));
});

// Alpine.js API Utility Functions and Error Handling
window.AlpineUtils = {
    // Core API request function with comprehensive error handling
    async apiRequest(url, options = {}) {
        const store = Alpine.store('app');
        const defaultOptions = {
            headers: {
                'Content-Type': 'application/json',
                'X-Requested-With': 'XMLHttpRequest',
            },
            credentials: 'same-origin',
            ...options
        };
        
        try {
            // Show loading indicator if requested
            if (options.showLoading !== false) {
                store.setLoading(true, options.loadingMessage || 'Processing...');
            }
            
            const response = await fetch(url, defaultOptions);
            
            // Handle different response types
            let data;
            const contentType = response.headers.get('content-type');
            
            if (contentType && contentType.includes('application/json')) {
                data = await response.json();
            } else {
                data = { success: response.ok, message: await response.text() };
            }
            
            // Handle HTTP errors
            if (!response.ok) {
                const errorMessage = data.message || data.error || `HTTP ${response.status}: ${response.statusText}`;
                throw new AlpineAPIError(errorMessage, response.status, data);
            }
            
            // Show success message if specified
            if (options.successMessage) {
                store.showMessage(options.successMessage, 'success');
            }
            
            return data;
            
        } catch (error) {
            // Handle different types of errors
            if (error instanceof AlpineAPIError) {
                // API error - already formatted
                if (options.showErrors !== false) {
                    store.showMessage(error.message, 'error');
                }
                throw error;
            } else if (error.name === 'NetworkError' || error.name === 'TypeError') {
                // Network errors
                const message = 'Network error. Please check your connection and try again.';
                if (options.showErrors !== false) {
                    store.showMessage(message, 'error');
                }
                throw new AlpineAPIError(message, 0, { type: 'network' });
            } else if (error.name === 'AbortError') {
                // Request was aborted
                const message = 'Request was cancelled.';
                if (options.showErrors !== false) {
                    store.showMessage(message, 'warning');
                }
                throw new AlpineAPIError(message, 0, { type: 'abort' });
            } else {
                // Unknown error
                const message = error.message || 'An unexpected error occurred.';
                if (options.showErrors !== false) {
                    store.showMessage(message, 'error');
                }
                throw new AlpineAPIError(message, 0, { type: 'unknown', originalError: error });
            }
        } finally {
            // Always hide loading indicator
            if (options.showLoading !== false) {
                store.setLoading(false);
            }
        }
    },
    
    // Specialized API methods
    async get(url, options = {}) {
        return this.apiRequest(url, { ...options, method: 'GET' });
    },
    
    async post(url, data, options = {}) {
        return this.apiRequest(url, {
            ...options,
            method: 'POST',
            body: data instanceof FormData ? data : JSON.stringify(data)
        });
    },
    
    async put(url, data, options = {}) {
        return this.apiRequest(url, {
            ...options,
            method: 'PUT',
            body: data instanceof FormData ? data : JSON.stringify(data)
        });
    },
    
    async delete(url, options = {}) {
        return this.apiRequest(url, { ...options, method: 'DELETE' });
    },
    
    // Form submission utilities
    async submitForm(formElement, options = {}) {
        const formData = new FormData(formElement);
        const url = formElement.action || window.location.pathname;
        const method = formElement.method?.toUpperCase() || 'POST';
        
        return this.apiRequest(url, {
            ...options,
            method,
            body: formData
        });
    },
    
    async submitJSON(url, data, options = {}) {
        return this.post(url, data, {
            loadingMessage: 'Submitting...',
            successMessage: 'Submitted successfully',
            ...options
        });
    },
    
    // File upload utilities
    async uploadFile(url, file, options = {}) {
        const formData = new FormData();
        formData.append('file', file);
        
        // Add additional fields if provided
        if (options.fields) {
            for (const [key, value] of Object.entries(options.fields)) {
                formData.append(key, value);
            }
        }
        
        return this.apiRequest(url, {
            ...options,
            method: 'POST',
            body: formData,
            headers: {
                // Don't set Content-Type for FormData - let browser set it with boundary
                'X-Requested-With': 'XMLHttpRequest',
                ...options.headers
            },
            loadingMessage: 'Uploading file...',
            successMessage: 'File uploaded successfully'
        });
    },
    
    // Validation utilities
    validateRequired(data, fields) {
        const missing = [];
        for (const field of fields) {
            if (!data[field] || (typeof data[field] === 'string' && !data[field].trim())) {
                missing.push(field);
            }
        }
        
        if (missing.length > 0) {
            throw new ValidationError(`Missing required fields: ${missing.join(', ')}`);
        }
    },
    
    validateEmail(email) {
        const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
        if (!emailRegex.test(email)) {
            throw new ValidationError('Invalid email address');
        }
    },
    
    // Retry mechanism for failed requests
    async withRetry(requestFn, options = {}) {
        const maxRetries = options.maxRetries || 3;
        const delay = options.delay || 1000;
        
        for (let attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                return await requestFn();
            } catch (error) {
                if (attempt === maxRetries || error.status < 500) {
                    // Don't retry on last attempt or client errors
                    throw error;
                }
                
                // Wait before retrying
                await new Promise(resolve => setTimeout(resolve, delay * attempt));
            }
        }
    },
    
    // Debounced API calls for search/autocomplete
    debounce(func, wait) {
        let timeout;
        return function executedFunction(...args) {
            const later = () => {
                clearTimeout(timeout);
                func(...args);
            };
            clearTimeout(timeout);
            timeout = setTimeout(later, wait);
        };
    },
    
    // Cache for API responses
    cache: new Map(),
    
    async cachedRequest(url, options = {}) {
        const cacheKey = `${options.method || 'GET'}:${url}`;
        const cacheTime = options.cacheTime || 300000; // 5 minutes default
        
        if (this.cache.has(cacheKey)) {
            const cached = this.cache.get(cacheKey);
            if (Date.now() - cached.timestamp < cacheTime) {
                return cached.data;
            }
        }
        
        const data = await this.apiRequest(url, options);
        this.cache.set(cacheKey, { data, timestamp: Date.now() });
        return data;
    },
    
    clearCache(pattern = null) {
        if (pattern) {
            for (const key of this.cache.keys()) {
                if (key.includes(pattern)) {
                    this.cache.delete(key);
                }
            }
        } else {
            this.cache.clear();
        }
    },
    
    // Performance monitoring utilities
    performance: {
        metrics: new Map(),
        
        startTimer(operation) {
            this.metrics.set(operation, { start: performance.now() });
        },
        
        endTimer(operation) {
            const metric = this.metrics.get(operation);
            if (metric) {
                metric.end = performance.now();
                metric.duration = metric.end - metric.start;
                
                // Log slow operations (> 100ms)
                if (metric.duration > 100) {
                    console.warn(`Slow operation detected: ${operation} took ${metric.duration}ms`);
                }
            }
        },
        
        getMetrics() {
            return Array.from(this.metrics.entries()).map(([operation, metric]) => ({
                operation,
                duration: metric.duration
            }));
        }
    },
    
    // Memory monitoring utilities
    memoryMonitor: {
        checkMemoryUsage() {
            if (performance.memory) {
                const memory = performance.memory;
                const usage = {
                    used: Math.round(memory.usedJSHeapSize / 1048576), // MB
                    total: Math.round(memory.totalJSHeapSize / 1048576), // MB
                    limit: Math.round(memory.jsHeapSizeLimit / 1048576) // MB
                };
                
                // Warn if memory usage is high
                if (usage.used / usage.limit > 0.8) {
                    console.warn('High memory usage detected:', usage);
                }
                
                return usage;
            }
            return null;
        }
    }
};

// Custom Error Classes
class AlpineAPIError extends Error {
    constructor(message, status = 0, data = {}) {
        super(message);
        this.name = 'AlpineAPIError';
        this.status = status;
        this.data = data;
    }
}

class ValidationError extends Error {
    constructor(message) {
        super(message);
        this.name = 'ValidationError';
    }
}

// Global error handler for unhandled promise rejections
window.addEventListener('unhandledrejection', (event) => {
    console.error('Unhandled promise rejection:', event.reason);
    
    if (Alpine.store('app')) {
        Alpine.store('app').showMessage(
            'An unexpected error occurred. Please try again.',
            'error'
        );
    }
});

// Legacy compatibility layer
window.Utils = {
    // Backward compatibility with existing code
    async fetchAPI(url, options = {}) {
        return AlpineUtils.apiRequest(url, options);
    }
};

// Make AlpineUtils globally available
window.AlpineUtils = AlpineUtils; 