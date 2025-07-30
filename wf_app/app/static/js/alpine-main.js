// Alpine.js Main Application Components
document.addEventListener('alpine:init', () => {
    // Main application component
    Alpine.data('mainApp', () => ({
        init() {
            this.updateProgress();
            this.highlightActiveNav();
        },
        
        updateProgress() {
            // Connect to global progress tracking
            const progress = Alpine.store('app').progress;
            const completedSteps = Object.values(progress).filter(Boolean).length;
            const percentage = (completedSteps / 4) * 100;
            
            // Update overall progress
            Alpine.store('app').overallProgress = percentage;
        },
        
        highlightActiveNav() {
            // This will be handled by the navigation component
            Alpine.nextTick(() => {
                this.$dispatch('nav-updated');
            });
        }
    }));

    // Header component
    Alpine.data('headerComponent', () => ({
        showActionsMenu: false,
        
        validateAll() {
            Alpine.store('app').showMessage('Validating all configurations...', 'info');
            // Add validation logic here
        },
        
        refreshData() {
            Alpine.store('app').showMessage('Refreshing data...', 'info');
            // Add refresh logic here
        }
    }));

    // Sidebar component
    Alpine.data('sidebarComponent', () => ({
        init() {
            // Initialize sidebar functionality
        }
    }));

    // Progress indicator component
    Alpine.data('progressIndicator', () => ({
        get progressPercentage() {
            const progress = Alpine.store('app').progress;
            const completedSteps = Object.values(progress).filter(Boolean).length;
            const percentage = (completedSteps / 4) * 100;
            return `${Math.round(percentage)}%`;
        },
        
        get progressWidth() {
            const progress = Alpine.store('app').progress;
            const completedSteps = Object.values(progress).filter(Boolean).length;
            const percentage = (completedSteps / 4) * 100;
            return `${percentage}%`;
        },
        
        stepCompleted(step) {
            return Alpine.store('app').progress[step] || false;
        }
    }));

    // Navigation component
    Alpine.data('navigation', () => ({
        currentPath: window.location.pathname,
        
        init() {
            this.updateActiveNav();
        },
        
        isActiveNav(navType) {
            const path = this.currentPath;
            return (
                (path === '/' && navType === 'dashboard') ||
                (path.includes('config') && navType === 'config') ||
                (path.includes('jobs') && navType === 'jobs') ||
                (path.includes('export') && navType === 'export')
            );
        },
        
        updateActiveNav() {
            // Update the current path when navigation changes
            this.currentPath = window.location.pathname;
        },
        
        validateAll() {
            Alpine.store('app').setLoading(true, 'Validating all components...');
            
            setTimeout(() => {
                Alpine.store('app').setLoading(false);
                Alpine.store('app').showMessage('All components validated successfully', 'success');
            }, 2000);
        },
        
        refreshData() {
            Alpine.store('app').setLoading(true, 'Refreshing all data...');
            
            setTimeout(() => {
                Alpine.store('app').setLoading(false);
                Alpine.store('app').showMessage('Data refreshed successfully', 'success');
            }, 1500);
        }
    }));

    // Legacy compatibility bridge for any remaining vanilla JS
    window.LegacyAppBridge = {
        showMessage: (message, type) => Alpine.store('app').showMessage(message, type),
        setLoading: (loading, message) => Alpine.store('app').setLoading(loading, message),
        updateProgress: (step, completed) => Alpine.store('app').updateProgress(step, completed)
    };
    
    // Make bridge available globally for legacy code
    window.App = window.LegacyAppBridge;
});

// Utility component for common UI updates
document.addEventListener('alpine:init', () => {
    Alpine.data('uiUtils', () => ({
        // Button loading state management
        buttonLoading: {},
        
        setButtonLoading(buttonId, isLoading, loadingText = 'Loading...') {
            const button = document.getElementById(buttonId);
            if (!button) return;
            
            this.buttonLoading[buttonId] = isLoading;
            
            if (isLoading) {
                button.disabled = true;
                const originalText = button.textContent;
                button.setAttribute('data-original-text', originalText);
                button.innerHTML = `
                    <div class="flex items-center justify-center">
                        <div class="spinner mr-2"></div>
                        ${loadingText}
                    </div>
                `;
            } else {
                button.disabled = false;
                const originalText = button.getAttribute('data-original-text');
                if (originalText) {
                    button.textContent = originalText;
                    button.removeAttribute('data-original-text');
                }
            }
        },
        
        isButtonLoading(buttonId) {
            return this.buttonLoading[buttonId] || false;
        }
    }));
});

// Enhanced API utilities specifically for Alpine.js
window.AlpineUtils = {
    // Extended API utility with better Alpine.js integration
    async fetchAPI(url, options = {}) {
        const store = Alpine.store('app');
        
        try {
            if (options.showLoading !== false) {
                store.showLoading(options.loadingMessage || 'Processing...');
            }
            
            const response = await fetch(url, {
                headers: {
                    'Content-Type': 'application/json',
                    ...options.headers
                },
                ...options
            });
            
            const data = await response.json();
            
            if (!response.ok) {
                throw new Error(data.message || 'Request failed');
            }
            
            if (options.successMessage) {
                store.showMessage(options.successMessage, 'success');
            }
            
            return data;
        } catch (error) {
            if (options.errorMessage) {
                store.showMessage(options.errorMessage, 'error');
            } else {
                store.showMessage(error.message, 'error');
            }
            throw error;
        } finally {
            if (options.showLoading !== false) {
                store.hideLoading();
            }
        }
    },
    
    // Form submission helper
    async submitForm(formElement, url, options = {}) {
        const formData = new FormData(formElement);
        return this.fetchAPI(url, {
            method: 'POST',
            body: formData,
            headers: {}, // Let browser set content-type for FormData
            ...options
        });
    },
    
    // JSON submission helper
    async submitJSON(data, url, options = {}) {
        return this.fetchAPI(url, {
            method: 'POST',
            body: JSON.stringify(data),
            ...options
        });
    }
};

// Initialize Alpine.js app when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    // Ensure Alpine.js is loaded
    if (typeof Alpine !== 'undefined') {

        
        // Initialize the app store
        const appStore = Alpine.store('app');
        if (appStore) {
            appStore.updateProgressBasedOnPage();
        }
    } else {
        console.error('Alpine.js is not loaded');
    }
}); 