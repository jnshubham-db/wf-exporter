// Alpine.js Config Page Component
// Replaces the vanilla JavaScript functionality in config.html

document.addEventListener('alpine:init', () => {
    Alpine.data('configManager', () => ({
        // State
        configPath: '',
        configContent: '',
        isLoading: false,
        loadingMessage: '',
        isEditing: false,
        
        // Feedback system
        feedback: {
            show: false,
            message: '',
            type: 'info'
        },
        
        // Editor statistics
        stats: {
            lines: 0,
            characters: 0
        },
        
        // Button loading states
        buttons: {
            load: false,
            save: false,
            validate: false,
            clear: false
        },
        
        // Computed properties
        get configPlaceholder() {
            return '/Workspace/path/to/config.yml';
        },
        
        get feedbackClass() {
            const baseClass = 'mt-4 p-4 rounded-md border';
            const typeClasses = {
                success: 'bg-green-50 border-green-200',
                error: 'bg-red-50 border-red-200',
                warning: 'bg-yellow-50 border-yellow-200',
                info: 'bg-blue-50 border-blue-200'
            };
            return `${baseClass} ${typeClasses[this.feedback.type] || typeClasses.info}`;
        },
        
        get feedbackMessageClass() {
            const typeClasses = {
                success: 'text-green-800',
                error: 'text-red-800',
                warning: 'text-yellow-800',
                info: 'text-blue-800'
            };
            return typeClasses[this.feedback.type] || typeClasses.info;
        },
        
        get feedbackIconClass() {
            const typeClasses = {
                success: 'text-green-400',
                error: 'text-red-400',
                warning: 'text-yellow-400',
                info: 'text-blue-400'
            };
            return `w-5 h-5 ${typeClasses[this.feedback.type] || typeClasses.info}`;
        },
        
        get feedbackIconSvg() {
            const icons = {
                success: '<path fill-rule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clip-rule="evenodd"></path>',
                error: '<path fill-rule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.707 7.293a1 1 0 00-1.414 1.414L8.586 10l-1.293 1.293a1 1 0 101.414 1.414L10 11.414l1.293 1.293a1 1 0 001.414-1.414L11.414 10l1.293-1.293a1 1 0 00-1.414-1.414L10 8.586 8.707 7.293z" clip-rule="evenodd"></path>',
                warning: '<path fill-rule="evenodd" d="M8.257 3.099c.765-1.36 2.722-1.36 3.486 0l5.58 9.92c.75 1.334-.213 2.98-1.742 2.98H4.42c-1.53 0-2.493-1.646-1.743-2.98l5.58-9.92zM11 13a1 1 0 11-2 0 1 1 0 012 0zm-1-8a1 1 0 00-1 1v3a1 1 0 002 0V6a1 1 0 00-1-1z" clip-rule="evenodd"></path>',
                info: '<path fill-rule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clip-rule="evenodd"></path>'
            };
            return icons[this.feedback.type] || icons.info;
        },
        
        // Methods
        init() {
            // Initialize with global config path from session
            const globalConfigPath = this.$store.app.getConfigPath();
            if (globalConfigPath) {
                this.configPath = globalConfigPath;
            }
            
            // Watch for config content changes to update stats (debounced)
            this.$watch('configContent', (value) => {
                if (value && value.length > 0) {
                    // Debounce expensive operations for better performance
                    AlpineUtils.debounce(() => {
                        this.updateEditorStats();
                    }, 150)();
                }
            });
            
            // Listen for global config path changes
            this.$listen('config-path-changed', (event) => {
                this.configPath = event.detail.path;
            });
        },
        
        showFeedback(message, type = 'info') {
            this.feedback.show = true;
            this.feedback.message = message;
            this.feedback.type = type;
            
            // Auto-hide success/info messages after 5 seconds
            if (type === 'success' || type === 'info') {
                setTimeout(() => {
                    this.feedback.show = false;
                }, 5000);
            }
        },
        
        hideFeedback() {
            this.feedback.show = false;
        },
        
        updateEditorStats() {
            const content = this.configContent;
            this.stats.lines = content.split('\n').length;
            this.stats.characters = content.length;
        },
        
        async loadConfig() {
            if (!this.configPath.trim()) {
                this.showFeedback('Please enter a config file path', 'warning');
                return;
            }
            
            this.buttons.load = true;
            let loadSucceeded = false;
            
            try {
                const response = await fetch('/config/load', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify({ path: this.configPath })
                });
                
                let data;
                try {
                    data = await response.json();
                } catch (jsonError) {
                    console.error('Error parsing JSON response:', jsonError);
                    this.showFeedback('Invalid response from server', 'error');
                    return;
                }
                
                if (response.ok && data.success) {
                    loadSucceeded = true;
                    this.configContent = data.content || '';
                    // Set to preview mode to show syntax highlighted content
                    this.isEditing = false;
                    
                    // Set the global config path when successfully loaded
                    try {
                        this.$store.app.setConfigPath(this.configPath);
                    } catch (storeError) {
                        console.warn('Warning: Could not update global store:', storeError);
                    }
                    
                    this.showFeedback(`Configuration loaded successfully from Databricks workspace`, 'success');
                    
                    // Trigger syntax highlighting (with error handling)
                    try {
                        this.updateSyntaxHighlighting();
                    } catch (highlightError) {
                        console.warn('Warning: Syntax highlighting failed:', highlightError);
                    }
                } else {
                    // Handle both HTTP errors and success=false responses
                    const errorMessage = data.message || `Failed to load configuration (HTTP ${response.status})`;
                    this.showFeedback(errorMessage, 'error');
                }
            } catch (error) {
                console.error('Error loading config:', error);
                // Only show network error if the load actually failed
                if (!loadSucceeded) {
                    this.showFeedback('Network error: Unable to connect to the server', 'error');
                } else {
                    // Load succeeded but there was an error in post-processing
                    console.warn('Config loaded successfully but post-processing failed:', error);
                }
            } finally {
                this.buttons.load = false;
            }
        },
        
        async saveConfig() {
            if (!this.configPath.trim()) {
                this.showFeedback('Please enter a Databricks workspace path', 'warning');
                return;
            }
            
            if (!this.configContent.trim()) {
                this.showFeedback('Please enter configuration content', 'warning');
                return;
            }
            
            this.buttons.save = true;
            
            try {
                const data = await AlpineUtils.fetchAPI('/config/save', {
                    method: 'POST',
                    body: JSON.stringify({ 
                        path: this.configPath,
                        content: this.configContent 
                    }),
                    loadingMessage: 'Saving configuration...',
                    showLoading: false
                });
                
                if (data.success) {
                    this.showFeedback(`Configuration saved successfully to Databricks workspace`, 'success');
                } else {
                    this.showFeedback(data.message || 'Failed to save configuration', 'error');
                }
            } catch (error) {
                console.error('Error saving config:', error);
                this.showFeedback('Failed to save configuration to Databricks workspace. Please check your permissions and try again.', 'error');
            } finally {
                this.buttons.save = false;
            }
        },
        
        async validateConfig() {
            if (!this.configContent.trim()) {
                this.showFeedback('No content to validate', 'warning');
                return;
            }
            
            this.buttons.validate = true;
            
            try {
                const data = await AlpineUtils.fetchAPI('/config/validate', {
                    method: 'POST',
                    body: JSON.stringify({ content: this.configContent }),
                    loadingMessage: 'Validating configuration...',
                    showLoading: false
                });
                
                if (data.success) {
                    const details = data.details || {};
                    let message = 'Configuration is valid';
                    if (details.workflow_count !== undefined) {
                        message += ` (${details.workflow_count} workflows found)`;
                    }
                    this.showFeedback(message, 'success');
                } else {
                    this.showFeedback(data.message || 'Configuration validation failed', 'error');
                }
            } catch (error) {
                console.error('Error validating config:', error);
                this.showFeedback('Failed to validate configuration', 'error');
            } finally {
                this.buttons.validate = false;
            }
        },
        
        clearEditor() {
            if (confirm('Are you sure you want to clear the editor? This action cannot be undone.')) {
                this.configContent = '';
                this.showFeedback('Editor cleared', 'info');
            }
        },
        
        updateSyntaxHighlighting() {
            // Trigger Prism.js to re-highlight the syntax
            try {
                this.$nextTick(() => {
                    try {
                        if (window.Prism && typeof window.Prism.highlightAll === 'function') {
                            window.Prism.highlightAll();
                        }
                    } catch (prismError) {
                        console.warn('Prism.js highlighting failed:', prismError);
                    }
                });
            } catch (nextTickError) {
                console.warn('$nextTick failed:', nextTickError);
                // Fallback: try direct highlighting
                try {
                    if (window.Prism && typeof window.Prism.highlightAll === 'function') {
                        window.Prism.highlightAll();
                    }
                } catch (fallbackError) {
                    console.warn('Fallback syntax highlighting failed:', fallbackError);
                }
            }
        },
        
        // Button state helpers
        getButtonClass(buttonType) {
            const baseClass = 'px-4 py-2 rounded-md focus:outline-none focus:ring-2 transition-opacity';
            const isLoading = this.buttons[buttonType];
            
            if (isLoading) {
                return `${baseClass} opacity-75 cursor-not-allowed`;
            }
            
            const buttonClasses = {
                load: `${baseClass} bg-blue-600 text-white hover:bg-blue-700 focus:ring-blue-500`,
                save: `${baseClass} bg-green-600 text-white hover:bg-green-700 focus:ring-green-500`,
                validate: `${baseClass} bg-purple-600 text-white hover:bg-purple-700 focus:ring-purple-500`,
                clear: `${baseClass} bg-red-600 text-white hover:bg-red-700 focus:ring-red-500`
            };
            
            return buttonClasses[buttonType] || baseClass;
        },
        
        getButtonText(buttonType, defaultText) {
            return this.buttons[buttonType] ? 'Loading...' : defaultText;
        },
        
        isButtonDisabled(buttonType) {
            return this.buttons[buttonType];
        }
    }));
}); 