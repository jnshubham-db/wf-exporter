// Alpine.js Job Selection Component
document.addEventListener('alpine:init', () => {
    Alpine.data('jobsManager', () => ({
        // State
        allJobs: [],
        selectedJobs: [],
        searchTerm: '',
        isLoading: true, // Start with loading true, will be set to false when jobs load
        isEmpty: false,
        
        // Pagination
        currentPage: 1,
        itemsPerPage: 10,
        
        // Configuration integration
        configPath: '',
        currentConfigContent: '',
        configPreview: '',
        
        // UI state
        loadingStates: {
            refreshJobs: false,
            updateConfig: false,
            loadConfig: false,
            saveConfig: false
        },
        
        // Feedback
        integrationFeedback: {
            show: false,
            message: '',
            type: 'info' // 'success', 'error', 'warning', 'info'
        },
        
        // Initialize
        init() {
            console.log('Jobs Manager: Initializing with isLoading =', this.isLoading);
            this.useGlobalJobs();
            this.loadConfigInfo();
            
            // Watch search term changes and reset pagination
            this.$watch('searchTerm', () => {
                this.resetPagination(); // Reset to first page when search changes
            });
            
            // Watch loading state changes for debugging
            this.$watch('isLoading', (newValue, oldValue) => {
                console.log(`Jobs Manager: isLoading changed from ${oldValue} to ${newValue}`);
            });
            
            // Listen for global jobs updates
            document.addEventListener('global-jobs-loaded', (event) => {
                console.log('Jobs Manager: Received global-jobs-loaded event');
                this.allJobs = event.detail.jobs;
                this.isLoading = false;
                this.isEmpty = this.allJobs.length === 0;
                console.log(`Jobs page: Received ${this.allJobs.length} jobs from global store, isLoading now =`, this.isLoading);
            });
        },
        
        // Computed properties
        get filteredJobs() {
            if (!this.searchTerm) return this.allJobs;
            
            const searchTerm = this.searchTerm.toLowerCase().trim();
            
            // Simple name-based search that definitely works
            return this.allJobs.filter(job => 
                job.name.toLowerCase().includes(searchTerm)
            );
        },
        
        get jobsCount() {
            const total = this.allJobs.length;
            const filtered = this.filteredJobs.length;
            
            if (this.searchTerm && filtered !== total) {
                return `${filtered} of ${total} jobs found`;
            }
            return `${total} jobs available`;
        },
        
        get selectedCount() {
            return `${this.selectedJobs.length} jobs selected`;
        },
        
        get hasSelectedJobs() {
            return this.selectedJobs.length > 0;
        },
        
        get canUpdateConfig() {
            return this.hasSelectedJobs;
        },
        
        // Pagination computed properties
        get paginatedJobs() {
            const startIndex = (this.currentPage - 1) * this.itemsPerPage;
            const endIndex = startIndex + this.itemsPerPage;
            return this.filteredJobs.slice(startIndex, endIndex);
        },
        
        get totalPages() {
            return Math.ceil(this.filteredJobs.length / this.itemsPerPage);
        },
        
        get paginationInfo() {
            const start = (this.currentPage - 1) * this.itemsPerPage + 1;
            const end = Math.min(this.currentPage * this.itemsPerPage, this.filteredJobs.length);
            const total = this.filteredJobs.length;
            
            if (total === 0) return 'No jobs found';
            return `Showing ${start}-${end} of ${total} jobs`;
        },
        
        get showPagination() {
            return this.totalPages > 1;
        },
        
        get canSaveConfig() {
            return this.$store.app.configLoaded && this.currentConfigContent && this.hasSelectedJobs;
        },
        
        // Fuzzy search scoring algorithm
        fuzzySearchScore(text, searchTerm) {
            if (!searchTerm) return 1;
            if (!text) return 0;
            
            let score = 0;
            
            // Exact match gets highest score
            if (text === searchTerm) {
                return 1000;
            }
            
            // Exact substring match gets high score
            if (text.includes(searchTerm)) {
                score += 500;
                
                // Bonus for starting with search term
                if (text.startsWith(searchTerm)) {
                    score += 200;
                }
                
                // Bonus for word boundary match
                const words = text.split(/[\s_-]+/);
                for (const word of words) {
                    if (word.startsWith(searchTerm)) {
                        score += 100;
                        break;
                    }
                }
                
                return score;
            }
            
            // Check for fuzzy character sequence match
            let searchIndex = 0;
            let lastMatchIndex = -1;
            let consecutiveMatches = 0;
            let wordBoundaryBonus = 0;
            
            for (let i = 0; i < text.length && searchIndex < searchTerm.length; i++) {
                if (text[i] === searchTerm[searchIndex]) {
                    // Character match found
                    score += 10;
                    
                    // Bonus for consecutive character matches
                    if (i === lastMatchIndex + 1) {
                        consecutiveMatches++;
                        score += consecutiveMatches * 5;
                    } else {
                        consecutiveMatches = 0;
                    }
                    
                    // Bonus for word boundary matches (after space, _, -, or start)
                    if (i === 0 || /[\s_-]/.test(text[i - 1])) {
                        wordBoundaryBonus += 20;
                    }
                    
                    lastMatchIndex = i;
                    searchIndex++;
                }
            }
            
            // Only return score if all search characters were found
            if (searchIndex === searchTerm.length) {
                score += wordBoundaryBonus;
                
                // Bonus for matching all characters with fewer gaps
                const matchRatio = searchTerm.length / (lastMatchIndex - (text.indexOf(searchTerm[0])) + 1);
                score += Math.floor(matchRatio * 50);
                
                return score;
            }
            
            return 0; // No match
        },
        
        // Highlight matching characters in job name
        highlightMatch(text, searchTerm) {
            if (!searchTerm || !text) return text;
            
            const lowerText = text.toLowerCase();
            const lowerSearch = searchTerm.toLowerCase();
            
            // Simple substring highlighting
            if (lowerText.includes(lowerSearch)) {
                const startIndex = lowerText.indexOf(lowerSearch);
                const endIndex = startIndex + lowerSearch.length;
                return text.substring(0, startIndex) + 
                       '<mark class="bg-yellow-200 px-1 rounded">' + 
                       text.substring(startIndex, endIndex) + 
                       '</mark>' + 
                       text.substring(endIndex);
            }
            
            // Fuzzy highlighting for character matches
            let highlighted = '';
            let searchIndex = 0;
            
            for (let i = 0; i < text.length && searchIndex < lowerSearch.length; i++) {
                if (lowerText[i] === lowerSearch[searchIndex]) {
                    highlighted += '<mark class="bg-yellow-200 px-0.5 rounded">' + text[i] + '</mark>';
                    searchIndex++;
                } else {
                    highlighted += text[i];
                }
            }
            
            // Add remaining characters if any
            highlighted += text.substring(highlighted.replace(/<[^>]*>/g, '').length);
            
            return highlighted;
        },
        
        // Job management methods
        useGlobalJobs() {
            // Use jobs from global store if available
            const globalJobs = this.$store.app.getGlobalJobs();
            
            console.log('useGlobalJobs called:', {
                jobsLoaded: this.$store.app.jobsLoaded,
                jobsLoading: this.$store.app.jobsLoading,
                globalJobsCount: globalJobs.length,
                currentIsLoading: this.isLoading
            });
            
            if (this.$store.app.jobsLoaded) {
                // Show loading for at least 500ms so user can see it
                setTimeout(() => {
                    this.allJobs = globalJobs;
                    this.isLoading = false;
                    this.isEmpty = this.allJobs.length === 0;
                    console.log(`Jobs page: Using ${this.allJobs.length} jobs from global store, loading set to false`);
                }, 500);
            } else if (this.$store.app.jobsLoading) {
                this.isLoading = true;
                this.isEmpty = false;
                console.log('Jobs page: Waiting for global jobs to load...');
            } else {
                // Fallback: trigger global job loading
                console.log('Jobs page: Triggering global job loading...');
                this.isLoading = true;
                this.$store.app.loadGlobalJobs();
            }
        },
        
        async loadJobs() {
            // Fallback method for manual loading (kept for compatibility)
            this.isLoading = true;
            this.isEmpty = false;
            
            try {
                const data = await this.apiCall('/jobs/list');
                this.allJobs = data.jobs || [];
                
                if (this.allJobs.length === 0) {
                    this.isEmpty = true;
                }
            } catch (error) {
                console.error('Error loading jobs:', error);
                this.isEmpty = true;
                this.showMessage('Failed to load jobs from Databricks', 'error');
            } finally {
                this.isLoading = false;
            }
        },
        
        async loadConfigInfo() {
            // Get config path from global store
            this.configPath = this.$store.app.getConfigPath();
            
            if (!this.configPath) {
                return;
            }
            
            try {
                const data = await this.apiCall('/config/load', {
                    method: 'POST',
                    body: JSON.stringify({ path: this.configPath })
                });
                
                if (data.success) {
                    this.configPreview = data.content || '';
                    this.currentConfigContent = data.content || '';
                    
                    // Trigger syntax highlighting for the preview
                    this.$nextTick(() => {
                        if (window.Prism) {
                            window.Prism.highlightAll();
                        }
                    });
                }
            } catch (error) {
                console.error('Error loading config info:', error);
            }
        },
        
        async refreshJobs() {
            this.loadingStates.refreshJobs = true;
            try {
                // Refresh global jobs
                await this.$store.app.refreshGlobalJobs();
                
                // Update local jobs from refreshed global store
                this.allJobs = this.$store.app.getGlobalJobs();
                this.isEmpty = this.allJobs.length === 0;
                this.resetPagination();
                
                this.showMessage(`Databricks jobs refreshed successfully - ${this.allJobs.length} jobs loaded`, 'success');
            } catch (error) {
                this.showMessage('Failed to refresh Databricks jobs', 'error');
            } finally {
                this.loadingStates.refreshJobs = false;
            }
        },
        
        isJobSelected(jobId) {
            return this.selectedJobs.some(j => j.job_id === jobId);
        },
        
        getSelectedJob(jobId) {
            return this.selectedJobs.find(j => j.job_id === jobId);
        },
        
        toggleJobSelection(job) {
            const isCurrentlySelected = this.isJobSelected(job.job_id);
            
            if (isCurrentlySelected) {
                this.selectedJobs = this.selectedJobs.filter(j => j.job_id !== job.job_id);
            } else {
                this.selectedJobs.push({
                    job_id: job.job_id,
                    name: job.name,
                    is_existing: true,
                    is_active: true,
                    export_libraries: true  // Default to true for new selections
                });
            }
        },
        
        toggleBindExisting(jobId) {
            const job = this.getSelectedJob(jobId);
            if (job) {
                job.is_existing = !job.is_existing;
            }
        },
        
        toggleExportLibraries(jobId) {
            const job = this.getSelectedJob(jobId);
            if (job) {
                job.export_libraries = !job.export_libraries;
            }
        },
        
        clearAllSelections() {
            if (confirm('Are you sure you want to clear all selected jobs?')) {
                this.selectedJobs = [];
                this.showMessage('All job selections cleared', 'info');
            }
        },
        
        async updateConfiguration() {
            if (!this.canUpdateConfig) {
                this.showMessage('Please select at least one job', 'warning');
                return;
            }
            
            const configPath = this.$store.app.getConfigPath();
            if (!configPath) {
                this.showMessage('No configuration file path available', 'error');
                return;
            }
            
            this.loadingStates.updateConfig = true;
            
            try {
                // First, select the jobs and store them in session
                await this.apiCall('/jobs/select', {
                    method: 'POST',
                    body: JSON.stringify({ jobs: this.selectedJobs })
                });
                
                // Then, update the configuration with selected jobs and refresh preview
                await this.updateConfigPreview();
                
                // Finally, save the updated configuration back to the file
                await this.apiCall('/config/save', {
                    method: 'POST',
                    body: JSON.stringify({ 
                        path: configPath,
                        content: this.currentConfigContent 
                    })
                });
                
                this.showMessage(`Configuration updated and saved successfully with ${this.selectedJobs.length} jobs`, 'success');
            } catch (error) {
                console.error('Error updating configuration:', error);
                this.showMessage('Failed to update and save configuration', 'error');
            } finally {
                this.loadingStates.updateConfig = false;
            }
        },
        
        async updateConfigPreview() {
            const configPath = this.$store.app.getConfigPath();
            if (!configPath) {
                return;
            }
            
            try {
                // Get the updated configuration with selected jobs
                const data = await this.apiCall('/jobs/build-config', {
                    method: 'POST',
                    body: JSON.stringify({ 
                        config_path: configPath,
                        selected_jobs: this.selectedJobs 
                    })
                });
                
                if (data.success) {
                    this.configPreview = data.updated_config || '';
                    this.currentConfigContent = data.updated_config || '';
                    
                    // Trigger syntax highlighting for the updated preview
                    this.$nextTick(() => {
                        if (window.Prism) {
                            window.Prism.highlightAll();
                        }
                    });
                }
            } catch (error) {
                console.error('Error updating config preview:', error);
            }
        },
        
        // Configuration integration methods
        async loadConfigForUpdate() {
            const configPath = this.$store.app.getConfigPath();
            if (!configPath) {
                this.showIntegrationFeedback('No configuration path set. Please set a config path first.', 'warning');
                return;
            }
            
            this.loadingStates.loadConfig = true;
            
            try {
                const data = await this.apiCall('/config/load', {
                    method: 'POST',
                    body: JSON.stringify({ path: configPath })
                });
                
                if (data.success) {
                    this.currentConfigContent = data.content;
                    this.configPreview = data.content;
                    this.showIntegrationFeedback(`Configuration loaded from Databricks workspace`, 'success');
                } else {
                    this.showIntegrationFeedback(data.message || 'Failed to load configuration', 'error');
                }
            } catch (error) {
                console.error('Error loading config:', error);
                this.showIntegrationFeedback('Failed to load configuration', 'error');
            } finally {
                this.loadingStates.loadConfig = false;
            }
        },
        
        async saveUpdatedConfig() {
            const configPath = this.$store.app.getConfigPath();
            if (!configPath) {
                this.showIntegrationFeedback('No configuration path set. Please set a config path first.', 'warning');
                return;
            }
            
            if (!this.currentConfigContent) {
                this.showIntegrationFeedback('Please load a configuration first', 'warning');
                return;
            }
            
            if (!this.hasSelectedJobs) {
                this.showIntegrationFeedback('Please select at least one job', 'warning');
                return;
            }
            
            this.loadingStates.saveConfig = true;
            
            try {
                const data = await this.apiCall('/jobs/update-config', {
                    method: 'POST',
                    body: JSON.stringify({
                        config_path: configPath,
                        config_content: this.currentConfigContent,
                        jobs: this.selectedJobs
                    })
                });
                
                if (data.success) {
                    this.currentConfigContent = data.updated_content;
                    this.configPreview = data.updated_content;
                    this.showIntegrationFeedback(`Configuration updated with ${data.jobs_count} jobs and saved to ${data.destination}`, 'success');
                } else {
                    this.showIntegrationFeedback(data.message || 'Failed to update configuration', 'error');
                }
            } catch (error) {
                console.error('Error updating config:', error);
                this.showIntegrationFeedback('Failed to update configuration', 'error');
            } finally {
                this.loadingStates.saveConfig = false;
            }
        },
        
        goToConfigPage() {
            window.location.href = '/config/';
        },
        
        // Feedback methods
        showMessage(message, type = 'info') {
            // Use global message system
            if (window.Alpine && Alpine.store('app')) {
                Alpine.store('app').showMessage(message, type);
            } else {
                console.log(`[${type.toUpperCase()}] ${message}`);
            }
        },
        
        showIntegrationFeedback(message, type = 'info') {
            this.integrationFeedback = {
                show: true,
                message: message,
                type: type
            };
            
            // Auto-hide success/info messages after 5 seconds
            if (type === 'success' || type === 'info') {
                setTimeout(() => {
                    this.integrationFeedback.show = false;
                }, 5000);
            }
        },
        
        hideIntegrationFeedback() {
            this.integrationFeedback.show = false;
        },
        
        // Utility methods
        async apiCall(url, options = {}) {
            const defaultOptions = {
                headers: {
                    'Content-Type': 'application/json',
                },
                ...options
            };
            
            const response = await fetch(url, defaultOptions);
            
            if (!response.ok) {
                throw new Error(`API call failed: ${response.status} ${response.statusText}`);
            }
            
            return await response.json();
        },
        
        // UI helper methods
        getJobStatusClasses(job) {
            const selectedJob = this.getSelectedJob(job.job_id);
            return selectedJob && selectedJob.is_existing 
                ? 'bg-green-100 text-green-800' 
                : 'bg-blue-100 text-blue-800';
        },
        
        getJobStatusText(job) {
            const selectedJob = this.getSelectedJob(job.job_id);
            return selectedJob && selectedJob.is_existing ? 'Existing' : 'New';
        },
        
        getFeedbackClasses() {
            const baseClasses = 'mt-4 p-4 rounded-md';
            const typeClasses = {
                success: 'bg-green-50 border border-green-200',
                error: 'bg-red-50 border border-red-200',
                warning: 'bg-yellow-50 border border-yellow-200',
                info: 'bg-blue-50 border border-blue-200'
            };
            
            return `${baseClasses} ${typeClasses[this.integrationFeedback.type] || typeClasses.info}`;
        },
        
        getFeedbackIconClasses() {
            const baseClasses = 'w-5 h-5';
            const typeClasses = {
                success: 'text-green-400',
                error: 'text-red-400',
                warning: 'text-yellow-400',
                info: 'text-blue-400'
            };
            
            return `${baseClasses} ${typeClasses[this.integrationFeedback.type] || typeClasses.info}`;
        },
        
        getFeedbackMessageClasses() {
            const typeClasses = {
                success: 'text-green-800',
                error: 'text-red-800',
                warning: 'text-yellow-800',
                info: 'text-blue-800'
            };
            
            return `text-sm font-medium ${typeClasses[this.integrationFeedback.type] || typeClasses.info}`;
        },
        
        // Pagination methods
        goToPage(page) {
            if (page >= 1 && page <= this.totalPages) {
                this.currentPage = page;
            }
        },
        
        previousPage() {
            if (this.currentPage > 1) {
                this.currentPage--;
            }
        },
        
        nextPage() {
            if (this.currentPage < this.totalPages) {
                this.currentPage++;
            }
        },
        
        resetPagination() {
            this.currentPage = 1;
        },
        
        getFeedbackIcon() {
            const icons = {
                success: '<svg fill="currentColor" viewBox="0 0 20 20"><path fill-rule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clip-rule="evenodd"></path></svg>',
                error: '<svg fill="currentColor" viewBox="0 0 20 20"><path fill-rule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.707 7.293a1 1 0 00-1.414 1.414L8.586 10l-1.293 1.293a1 1 0 101.414 1.414L10 11.414l1.293 1.293a1 1 0 001.414-1.414L11.414 10l1.293-1.293a1 1 0 00-1.414-1.414L10 8.586 8.707 7.293z" clip-rule="evenodd"></path></svg>',
                warning: '<svg fill="currentColor" viewBox="0 0 20 20"><path fill-rule="evenodd" d="M8.257 3.099c.765-1.36 2.722-1.36 3.486 0l5.58 9.92c.75 1.334-.213 2.98-1.742 2.98H4.42c-1.53 0-2.493-1.646-1.743-2.98l5.58-9.92zM11 13a1 1 0 11-2 0 1 1 0 012 0zm-1-8a1 1 0 00-1 1v3a1 1 0 002 0V6a1 1 0 00-1-1z" clip-rule="evenodd"></path></svg>',
                info: '<svg fill="currentColor" viewBox="0 0 20 20"><path fill-rule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clip-rule="evenodd"></path></svg>'
            };
            
            return icons[this.integrationFeedback.type] || icons.info;
        },
        
        // Configuration preview helpers
        getWorkflowCountFromPreview() {
            if (!this.configPreview) return 0;
            
            const lines = this.configPreview.split('\n');
            let count = 0;
            let inWorkflowsSection = false;
            
            for (let line of lines) {
                const trimmedLine = line.trim();
                if (trimmedLine === 'workflows:') {
                    inWorkflowsSection = true;
                    continue;
                }
                if (trimmedLine.endsWith(':') && !trimmedLine.startsWith(' ') && !trimmedLine.startsWith('-')) {
                    if (trimmedLine !== 'workflows:') {
                        inWorkflowsSection = false;
                    }
                }
                if (inWorkflowsSection && trimmedLine.startsWith('- job_name:')) {
                    count++;
                }
            }
            return count;
        },
        
        getPipelineCountFromPreview() {
            if (!this.configPreview) return 0;
            
            const lines = this.configPreview.split('\n');
            let count = 0;
            let inPipelinesSection = false;
            
            for (let line of lines) {
                const trimmedLine = line.trim();
                if (trimmedLine === 'pipelines:') {
                    inPipelinesSection = true;
                    continue;
                }
                if (trimmedLine.endsWith(':') && !trimmedLine.startsWith(' ') && !trimmedLine.startsWith('-')) {
                    if (trimmedLine !== 'pipelines:') {
                        inPipelinesSection = false;
                    }
                }
                if (inPipelinesSection && trimmedLine.startsWith('- pipeline_name:')) {
                    count++;
                }
            }
            return count;
        },
        
        getExportLibrariesSummary() {
            if (!this.configPreview) return 'N/A';
            
            const lines = this.configPreview.split('\n');
            let exportLibrariesCount = 0;
            let totalItems = 0;
            let inWorkflowsSection = false;
            let inPipelinesSection = false;
            
            for (let line of lines) {
                const trimmedLine = line.trim();
                
                if (trimmedLine === 'workflows:') {
                    inWorkflowsSection = true;
                    inPipelinesSection = false;
                    continue;
                }
                if (trimmedLine === 'pipelines:') {
                    inPipelinesSection = true;
                    inWorkflowsSection = false;
                    continue;
                }
                if (trimmedLine.endsWith(':') && !trimmedLine.startsWith(' ') && !trimmedLine.startsWith('-')) {
                    if (!['workflows:', 'pipelines:'].includes(trimmedLine)) {
                        inWorkflowsSection = false;
                        inPipelinesSection = false;
                    }
                }
                
                if ((inWorkflowsSection && trimmedLine.startsWith('- job_name:')) ||
                    (inPipelinesSection && trimmedLine.startsWith('- pipeline_name:'))) {
                    totalItems++;
                }
                
                if ((inWorkflowsSection || inPipelinesSection) && trimmedLine.startsWith('export_libraries: true')) {
                    exportLibrariesCount++;
                }
            }
            
            if (totalItems === 0) return 'N/A';
            return `${exportLibrariesCount}/${totalItems} enabled`;
        }
    }));
}); 