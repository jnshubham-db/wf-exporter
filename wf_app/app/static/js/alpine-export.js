// Simple Alpine.js Export Component
document.addEventListener('alpine:init', () => {
    Alpine.data('exportManager', () => ({
        // Simple state
        isExporting: false,
        showProgress: false,
        showResults: false,
        currentRunId: null,
        pollInterval: null,
        currentWorkflowRunUrl: null,
        
        // Last known export for potential resume
        lastKnownExportId: null,
        
        // Config info for workflow counting
        workflowCount: 0,
        pipelineCount: 0,
        vStartPath: '',
        
        // Workflow and pipeline details
        workflowNames: [],
        pipelineNames: [],
        showWorkflowDetails: false,
        showPipelineDetails: false,
        
        // Progress
        progress: {
            percentage: 0,
            status: 'Initializing...',
            startTime: null,
            elapsedTime: 0
        },
        
        // Smart progress tracking
        smartProgress: {
            estimatedDuration: 0, // in seconds
            initializationTime: 0, // in seconds
            workflowProcessingTime: 0, // in seconds per workflow
            pipelineProcessingTime: 0, // in seconds per pipeline
            totalItems: 0, // total workflows + pipelines
            clusterType: 'unknown',
            jobDetails: null,
            phase: 'initializing', // 'initializing', 'processing', 'completed'
            phasesInfo: {
                initialization: { duration: 0, label: 'Starting cluster and initializing...' },
                processing: { duration: 0, label: 'Processing items...' }
            }
        },
        
        // Results
        results: {
            success: false,
            message: '',
            exportedJobs: [],
            workspaceUrl: ''
        },
        
        // App config info (auto-loaded)
        exportJobInfo: null,
        appConfigPath: 'app_config.yml',
        
        // Initialize
        init() {
            console.log('Export manager initializing...');
            this.loadAppConfig();
            this.autoLoadConfig();
            this.checkActiveExport();
        },
        
        // Load app configuration
        async loadAppConfig() {
            try {
                const response = await fetch('/export/app-config/load', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ path: this.appConfigPath })
                });
                
                const data = await response.json();
                if (data.success) {
                    this.exportJobInfo = data.export_job;
                    console.log('App config loaded:', this.exportJobInfo);
                }
            } catch (error) {
                console.log('App config not found (will need manual setup)');
            }
                 },
         
         // Config auto-loading to extract workflow count
         async autoLoadConfig() {
             // Get config path from global store
             const configPath = this.$store?.app?.getConfigPath();
             
             if (!configPath) {
                 return;
             }
             
             try {
                 const response = await fetch('/config/load', {
                     method: 'POST',
                     headers: { 'Content-Type': 'application/json' },
                     body: JSON.stringify({ path: configPath })
                 });
                 
                 const data = await response.json();
                 if (data.success) {
                     this.extractConfigInfo(data.content);
                 }
             } catch (error) {
                 console.error('Error auto-loading config info:', error);
             }
         },
         
         extractConfigInfo(configContent) {
             try {
                 // Parse YAML content to extract workflow count, pipeline count, names, and v_start_path
                 const lines = configContent.split('\n');
                 let workflowCount = 0;
                 let pipelineCount = 0;
                 let vStartPath = '';
                 let workflowNames = [];
                 let pipelineNames = [];
                 let inWorkflowsSection = false;
                 let inPipelinesSection = false;
                 let inInitialVariables = false;
                 
                 for (let line of lines) {
                     const trimmedLine = line.trim();
                     
                     // Check for workflows section
                     if (trimmedLine === 'workflows:') {
                         inWorkflowsSection = true;
                         inPipelinesSection = false;
                         inInitialVariables = false;
                         continue;
                     }
                     
                     // Check for pipelines section
                     if (trimmedLine === 'pipelines:') {
                         inPipelinesSection = true;
                         inWorkflowsSection = false;
                         inInitialVariables = false;
                         continue;
                     }
                     
                     // Check for initial_variables section
                     if (trimmedLine === 'initial_variables:') {
                         inInitialVariables = true;
                         inWorkflowsSection = false;
                         inPipelinesSection = false;
                         continue;
                     }
                     
                     // If we hit another top-level section, reset flags
                     if (trimmedLine.endsWith(':') && !trimmedLine.startsWith(' ') && !trimmedLine.startsWith('-')) {
                         if (!['workflows:', 'pipelines:', 'initial_variables:'].includes(trimmedLine)) {
                             inWorkflowsSection = false;
                             inPipelinesSection = false;
                             inInitialVariables = false;
                         }
                     }
                     
                     // Extract workflow names (look for job_name entries)
                     if (inWorkflowsSection && trimmedLine.startsWith('- job_name:')) {
                         workflowCount++;
                         const jobName = trimmedLine.replace('- job_name:', '').trim().replace(/['"]/g, '');
                         if (jobName) {
                             workflowNames.push(jobName);
                         }
                     }
                     
                     // Extract pipeline names (look for pipeline_name entries)
                     if (inPipelinesSection && trimmedLine.startsWith('- pipeline_name:')) {
                         pipelineCount++;
                         const pipelineName = trimmedLine.replace('- pipeline_name:', '').trim().replace(/['"]/g, '');
                         if (pipelineName) {
                             pipelineNames.push(pipelineName);
                         }
                     }
                     
                     // Extract v_start_path from initial_variables
                     if (inInitialVariables && trimmedLine.startsWith('v_start_path:')) {
                         vStartPath = trimmedLine.replace('v_start_path:', '').trim();
                     }
                 }
                 
                 this.workflowCount = workflowCount;
                 this.pipelineCount = pipelineCount;
                 this.vStartPath = vStartPath;
                 this.workflowNames = workflowNames;
                 this.pipelineNames = pipelineNames;
                 
                 console.log(`Config parsed: ${workflowCount} workflows, ${pipelineCount} pipelines`);
                 console.log('Workflow names:', workflowNames);
                 console.log('Pipeline names:', pipelineNames);
                 
             } catch (error) {
                 console.error('Error extracting config info:', error);
                 this.workflowCount = 0;
                 this.pipelineCount = 0;
                 this.vStartPath = '';
                 this.workflowNames = [];
                 this.pipelineNames = [];
             }
         },
         
         // Toggle workflow details visibility
         toggleWorkflowDetails() {
             this.showWorkflowDetails = !this.showWorkflowDetails;
             if (this.showWorkflowDetails) {
                 this.showPipelineDetails = false; // Close pipeline details if open
             }
         },
         
         // Toggle pipeline details visibility
         togglePipelineDetails() {
             this.showPipelineDetails = !this.showPipelineDetails;
             if (this.showPipelineDetails) {
                 this.showWorkflowDetails = false; // Close workflow details if open
             }
         },
         
         // Smart progress calculation methods
         calculateEstimatedTime(clusterType, workflowCount, pipelineCount = 0) {
             let initTime, processingTimePerWorkflow, processingTimePerPipeline;
             
             switch(clusterType) {
                 case 'job_cluster':
                     initTime = 5 * 60; // 5 minutes
                     processingTimePerWorkflow = 30; // 30 seconds
                     processingTimePerPipeline = 45; // 45 seconds (pipelines typically take longer)
                     break;
                 case 'serverless_performance':
                     initTime = 1 * 60; // 1 minute
                     processingTimePerWorkflow = 30; // 30 seconds
                     processingTimePerPipeline = 45; // 45 seconds
                     break;
                 case 'serverless':
                     initTime = 5 * 60; // 5 minutes (normal serverless)
                     processingTimePerWorkflow = 30; // 30 seconds
                     processingTimePerPipeline = 45; // 45 seconds
                     break;
                 case 'existing_cluster':
                     initTime = 30; // 30 seconds (cluster already running)
                     processingTimePerWorkflow = 30; // 30 seconds
                     processingTimePerPipeline = 45; // 45 seconds
                     break;
                 default:
                     initTime = 5 * 60; // Default to job cluster timing
                     processingTimePerWorkflow = 30;
                     processingTimePerPipeline = 45;
             }
             
             const totalWorkflowTime = workflowCount * processingTimePerWorkflow;
             const totalPipelineTime = pipelineCount * processingTimePerPipeline;
             const totalProcessingTime = totalWorkflowTime + totalPipelineTime;
             const totalEstimatedTime = initTime + totalProcessingTime;
             const totalItems = workflowCount + pipelineCount;
             
             this.smartProgress.initializationTime = initTime;
             this.smartProgress.workflowProcessingTime = processingTimePerWorkflow;
             this.smartProgress.pipelineProcessingTime = processingTimePerPipeline;
             this.smartProgress.estimatedDuration = totalEstimatedTime;
             this.smartProgress.clusterType = clusterType;
             this.smartProgress.totalItems = totalItems;
             
             // Update phase info
             this.smartProgress.phasesInfo.initialization.duration = initTime;
             this.smartProgress.phasesInfo.processing.duration = totalProcessingTime;
             
             const clusterTypeLabels = {
                 'job_cluster': 'Job Cluster',
                 'serverless_performance': 'Serverless (Performance Optimized)',
                 'serverless': 'Serverless (Standard)',
                 'existing_cluster': 'Existing Cluster'
             };
             
             this.smartProgress.phasesInfo.initialization.label = 
                 `Starting ${clusterTypeLabels[clusterType] || 'cluster'} and initializing...`;
             
             // Build processing label with workflows and pipelines
             const items = [];
             if (workflowCount > 0) {
                 items.push(`${workflowCount} workflow${workflowCount !== 1 ? 's' : ''}`);
             }
             if (pipelineCount > 0) {
                 items.push(`${pipelineCount} pipeline${pipelineCount !== 1 ? 's' : ''}`);
             }
             const itemsText = items.length > 0 ? items.join(' and ') : 'items';
             
             this.smartProgress.phasesInfo.processing.label = `Processing ${itemsText}...`;
             
             console.log(`Estimated time calculation: ${clusterType}, ${workflowCount} workflows, ${pipelineCount} pipelines = ${totalEstimatedTime}s total`);
             return totalEstimatedTime;
         },
         
         getSmartProgress(elapsedSeconds) {
             if (!this.smartProgress.estimatedDuration) return 50; // Fallback
             
             const initTime = this.smartProgress.initializationTime;
             const totalEstimated = this.smartProgress.estimatedDuration;
             
             if (elapsedSeconds <= initTime) {
                 // Initialization phase
                 this.smartProgress.phase = 'initializing';
                 return Math.min(30, (elapsedSeconds / initTime) * 30); // 0-30% during init
             } else {
                 // Processing phase
                 this.smartProgress.phase = 'processing';
                 const processingElapsed = elapsedSeconds - initTime;
                 const processingDuration = this.smartProgress.phasesInfo.processing.duration;
                 const processingProgress = Math.min(1, processingElapsed / processingDuration);
                 return 30 + (processingProgress * 65); // 30-95% during processing
             }
         },
         
         getSmartProgressStatus(elapsedSeconds) {
             const phase = this.smartProgress.phase;
             const elapsed = this.elapsedTimeFormatted;
             const estimated = this.formatDuration(this.smartProgress.estimatedDuration);
             
             if (phase === 'initializing') {
                 return `${this.smartProgress.phasesInfo.initialization.label} (${elapsed} / ~${this.formatDuration(this.smartProgress.initializationTime)})`;
             } else if (phase === 'processing') {
                 const processingElapsed = elapsedSeconds - this.smartProgress.initializationTime;
                 const processingEstimated = this.smartProgress.phasesInfo.processing.duration;
                 return `${this.smartProgress.phasesInfo.processing.label} (${elapsed} / ~${estimated} total)`;
             } else {
                 return `Workflow running... (${elapsed} elapsed)`;
             }
         },
         
         formatDuration(seconds) {
             const minutes = Math.floor(seconds / 60);
             const secs = seconds % 60;
             if (minutes > 0) {
                 return `${minutes}m ${secs}s`;
             } else {
                 return `${secs}s`;
             }
         },
         
         // Check for active export state persistence
         async checkActiveExport() {
             try {
                 const response = await fetch('/export/current');
                 const data = await response.json();
                 
                 if (data.success && data.has_active_export) {
                     const exportData = data.export_data;
                     console.log('Found previous export in session:', exportData);
                     
                     // Check if it's actually still running
                     if (exportData.run_id) {
                         try {
                             const statusResponse = await fetch(`/export/status/${exportData.run_id}`);
                             const statusData = await statusResponse.json();
                             
                             if (statusData.success && !this.isTerminalStatus(statusData.status)) {
                                 console.log(`Previous export (${exportData.run_id}) is still running with status: ${statusData.status}`);
                                 
                                 // Automatically resume the export
                                 this.currentRunId = exportData.run_id;
                                 this.isExporting = true;
                                 this.showProgress = true;
                                 this.progress.startTime = new Date(exportData.start_time * 1000); // Convert from timestamp
                                 
                                 // Restore workflow count from export data
                                 if (exportData.workflows) {
                                     this.workflowCount = exportData.workflows.length;
                                 }
                                 if (exportData.pipelines) {
                                     this.pipelineCount = exportData.pipelines.length;
                                 }
                                 
                                 // Calculate smart progress if we have the export job info
                                 if (this.exportJobInfo) {
                                     try {
                                         const jobDetailsResponse = await fetch(`/jobs/details/${this.exportJobInfo.job_id}`);
                                         const jobDetailsData = await jobDetailsResponse.json();
                                         
                                         if (jobDetailsData.success) {
                                             const clusterType = jobDetailsData.job_details.cluster_type || 'job_cluster';
                                             this.calculateEstimatedTime(clusterType, this.workflowCount || 1, this.pipelineCount || 0);
                                         }
                                     } catch (jobError) {
                                         console.warn('Could not get job details for resumed export');
                                     }
                                 }
                                 
                                 this.startSmartPolling();
                                 this.showMessage(`Resumed monitoring export ${exportData.run_id} that was still running`, 'success');
                             } else {
                                 console.log(`Previous export (${exportData.run_id}) has completed with status: ${statusData.status}`);
                                 // Clear the session since it's no longer running
                                 await fetch('/export/clear-session', { method: 'POST' });
                                 this.resetExport();
                             }
                         } catch (statusError) {
                             console.log('Could not check previous export status:', statusError);
                             this.lastKnownExportId = exportData.run_id;
                         }
                     }
                 }
             } catch (error) {
                 console.error('Error checking active export:', error);
             }
         },
         
         // Resume monitoring a known export
         async resumeMonitoring() {
             if (!this.lastKnownExportId) {
                 this.showMessage('No previous export to resume', 'warning');
                 return;
             }
             
             try {
                 const response = await fetch(`/export/status/${this.lastKnownExportId}`);
                 const data = await response.json();
                 
                 if (data.success && !this.isTerminalStatus(data.status)) {
                     this.currentRunId = this.lastKnownExportId;
                     this.isExporting = true;
                     this.showProgress = true;
                     this.startSmartPolling();
                     this.showMessage(`Resumed monitoring export ${this.lastKnownExportId}`, 'success');
                     this.lastKnownExportId = null; // Clear it since we're now actively monitoring
                 } else {
                     this.showMessage(`Export ${this.lastKnownExportId} is no longer running`, 'info');
                     this.lastKnownExportId = null;
                 }
             } catch (error) {
                 console.error('Error resuming monitoring:', error);
                 this.showMessage('Could not resume monitoring - export may no longer exist', 'error');
                 this.lastKnownExportId = null;
             }
         },
         
         // Main export function - simplified
        async triggerExport() {
            console.log('Triggering export...');
            
            // Simple check - just need export job info
            if (!this.exportJobInfo) {
                this.showMessage('Please configure export job in app_config.yml first', 'error');
                return;
            }
            
            // Get config path from store
            const configPath = this.$store?.app?.getConfigPath();
            if (!configPath) {
                this.showMessage('Please load a configuration file first from the Config page', 'error');
                return;
            }
            
                     try {
             // Start export UI
             this.startExport();
             
             // Get job details to determine cluster type and calculate estimated time
             this.progress.status = 'Getting job details and calculating estimated time...';
             this.progress.percentage = 5;
             
             const jobId = this.exportJobInfo.job_id;
             console.log(`Getting details for job ${jobId}...`);
             
             try {
                 const jobDetailsResponse = await fetch(`/jobs/details/${jobId}`);
                 const jobDetailsData = await jobDetailsResponse.json();
                 
                 if (jobDetailsData.success) {
                     this.smartProgress.jobDetails = jobDetailsData.job_details;
                     const clusterType = jobDetailsData.job_details.cluster_type || 'job_cluster';
                     const workflowCount = this.workflowCount || 1;
                     const pipelineCount = this.pipelineCount || 0;
                     
                     // Calculate estimated time based on cluster type and total items
                     this.calculateEstimatedTime(clusterType, workflowCount, pipelineCount);
                     
                     this.progress.status = `Triggering Databricks job ${jobId} (${clusterType})...`;
                     this.progress.percentage = 10;
                     
                     // Show cluster type info to user
                     const clusterTypeMessage = {
                         'job_cluster': `Job Cluster detected - estimated time: ~${this.formatDuration(this.smartProgress.estimatedDuration)}`,
                         'serverless_performance': `Serverless Performance Optimized detected - estimated time: ~${this.formatDuration(this.smartProgress.estimatedDuration)}`,
                         'serverless': `Serverless Standard detected - estimated time: ~${this.formatDuration(this.smartProgress.estimatedDuration)}`,
                         'existing_cluster': `Existing Cluster detected - estimated time: ~${this.formatDuration(this.smartProgress.estimatedDuration)}`
                     };
                     
                     this.showMessage(clusterTypeMessage[clusterType] || `Cluster type: ${clusterType} - estimated time: ~${this.formatDuration(this.smartProgress.estimatedDuration)}`, 'info');
                 } else {
                     console.warn('Could not get job details, using default timing');
                     this.calculateEstimatedTime('job_cluster', this.workflowCount || 1, this.pipelineCount || 0);
                 }
             } catch (jobDetailsError) {
                 console.warn('Error getting job details, using default timing:', jobDetailsError);
                 this.calculateEstimatedTime('job_cluster', this.workflowCount || 1, this.pipelineCount || 0);
             }
             
             // Trigger the export
             const response = await fetch('/export/trigger', {
                 method: 'POST',
                 headers: { 'Content-Type': 'application/json' },
                 body: JSON.stringify({
                     config_path: configPath,
                     app_config_path: this.appConfigPath
                 })
             });
             
             const data = await response.json();
             
             if (data.success) {
                 this.currentRunId = data.run_id;
                 
                 // Build success message showing both workflows and pipelines
                 const items = [];
                 if (data.workflows_count > 0) {
                     items.push(`${data.workflows_count} workflow${data.workflows_count !== 1 ? 's' : ''}`);
                 }
                 if (data.pipelines_count > 0) {
                     items.push(`${data.pipelines_count} pipeline${data.pipelines_count !== 1 ? 's' : ''}`);
                 }
                 const itemsText = items.length > 0 ? items.join(' and ') : 'items';
                 
                 this.showMessage(`Export started successfully: ${itemsText} (${data.total_items || 0} total)`, 'success');
                 this.startSmartPolling();
             } else {
                 this.hideProgress();
                 this.showMessage(data.message || 'Failed to start export', 'error');
             }
             
         } catch (error) {
             console.error('Export error:', error);
             this.hideProgress();
             this.showMessage('Failed to start export. Please try again.', 'error');
         }
        },
        
        // Start export UI
        startExport() {
            this.isExporting = true;
            this.showProgress = true;
            this.showResults = false;
            this.progress.startTime = new Date();
            this.progress.percentage = 10;
            this.progress.status = 'Starting export workflow...';
        },
        
                 // Poll for status with smart progress
         startSmartPolling() {
             this.pollInterval = setInterval(async () => {
                 try {
                     const response = await fetch(`/export/status/${this.currentRunId}`);
                     const data = await response.json();
                     
                     if (data.success) {
                         this.updateSmartProgress(data);
                         
                         // Check if completed
                         if (this.isTerminalStatus(data.status)) {
                             this.stopPolling();
                             this.handleCompletion(data);
                         }
                     } else {
                         this.stopPolling();
                         this.showError('Failed to get export status');
                     }
                 } catch (error) {
                     console.error('Polling error:', error);
                     this.stopPolling();
                     this.showError('Lost connection to export status');
                 }
             }, 2000); // Poll every 2 seconds for more responsive updates
         },
         
         // Fallback polling (for backwards compatibility)
         startPolling() {
             this.startSmartPolling();
         },
        
                 // Update progress with smart calculation
         updateSmartProgress(data) {
             if (!data.success) {
                 this.stopPolling();
                 this.showError(data.message || 'Failed to get status update');
                 return;
             }
             
             this.progress.elapsedTime = Math.floor(data.elapsed_time || 0);
             
             // Use smart progress calculation based on estimated timing
             if (this.smartProgress.estimatedDuration > 0) {
                 this.progress.percentage = Math.min(95, this.getSmartProgress(this.progress.elapsedTime));
                 this.progress.status = this.getSmartProgressStatus(this.progress.elapsedTime);
             } else {
                 // Fallback to regular progress if smart progress isn't available
                 this.progress.percentage = data.progress || 50;
                 this.progress.status = `Export running... (${this.elapsedTimeFormatted} elapsed)`;
             }
             
             // Show additional context
             if (data.databricks_state && data.databricks_state !== 'UNKNOWN') {
                 this.progress.status += ` [${data.databricks_state}]`;
             }
             
             // Show cluster type info during initialization phase
             if (this.smartProgress.phase === 'initializing' && this.smartProgress.clusterType !== 'unknown') {
                 const clusterTypeLabels = {
                     'job_cluster': 'Job Cluster',
                     'serverless_performance': 'Serverless (Perf)',
                     'serverless': 'Serverless',
                     'existing_cluster': 'Existing Cluster'
                 };
                 this.progress.status += ` | ${clusterTypeLabels[this.smartProgress.clusterType]}`;
             }
             
             // Update workflow run URL if available
             if (data.workflow_run_url) {
                 this.currentWorkflowRunUrl = data.workflow_run_url;
             }
         },
         
         // Update progress (fallback for backwards compatibility)
         updateProgress(data) {
             this.progress.percentage = Math.min(90, data.progress || 50);
             this.progress.elapsedTime = Math.floor(data.elapsed_time || 0);
             this.progress.status = `Export running... (${this.elapsedTimeFormatted} elapsed)`;
         },
        
                 // Handle completion
         handleCompletion(data) {
             this.hideProgress();
             this.showResults = true;
             
             if (data.status === 'success') {
                 this.results.success = true;
                 this.results.message = 'Export completed successfully!';
                 this.results.exportedJobs = data.exported_jobs || [];
                 this.results.workspaceUrl = data.workspace_url || '';
                 this.showMessage('Export completed successfully!', 'success');
             } else {
                 this.results.success = false;
                 this.results.message = data.error || data.message || 'Export failed';
                 this.showMessage(this.results.message, 'error');
             }
             
             // Clear the session state since export is complete
             this.clearExportSession();
         },
         
         // Clear export session from server
         async clearExportSession() {
             try {
                 await fetch('/export/clear-session', { method: 'POST' });
                 console.log('Export session cleared');
             } catch (error) {
                 console.warn('Could not clear export session:', error);
             }
         },
        
        // Helper methods
        isTerminalStatus(status) {
            const terminalStates = ['success', 'successful', 'completed', 'failed', 'failure', 'error', 'cancelled', 'canceled'];
            return terminalStates.includes((status || '').toLowerCase());
        },
        
        stopPolling() {
            if (this.pollInterval) {
                clearInterval(this.pollInterval);
                this.pollInterval = null;
            }
        },
        
        hideProgress() {
            this.showProgress = false;
            this.isExporting = false;
        },
        
                 resetExport() {
             this.stopPolling();
             this.hideProgress();
             this.showResults = false;
             this.currentRunId = null;
             this.currentWorkflowRunUrl = null;
             this.lastKnownExportId = null;
             this.progress = {
                 percentage: 0,
                 status: 'Initializing...',
                 startTime: null,
                 elapsedTime: 0
             };
             
             // Reset smart progress state
             this.smartProgress = {
                 estimatedDuration: 0,
                 initializationTime: 0,
                 workflowProcessingTime: 0,
                 pipelineProcessingTime: 0,
                 totalItems: 0,
                 clusterType: 'unknown',
                 jobDetails: null,
                 phase: 'initializing',
                 phasesInfo: {
                     initialization: { duration: 0, label: 'Starting cluster and initializing...' },
                     processing: { duration: 0, label: 'Processing items...' }
                 }
             };
             
             this.results = {
                 success: false,
                 message: '',
                 exportedJobs: [],
                 workspaceUrl: ''
             };
             
             // Reset workflow/pipeline details display
             this.showWorkflowDetails = false;
             this.showPipelineDetails = false;
         },
        
        // Computed properties
        get elapsedTimeFormatted() {
            const minutes = Math.floor(this.progress.elapsedTime / 60);
            const seconds = this.progress.elapsedTime % 60;
            return minutes > 0 ? `${minutes}m ${seconds}s` : `${seconds}s`;
        },
        
        get progressWidth() {
            return `${this.progress.percentage}%`;
        },
        
        get canExport() {
            return !this.isExporting && !!this.exportJobInfo;
        },
        
        // Utility
        showMessage(message, type = 'info') {
            if (this.$store?.app?.showMessage) {
                this.$store.app.showMessage(message, type);
            } else {
                console.log(`[${type.toUpperCase()}] ${message}`);
            }
        },
        
        showError(message) {
            this.showMessage(message, 'error');
        }
    }));
}); 