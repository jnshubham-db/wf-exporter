# Databricks Workflow Manager

A comprehensive web application for managing and exporting Databricks workflows and Lakeflow pipelines with advanced configuration management and state persistence.

## 🚀 Features

### Workflow Management
- **Interactive Job Selection**: Browse, search, and select Databricks jobs with pagination
- **Export Libraries Toggle**: Configure per-workflow library export settings
- **Workflow Caching**: Background caching for improved performance
- **Configuration Preview**: Structured previews showing workflow details and export settings

### Lakeflow Pipeline Support *(New in V3)*
- **Pipeline Selection**: Full feature parity with workflow selection
- **GUID Support**: Native support for pipeline IDs in GUID format
- **Pipeline Caching**: Optimized pipeline data loading and caching
- **Export Libraries**: Per-pipeline library export configuration

### Centralized Configuration Management *(New in V3)*
- **app_config.yml**: Centralized export job configuration
- **CRUD Operations**: Load, edit, validate, and save configurations via Databricks SDK
- **Default Generation**: Automatic creation of default configurations
- **Real-time Validation**: Live validation of configuration syntax and structure

### Export State Persistence *(New in V3)*
- **File-based Storage**: Persistent export state across server restarts
- **State Recovery**: Automatic recovery of active exports on startup
- **Session Management**: Seamless state management across page refreshes
- **Background Cleanup**: Automatic cleanup of expired export states

### Enhanced Export Experience *(New in V3)*
- **Unified Progress Tracking**: Combined workflow and pipeline progress monitoring
- **Direct Links**: Direct links to running workflows in Databricks workspace
- **Smart Estimations**: Intelligent time estimation based on cluster types
- **Real-time Updates**: Live progress updates with detailed status information

## 📋 Configuration Structure

### Main Configuration (config.yml)
```yaml
workflows:
  - job_name: "Sample_Job"
    job_id: 810105583362026
    is_existing: true
    is_active: true
    export_libraries: true

pipelines:
  - pipeline_name: "sample_pipeline"
    pipeline_id: "d4623163-fa76-4fb3-a117-663851c9f32f"
    is_existing: true
    is_active: true
    export_libraries: false

initial_variables:
  v_start_path: "/Workspace/Shared/exports"
```

### App Configuration (app_config.yml) *(New in V3)*
```yaml
export-job:
  job_name: "Export_Workflow_Job"
  job_id: 1290381902
```

## 🛠 Installation & Setup

### Prerequisites
- Python 3.8+
- Access to Databricks workspace
- Databricks SDK configured with appropriate permissions

### Installation
1. Clone the repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Configure Databricks credentials (see Databricks SDK documentation)
4. Run the application:
   ```bash
   python main.py
   ```

## 🎯 Quick Start Guide

### 1. Initial Setup
1. Navigate to **Configuration** page
2. Set your main config file path (e.g., `/Workspace/Shared/config.yml`)
3. Load or create your configuration

### 2. Configure Export Job *(New in V3)*
1. Go to **Export** page
2. Set app config file path (e.g., `/Workspace/Shared/app_config.yml`)
3. Create or load your app configuration
4. Validate the export job settings

### 3. Select Workflows
1. Navigate to **Jobs** page
2. Load available jobs from your workspace
3. Select desired workflows
4. Configure export libraries for each workflow
5. Update configuration

### 4. Select Pipelines *(New in V3)*
1. Navigate to **Lakeflow Pipelines** page
2. Load available pipelines from your workspace
3. Select desired pipelines
4. Configure export libraries for each pipeline
5. Update configuration

### 5. Export
1. Return to **Export** page
2. Verify configuration summary shows both workflows and pipelines
3. Ensure app configuration is loaded and valid
4. Click "Start Export"
5. Monitor progress with real-time updates
6. Access direct links to running workflows

## 📖 Detailed Features

### Workflow Selection
- **Search**: Fuzzy search across job names
- **Pagination**: Efficient browsing of large job lists
- **Filtering**: Filter by job status and properties
- **Export Libraries**: Per-job library export configuration
- **Configuration Preview**: Real-time YAML preview with validation

### Pipeline Selection *(New in V3)*
- **Full Feature Parity**: All workflow features available for pipelines
- **GUID Support**: Native handling of pipeline IDs in GUID format
- **Pipeline Metadata**: Display creator, state, and pipeline details
- **Unified Interface**: Consistent UI/UX with workflow selection

### Export Management
- **Simplified Triggering**: No manual job selection required
- **Progress Monitoring**: Real-time status updates with smart estimation
- **State Persistence**: Survives page refreshes and server restarts
- **Error Handling**: Comprehensive error reporting and recovery
- **Direct Access**: Links to running workflows in Databricks

### Configuration Management
- **Dual Configuration**: Separate main config and app config files
- **Live Validation**: Real-time syntax and structure validation
- **CRUD Operations**: Full create, read, update, delete functionality
- **Default Generation**: Automatic creation of standard configurations
- **Structured Previews**: Enhanced previews with item counts and summaries

## 🔧 API Reference

### Export Endpoints
- `POST /export/trigger` - Trigger export with app_config.yml
- `GET /export/status/<run_id>` - Get export status with workflow links
- `GET /export/active-exports` - List all active exports
- `POST /export/recover-state/<run_id>` - Recover export state

### App Config Endpoints *(New in V3)*
- `POST /export/app-config/load` - Load app configuration
- `POST /export/app-config/save` - Save app configuration
- `POST /export/app-config/validate` - Validate app configuration
- `POST /export/app-config/create-default` - Create default configuration

### Pipeline Endpoints *(New in V3)*
- `GET /pipelines/` - Pipeline selection page
- `POST /pipelines/select` - Update selected pipelines
- `GET /api/pipelines` - Get available pipelines with caching

## 📊 State Management

### Export State Service *(New in V3)*
The application includes a robust state management system:

- **Persistent Storage**: Export states stored in `export_state.json`
- **Automatic Recovery**: Active exports recovered on application startup
- **Background Cleanup**: Expired states cleaned up automatically (24-hour retention)
- **Thread Safety**: Concurrent access handled with proper locking
- **Session Integration**: Seamless integration with Flask sessions

### Caching System
- **Job Caching**: Background loading and caching of Databricks jobs
- **Pipeline Caching**: Dedicated caching for Lakeflow pipelines
- **Cache Validation**: Automatic cache invalidation and refresh
- **Performance Optimization**: Reduced API calls and improved response times

## 🚨 Error Handling

### Comprehensive Error Management
- **Validation Errors**: Clear error messages for configuration issues
- **API Failures**: Graceful handling of Databricks API failures
- **State Recovery**: Automatic recovery from unexpected failures
- **User Feedback**: Detailed error reporting with suggested actions

### Logging
- **Structured Logging**: Comprehensive logging across all components
- **Export Tracking**: Detailed tracking of export operations
- **Error Reporting**: Automatic error reporting with context
- **Debug Information**: Extensive debug information for troubleshooting

## 🔒 Security Considerations

- **Databricks Authentication**: Secure authentication via Databricks SDK
- **Session Management**: Secure session handling with appropriate timeouts
- **Input Validation**: Comprehensive validation of all user inputs
- **File Access**: Controlled access to workspace files via Databricks APIs

## 📈 Performance Features

### Optimization Strategies
- **Background Loading**: Non-blocking data loading with progress indicators
- **Intelligent Caching**: Smart caching strategies for improved performance
- **Pagination**: Efficient handling of large datasets
- **Progress Estimation**: Smart progress estimation based on cluster types

### Scalability
- **Thread Safety**: Proper handling of concurrent operations
- **Resource Management**: Efficient resource usage and cleanup
- **State Cleanup**: Automatic cleanup of expired data
- **Memory Management**: Optimized memory usage patterns

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

For support and questions:
1. Check the logs for detailed error information
2. Verify Databricks connectivity and permissions
3. Ensure proper configuration file structure
4. Review the API documentation for endpoint usage

---

## Version History

### V3.0 *(Latest)*
- ✅ Lakeflow Pipeline Support
- ✅ Centralized App Configuration Management
- ✅ Export State Persistence
- ✅ Enhanced Progress Tracking
- ✅ Direct Workflow Links
- ✅ Export Libraries Configuration
- ✅ Unified Configuration Previews

### V2.0
- Job Selection and Configuration
- Export Workflow Management  
- Basic Progress Tracking
- Configuration Management

### V1.0
- Initial Release
- Basic Databricks Integration
