from flask import Blueprint, render_template, request, jsonify, session
from ..services import DatabricksService, ConfigService, ExportStateService
import logging
import time
from typing import Dict, Any

export_bp = Blueprint('export', __name__)
logger = logging.getLogger(__name__)


@export_bp.route('/')
def export_workflow():
    """Export workflow page."""
    return render_template('export.html')


# ====================================================================================
# LEGACY EXPORT TRIGGER (COMMENTED OUT - Replaced by simplified app_config.yml version)
# ====================================================================================
# @export_bp.route('/trigger', methods=['POST'])
# def trigger_export_legacy():
#     """[LEGACY] Trigger Databricks workflow export with manual job selection."""
#     try:
#         data = request.get_json()
#         config_path = data.get('config_path', '').strip()
#         job_id = data.get('job_id')
#         
#         if not config_path:
#             return jsonify({
#                 'success': False,
#                 'message': 'Configuration path is required'
#             }), 400
#         
#         if not job_id:
#             return jsonify({
#                 'success': False,
#                 'message': 'Job ID is required'
#             }), 400
#         
#         try:
#             job_id = int(job_id)
#         except (ValueError, TypeError):
#             return jsonify({
#                 'success': False,
#                 'message': 'Job ID must be a valid integer'
#             }), 400
#         
#         # Load and validate configuration
#         config_service = ConfigService()
#         databricks_service = DatabricksService()
#         
#         # Read configuration content
#         try:
#             config_content = databricks_service.read_workspace_file(config_path)
#             config_data = config_service.validate_yaml(config_content)
#         except Exception as e:
#             return jsonify({
#                 'success': False,
#                 'message': f'Failed to load configuration from Databricks workspace: {str(e)}'
#             }), 400
#         
#         # Extract workflows and v_start_path
#         workflows = config_data.get('workflows', [])
#         # Look for v_start_path in initial_variables first, then fall back to export section
#         v_start_path = config_data.get('initial_variables', {}).get('v_start_path', 
#                                       config_data.get('export', {}).get('v_start_path', '/Workspace/Shared/exports'))
#         
#         logger.info(f"Extracted v_start_path: {v_start_path} from config: {config_path}")
#         
#         if not workflows:
#             return jsonify({
#                 'success': False,
#                 'message': 'No workflows found in configuration'
#             }), 400
#         
#         # Use the provided job_id to trigger the export workflow
#         # This job should be configured to handle the workflow export process
#         
#         # Trigger the workflow with config path as parameter
#         run_id = databricks_service.trigger_workflow(config_path, job_id)
#         
#         # Store export session data
#         session['current_export'] = {
#             'run_id': run_id,
#             'config_path': config_path,
#             'workflows': workflows,
#             'v_start_path': v_start_path,
#             'start_time': time.time(),
#             'status': 'running'
#         }
#         
#         logger.info(f"Export workflow triggered: run_id={run_id}, config_path={config_path}")
#         
#         return jsonify({
#             'success': True,
#             'run_id': run_id,
#             'message': 'Export workflow triggered successfully',
#             'config_path': config_path,
#             'workflows_count': len(workflows)
#         })
#         
#     except Exception as e:
#         logger.error(f"Error triggering export: {e}")
#         return jsonify({
#             'success': False,
#             'message': f'Failed to trigger export: {str(e)}'
#         }), 500


@export_bp.route('/trigger', methods=['POST'])
def trigger_export():
    """Trigger Databricks workflow export using app_config.yml for job selection."""
    try:
        data = request.get_json()
        config_path = data.get('config_path', '').strip()
        app_config_path = data.get('app_config_path', '').strip()
        
        if not config_path:
            return jsonify({
                'success': False,
                'message': 'Configuration path is required'
            }), 400
        
        if not app_config_path:
            return jsonify({
                'success': False,
                'message': 'App configuration path is required'
            }), 400
        
        # Load and validate configurations
        config_service = ConfigService()
        databricks_service = DatabricksService()
        
        # Read and validate main configuration
        try:
            config_content = databricks_service.read_workspace_file(config_path)
            config_data = config_service.validate_yaml(config_content)
        except Exception as e:
            return jsonify({
                'success': False,
                'message': f'Failed to load configuration from Databricks workspace: {str(e)}'
            }), 400
        
        # Read and validate app configuration
        try:
            app_config_content = config_service.load_app_config(app_config_path)
            export_job = config_service.get_export_job_from_app_config(app_config_content)
        except Exception as e:
            return jsonify({
                'success': False,
                'message': f'Failed to load app configuration: {str(e)}'
            }), 400
        
        if not export_job or 'job_id' not in export_job:
            return jsonify({
                'success': False,
                'message': 'No valid export job found in app configuration'
            }), 400
        
        job_id = export_job['job_id']
        job_name = export_job.get('job_name', f'Job {job_id}')
        
        # Extract workflows, pipelines, and v_start_path
        workflows = config_data.get('workflows', [])
        pipelines = config_data.get('pipelines', [])
        
        # Look for v_start_path in initial_variables first, then fall back to export section
        v_start_path = config_data.get('initial_variables', {}).get('v_start_path', 
                                      config_data.get('export', {}).get('v_start_path', '/Workspace/Shared/exports'))
        
        logger.info(f"Using export job: {job_name} (ID: {job_id}) from app config: {app_config_path}")
        logger.info(f"Extracted v_start_path: {v_start_path} from config: {config_path}")
        logger.info(f"Found {len(workflows)} workflows and {len(pipelines)} pipelines to export")
        
        if not workflows and not pipelines:
            return jsonify({
                'success': False,
                'message': 'No workflows or pipelines found in configuration'
            }), 400
        
        # Trigger the workflow with config path as parameter
        run_id = databricks_service.trigger_workflow(config_path, job_id)
        
        # Store export session data with enhanced information
        export_state = {
            'run_id': run_id,
            'config_path': config_path,
            'app_config_path': app_config_path,
            'export_job': export_job,
            'workflows': workflows,
            'pipelines': pipelines,
            'v_start_path': v_start_path,
            'start_time': time.time(),
            'status': 'running'
        }
        
        # Store in session for immediate access
        session['current_export'] = export_state
        
        # Store in persistent storage for recovery
        export_state_service = ExportStateService()
        export_state_service.save_export_state(str(run_id), export_state)
        
        logger.info(f"Export workflow triggered: run_id={run_id}, export_job={job_name}")
        
        return jsonify({
            'success': True,
            'run_id': run_id,
            'message': f'Export workflow triggered successfully using {job_name}',
            'config_path': config_path,
            'app_config_path': app_config_path,
            'export_job': export_job,
            'workflows_count': len(workflows),
            'pipelines_count': len(pipelines),
            'total_items': len(workflows) + len(pipelines)
        })
        
    except Exception as e:
        logger.error(f"Error triggering export: {e}")
        return jsonify({
            'success': False,
            'message': f'Failed to trigger export: {str(e)}'
        }), 500


@export_bp.route('/status/<run_id>')
def export_status(run_id):
    """Check export workflow status."""
    try:

        
        # Get export session data from session first, then try persistent storage
        export_data = session.get('current_export', {})
        
        # If not in session or different run_id, try persistent storage
        if not export_data or export_data.get('run_id') != run_id:
            export_state_service = ExportStateService()
            persistent_data = export_state_service.get_export_state(run_id)
            
            if persistent_data:
                export_data = persistent_data
                # Restore to session for future requests
                session['current_export'] = export_data
                logger.info(f"Export state recovered from persistent storage for run_id: {run_id}")
            else:
                return jsonify({
                    'success': False,
                    'message': 'Export run not found in session or persistent storage'
                }), 404
        
        databricks_service = DatabricksService()
        
        # Get workflow run status from Databricks
        run_info = databricks_service.get_workflow_run_status(run_id)
        
        # Extract status values (they come as enum strings like "RunLifeCycleState.RUNNING")
        lifecycle_raw = run_info.get('status', 'UNKNOWN')
        result_raw = run_info.get('result_state', '')
        
        # Extract the actual state name from enum string (e.g., "RunLifeCycleState.RUNNING" -> "RUNNING")
        if '.' in lifecycle_raw:
            lifecycle_state = lifecycle_raw.split('.')[-1].upper()
        else:
            lifecycle_state = lifecycle_raw.upper()
            
        if result_raw and '.' in result_raw:
            result_state = result_raw.split('.')[-1].upper()
        else:
            result_state = result_raw.upper() if result_raw else ''
        
        logger.info(f"Parsed Databricks states - Lifecycle: {lifecycle_state}, Result: {result_state}")
        
        # Map Databricks states to our application states
        # Check lifecycle state first
        if lifecycle_state in ['PENDING', 'RUNNING', 'QUEUED']:
            app_status = 'running'
            progress = 50 if lifecycle_state == 'RUNNING' else 20
        elif lifecycle_state == 'TERMINATED':
            # Job finished, check result state to determine success/failure
            if result_state == 'SUCCESS':
                app_status = 'success'
                progress = 100
                # Update session and persistent status
                export_data['status'] = 'success'
                session['current_export'] = export_data
                # Update persistent storage
                export_state_service = ExportStateService()
                export_state_service.update_export_status(run_id, 'success')
            elif result_state in ['FAILED', 'TIMEDOUT', 'CANCELLED']:
                app_status = 'failed'
                progress = 0
                # Update session and persistent status
                export_data['status'] = 'failed'
                session['current_export'] = export_data
                # Update persistent storage
                export_state_service = ExportStateService()
                export_state_service.update_export_status(run_id, 'failed')
            else:
                # Terminated but unknown result - treat as failed
                app_status = 'failed'
                progress = 0
                export_data['status'] = 'failed'
                session['current_export'] = export_data
                # Update persistent storage
                export_state_service = ExportStateService()
                export_state_service.update_export_status(run_id, 'failed')
        elif lifecycle_state in ['CANCELLED', 'INTERNAL_ERROR']:
            app_status = 'failed'
            progress = 0
            # Update session and persistent status
            export_data['status'] = 'failed'
            session['current_export'] = export_data
            # Update persistent storage
            export_state_service = ExportStateService()
            export_state_service.update_export_status(run_id, 'failed')
        else:
            # Unknown state, keep as running to continue polling
            app_status = 'running'
            progress = 30
        
        # Calculate elapsed time
        start_time = export_data.get('start_time', time.time())
        elapsed_time = time.time() - start_time
        
        # Build workflow run URL
        export_job = export_data.get('export_job', {})
        job_id = export_job.get('job_id') if export_job else None
        workflow_run_url = None
        
        if job_id:
            try:
                workflow_run_url = databricks_service.build_workflow_run_url(job_id, run_id)
            except Exception as e:
                logger.warning(f"Could not build workflow run URL: {e}")
        
        response_data = {
            'success': True,
            'run_id': run_id,
            'status': app_status,
            'progress': progress,
            'elapsed_time': elapsed_time,
            'databricks_state': lifecycle_state,
            'databricks_result': result_state,
            'raw_lifecycle': lifecycle_raw,
            'raw_result': result_raw,
            'start_time': export_data.get('start_time'),
            'workflow_run_url': workflow_run_url
        }
        
        # If completed, add additional information
        if app_status in ['success', 'failed']:
            workflows = export_data.get('workflows', [])
            v_start_path = export_data.get('v_start_path', '/Workspace/Shared/exports')
            
            if app_status == 'success':
                exported_jobs = [w.get('job_name', f"Job {w.get('job_id')}") for w in workflows if w.get('is_active', True)]
                workspace_url = databricks_service.build_workspace_url(v_start_path)
                
                response_data.update({
                    'exported_jobs': exported_jobs,
                    'workspace_url': workspace_url,
                    'v_start_path': v_start_path
                })
            else:
                response_data['error'] = run_info.get('state_message', 'Workflow execution failed')
        
        return jsonify(response_data)
        
    except Exception as e:
        logger.error(f"Error getting export status: {e}")
        return jsonify({
            'success': False,
            'message': f'Failed to get export status: {str(e)}'
        }), 500


@export_bp.route('/validate', methods=['POST'])
def validate_export():
    """Validate configuration before export."""
    try:
        data = request.get_json()
        config_path = data.get('config_path', '').strip()
        
        if not config_path:
            return jsonify({
                'success': False,
                'message': 'Configuration path is required'
            }), 400
        
        config_service = ConfigService()
        
        # Read and validate configuration
        databricks_service = DatabricksService()
        config_content = databricks_service.read_workspace_file(config_path)
        
        config_data = config_service.validate_yaml(config_content)
        
        # Validate export requirements
        workflows = config_data.get('workflows', [])
        # Look for v_start_path in initial_variables first, then fall back to export section
        v_start_path = config_data.get('initial_variables', {}).get('v_start_path', 
                                      config_data.get('export', {}).get('v_start_path'))
        
        validation_results = {
            'success': True,
            'message': 'Configuration is valid for export',
            'details': {
                'has_workflows': len(workflows) > 0,
                'workflows_count': len(workflows),
                'active_workflows': len([w for w in workflows if w.get('is_active', True)]),
                'has_v_start_path': bool(v_start_path),
                'v_start_path': v_start_path
            }
        }
        
        # Check for potential issues
        warnings = []
        if not workflows:
            warnings.append('No workflows defined in configuration')
        elif not any(w.get('is_active', True) for w in workflows):
            warnings.append('No active workflows found')
        
        if not v_start_path:
            warnings.append('No v_start_path specified in export configuration')
        
        if warnings:
            validation_results['warnings'] = warnings
        
        return jsonify(validation_results)
        
    except Exception as e:
        logger.error(f"Error validating export configuration: {e}")
        return jsonify({
            'success': False,
            'message': f'Validation failed: {str(e)}'
        }), 500


@export_bp.route('/current')
def get_current_export():
    """Get current export status from session."""
    try:
        export_data = session.get('current_export')
        
        if not export_data:
            return jsonify({
                'success': True,
                'has_active_export': False,
                'message': 'No active export found'
            })
        
        return jsonify({
            'success': True,
            'has_active_export': True,
            'export_data': {
                'run_id': export_data.get('run_id'),
                'config_path': export_data.get('config_path'),
                'status': export_data.get('status'),
                'workflows_count': len(export_data.get('workflows', [])),
                'start_time': export_data.get('start_time')
            }
        })
        
    except Exception as e:
        logger.error(f"Error getting current export: {e}")
        return jsonify({
            'success': False,
            'message': f'Failed to get current export: {str(e)}'
        }), 500


@export_bp.route('/clear-session', methods=['POST'])
def clear_export_session():
    """Clear the current export session."""
    try:
        # Clear export-related session data
        session.pop('current_export', None)
        
        logger.info("Export session cleared")
        
        return jsonify({
            'success': True,
            'message': 'Export session cleared successfully'
        })
        
    except Exception as e:
        logger.error(f"Error clearing export session: {e}")
        return jsonify({
            'success': False,
            'message': f'Failed to clear export session: {str(e)}'
        }), 500


@export_bp.route('/test-status/<run_id>')
def test_status_values(run_id):
    """Test endpoint to validate actual Databricks SDK status values."""
    try:
        databricks_service = DatabricksService()
        
        # Get raw run info directly from Databricks SDK
        raw_run = databricks_service.client.jobs.get_run(int(run_id))
        
        # Extract all possible status information
        status_debug = {
            'raw_run_id': raw_run.run_id,
            'has_state': hasattr(raw_run, 'state') and raw_run.state is not None,
        }
        
        if raw_run.state:
            state = raw_run.state
            status_debug.update({
                'lifecycle_state_type': type(state.life_cycle_state).__name__ if state.life_cycle_state else 'None',
                'lifecycle_state_value': str(state.life_cycle_state) if state.life_cycle_state else 'None',
                'result_state_type': type(state.result_state).__name__ if state.result_state else 'None',
                'result_state_value': str(state.result_state) if state.result_state else 'None',
                'state_message': state.state_message if hasattr(state, 'state_message') else 'No message',
                'state_attributes': [attr for attr in dir(state) if not attr.startswith('_')]
            })
            
            # Try to get the actual enum values
            try:
                if state.life_cycle_state:
                    if hasattr(state.life_cycle_state, 'value'):
                        status_debug['lifecycle_state_enum_value'] = state.life_cycle_state.value
                    if hasattr(state.life_cycle_state, 'name'):
                        status_debug['lifecycle_state_enum_name'] = state.life_cycle_state.name
                        
                if state.result_state:
                    if hasattr(state.result_state, 'value'):
                        status_debug['result_state_enum_value'] = state.result_state.value
                    if hasattr(state.result_state, 'name'):
                        status_debug['result_state_enum_name'] = state.result_state.name
            except Exception as e:
                status_debug['enum_extraction_error'] = str(e)
        
        # Also test our current service method
        try:
            service_response = databricks_service.get_workflow_run_status(run_id)
            status_debug['service_response'] = service_response
        except Exception as e:
            status_debug['service_error'] = str(e)
        
        return jsonify({
            'success': True,
            'debug_info': status_debug
        })
        
    except Exception as e:
        logger.error(f"Error in test status endpoint: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@export_bp.route('/debug-job/<int:job_id>')
def debug_job_structure(job_id):
    """Debug endpoint to inspect job structure for cluster type detection."""
    try:
        databricks_service = DatabricksService()
        
        # Get raw job object
        raw_job = databricks_service.client.jobs.get(job_id)
        
        debug_info = {
            'job_id': job_id,
            'job_name': raw_job.settings.name if raw_job.settings else 'No name',
            'has_settings': hasattr(raw_job, 'settings') and raw_job.settings is not None,
            'has_tasks': False,
            'tasks_count': 0,
            'first_task_attributes': [],
            'first_task_string': '',
            'cluster_detection_debug': {}
        }
        
        if raw_job.settings and hasattr(raw_job.settings, 'tasks') and raw_job.settings.tasks:
            debug_info['has_tasks'] = True
            debug_info['tasks_count'] = len(raw_job.settings.tasks)
            
            first_task = raw_job.settings.tasks[0]
            debug_info['first_task_attributes'] = [attr for attr in dir(first_task) if not attr.startswith('_')]
            debug_info['first_task_string'] = str(first_task)[:500]  # Truncate for readability
            
            # Test each detection method
            detection_debug = {}
            
            # SQL warehouse check
            sql_task = getattr(first_task, 'sql_task', None)
            if sql_task:
                detection_debug['sql_task'] = {
                    'exists': True,
                    'warehouse_id': getattr(sql_task, 'warehouse_id', None)
                }
            
            # Compute key check
            compute_key = getattr(first_task, 'compute_key', None)
            detection_debug['compute_key'] = compute_key
            
            # Environment key check
            environment_key = getattr(first_task, 'environment_key', None)
            detection_debug['environment_key'] = environment_key
            
            # Traditional cluster checks
            detection_debug['new_cluster'] = getattr(first_task, 'new_cluster', None) is not None
            detection_debug['existing_cluster_id'] = getattr(first_task, 'existing_cluster_id', None)
            detection_debug['job_cluster_key'] = getattr(first_task, 'job_cluster_key', None)
            
            # Notebook task check
            notebook_task = getattr(first_task, 'notebook_task', None)
            detection_debug['notebook_task'] = notebook_task is not None
            
            # String-based detection
            task_str = str(first_task).lower()
            detection_debug['contains_serverless'] = 'serverless' in task_str
            detection_debug['contains_performance'] = 'performance' in task_str
            
            debug_info['cluster_detection_debug'] = detection_debug
        
        # Also get the processed job details
        try:
            job_details = databricks_service.get_job_details(job_id)
            debug_info['processed_job_details'] = {
                'cluster_type': job_details.get('cluster_type'),
                'cluster_info': job_details.get('cluster_info')
            }
        except Exception as e:
            debug_info['processed_job_details'] = {'error': str(e)}
        
        return jsonify({
            'success': True,
            'debug_info': debug_info
        })
        
    except Exception as e:
        logger.error(f"Error debugging job structure for {job_id}: {e}")
        return jsonify({
            'success': False,
            'message': f'Failed to debug job structure: {str(e)}'
        }), 500


@export_bp.route('/app-config/load', methods=['POST'])
def load_app_config():
    """Load app_config.yml from Databricks workspace."""
    try:
        data = request.get_json()
        config_path = data.get('path', '').strip()
        
        if not config_path:
            return jsonify({
                'success': False,
                'message': 'App config path is required'
            }), 400
        
        config_service = ConfigService()
        content = config_service.load_app_config(config_path)
        
        # Validate the loaded content
        export_job = config_service.get_export_job_from_app_config(content)
        
        logger.info(f"App config loaded successfully from {config_path}")
        
        return jsonify({
            'success': True,
            'content': content,
            'export_job': export_job,
            'message': f'App configuration loaded from {config_path}'
        })
        
    except FileNotFoundError as e:
        logger.error(f"App config file not found: {e}")
        return jsonify({
            'success': False,
            'message': f'App configuration file not found: {str(e)}'
        }), 404
    except Exception as e:
        logger.error(f"Error loading app config: {e}")
        return jsonify({
            'success': False,
            'message': f'Failed to load app configuration: {str(e)}'
        }), 500


@export_bp.route('/app-config/save', methods=['POST'])
def save_app_config():
    """Save app_config.yml to local filesystem."""
    try:
        data = request.get_json()
        config_path = data.get('config_path', data.get('path', '')).strip()
        content = data.get('content', '').strip()
        
        if not config_path:
            return jsonify({
                'success': False,
                'message': 'App config path is required'
            }), 400
        
        if not content:
            return jsonify({
                'success': False,
                'message': 'App config content is required'
            }), 400
        
        # Validate the content before saving
        config_service = ConfigService()
        config_service.validate_app_config(content)
        
        # Save to local filesystem
        import os
        
        # If path is relative, make it relative to the application root
        if not os.path.isabs(config_path):
            # Get the application root directory (where main.py is located)
            app_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
            full_path = os.path.join(app_root, config_path)
        else:
            full_path = config_path
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        
        # Write the file
        with open(full_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        logger.info(f"App config saved successfully to local file: {full_path}")
        
        return jsonify({
            'success': True,
            'message': f'App configuration saved to {config_path}'
        })
        
    except Exception as e:
        logger.error(f"Error saving app config: {e}")
        return jsonify({
            'success': False,
            'message': f'Failed to save app configuration: {str(e)}'
        }), 500


@export_bp.route('/app-config/validate', methods=['POST'])
def validate_app_config():
    """Validate app_config.yml content."""
    try:
        data = request.get_json()
        content = data.get('content', '').strip()
        
        if not content:
            return jsonify({
                'success': False,
                'message': 'App config content is required'
            }), 400
        
        config_service = ConfigService()
        config_data = config_service.validate_app_config(content)
        export_job = config_service.get_export_job_from_app_config(content)
        
        logger.info("App config validation successful")
        
        return jsonify({
            'success': True,
            'message': 'App configuration is valid',
            'export_job': export_job,
            'config_data': config_data
        })
        
    except Exception as e:
        logger.error(f"App config validation failed: {e}")
        return jsonify({
            'success': False,
            'message': f'App configuration validation failed: {str(e)}'
        }), 400


@export_bp.route('/app-config/create-default', methods=['POST'])
def create_default_app_config():
    """Create a default app_config.yml with provided job details."""
    try:
        data = request.get_json()
        job_name = data.get('job_name', '').strip()
        job_id = data.get('job_id')
        
        if not job_name:
            return jsonify({
                'success': False,
                'message': 'Job name is required'
            }), 400
        
        if not job_id or not isinstance(job_id, int):
            return jsonify({
                'success': False,
                'message': 'Valid job ID (integer) is required'
            }), 400
        
        config_service = ConfigService()
        content = config_service.create_default_app_config(job_name, job_id)
        
        logger.info(f"Default app config created for job: {job_name} (ID: {job_id})")
        
        return jsonify({
            'success': True,
            'content': content,
            'message': f'Default app configuration created for job: {job_name}'
        })
        
    except Exception as e:
        logger.error(f"Error creating default app config: {e}")
        return jsonify({
            'success': False,
            'message': f'Failed to create default app configuration: {str(e)}'
        }), 500


@export_bp.route('/active-exports', methods=['GET'])
def get_active_exports():
    """Get all currently active export runs."""
    try:
        export_state_service = ExportStateService()
        active_exports = export_state_service.get_active_exports()
        
        # Also include current session export if it exists
        session_export = session.get('current_export')
        if session_export and session_export.get('status') in ['running', 'pending']:
            run_id = str(session_export.get('run_id'))
            if run_id not in active_exports:
                active_exports[run_id] = session_export
        
        logger.info(f"Retrieved {len(active_exports)} active export runs")
        
        return jsonify({
            'success': True,
            'active_exports': active_exports,
            'count': len(active_exports)
        })
        
    except Exception as e:
        logger.error(f"Error getting active exports: {e}")
        return jsonify({
            'success': False,
            'message': f'Failed to get active exports: {str(e)}'
        }), 500


@export_bp.route('/recover-state/<run_id>', methods=['POST'])
def recover_export_state(run_id):
    """Recover export state for a specific run."""
    try:
        export_state_service = ExportStateService()
        export_state = export_state_service.get_export_state(run_id)
        
        if not export_state:
            return jsonify({
                'success': False,
                'message': f'No export state found for run_id: {run_id}'
            }), 404
        
        # Restore to session
        session['current_export'] = export_state
        
        logger.info(f"Export state recovered and restored to session for run_id: {run_id}")
        
        return jsonify({
            'success': True,
            'message': f'Export state recovered for run_id: {run_id}',
            'export_state': export_state
        })
        
    except Exception as e:
        logger.error(f"Error recovering export state for {run_id}: {e}")
        return jsonify({
            'success': False,
            'message': f'Failed to recover export state: {str(e)}'
        }), 500


@export_bp.route('/state-summary', methods=['GET'])
def get_export_state_summary():
    """Get summary of export state service."""
    try:
        export_state_service = ExportStateService()
        summary = export_state_service.get_state_summary()
        
        return jsonify({
            'success': True,
            'summary': summary
        })
        
    except Exception as e:
        logger.error(f"Error getting export state summary: {e}")
        return jsonify({
            'success': False,
            'message': f'Failed to get state summary: {str(e)}'
        }), 500