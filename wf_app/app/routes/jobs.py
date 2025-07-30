from flask import Blueprint, render_template, request, jsonify, session
from ..services import DatabricksService, ConfigService, JobCacheService
import logging

jobs_bp = Blueprint('jobs', __name__)
logger = logging.getLogger(__name__)


@jobs_bp.route('/')
def job_selection():
    """Job selection page."""
    return render_template('jobs.html')


@jobs_bp.route('/list', methods=['GET'])
def list_jobs():
    """List all Databricks jobs from cache."""
    try:
        # Get jobs from cache (much faster than API call)
        job_cache = JobCacheService()
        jobs = job_cache.get_jobs()
        
        logger.info(f"Successfully returned {len(jobs)} Databricks jobs from cache")
        
        return jsonify({
            'success': True,
            'jobs': jobs,
            'count': len(jobs),
            'from_cache': True
        })
        
    except Exception as e:
        logger.error(f"Error listing jobs from cache: {e}")
        return jsonify({
            'success': False,
            'message': f'Failed to list jobs: {str(e)}',
            'jobs': []
        }), 500



@jobs_bp.route('/details/<int:job_id>', methods=['GET'])
def get_job_details(job_id):
    """Get details for a specific job including cluster type for timing estimation."""
    try:
        databricks_service = DatabricksService()
        job_details = databricks_service.get_job_details(job_id)
        
        logger.info(f"Retrieved details for job {job_id}: cluster_type={job_details.get('cluster_type', 'unknown')}")
        
        return jsonify({
            'success': True,
            'job_details': job_details  # Changed from 'job' to 'job_details' to match frontend expectation
        })
        
    except Exception as e:
        logger.error(f"Error getting job details for {job_id}: {e}")
        return jsonify({
            'success': False,
            'message': f'Failed to get job details: {str(e)}'
        }), 500


@jobs_bp.route('/select', methods=['POST'])
def select_jobs():
    """Handle job selection and store in session."""
    try:
        data = request.get_json()
        jobs = data.get('jobs', [])
        
        if not jobs:
            return jsonify({
                'success': False,
                'message': 'No jobs selected'
            }), 400
        
        # Validate job structure
        for job in jobs:
            required_fields = ['job_id', 'name', 'is_existing', 'is_active']
            if not all(field in job for field in required_fields):
                return jsonify({
                    'success': False,
                    'message': 'Invalid job structure. Missing required fields.'
                }), 400
        
        # Store selected jobs in session
        session['selected_jobs'] = jobs
        
        logger.info(f"Selected {len(jobs)} jobs for configuration update")
        
        return jsonify({
            'success': True,
            'message': f'Successfully selected {len(jobs)} jobs',
            'selected_jobs': jobs
        })
        
    except Exception as e:
        logger.error(f"Error selecting jobs: {e}")
        return jsonify({
            'success': False,
            'message': f'Failed to select jobs: {str(e)}'
        }), 500


@jobs_bp.route('/update-config', methods=['POST'])
def update_config_with_jobs():
    """Update configuration file with selected jobs."""
    try:
        data = request.get_json()
        config_path = data.get('config_path', '').strip()
        config_content = data.get('config_content', '').strip()
        jobs = data.get('jobs', [])
        
        if not config_path:
            return jsonify({
                'success': False,
                'message': 'Configuration path is required'
            }), 400
        
        if not config_content:
            return jsonify({
                'success': False,
                'message': 'Configuration content is required'
            }), 400
        
        if not jobs:
            return jsonify({
                'success': False,
                'message': 'No jobs provided for configuration update'
            }), 400
        
        # Validate job structure
        for job in jobs:
            required_fields = ['job_id', 'name', 'is_existing', 'is_active', 'export_libraries']
            if not all(field in job for field in required_fields):
                return jsonify({
                    'success': False,
                    'message': f'Invalid job structure for job {job.get("name", "unknown")}. Missing required fields.'
                }), 400
        
        # Update configuration with selected jobs
        config_service = ConfigService()
        updated_content = config_service.update_workflows_section(config_content, jobs)
        
        # Save the updated configuration
        databricks_service = DatabricksService()
        success = databricks_service.write_workspace_file(config_path, updated_content)
        
        if not success:
            return jsonify({
                'success': False,
                'message': 'Failed to save updated configuration to Databricks workspace. Please check your permissions.'
            }), 500
        
        logger.info(f"Successfully updated config with {len(jobs)} jobs and saved to Databricks workspace")
        
        return jsonify({
            'success': True,
            'message': f'Configuration updated with {len(jobs)} jobs and saved to Databricks workspace',
            'updated_content': updated_content,
            'jobs_count': len(jobs),
            'destination': 'databricks'
        })
        
    except Exception as e:
        logger.error(f"Error updating config with jobs: {e}")
        return jsonify({
            'success': False,
            'message': f'Failed to update configuration in Databricks workspace: {str(e)}'
        }), 500


@jobs_bp.route('/selected', methods=['GET'])
def get_selected_jobs():
    """Get currently selected jobs from session."""
    try:
        selected_jobs = session.get('selected_jobs', [])
        
        return jsonify({
            'success': True,
            'selected_jobs': selected_jobs,
            'count': len(selected_jobs)
        })
        
    except Exception as e:
        logger.error(f"Error getting selected jobs: {e}")
        return jsonify({
            'success': False,
            'message': f'Failed to get selected jobs: {str(e)}'
        }), 500


@jobs_bp.route('/validate-selection', methods=['POST'])
def validate_job_selection():
    """Validate job selection structure and data."""
    try:
        data = request.get_json()
        jobs = data.get('jobs', [])
        
        if not jobs:
            return jsonify({
                'success': False,
                'message': 'No jobs provided for validation'
            }), 400
        
        config_service = ConfigService()
        
        # Validate job structure
        is_valid = config_service.validate_workflow_structure(jobs)
        
        if is_valid:
            return jsonify({
                'success': True,
                'message': f'Successfully validated {len(jobs)} job selections',
                'jobs_count': len(jobs),
                'validation_details': {
                    'valid_structure': True,
                    'total_jobs': len(jobs),
                    'existing_jobs': len([j for j in jobs if j.get('is_existing', True)]),
                    'new_jobs': len([j for j in jobs if not j.get('is_existing', True)]),
                    'active_jobs': len([j for j in jobs if j.get('is_active', True)])
                }
            })
        else:
            return jsonify({
                'success': False,
                'message': 'Job selection validation failed',
                'validation_details': {
                    'valid_structure': False
                }
            }), 400
        
    except Exception as e:
        logger.error(f"Error validating job selection: {e}")
        return jsonify({
            'success': False,
            'message': f'Validation failed: {str(e)}'
        }), 500


@jobs_bp.route('/connection-test', methods=['POST'])
def test_databricks_connection():
    """Test connection to Databricks workspace."""
    try:

        
        databricks_service = DatabricksService()
        connection_info = databricks_service.test_connection()
        
        if connection_info.get('connected'):
            logger.info("Databricks connection test successful")
            return jsonify({
                'success': True,
                'message': 'Successfully connected to Databricks',
                'connection_info': connection_info
            })
        else:
            logger.warning(f"Databricks connection test failed: {connection_info.get('error')}")
            return jsonify({
                'success': False,
                'message': f"Connection failed: {connection_info.get('error')}",
                'connection_info': connection_info
            }), 500
        
    except Exception as e:
        logger.error(f"Error testing Databricks connection: {e}")
        return jsonify({
            'success': False,
            'message': f'Connection test failed: {str(e)}'
        }), 500


@jobs_bp.route('/build-config', methods=['POST'])
def build_config():
    """Build updated configuration with selected jobs."""
    try:
        data = request.get_json()
        config_path = data.get('config_path')
        selected_jobs = data.get('selected_jobs', [])
        
        if not config_path:
            return jsonify({
                'success': False,
                'message': 'Config path is required'
            }), 400
        
        # Load the current configuration
        config_service = ConfigService()
        config_content = config_service.load_config(config_path)
        
        if not config_content:
            return jsonify({
                'success': False,
                'message': 'Failed to load configuration file'
            }), 400
        
        # Update the configuration with selected jobs
        updated_config = config_service.update_config_with_jobs(config_content, selected_jobs)
        
        logger.info(f"Built updated configuration with {len(selected_jobs)} jobs")
        
        return jsonify({
            'success': True,
            'updated_config': updated_config,
            'job_count': len(selected_jobs)
        })
        
    except Exception as e:
        logger.error(f"Error building config: {e}")
        return jsonify({
            'success': False,
            'message': f'Failed to build configuration: {str(e)}'
        }), 500


@jobs_bp.route('/refresh', methods=['POST'])
def refresh_jobs():
    """Refresh the job cache by fetching latest jobs from Databricks."""
    try:
        job_cache = JobCacheService()
        jobs = job_cache.get_jobs(force_refresh=True)
        
        logger.info(f"Successfully refreshed job cache with {len(jobs)} jobs")
        
        return jsonify({
            'success': True,
            'jobs': jobs,
            'count': len(jobs),
            'message': 'Job cache refreshed successfully'
        })
        
    except Exception as e:
        logger.error(f"Error refreshing job cache: {e}")
        return jsonify({
            'success': False,
            'message': f'Failed to refresh jobs: {str(e)}'
        }), 500


@jobs_bp.route('/cache-info', methods=['GET'])
def get_cache_info():
    """Get information about the job cache."""
    try:
        job_cache = JobCacheService()
        cache_info = job_cache.get_cache_info()
        
        return jsonify({
            'success': True,
            'cache_info': cache_info
        })
        
    except Exception as e:
        logger.error(f"Error getting cache info: {e}")
        return jsonify({
            'success': False,
            'message': f'Failed to get cache info: {str(e)}'
        }), 500