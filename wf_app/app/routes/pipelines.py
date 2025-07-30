from flask import Blueprint, render_template, request, jsonify, session
from ..services import DatabricksService, ConfigService, JobCacheService
import logging

pipelines_bp = Blueprint('pipelines', __name__)
logger = logging.getLogger(__name__)


@pipelines_bp.route('/')
def pipeline_selection():
    """Pipeline selection page."""
    return render_template('pipelines.html')


@pipelines_bp.route('/list', methods=['GET'])
def list_pipelines():
    """List all Databricks pipelines from cache."""
    try:
        # Get jobs from cache (much faster than API call)
        job_cache = JobCacheService()
        pipelines = job_cache.get_pipelines()
        
        logger.info(f"Successfully returned {len(pipelines)} Databricks pipelines from cache")
        
        return jsonify({
            'success': True,
            'pipelines': pipelines,
            'count': len(pipelines),
            'from_cache': True
        })
        
    except Exception as e:
        logger.error(f"Error listing pipelines from cache: {e}")
        return jsonify({
            'success': False,
            'message': f'Failed to list pipelines: {str(e)}',
            'pipelines': []
        }), 500



@pipelines_bp.route('/details/<string:pipeline_id>', methods=['GET'])
def get_pipeline_details(pipeline_id):
    """Get details for a specific pipeline including cluster type for timing estimation."""
    try:
        databricks_service = DatabricksService()
        pipeline_details = databricks_service.get_pipeline_details(pipeline_id)
        
        logger.info(f"Retrieved details for pipeline {pipeline_id}: cluster_type={pipeline_details.get('cluster_type', 'unknown')}")
        
        return jsonify({
            'success': True,
            'pipeline_details': pipeline_details  # Changed from 'job' to 'job_details' to match frontend expectation
        })
        
    except Exception as e:
        logger.error(f"Error getting pipeline details for {pipeline_id}: {e}")
        return jsonify({
            'success': False,
            'message': f'Failed to get pipeline details: {str(e)}'
        }), 500


@pipelines_bp.route('/select', methods=['POST'])
def select_pipelines():
    """Handle pipeline selection and store in session."""
    try:
        data = request.get_json()
        pipelines = data.get('pipelines', [])
        
        if not pipelines:
            return jsonify({
                'success': False,
                'message': 'No pipelines selected'
            }), 400
        
        # Validate pipeline structure
        for i, pipeline in enumerate(pipelines):
            required_fields = ['pipeline_id', 'pipeline_name', 'is_existing', 'is_active', 'export_libraries']
            missing_fields = [field for field in required_fields if field not in pipeline]
            if missing_fields:
                error_msg = f'Pipeline {i+1} missing required fields: {", ".join(missing_fields)}. Pipeline data: {pipeline}'
                logger.error(f"Pipeline validation error: {error_msg}")
                return jsonify({
                    'success': False,
                    'message': f'Invalid pipeline structure. Missing required fields: {", ".join(missing_fields)}'
                }), 400
        
        # Store selected jobs in session
        session['selected_pipelines'] = pipelines
        
        logger.info(f"Selected {len(pipelines)} pipelines for configuration update")
        
        return jsonify({
            'success': True,
            'message': f'Successfully selected {len(pipelines)} pipelines',
            'selected_pipelines': pipelines
        })
        
    except Exception as e:
        logger.error(f"Error selecting pipelines: {e}")
        return jsonify({
            'success': False,
            'message': f'Failed to select pipelines: {str(e)}'
        }), 500


@pipelines_bp.route('/update-config', methods=['POST'])
def update_config_with_pipelines():
    """Update configuration file with selected pipelines."""
    try:
        data = request.get_json()
        config_path = data.get('config_path', '').strip()
        config_content = data.get('config_content', '').strip()
        pipelines = data.get('pipelines', [])
        
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
        
        if not pipelines:
            return jsonify({
                'success': False,
                'message': 'No pipelines provided for configuration update'
            }), 400
        
        # Validate pipeline structure
        for i, pipeline in enumerate(pipelines):
            required_fields = ['pipeline_id', 'name', 'is_existing', 'is_active', 'export_libraries']
            missing_fields = [field for field in required_fields if field not in pipeline]
            if missing_fields:
                error_msg = f'Pipeline {i+1} missing required fields: {", ".join(missing_fields)}. Pipeline data: {pipeline}'
                logger.error(f"Pipeline config update validation error: {error_msg}")
                return jsonify({
                    'success': False,
                    'message': f'Invalid pipeline structure. Missing required fields: {", ".join(missing_fields)}'
                }), 400
        
        # Update configuration with selected jobs
        config_service = ConfigService()
        updated_content = config_service.update_pipelines_section(config_content, pipelines)
        
        # Save the updated configuration
        databricks_service = DatabricksService()
        success = databricks_service.write_workspace_file(config_path, updated_content)
        
        if not success:
            return jsonify({
                'success': False,
                'message': 'Failed to save updated configuration to Databricks workspace. Please check your permissions.'
            }), 500
        
        logger.info(f"Successfully updated config with {len(pipelines)} pipelines and saved to Databricks workspace")
        
        return jsonify({
            'success': True,
            'message': f'Configuration updated with {len(pipelines)} pipelines and saved to Databricks workspace',
            'updated_content': updated_content,
            'pipelines_count': len(pipelines),
            'destination': 'databricks'
        })
        
    except Exception as e:
        logger.error(f"Error updating config with pipelines: {e}")
        return jsonify({
            'success': False,
            'message': f'Failed to update configuration in Databricks workspace: {str(e)}'
        }), 500


@pipelines_bp.route('/selected', methods=['GET'])
def get_selected_pipelines():
    """Get currently selected pipelines from session."""
    try:
        selected_pipelines = session.get('selected_pipelines', [])
        
        return jsonify({
            'success': True,
            'selected_pipelines': selected_pipelines,
            'count': len(selected_pipelines)
        })
        
    except Exception as e:
        logger.error(f"Error getting selected pipelines: {e}")
        return jsonify({
            'success': False,
            'message': f'Failed to get selected pipelines: {str(e)}'
        }), 500


@pipelines_bp.route('/validate-selection', methods=['POST'])
def validate_pipeline_selection():
    """Validate pipeline selection structure and data."""
    try:
        data = request.get_json()
        pipelines = data.get('pipelines', [])
        
        if not pipelines:
            return jsonify({
                'success': False,
                'message': 'No pipelines provided for validation'
            }), 400
        
        config_service = ConfigService()
        
        # Validate pipeline structure
        is_valid = config_service.validate_pipeline_structure(pipelines)
        
        if is_valid:
            return jsonify({
                'success': True,
                'message': f'Successfully validated {len(pipelines)} pipeline selections',
                'pipelines_count': len(pipelines),
                'validation_details': {
                    'valid_structure': True,
                    'total_pipelines': len(pipelines),
                    'existing_pipelines': len([p for p in pipelines if p.get('is_existing', True)]),
                    'new_pipelines': len([p for p in pipelines if not p.get('is_existing', True)]),
                    'active_pipelines': len([p for p in pipelines if p.get('is_active', True)])
                }
            })
        else:
            return jsonify({
                'success': False,
                'message': 'Pipeline selection validation failed',
                'validation_details': {
                    'valid_structure': False
                }
            }), 400
        
    except Exception as e:
        logger.error(f"Error validating pipeline selection: {e}")
        return jsonify({
            'success': False,
            'message': f'Validation failed: {str(e)}'
        }), 500


@pipelines_bp.route('/connection-test', methods=['POST'])
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


@pipelines_bp.route('/build-config', methods=['POST'])
def build_config():
    """Build updated configuration with selected pipelines."""
    try:
        data = request.get_json()
        config_path = data.get('config_path')
        selected_pipelines = data.get('selected_pipelines', [])
        
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
        updated_config = config_service.update_config_with_pipelines(config_content, selected_pipelines)
        
        logger.info(f"Built updated configuration with {len(selected_pipelines)} pipelines")
        
        return jsonify({
            'success': True,
            'updated_config': updated_config,
            'pipeline_count': len(selected_pipelines)
        })
        
    except Exception as e:
        logger.error(f"Error building config: {e}")
        return jsonify({
            'success': False,
            'message': f'Failed to build configuration: {str(e)}'
        }), 500


@pipelines_bp.route('/refresh', methods=['POST'])
def refresh_pipelines():
    """Refresh the pipeline cache by fetching latest pipelines from Databricks."""
    try:
        pipeline_cache = JobCacheService()
        pipelines = pipeline_cache.get_pipelines(force_refresh=True)
        
        logger.info(f"Successfully refreshed pipeline cache with {len(pipelines)} pipelines")
        
        return jsonify({
            'success': True,
            'pipelines': pipelines,
            'count': len(pipelines),
            'message': 'Pipeline cache refreshed successfully'
        })
        
    except Exception as e:
        logger.error(f"Error refreshing pipeline cache: {e}")
        return jsonify({
            'success': False,
            'message': f'Failed to refresh pipelines: {str(e)}'
        }), 500


@pipelines_bp.route('/cache-info', methods=['GET'])
def get_cache_info():
    """Get information about the pipeline cache."""
    try:
        pipeline_cache = JobCacheService()
        cache_info = pipeline_cache.get_cache_info()
        
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