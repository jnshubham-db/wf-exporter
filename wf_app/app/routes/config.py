from flask import Blueprint, render_template, request, jsonify
from ..services import ConfigService, DatabricksService
import logging

config_bp = Blueprint('config', __name__)
logger = logging.getLogger(__name__)


@config_bp.route('/')
def config_management():
    """Configuration management page."""
    return render_template('config.html')


@config_bp.route('/load', methods=['POST'])
def load_config():
    """Load configuration file from Databricks workspace."""
    try:
        data = request.get_json()
        file_path = data.get('path', '').strip()
        
        if not file_path:
            return jsonify({
                'success': False,
                'message': 'File path is required'
            }), 400
        
        config_service = ConfigService()
        databricks_service = DatabricksService()
        
        # Load from Databricks workspace
        content = databricks_service.read_workspace_file(file_path)
        
        # Validate YAML content
        try:
            config_service.validate_yaml(content)
        except ValueError as e:
            return jsonify({
                'success': False,
                'message': f'Invalid YAML: {str(e)}'
            }), 400
        
        logger.info(f"Config loaded from Databricks workspace: {file_path}")
        
        return jsonify({
            'success': True,
            'content': content,
            'path': file_path,
            'source': 'databricks'
        })
        
    except FileNotFoundError:
        return jsonify({
            'success': False,
            'message': 'Configuration file not found in Databricks workspace. Please check the workspace path.'
        }), 404
    except Exception as e:
        logger.error(f"Error loading config: {e}")
        return jsonify({
            'success': False,
            'message': f'Failed to load configuration from Databricks workspace: {str(e)}'
        }), 500


@config_bp.route('/save', methods=['POST'])
def save_config():
    """Save configuration file to Databricks workspace."""
    try:
        data = request.get_json()
        file_path = data.get('path', '').strip()
        content = data.get('content', '').strip()
        
        if not file_path:
            return jsonify({
                'success': False,
                'message': 'File path is required'
            }), 400
        
        if not content:
            return jsonify({
                'success': False,
                'message': 'Configuration content is required'
            }), 400
        
        config_service = ConfigService()
        
        # Validate YAML content before saving
        try:
            config_service.validate_yaml(content)
        except ValueError as e:
            return jsonify({
                'success': False,
                'message': f'Invalid YAML: {str(e)}'
            }), 400
        
        # Save to Databricks workspace
        databricks_service = DatabricksService()
        success = databricks_service.write_workspace_file(file_path, content)
        
        if not success:
            return jsonify({
                'success': False,
                'message': 'Failed to save configuration to Databricks workspace. Please check your permissions.'
            }), 500
        
        logger.info(f"Config saved to Databricks workspace: {file_path}")
        
        return jsonify({
            'success': True,
            'message': 'Configuration saved successfully to Databricks workspace',
            'path': file_path,
            'destination': 'databricks'
        })
        
    except Exception as e:
        logger.error(f"Error saving config: {e}")
        return jsonify({
            'success': False,
            'message': f'Failed to save configuration to Databricks workspace: {str(e)}'
        }), 500


@config_bp.route('/validate', methods=['POST'])
def validate_config():
    """Validate YAML configuration content."""
    try:
        data = request.get_json()
        content = data.get('content', '').strip()
        
        if not content:
            return jsonify({
                'success': False,
                'message': 'No content to validate'
            }), 400
        
        config_service = ConfigService()
        config_data = config_service.validate_yaml(content)
        
        # Additional validation for workflow structure
        validation_results = {
            'success': True,
            'message': 'Configuration is valid',
            'details': {
                'has_workflows': 'workflows' in config_data,
                'workflow_count': len(config_data.get('workflows', [])),
                'valid_yaml': True
            }
        }
        
        return jsonify(validation_results)
        
    except ValueError as e:
        return jsonify({
            'success': False,
            'message': f'Invalid YAML: {str(e)}',
            'details': {
                'valid_yaml': False
            }
        }), 400
    except Exception as e:
        logger.error(f"Error validating config: {e}")
        return jsonify({
            'success': False,
            'message': 'Validation failed'
        }), 500 