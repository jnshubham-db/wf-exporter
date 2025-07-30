"""API routes for backend functionality."""

from flask import Blueprint, request, jsonify, session, current_app
import logging

api_bp = Blueprint('api', __name__)
logger = logging.getLogger(__name__)



@api_bp.route('/status', methods=['GET'])
def get_status():
    """Get application status information."""
    try:
        status_info = {
            'success': True,
            'mode': 'databricks',
            'session_id': session.get('_permanent_id', 'none'),
            'features': {
                'config_management': True,
                'job_selection': True,
                'workflow_export': True,
                'databricks_integration': True
            }
        }
        
        return jsonify(status_info)
        
    except Exception as e:
        logger.error(f"Error getting status: {e}")
        return jsonify({
            'success': False,
            'message': 'Failed to get status'
        }), 500

 