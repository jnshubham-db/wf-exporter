"""
Authentication configuration routes for the Databricks Workflow Manager
"""

from flask import Blueprint, request, jsonify, session, render_template
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import DatabricksError
import logging
from datetime import datetime
import traceback
import base64

# Create authentication blueprint
auth_bp = Blueprint('auth', __name__)

# Configure logging
logger = logging.getLogger(__name__)

# Session keys for authentication storage
AUTH_CONFIG_KEY = 'databricks_auth_config'
AUTH_STATUS_KEY = 'databricks_auth_status'

@auth_bp.route('/auth')
def auth_page():
    """Render the authentication configuration page"""
    return render_template('auth.html')

@auth_bp.route('/api/auth/status')
def get_auth_status():
    """Get current authentication status"""
    try:
        status = session.get(AUTH_STATUS_KEY, {
            'connected': False,
            'details': 'No authentication configured',
            'last_tested': None
        })
        
        # If connected via session config, get config details for display
        if status.get('connected', False):
            config = session.get(AUTH_CONFIG_KEY, {})
            auth_type = config.get('auth_type', '')
            
            # Add workspace URL for display
            if auth_type == 'pat':
                status['workspace_url'] = config.get('host', '')
            elif auth_type == 'azure-client-secret':
                # For Azure, use the host URL directly
                status['workspace_url'] = config.get('host', '')
            
            # Extract user name from details if available
            details = status.get('details', '')
            if 'Connected as' in details:
                # Extract user name from "Connected as username (Workspace ID: xxxxx)"
                start = details.find('Connected as ') + len('Connected as ')
                end = details.find(' (Workspace ID:')
                if end > start:
                    status['user_name'] = details[start:end]
                else:
                    # Fallback: try to get just the part after "Connected as "
                    user_part = details[start:].split('(')[0].strip()
                    status['user_name'] = user_part if user_part else 'User'
            else:
                status['user_name'] = 'Connected User'
        
        # If no session config, check if we can connect using default workspace client
        elif not session.get(AUTH_CONFIG_KEY):
            # Check if we have cached default auth status that's still valid (cache for 2 minutes)
            DEFAULT_AUTH_CACHE_KEY = 'default_auth_status'
            DEFAULT_AUTH_CACHE_TIME_KEY = 'default_auth_cache_time'
            
            cached_status = session.get(DEFAULT_AUTH_CACHE_KEY)
            cache_time = session.get(DEFAULT_AUTH_CACHE_TIME_KEY, 0)
            cache_duration = 120  # 2 minutes cache for default auth
            
            if cached_status and (datetime.now().timestamp() - cache_time) < cache_duration:
                status = cached_status
            else:
                try:
                    # Try to get user info from default workspace client
                    default_client = WorkspaceClient()
                    current_user = default_client.current_user.me()
                    workspace_id = default_client.get_workspace_id()
                    
                    # Get workspace URL from current client config
                    workspace_url = getattr(default_client.config, 'host', 'Databricks Workspace')
                    
                    status = {
                        'connected': True,
                        'details': f"Connected as {current_user.user_name} (Workspace ID: {workspace_id})",
                        'user_name': current_user.user_name,
                        'workspace_url': workspace_url,
                        'auth_type': 'default',
                        'last_tested': datetime.now().isoformat()
                    }
                    
                    # Cache the successful default auth status
                    session[DEFAULT_AUTH_CACHE_KEY] = status
                    session[DEFAULT_AUTH_CACHE_TIME_KEY] = datetime.now().timestamp()
                    
                except Exception as e:
                    logger.debug(f"Could not get default workspace client info: {e}")
                    # Cache the failure too (shorter duration)
                    status = {
                        'connected': False,
                        'details': 'No authentication configured',
                        'last_tested': None
                    }
                    session[DEFAULT_AUTH_CACHE_KEY] = status
                    session[DEFAULT_AUTH_CACHE_TIME_KEY] = datetime.now().timestamp() - cache_duration + 30  # Cache failure for 30 seconds only
        
        return jsonify(status)
    except Exception as e:
        logger.error(f"Error getting auth status: {str(e)}")
        return jsonify({'error': str(e)}), 500

@auth_bp.route('/api/auth/config')
def get_auth_config():
    """Get current authentication configuration (without sensitive data)"""
    try:
        config = session.get(AUTH_CONFIG_KEY, {})
        
        # Return only non-sensitive configuration data
        safe_config = {
            'auth_type': config.get('auth_type', ''),
            'host': config.get('host', ''),
            'keyvault_name': config.get('keyvault_name', ''),
            'secret_name': config.get('secret_name', ''),
            'azure_workspace_resource_id': config.get('azure_workspace_resource_id', ''),
            'azure_tenant_id': config.get('azure_tenant_id', ''),
            'azure_client_id': config.get('azure_client_id', ''),
            'azure_tenant_id_key': config.get('azure_tenant_id_key', ''),
            'azure_client_id_key': config.get('azure_client_id_key', ''),
            'azure_client_secret_key': config.get('azure_client_secret_key', '')
        }
        
        return jsonify(safe_config)
    except Exception as e:
        logger.error(f"Error getting auth config: {str(e)}")
        return jsonify({'error': str(e)}), 500

@auth_bp.route('/api/auth/configure', methods=['POST'])
def configure_auth():
    """Configure Databricks authentication"""
    try:
        config = request.get_json()
        
        if not config or not config.get('auth_type'):
            return jsonify({'error': 'Authentication type is required'}), 400
        
        # Validate configuration based on auth type
        validation_error = validate_auth_config(config)
        if validation_error:
            return jsonify({'error': validation_error}), 400
        
        # Test the connection before saving
        test_result = test_databricks_connection(config)
        
        if test_result['success']:
            # Save configuration to session
            session[AUTH_CONFIG_KEY] = config
            
            # Save status
            status = {
                'connected': True,
                'details': test_result['details'],
                'auth_type': config['auth_type'],
                'user_name': test_result.get('user_name', ''),
                'workspace_id': test_result.get('workspace_id', ''),
                'workspace_url': test_result.get('workspace_url', ''),
                'last_tested': datetime.now().isoformat()
            }
            session[AUTH_STATUS_KEY] = status
            
            # Clear job cache when authentication changes
            try:
                from ..services import JobCacheService
                job_cache = JobCacheService()
                job_cache.clear_cache()
                logger.info("Job cache cleared due to authentication change")
            except Exception as e:
                logger.warning(f"Failed to clear job cache: {e}")
            
            logger.info(f"Authentication configured successfully: {config['auth_type']}")
            
            return jsonify({
                'message': 'Authentication configured and tested successfully',
                'status': status
            })
        else:
            return jsonify({'error': f"Connection test failed: {test_result['error']}"}), 400
            
    except Exception as e:
        logger.error(f"Error configuring authentication: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({'error': str(e)}), 500

@auth_bp.route('/api/auth/test', methods=['POST'])
def test_auth():
    """Test current authentication configuration"""
    try:
        config = session.get(AUTH_CONFIG_KEY)
        
        if not config:
            return jsonify({'error': 'No authentication configuration found'}), 400
        
        test_result = test_databricks_connection(config)
        
        if test_result['success']:
            status = {
                'connected': True,
                'details': test_result['details'],
                'auth_type': config['auth_type'],
                'user_name': test_result.get('user_name', ''),
                'workspace_id': test_result.get('workspace_id', ''),
                'workspace_url': test_result.get('workspace_url', ''),
                'last_tested': datetime.now().isoformat()
            }
            session[AUTH_STATUS_KEY] = status
            
            return jsonify({
                'message': 'Connection test successful',
                'status': status
            })
        else:
            # Update status to disconnected
            status = {
                'connected': False,
                'details': f"Test failed: {test_result['error']}",
                'auth_type': config.get('auth_type', 'unknown'),
                'last_tested': datetime.now().isoformat()
            }
            session[AUTH_STATUS_KEY] = status
            
            return jsonify({'error': f"Connection test failed: {test_result['error']}"}), 400
            
    except Exception as e:
        logger.error(f"Error testing authentication: {str(e)}")
        return jsonify({'error': str(e)}), 500

@auth_bp.route('/api/auth/clear', methods=['POST'])
def clear_auth():
    """Clear authentication configuration"""
    try:
        # Remove authentication data from session
        session.pop(AUTH_CONFIG_KEY, None)
        session.pop(AUTH_STATUS_KEY, None)
        
        # Clear default auth cache too
        session.pop('default_auth_status', None)
        session.pop('default_auth_cache_time', None)
        
        # Clear job cache when authentication is cleared
        try:
            from ..services import JobCacheService
            job_cache = JobCacheService()
            job_cache.clear_cache()
            logger.info("Job cache cleared due to authentication clear")
        except Exception as e:
            logger.warning(f"Failed to clear job cache: {e}")
        
        logger.info("Authentication configuration cleared")
        
        return jsonify({'message': 'Authentication configuration cleared successfully'})
        
    except Exception as e:
        logger.error(f"Error clearing authentication: {str(e)}")
        return jsonify({'error': str(e)}), 500

@auth_bp.route('/api/auth/logout', methods=['POST'])
def logout():
    """Logout current user and clear authentication"""
    try:
        # Get current user info for logging
        status = session.get(AUTH_STATUS_KEY, {})
        user_name = status.get('user_name', 'Unknown user')
        
        # Remove authentication data from session
        session.pop(AUTH_CONFIG_KEY, None)
        session.pop(AUTH_STATUS_KEY, None)
        
        # Clear default auth cache too
        session.pop('default_auth_status', None)
        session.pop('default_auth_cache_time', None)
        
        # Clear job cache when user logs out
        try:
            from ..services import JobCacheService
            job_cache = JobCacheService()
            job_cache.clear_cache()
            logger.info("Job cache cleared due to logout")
        except Exception as e:
            logger.warning(f"Failed to clear job cache: {e}")
        
        logger.info(f"User logged out: {user_name}")
        
        return jsonify({'message': 'Logged out successfully'})
        
    except Exception as e:
        logger.error(f"Error during logout: {str(e)}")
        return jsonify({'error': str(e)}), 500

def validate_auth_config(config):
    """Validate authentication configuration"""
    auth_type = config.get('auth_type')
    
    if auth_type == 'pat':
        if not config.get('host'):
            return 'Databricks host URL is required for PAT authentication'
        if not config.get('keyvault_name'):
            return 'Key Vault name is required for PAT authentication'
        if not config.get('secret_name'):
            return 'Secret name is required for PAT authentication'
    
    elif auth_type == 'azure-client-secret':
        if not config.get('host'):
            return 'Databricks host URL is required for Azure Service Principal authentication'
        if not config.get('keyvault_name'):
            return 'Key Vault name is required for Azure Service Principal authentication'
        if not config.get('azure_tenant_id_key'):
            return 'Azure tenant ID key is required for Azure Service Principal authentication'
        if not config.get('azure_client_id_key'):
            return 'Azure client ID key is required for Azure Service Principal authentication'
        if not config.get('azure_client_secret_key'):
            return 'Azure client secret key is required for Azure Service Principal authentication'
    
    else:
        return f'Unsupported authentication type: {auth_type}'
    
    return None

def get_secret_from_keyvault(keyvault_name, secret_key):
    """Get secret from Azure Key Vault using default Databricks client"""
    try:
        # Use default workspace client without parameters for first-time login
        w = WorkspaceClient()
        secret_response = w.secrets.get_secret(scope=keyvault_name, key=secret_key)
        if not secret_response.value:
            raise Exception(f"Secret '{secret_key}' not found or empty in Key Vault '{keyvault_name}'")
        decoded_secret = base64.b64decode(secret_response.value).decode('utf-8')
        return decoded_secret
    except Exception as e:
        logger.error(f"Failed to get secret '{secret_key}' from Key Vault '{keyvault_name}': {str(e)}")
        raise Exception(f"Failed to get secret from Key Vault: {str(e)}")

def test_databricks_connection(config):
    """Test Databricks connection with provided configuration"""
    try:
        # Create WorkspaceClient with the provided configuration using Config object
        from databricks.sdk.core import Config
        
        auth_type = config.get('auth_type')
        
        if auth_type == 'pat':
            # Fetch token from Key Vault
            token = get_secret_from_keyvault(config['keyvault_name'], config['secret_name'])
            
            # Create isolated config for PAT authentication
            databricks_config = Config(
                host=config['host'],
                token=token,
                auth_type='pat'
            )
        
        elif auth_type == 'azure-client-secret':
            # Fetch Azure credentials from Key Vault
            tenant_id = get_secret_from_keyvault(config['keyvault_name'], config['azure_tenant_id_key'])
            client_id = get_secret_from_keyvault(config['keyvault_name'], config['azure_client_id_key'])
            client_secret = get_secret_from_keyvault(config['keyvault_name'], config['azure_client_secret_key'])
            
            # Create isolated config for Azure Service Principal authentication
            databricks_config = Config(
                host=config['host'],
                azure_tenant_id=tenant_id,
                azure_client_id=client_id,
                azure_client_secret=client_secret,
                auth_type='azure-client-secret'
            )
        
        else:
            return {
                'success': False,
                'error': f'Unsupported authentication type: {auth_type}'
            }
        
        # Create client with isolated config
        client = WorkspaceClient(config=databricks_config)
        
        # Get current user info to test connection
        current_user = client.current_user.me()
        workspace_id = client.get_workspace_id()
        
        details = f"Connected as {current_user.user_name} (Workspace ID: {workspace_id})"
        
        # Get workspace URL based on auth type
        workspace_url = ''
        if auth_type == 'pat':
            workspace_url = config['host']
        elif auth_type == 'azure-client-secret':
            # For Azure, use the host URL directly
            workspace_url = config['host']
        
        return {
            'success': True,
            'details': details,
            'user_name': current_user.user_name,
            'workspace_id': workspace_id,
            'workspace_url': workspace_url
        }
    
    except DatabricksError as e:
        logger.error(f"Databricks authentication error: {str(e)}")
        return {
            'success': False,
            'error': f'Databricks error: {str(e)}'
        }
    
    except Exception as e:
        logger.error(f"Connection test error: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'success': False,
            'error': f'Connection error: {str(e)}'
        }

def get_databricks_client():
    """Get authenticated Databricks client from session configuration"""
    config = session.get(AUTH_CONFIG_KEY)
    
    if not config:
        # Return default workspace client for first-time login
        return WorkspaceClient()
    
    auth_type = config.get('auth_type')
    
    # Use Config objects to create isolated configurations
    from databricks.sdk.core import Config
    
    if auth_type == 'pat':
        # Fetch token from Key Vault
        token = get_secret_from_keyvault(config['keyvault_name'], config['secret_name'])
        
        # Create isolated config for PAT authentication
        databricks_config = Config(
            host=config['host'],
            token=token,
            auth_type='pat'
        )
        return WorkspaceClient(config=databricks_config)
    
    elif auth_type == 'azure-client-secret':
        # Fetch Azure credentials from Key Vault
        tenant_id = get_secret_from_keyvault(config['keyvault_name'], config['azure_tenant_id_key'])
        client_id = get_secret_from_keyvault(config['keyvault_name'], config['azure_client_id_key'])
        client_secret = get_secret_from_keyvault(config['keyvault_name'], config['azure_client_secret_key'])
        
        # Create isolated config for Azure Service Principal authentication
        databricks_config = Config(
            host=config['host'],
            azure_tenant_id=tenant_id,
            azure_client_id=client_id,
            azure_client_secret=client_secret,
            auth_type='azure-client-secret'
        )
        return WorkspaceClient(config=databricks_config)
    
    else:
        raise Exception(f"Unsupported authentication type: {auth_type}")

def is_authenticated():
    """Check if user has valid authentication configured or default auth available"""
    config = session.get(AUTH_CONFIG_KEY)
    status = session.get(AUTH_STATUS_KEY, {})
    
    # Check configured session first
    if config is not None and status.get('connected', False):
        return True
    
    # If no configured session, check for default authentication
    if not config:
        try:
            # Try default workspace client
            default_client = WorkspaceClient()
            current_user = default_client.current_user.me()
            return True  # Default authentication works
        except Exception:
            return False  # No default authentication available
    
    return False

@auth_bp.route('/api/auth/session/validate', methods=['POST'])
def validate_session():
    """Validate current session and check for timeout"""
    try:
        # Check if session has authentication config
        config = session.get(AUTH_CONFIG_KEY)
        status = session.get(AUTH_STATUS_KEY, {})
        
        # If no config, check for default authentication
        if not config:
            try:
                # Try default workspace client
                default_client = WorkspaceClient()
                current_user = default_client.current_user.me()
                workspace_id = default_client.get_workspace_id()
                workspace_url = getattr(default_client.config, 'host', 'Databricks Workspace')
                
                return jsonify({
                    'valid': True,
                    'auth_type': 'default',
                    'user_name': current_user.user_name,
                    'workspace_url': workspace_url,
                    'details': f"Connected as {current_user.user_name} (Workspace ID: {workspace_id})"
                })
            except Exception:
                return jsonify({
                    'valid': False,
                    'error': 'No valid authentication found'
                }), 401
        
        # Validate configured session
        if status.get('connected', False):
            return jsonify({
                'valid': True,
                'auth_type': config.get('auth_type', 'configured'),
                'user_name': status.get('user_name', ''),
                'workspace_url': status.get('workspace_url', ''),
                'details': status.get('details', '')
            })
        else:
            return jsonify({
                'valid': False,
                'error': 'Session not properly authenticated'
            }), 401
        
    except Exception as e:
        logger.error(f"Session validation error: {str(e)}")
        return jsonify({
            'valid': False,
            'error': str(e)
        }), 500

@auth_bp.route('/api/auth/session/extend', methods=['POST'])
def extend_session():
    """Extend current session timeout"""
    try:
        # Update session timestamp (Flask handles this automatically when session is accessed)
        session.permanent = True
        
        # Get current status
        status = session.get(AUTH_STATUS_KEY, {})
        if status.get('connected', False):
            # Update last tested time
            status['last_tested'] = datetime.now().isoformat()
            session[AUTH_STATUS_KEY] = status
            
            return jsonify({
                'message': 'Session extended successfully',
                'status': status
            })
        else:
            return jsonify({
                'message': 'No active session to extend'
            }), 400
        
    except Exception as e:
        logger.error(f"Session extension error: {str(e)}")
        return jsonify({'error': str(e)}), 500

@auth_bp.route('/api/auth/test-connection', methods=['POST'])
def test_current_connection():
    """Test current authentication by listing jobs"""
    try:
        if not is_authenticated():
            return jsonify({'error': 'No authentication configured'}), 400
        
        # Test the connection by trying to list jobs
        from ..services.databricks_service import DatabricksService
        databricks_service = DatabricksService()
        connection_info = databricks_service.test_connection()
        
        return jsonify({
            'success': True,
            'message': f'Authentication working! {connection_info}',
            'connection_info': connection_info
        })
        
    except Exception as e:
        logger.error(f"Connection test failed: {str(e)}")
        return jsonify({
            'error': f'Connection test failed: {str(e)}'
        }), 500 