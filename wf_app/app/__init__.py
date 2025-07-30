from flask import Flask, session, g
from datetime import timedelta
from .routes import main_bp, config_bp, jobs_bp, export_bp, api_bp, auth_bp, pipelines_bp
from .services import JobCacheService, ExportStateService
import logging


def create_app(config_name=None):
    """Flask application factory."""
    app = Flask(__name__)
    
    # Configuration
    if config_name == 'development':
        app.config['DEBUG'] = True
    else:
        app.config['DEBUG'] = False
    
    app.config['SECRET_KEY'] = 'dev-secret-key-change-in-production'
    
    # Session configuration - 20 minute timeout for configured sessions
    app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(minutes=20)
    app.config['SESSION_PERMANENT'] = True
    app.config['SESSION_COOKIE_HTTPONLY'] = True
    app.config['SESSION_COOKIE_SECURE'] = False  # Set to True in production with HTTPS
    
    # Register Blueprints
    app.register_blueprint(main_bp)
    app.register_blueprint(config_bp, url_prefix='/config')
    app.register_blueprint(jobs_bp, url_prefix='/jobs')
    app.register_blueprint(pipelines_bp, url_prefix='/pipelines')
    app.register_blueprint(export_bp, url_prefix='/export')
    app.register_blueprint(api_bp, url_prefix='/api')
    app.register_blueprint(auth_bp)
    
    # Add context processors for templates
    @app.context_processor
    def inject_app_info():
        """Make app information available in all templates."""
        return {
            'current_mode': 'databricks',
            'is_databricks_mode': True,
            'is_local_mode': False
        }
    
    # Initialize services on app startup
    def initialize_services():
        """Initialize job cache and export state services with background loading."""
        try:
            logger = logging.getLogger(__name__)
            logger.info("Initializing application services...")
            
            # Initialize job cache service
            job_cache = JobCacheService()
            job_cache.load_jobs_background()
            logger.info("Job cache service initialized successfully")
            
            # Initialize export state service and recover active exports
            export_state_service = ExportStateService()
            active_exports = export_state_service.get_active_exports()
            
            if active_exports:
                logger.info(f"Recovered {len(active_exports)} active export runs on startup")
                for run_id, state in active_exports.items():
                    logger.info(f"Active export recovered: run_id={run_id}, status={state.get('status')}, config={state.get('config_path')}")
            else:
                logger.info("No active exports found to recover")
            
            logger.info("Export state service initialized successfully")
            
        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.error(f"Failed to initialize services: {e}")
    
    # Initialize services when app is created
    initialize_services()
    
    return app 