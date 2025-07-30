import os


class Config:
    """Base configuration class."""
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'dev-secret-key-change-in-production'
    
    # Databricks Configuration
    DATABRICKS_HOST = os.environ.get('DATABRICKS_HOST')
    DATABRICKS_TOKEN = os.environ.get('DATABRICKS_TOKEN')
    DATABRICKS_PROFILE = os.environ.get('DATABRICKS_PROFILE')
    
    # Application Settings
    DEFAULT_MODE = os.environ.get('DEFAULT_MODE', 'local')
    

class DevelopmentConfig(Config):
    """Development configuration."""
    DEBUG = True
    
    # Development-specific Databricks settings
    DATABRICKS_TIMEOUT = 30  # seconds
    DATABRICKS_RETRY_COUNT = 3
    

class ProductionConfig(Config):
    """Production configuration."""
    DEBUG = False
    
    # Production-specific Databricks settings
    DATABRICKS_TIMEOUT = 60  # seconds
    DATABRICKS_RETRY_COUNT = 5
    

class TestingConfig(Config):
    """Testing configuration."""
    TESTING = True
    DEBUG = True
    
    # Testing-specific settings
    DATABRICKS_TIMEOUT = 10  # seconds
    DATABRICKS_RETRY_COUNT = 1
    

config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'testing': TestingConfig,
    'default': DevelopmentConfig
} 