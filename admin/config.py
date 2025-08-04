"""
Flask Configuration for Drug Analysis Admin Interface
"""
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class Config:
    """Base configuration"""
    SECRET_KEY = os.getenv('FLASK_SECRET_KEY', 'dev-key-change-in-production')
    
    # Database Configuration
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_USER = os.getenv('DB_USER', 'postgres')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    DB_NAME = os.getenv('DB_NAME', 'pharmacy_db')
    
    if not DB_PASSWORD:
        raise ValueError("DB_PASSWORD environment variable must be set")
    
    SQLALCHEMY_DATABASE_URI = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}'
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_ENGINE_OPTIONS = {
        'pool_pre_ping': True,
        'pool_size': 10,
        'max_overflow': 20
    }
    
    # Flask-Admin Configuration
    FLASK_ADMIN_SWATCH = 'cerulean'  # Bootstrap theme
    
    # Pagination
    DEFAULT_PAGE_SIZE = 25
    MAX_PAGE_SIZE = 100

class DevelopmentConfig(Config):
    """Development configuration"""
    DEBUG = True
    FLASK_ENV = 'development'

class ProductionConfig(Config):
    """Production configuration"""
    DEBUG = False
    FLASK_ENV = 'production'

# Configuration selector
config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'default': DevelopmentConfig
}