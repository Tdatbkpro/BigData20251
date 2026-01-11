import os
from dotenv import load_dotenv
from pydantic_settings import BaseSettings

load_dotenv()

class Settings(BaseSettings):
    # API Configuration
    EODDATA_API_KEY: str = os.getenv('EODDATA_API_KEY', '')

    # HDFS Configuration
    HDFS_NAMENODE: str = os.getenv('HDFS_NAMENODE', 'namenode:9870')
    HDFS_USER: str = os.getenv('HDFS_USER', 'hadoop')
    HDFS_BASE_PATH: str = os.getenv('HDFS_BASE_PATH', '/stock_data')

    # Database Configuration
    DATABASE_URL: str = os.getenv(
        'DATABASE_URL',
        'postgresql://stockuser:stockpass@postgres:5432/stockdb'
    )

    # Scheduler Configuration
    COLLECTION_INTERVAL_HOURS: int = int(os.getenv('COLLECTION_INTERVAL_HOURS', '6'))
    DAYS_OF_DATA: int = int(os.getenv('DAYS_OF_DATA', '30'))

    # Data Collection Configuration
    SYMBOLS_PER_EXCHANGE: int = int(os.getenv('SYMBOLS_PER_EXCHANGE', '10'))
    MAX_RETRIES: int = int(os.getenv('MAX_RETRIES', '3'))

    # Local Storage
    LOCAL_DATA_DIR: str = os.getenv('LOCAL_DATA_DIR', '/data')
    PROCESSED_DIR: str = os.getenv('PROCESSED_DIR', '/data/processed')
    LOGS_DIR: str = os.getenv('LOGS_DIR', '/data/logs')
    REPORTS_DIR: str = os.getenv('REPORTS_DIR', '/data/reports')

    # Logging
    LOG_LEVEL: str = os.getenv('LOG_LEVEL', 'INFO')

    class Config:
        env_file = ".env"
        case_sensitive = False

# Create settings instance
settings = Settings()

def validate_config():
    """Validate configuration settings"""
    # Check required environment variables
    if not settings.EODDATA_API_KEY:
        raise ValueError("EODDATA_API_KEY is required")
    
    # Check required directories exist
    required_dirs = [
        settings.LOCAL_DATA_DIR,
        settings.PROCESSED_DIR,
        settings.LOGS_DIR,
        settings.REPORTS_DIR
    ]
    
    for directory in required_dirs:
        os.makedirs(directory, exist_ok=True)
    
    return True

# Validate on import
try:
    validate_config()
    print("✓ Configuration validated successfully")
except Exception as e:
    print(f"✗ Configuration error: {e}")