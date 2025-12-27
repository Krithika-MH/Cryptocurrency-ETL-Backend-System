# core/config.py

import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    """Application configuration"""
    
    # Database - Use port 5433 to avoid conflict with local PostgreSQL
    DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://kasparro:password@127.0.0.1:5433/kasparro")
    
    # File paths
    CSV_FILE_PATH = os.getenv("CSV_FILE_PATH", "data/crypto_sample.csv")
    
    # ETL Settings
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
    RATE_LIMIT_CALLS_PER_MINUTE = int(os.getenv("RATE_LIMIT_CALLS_PER_MINUTE", "30"))
    ETL_RETRY_ATTEMPTS = int(os.getenv("ETL_RETRY_ATTEMPTS", "3"))
    ETL_RETRY_DELAY_SECONDS = int(os.getenv("ETL_RETRY_DELAY_SECONDS", "2"))
    
    # API URLs
    COINPAPRIKA_BASE_URL = "https://api.coinpaprika.com/v1"
    COINGECKO_BASE_URL = "https://api.coingecko.com/api/v3"
    
    @classmethod
    def validate(cls):
        """Check if all required settings are present"""
        if not os.path.exists(cls.CSV_FILE_PATH):
            print(f"⚠️  Warning: CSV file not found at {cls.CSV_FILE_PATH}")
        
        print("✓ Configuration loaded successfully")


config = Config()


if __name__ == "__main__":
    print("Current configuration:")
    print(f"  DATABASE_URL: {config.DATABASE_URL}")
    print(f"  CSV_FILE_PATH: {config.CSV_FILE_PATH}")
    print(f"  BATCH_SIZE: {config.BATCH_SIZE}")
    config.validate()
