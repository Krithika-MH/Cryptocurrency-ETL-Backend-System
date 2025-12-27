# services/database.py

from sqlalchemy import create_engine, Column, Integer, String, DateTime, JSON, Float, Boolean, text
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timezone
import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://kasparro:password@127.0.0.1:5433/kasparro")

engine = create_engine(DATABASE_URL, echo=False)  # Set echo=False to reduce logs
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


# ==================== DATABASE TABLES ====================

class RawAPIData(Base):
    __tablename__ = "raw_api_data"
    id = Column(Integer, primary_key=True, autoincrement=True)
    source = Column(String(50), nullable=False)
    raw_data = Column(JSON, nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

class RawCSVData(Base):
    __tablename__ = "raw_csv_data"
    id = Column(Integer, primary_key=True, autoincrement=True)
    raw_data = Column(JSON, nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

class CleanedData(Base):
    __tablename__ = "cleaned_data"
    id = Column(Integer, primary_key=True, autoincrement=True)
    data_source = Column(String(50), nullable=False)
    crypto_id = Column(String(100))
    crypto_name = Column(String(100))
    price_usd = Column(Float)
    market_cap_usd = Column(Float, nullable=True)
    volume_24h_usd = Column(Float, nullable=True)
    change_24h_percent = Column(Float, nullable=True)
    normalized_data = Column(JSON)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

class ETLCheckpoint(Base):
    __tablename__ = "etl_checkpoints"
    id = Column(Integer, primary_key=True, autoincrement=True)
    source = Column(String(50), unique=True, nullable=False)
    last_processed_id = Column(Integer, default=0)
    last_processed_timestamp = Column(DateTime, nullable=True)
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))

class ETLRun(Base):
    __tablename__ = "etl_runs"
    id = Column(Integer, primary_key=True, autoincrement=True)
    started_at = Column(DateTime, nullable=False)
    ended_at = Column(DateTime, nullable=True)
    records_processed = Column(Integer, default=0)
    success = Column(Boolean, default=False)
    error_message = Column(String, nullable=True)
    source = Column(String(50), nullable=True)


# ==================== HELPER FUNCTIONS ====================

def init_db():
    """Initialize database - create all tables"""
    print("Creating database tables...")
    Base.metadata.create_all(bind=engine)
    print("✓ Database tables created successfully!")

def get_db():
    """Get database session"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def test_connection():
    """Test if database connection works"""
    try:
        db = SessionLocal()
        db.execute(text("SELECT 1"))  # Fixed: use text() wrapper
        db.close()
        print("✓ Database connection successful!")
        return True
    except Exception as e:
        print(f"✗ Database connection failed: {e}")
        return False


# ==================== RUN THIS TO TEST ====================

if __name__ == "__main__":
    print("\n" + "="*60)
    print("Testing Database Connection")
    print("="*60 + "\n")
    
    print("Database URL:", DATABASE_URL)
    print()
    
    if test_connection():
        print("\nInitializing database tables...")
        init_db()
        print("\n" + "="*60)
        print("✅ Database setup complete!")
        print("="*60 + "\n")
    else:
        print("\n" + "="*60)
        print("❌ Database connection failed!")
        print("="*60 + "\n")
        print("Troubleshooting:")
        print("1. Check Docker is running: docker ps")
        print("2. Start database: docker-compose up -d postgres")
        print("3. Check .env file has correct DATABASE_URL")
