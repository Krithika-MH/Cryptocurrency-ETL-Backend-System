# tests/conftest.py

import pytest
import sys
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from fastapi.testclient import TestClient

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from services.database import Base, get_db
from api.main import app


# Test database URL
TEST_DATABASE_URL = "postgresql://kasparro:password@127.0.0.1:5433/kasparro_test"


@pytest.fixture(scope="session")
def test_engine():
    """Create test database engine"""
    engine = create_engine(TEST_DATABASE_URL)
    Base.metadata.drop_all(bind=engine)  # Clean start
    Base.metadata.create_all(bind=engine)
    yield engine
    Base.metadata.drop_all(bind=engine)
    engine.dispose()


@pytest.fixture(scope="function")
def test_db(test_engine):
    """Create a fresh database session for each test"""
    connection = test_engine.connect()
    transaction = connection.begin()
    TestSessionLocal = sessionmaker(bind=connection)
    db = TestSessionLocal()
    
    try:
        yield db
    finally:
        db.close()
        transaction.rollback()
        connection.close()


@pytest.fixture(scope="function")
def client(test_db):
    """Create test client with test database"""
    def override_get_db():
        try:
            yield test_db
        finally:
            pass
    
    app.dependency_overrides[get_db] = override_get_db
    
    with TestClient(app) as test_client:
        yield test_client
    
    app.dependency_overrides.clear()


@pytest.fixture
def sample_crypto_data():
    """Sample cryptocurrency data for testing"""
    return {
        "crypto_id": "btc-bitcoin",
        "crypto_name": "Bitcoin",
        "price_usd": 50000.0,
        "market_cap_usd": 1000000000000.0,
        "volume_24h_usd": 50000000000.0,
        "change_24h_percent": 2.5,
        "source": "test"
    }


@pytest.fixture
def sample_coinpaprika_response():
    """Sample CoinPaprika API response"""
    return {
        "id": "btc-bitcoin",
        "name": "Bitcoin",
        "symbol": "BTC",
        "rank": 1,
        "quotes": {
            "USD": {
                "price": 50000.0,
                "market_cap": 1000000000000.0,
                "volume_24h": 50000000000.0,
                "percent_change_24h": 2.5
            }
        }
    }


@pytest.fixture
def sample_coingecko_response():
    """Sample CoinGecko API response"""
    return {
        "id": "bitcoin",
        "symbol": "btc",
        "name": "Bitcoin",
        "current_price": 50000.0,
        "market_cap": 1000000000000.0,
        "total_volume": 50000000000.0,
        "price_change_percentage_24h": 2.5
    }


@pytest.fixture
def sample_csv_data():
    """Sample CSV data - FIXED with valid price"""
    return {
        "id": "bitcoin",
        "name": "Bitcoin",
        "price": "50000.0",  # String, will be converted
        "market_cap": "1000000000000.0",
        "volume": "50000000000.0"
    }
