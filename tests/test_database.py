# tests/test_database.py

import pytest
from datetime import datetime, timezone
from services.database import (
    CleanedData,
    RawAPIData,
    RawCSVData,
    ETLRun,
    ETLCheckpoint
)


@pytest.mark.unit
class TestDatabaseModels:
    """Test database models and operations"""

    def test_create_cleaned_data(self, test_db, sample_crypto_data):
        """Test creating a CleanedData record"""
        record = CleanedData(
            data_source=sample_crypto_data["source"],
            crypto_id=sample_crypto_data["crypto_id"],
            crypto_name=sample_crypto_data["crypto_name"],
            price_usd=sample_crypto_data["price_usd"],
            market_cap_usd=sample_crypto_data["market_cap_usd"],
            volume_24h_usd=sample_crypto_data["volume_24h_usd"],
            change_24h_percent=sample_crypto_data["change_24h_percent"],
            normalized_data=sample_crypto_data
        )

        test_db.add(record)
        test_db.commit()

        assert record.id is not None
        assert record.crypto_id == "btc-bitcoin"
        assert record.price_usd == 50000.0

    def test_query_cleaned_data(self, test_db, sample_crypto_data):
        """Test querying CleanedData records"""
        # Add test data
        record = CleanedData(
            data_source="test",
            crypto_id="btc-bitcoin",
            crypto_name="Bitcoin",
            price_usd=50000.0,
            market_cap_usd=1000000000000.0,
            normalized_data=sample_crypto_data
        )
        test_db.add(record)
        test_db.commit()

        # Query
        result = test_db.query(CleanedData).filter_by(crypto_id="btc-bitcoin").first()

        assert result is not None
        assert result.crypto_name == "Bitcoin"
        assert result.price_usd == 50000.0

    def test_create_etl_run(self, test_db):
        """Test creating an ETL run record"""
        run = ETLRun(
            started_at=datetime.now(timezone.utc),
            source="test",
            success=False
        )

        test_db.add(run)
        test_db.commit()

        assert run.id is not None
        assert run.source == "test"
        assert run.success is False

    def test_create_checkpoint(self, test_db):
        """Test creating a checkpoint"""
        checkpoint = ETLCheckpoint(
            source="test",
            last_processed_id=100,
            last_processed_timestamp=datetime.now(timezone.utc)
        )

        test_db.add(checkpoint)
        test_db.commit()

        assert checkpoint.id is not None
        assert checkpoint.source == "test"
        assert checkpoint.last_processed_id == 100

    def test_raw_api_data(self, test_db, sample_coinpaprika_response):
        """Test storing raw API data"""
        raw = RawAPIData(
            source="coinpaprika",
            raw_data=sample_coinpaprika_response
        )

        test_db.add(raw)
        test_db.commit()

        assert raw.id is not None
        assert raw.source == "coinpaprika"
        assert raw.raw_data["id"] == "btc-bitcoin"
