# tests/test_api.py

import pytest
from fastapi import status
from datetime import datetime, timezone
from services.database import CleanedData, ETLRun


@pytest.mark.api
class TestAPIEndpoints:
    """Test API endpoints"""

    def test_root_endpoint(self, client):
        """Test root endpoint returns welcome message"""
        response = client.get("/")

        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert "message" in data
        assert "version" in data
        assert data["version"] == "1.0.0"

    def test_health_endpoint_healthy(self, client, test_db):
        """Test health endpoint when system is healthy"""
        response = client.get("/health")

        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["status"] == "healthy"
        assert data["database"] == "connected"
        assert "timestamp" in data

    def test_get_data_endpoint_empty(self, client):
        """Test /data endpoint with no data"""
        response = client.get("/data")

        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["total"] == 0
        assert len(data["data"]) == 0

    def test_get_data_endpoint_with_data(self, client, test_db):
        """Test /data endpoint with sample data"""
        # Add test data
        record = CleanedData(
            data_source="test",
            crypto_id="btc-bitcoin",
            crypto_name="Bitcoin",
            price_usd=50000.0,
            market_cap_usd=1000000000000.0,
            volume_24h_usd=50000000000.0,
            change_24h_percent=2.5,
            normalized_data={"test": "data"}
        )
        test_db.add(record)
        test_db.commit()

        response = client.get("/data?limit=10")

        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["total"] == 1
        assert len(data["data"]) == 1
        assert data["data"][0]["crypto_id"] == "btc-bitcoin"

    def test_get_data_pagination(self, client, test_db):
        """Test pagination works correctly"""
        # Add multiple records
        for i in range(15):
            record = CleanedData(
                data_source="test",
                crypto_id=f"coin-{i}",
                crypto_name=f"Coin {i}",
                price_usd=float(i * 100),
                market_cap_usd=float(i * 1000000),
                normalized_data={}
            )
            test_db.add(record)
        test_db.commit()

        # Test first page
        response = client.get("/data?limit=10&offset=0")
        data = response.json()
        assert data["total"] == 15
        assert len(data["data"]) == 10
        assert data["page"] == 1

        # Test second page
        response = client.get("/data?limit=10&offset=10")
        data = response.json()
        assert len(data["data"]) == 5
        assert data["page"] == 2

    def test_get_data_filter_by_source(self, client, test_db):
        """Test filtering by data source"""
        # Add records from different sources
        record1 = CleanedData(
            data_source="coinpaprika",
            crypto_id="btc-bitcoin",
            crypto_name="Bitcoin",
            price_usd=50000.0,
            normalized_data={}
        )
        record2 = CleanedData(
            data_source="coingecko",
            crypto_id="eth-ethereum",
            crypto_name="Ethereum",
            price_usd=3000.0,
            normalized_data={}
        )
        test_db.add_all([record1, record2])
        test_db.commit()

        response = client.get("/data?source=coinpaprika")
        data = response.json()

        assert data["total"] == 1
        assert data["data"][0]["data_source"] == "coinpaprika"

    def test_get_crypto_by_id(self, client, test_db):
        """Test getting specific crypto by ID"""
        record = CleanedData(
            data_source="test",
            crypto_id="btc-bitcoin",
            crypto_name="Bitcoin",
            price_usd=50000.0,
            normalized_data={}
        )
        test_db.add(record)
        test_db.commit()

        response = client.get("/data/btc-bitcoin")

        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["crypto_id"] == "btc-bitcoin"
        assert data["crypto_name"] == "Bitcoin"

    def test_get_crypto_by_id_not_found(self, client):
        """Test 404 when crypto not found"""
        response = client.get("/data/nonexistent-coin")

        assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_stats_endpoint(self, client, test_db):
        """Test /stats endpoint"""
        # Add some test data
        record = CleanedData(
            data_source="test",
            crypto_id="btc-bitcoin",
            crypto_name="Bitcoin",
            price_usd=50000.0,
            normalized_data={}
        )
        test_db.add(record)

        # Add ETL run
        run = ETLRun(
            started_at=datetime.now(timezone.utc),
            ended_at=datetime.now(timezone.utc),
            source="test",
            records_processed=1,
            success=True
        )
        test_db.add(run)
        test_db.commit()

        response = client.get("/stats")

        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert "total_cleaned_records" in data
        assert "sources" in data
        assert "recent_etl_runs" in data

    def test_stats_summary_endpoint(self, client, test_db):
        """Test /stats/summary endpoint"""
        response = client.get("/stats/summary")

        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert "total_records" in data
        assert "records_by_source" in data
