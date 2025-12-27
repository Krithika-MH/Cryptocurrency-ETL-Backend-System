# tests/test_transformers.py

import pytest
from ingestion.transformers import DataTransformer
from ingestion.schemas import CryptoData


@pytest.mark.unit
class TestDataTransformers:
    """Test data transformation logic"""

    def test_transform_coinpaprika(self, sample_coinpaprika_response):
        """Test CoinPaprika data transformation"""
        result = DataTransformer.transform_coinpaprika([sample_coinpaprika_response])

        assert len(result) == 1
        assert isinstance(result[0], CryptoData)
        assert result[0].crypto_id == "btc-bitcoin"
        assert result[0].crypto_name == "Bitcoin"
        assert result[0].price_usd == 50000.0
        assert result[0].source == "coinpaprika"

    def test_transform_coingecko(self, sample_coingecko_response):
        """Test CoinGecko data transformation"""
        result = DataTransformer.transform_coingecko([sample_coingecko_response])

        assert len(result) == 1
        assert isinstance(result[0], CryptoData)
        assert result[0].crypto_id == "bitcoin"
        assert result[0].crypto_name == "Bitcoin"
        assert result[0].price_usd == 50000.0
        assert result[0].source == "coingecko"

    def test_transform_csv(self, sample_csv_data):
        """Test CSV data transformation"""
        # Ensure price is numeric string
        sample_csv_data["price"] = "50000.0"
        sample_csv_data["market_cap"] = "1000000000000.0"
        sample_csv_data["volume"] = "50000000000.0"
        
        result = DataTransformer.transform_csv([sample_csv_data])
        
        assert len(result) == 1
        assert isinstance(result[0], CryptoData)
        assert result[0].crypto_id == "bitcoin"
        assert result[0].crypto_name == "Bitcoin"
        assert result[0].price_usd == 50000.0
        assert result[0].source == "csv"


    def test_transform_empty_list(self):
        """Test transformation with empty list"""
        result = DataTransformer.transform_coinpaprika([])
        assert len(result) == 0

    def test_transform_missing_price(self):
        """Test transformation handles missing price"""
        data = {
            "id": "test-coin",
            "name": "Test Coin",
            "symbol": "TEST",
            "quotes": {
                "USD": {
                    "market_cap": 1000000.0
                    # price missing
                }
            }
        }

        result = DataTransformer.transform_coinpaprika([data])

        # Should handle gracefully - either skip or use default
        assert len(result) == 0 or result[0].price_usd == 0.0
