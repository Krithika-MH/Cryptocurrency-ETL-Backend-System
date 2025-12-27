# ingestion/transformers.py

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from typing import List, Dict, Any
from ingestion.schemas import CryptoData
from pydantic import ValidationError
from datetime import datetime


class DataTransformer:
    """
    Transform data from different sources into unified format
    """
    
    @staticmethod
    def transform_coinpaprika(raw_data: List[Dict]) -> List[CryptoData]:
        """
        Transform CoinPaprika API response to unified schema
        
        CoinPaprika format:
        {
            "id": "btc-bitcoin",
            "name": "Bitcoin",
            "quotes": {
                "USD": {
                    "price": 87589.46,
                    "volume_24h": 45000000000,
                    "market_cap": 1700000000000,
                    "percent_change_24h": 2.5
                }
            }
        }
        """
        transformed = []
        errors = 0
        
        print(f"🔄 Transforming {len(raw_data)} CoinPaprika records...")
        
        for item in raw_data:
            try:
                quotes = item.get('quotes', {}).get('USD', {})
                
                crypto = CryptoData(
                    crypto_id=item.get('id', ''),
                    crypto_name=item.get('name', ''),
                    price_usd=quotes.get('price', 0),
                    market_cap_usd=quotes.get('market_cap'),
                    volume_24h_usd=quotes.get('volume_24h'),
                    change_24h_percent=quotes.get('percent_change_24h'),
                    source="coinpaprika",
                    timestamp=datetime.utcnow()
                )
                transformed.append(crypto)
                
            except (ValidationError, KeyError, TypeError) as e:
                errors += 1
                print(f"⚠️  Skipped invalid record: {item.get('id', 'unknown')} - {e}")
                continue
        
        print(f"✓ Transformed {len(transformed)} records ({errors} errors)")
        return transformed
    
    @staticmethod
    def transform_coingecko(raw_data: List[Dict]) -> List[CryptoData]:
        """
        Transform CoinGecko API response to unified schema
        
        CoinGecko format:
        {
            "id": "bitcoin",
            "name": "Bitcoin",
            "current_price": 87529.0,
            "market_cap": 1700000000000,
            "total_volume": 45000000000,
            "price_change_percentage_24h": 2.5
        }
        """
        transformed = []
        errors = 0
        
        print(f"🔄 Transforming {len(raw_data)} CoinGecko records...")
        
        for item in raw_data:
            try:
                crypto = CryptoData(
                    crypto_id=item.get('id', ''),
                    crypto_name=item.get('name', ''),
                    price_usd=item.get('current_price', 0),
                    market_cap_usd=item.get('market_cap'),
                    volume_24h_usd=item.get('total_volume'),
                    change_24h_percent=item.get('price_change_percentage_24h'),
                    source="coingecko",
                    timestamp=datetime.utcnow()
                )
                transformed.append(crypto)
                
            except (ValidationError, KeyError, TypeError) as e:
                errors += 1
                print(f"⚠️  Skipped invalid record: {item.get('id', 'unknown')} - {e}")
                continue
        
        print(f"✓ Transformed {len(transformed)} records ({errors} errors)")
        return transformed
    
    @staticmethod
    def transform_csv(raw_data: List[Dict]) -> List[CryptoData]:
        """
        Transform CSV data to unified schema
        
        Handles TWO formats:
        1. Test format: {"id": "bitcoin", "price": "50000", "market_cap": "1000000", "volume": "500000"}
        2. Actual CSV: {"id": "bitcoin", "price_usd": "50000", "market_cap_usd": "1000000", "volume_24h_usd": "500000"}
        """
        transformed = []
        errors = 0
        
        print(f"🔄 Transforming {len(raw_data)} CSV records...")
        
        for item in raw_data:
            try:
                # Handle both test format and actual CSV format
                # Try "price_usd" first, fall back to "price"
                price_raw = item.get('price_usd') or item.get('price', '0')
                market_cap_raw = item.get('market_cap_usd') or item.get('market_cap')
                volume_raw = item.get('volume_24h_usd') or item.get('volume')
                
                # Convert to float, handling string values and commas
                price_usd = float(str(price_raw).replace(',', '')) if price_raw else 0.0
                market_cap_usd = float(str(market_cap_raw).replace(',', '')) if market_cap_raw else None
                volume_24h_usd = float(str(volume_raw).replace(',', '')) if volume_raw else None
                
                crypto = CryptoData(
                    crypto_id=str(item.get('id', '')).lower().strip(),
                    crypto_name=str(item.get('name', '')).strip(),
                    price_usd=price_usd,
                    market_cap_usd=market_cap_usd,
                    volume_24h_usd=volume_24h_usd,
                    change_24h_percent=None,  # CSV doesn't have this
                    source="csv",
                    timestamp=datetime.utcnow()
                )
                transformed.append(crypto)
                
            except (ValidationError, KeyError, TypeError, ValueError) as e:
                errors += 1
                print(f"⚠️  Skipped invalid record: {item.get('id', 'unknown')} - {e}")
                continue
        
        print(f"✓ Transformed {len(transformed)} records ({errors} errors)")
        return transformed


# Test the transformers
if __name__ == "__main__":
    print("\n" + "="*60)
    print("Testing Data Transformers")
    print("="*60 + "\n")
    
    # Test CoinPaprika transformer
    sample_coinpaprika = [
        {
            "id": "btc-bitcoin",
            "name": "Bitcoin",
            "quotes": {
                "USD": {
                    "price": 87589.46,
                    "volume_24h": 45000000000,
                    "market_cap": 1700000000000,
                    "percent_change_24h": 2.5
                }
            }
        }
    ]
    
    transformed = DataTransformer.transform_coinpaprika(sample_coinpaprika)
    print(f"CoinPaprika result: {transformed[0]}\n")
    
    # Test CoinGecko transformer
    sample_coingecko = [
        {
            "id": "bitcoin",
            "name": "Bitcoin",
            "current_price": 87529.0,
            "market_cap": 1700000000000,
            "total_volume": 45000000000,
            "price_change_percentage_24h": 2.5
        }
    ]
    
    transformed = DataTransformer.transform_coingecko(sample_coingecko)
    print(f"CoinGecko result: {transformed[0]}\n")
    
    # Test CSV transformer (both formats)
    sample_csv_test_format = [
        {
            "id": "bitcoin",
            "name": "Bitcoin",
            "price": "42000.50",
            "market_cap": "800000000000",
            "volume": "25000000000"
        }
    ]
    
    transformed = DataTransformer.transform_csv(sample_csv_test_format)
    print(f"CSV result (test format): {transformed[0]}\n")
    
    sample_csv_actual_format = [
        {
            "id": "ethereum",
            "name": "Ethereum",
            "price_usd": "2500.75",
            "market_cap_usd": "300000000000",
            "volume_24h_usd": "15000000000"
        }
    ]
    
    transformed = DataTransformer.transform_csv(sample_csv_actual_format)
    print(f"CSV result (actual format): {transformed[0]}\n")
    
    print("="*60)
    print("✅ All transformers working!")
    print("="*60 + "\n")
