# ingestion/sources/coingecko.py

import requests
import time
from typing import List, Dict, Any
from core.config import config


class CoinGeckoClient:
    """
    Client for CoinGecko API
    Docs: https://www.coingecko.com/en/api/documentation
    Free tier: 10-50 calls/minute
    """
    
    def __init__(self):
        self.base_url = config.COINGECKO_BASE_URL
        self.headers = {}
    
    def get_coins_markets(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get list of cryptocurrencies with market data
        
        Args:
            limit: Number of cryptos to fetch
        
        Returns:
            List of crypto data dictionaries
        """
        url = f"{self.base_url}/coins/markets"
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": min(limit, 250),  # API max is 250
            "page": 1,
            "sparkline": False
        }
        
        print(f"🔄 Fetching {limit} coins from CoinGecko...")
        
        try:
            response = requests.get(
                url,
                headers=self.headers,
                params=params,
                timeout=30
            )
            response.raise_for_status()
            
            data = response.json()
            print(f"✓ Successfully fetched {len(data)} coins from CoinGecko")
            return data
        
        except requests.exceptions.RequestException as e:
            print(f"✗ CoinGecko API error: {e}")
            raise
    
    def get_coin_by_id(self, coin_id: str) -> Dict[str, Any]:
        """
        Get specific cryptocurrency data
        
        Args:
            coin_id: Coin ID like "bitcoin"
        
        Returns:
            Coin data dictionary
        """
        url = f"{self.base_url}/coins/{coin_id}"
        params = {
            "localization": False,
            "tickers": False,
            "market_data": True,
            "community_data": False,
            "developer_data": False
        }
        
        print(f"🔄 Fetching {coin_id} from CoinGecko...")
        
        try:
            response = requests.get(
                url,
                headers=self.headers,
                params=params,
                timeout=30
            )
            response.raise_for_status()
            
            data = response.json()
            print(f"✓ Successfully fetched {coin_id}")
            return data
        
        except requests.exceptions.RequestException as e:
            print(f"✗ CoinGecko API error: {e}")
            raise


# Test the client
if __name__ == "__main__":
    client = CoinGeckoClient()
    
    print("\nTesting CoinGecko API...")
    print("="*50)
    
    try:
        # Fetch top 5 cryptos
        data = client.get_coins_markets(limit=5)
        
        print(f"\nReceived {len(data)} cryptocurrencies:")
        for crypto in data[:3]:  # Show first 3
            print(f"  - {crypto['name']}: ${crypto['current_price']:.2f}")
    
    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        print("\nTroubleshooting:")
        print("1. Make sure you have internet connection")
        print("2. CoinGecko might be rate limiting (wait 1 minute and try again)")
