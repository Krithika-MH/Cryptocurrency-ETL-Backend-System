# ingestion/sources/coinpaprika.py

import requests
import time
from typing import List, Dict, Any
from core.config import config


class CoinPaprikaClient:
    """
    Client for CoinPaprika API
    FREE - No API key required!
    Rate limit: 1000 requests/day
    Docs: https://api.coinpaprika.com/
    """
    
    def __init__(self):
        self.base_url = config.COINPAPRIKA_BASE_URL
        # NO HEADERS NEEDED - API is completely free!
        self.headers = {
            "User-Agent": "Kasparro-Backend-ETL/1.0"
        }
    
    def get_tickers(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get list of cryptocurrencies with prices
        
        Args:
            limit: Number of cryptos to fetch (max 100)
        
        Returns:
            List of crypto data dictionaries
        
        Example response:
        [
            {
                "id": "btc-bitcoin",
                "name": "Bitcoin",
                "symbol": "BTC",
                "rank": 1,
                "quotes": {
                    "USD": {
                        "price": 42000.50,
                        "volume_24h": 25000000000,
                        "market_cap": 800000000000,
                        "percent_change_24h": 2.5
                    }
                }
            }
        ]
        """
        url = f"{self.base_url}/tickers"
        params = {
            "quotes": "USD",  # Get prices in USD
            "limit": limit
        }
        
        print(f"🔄 Fetching {limit} tickers from CoinPaprika (FREE API, no key!)...")
        
        try:
            response = requests.get(
                url,
                headers=self.headers,
                params=params,
                timeout=30
            )
            
            # Check for rate limiting
            if response.status_code == 429:
                print("⚠️  Rate limit reached (1000 requests/day). Waiting 60 seconds...")
                time.sleep(60)
                return self.get_tickers(limit)
            
            response.raise_for_status()
            data = response.json()
            
            print(f"✓ Successfully fetched {len(data)} tickers from CoinPaprika")
            return data
        
        except requests.exceptions.RequestException as e:
            print(f"✗ CoinPaprika API error: {e}")
            raise
    
    def get_ticker_by_id(self, crypto_id: str) -> Dict[str, Any]:
        """
        Get specific cryptocurrency data
        
        Args:
            crypto_id: Crypto ID like "btc-bitcoin"
        
        Returns:
            Crypto data dictionary
        """
        url = f"{self.base_url}/tickers/{crypto_id}"
        params = {"quotes": "USD"}
        
        print(f"🔄 Fetching {crypto_id} from CoinPaprika...")
        
        try:
            response = requests.get(
                url,
                headers=self.headers,
                params=params,
                timeout=30
            )
            
            if response.status_code == 429:
                print("⚠️  Rate limit reached. Waiting 60 seconds...")
                time.sleep(60)
                return self.get_ticker_by_id(crypto_id)
            
            response.raise_for_status()
            data = response.json()
            
            print(f"✓ Successfully fetched {crypto_id}")
            return data
        
        except requests.exceptions.RequestException as e:
            print(f"✗ CoinPaprika API error: {e}")
            raise
    
    def get_global_stats(self) -> Dict[str, Any]:
        """
        Get global cryptocurrency market stats
        
        Returns:
            Global market data
        """
        url = f"{self.base_url}/global"
        
        print("🔄 Fetching global stats from CoinPaprika...")
        
        try:
            response = requests.get(url, headers=self.headers, timeout=30)
            
            if response.status_code == 429:
                print("⚠️  Rate limit reached. Waiting 60 seconds...")
                time.sleep(60)
                return self.get_global_stats()
            
            response.raise_for_status()
            data = response.json()
            
            print("✓ Successfully fetched global stats")
            return data
        
        except requests.exceptions.RequestException as e:
            print(f"✗ CoinPaprika API error: {e}")
            raise


# Test the client
if __name__ == "__main__":
    print("\n" + "="*60)
    print("Testing CoinPaprika API (FREE - No Key Required!)")
    print("="*60 + "\n")
    
    client = CoinPaprikaClient()
    
    try:
        # Test 1: Fetch top 5 cryptos
        print("Test 1: Fetching top 5 cryptocurrencies...")
        data = client.get_tickers(limit=5)
        
        print(f"\n✓ SUCCESS! Received {len(data)} cryptocurrencies:\n")
        for crypto in data:
            price = crypto.get('quotes', {}).get('USD', {}).get('price', 0)
            print(f"  • {crypto['name']}: ${price:,.2f}")
        
        print("\n" + "-"*60 + "\n")
        
        # Test 2: Fetch Bitcoin specifically
        print("Test 2: Fetching Bitcoin data...")
        btc_data = client.get_ticker_by_id("btc-bitcoin")
        
        btc_price = btc_data.get('quotes', {}).get('USD', {}).get('price', 0)
        print(f"\n✓ SUCCESS! Bitcoin: ${btc_price:,.2f}\n")
        
        print("\n" + "="*60)
        print("✅ All tests passed! CoinPaprika API is working!")
        print("="*60 + "\n")
    
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        print("\nTroubleshooting:")
        print("1. Make sure you have internet connection")
        print("2. Check if CoinPaprika API is up: https://api.coinpaprika.com/v1/global")
        print("3. If rate limited, wait 1 minute and try again")
