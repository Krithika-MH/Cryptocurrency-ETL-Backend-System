# ingestion/sources/csv_reader.py

import pandas as pd
import os
from typing import List, Dict, Any
from core.config import config


class CSVReader:
    """
    Reader for CSV files containing cryptocurrency data
    """
    
    def __init__(self, file_path: str = None):
        self.file_path = file_path or config.CSV_FILE_PATH
    
    def read(self) -> List[Dict[str, Any]]:
        """
        Read CSV file and return as list of dictionaries
        
        Returns:
            List of dictionaries (one per row)
        """
        print(f"🔄 Reading CSV file: {self.file_path}")
        
        try:
            # Check if file exists
            if not os.path.exists(self.file_path):
                raise FileNotFoundError(f"CSV file not found: {self.file_path}")
            
            # Read CSV
            df = pd.read_csv(self.file_path)
            
            print(f"✓ Successfully read {len(df)} rows from CSV")
            
            # Convert to list of dictionaries
            data = df.to_dict(orient='records')
            return data
        
        except FileNotFoundError as e:
            print(f"✗ File not found: {e}")
            raise
        except Exception as e:
            print(f"✗ Error reading CSV: {e}")
            raise


# Test the reader
if __name__ == "__main__":
    reader = CSVReader()
    
    print("\nTesting CSV Reader...")
    print("="*50)
    
    try:
        data = reader.read()
        
        print(f"\nRead {len(data)} records:")
        for record in data[:3]:  # Show first 3
            print(f"  - {record.get('name')}: ${record.get('price_usd')}")
    
    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        print("\nTroubleshooting:")
        print("1. Make sure data/crypto_sample.csv exists")
        print("2. Check CSV has headers: id,name,price_usd,...")
