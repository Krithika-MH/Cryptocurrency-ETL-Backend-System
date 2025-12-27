# ingestion/etl.py

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from datetime import datetime, timezone
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List

from ingestion.sources.coinpaprika import CoinPaprikaClient
from ingestion.sources.coingecko import CoinGeckoClient
from ingestion.sources.csv_reader import CSVReader
from ingestion.transformers import DataTransformer
from ingestion.schemas import CryptoData

from services.database import (
    SessionLocal, 
    RawAPIData, 
    RawCSVData,
    CleanedData, 
    ETLCheckpoint, 
    ETLRun
)


class ETLPipeline:
    """
    Complete ETL Pipeline
    Extract → Transform → Load
    """
    
    def __init__(self):
        self.db: Session = SessionLocal()
        self.run_id = None
        self.start_time = None
    
    def __del__(self):
        """Close database connection when done"""
        if self.db:
            self.db.close()
    
    # ==================== CHECKPOINT MANAGEMENT ====================
    
    def get_checkpoint(self, source: str) -> int:
        """Get last processed ID for a source"""
        checkpoint = self.db.query(ETLCheckpoint).filter_by(source=source).first()
        last_id = checkpoint.last_processed_id if checkpoint else 0
        print(f"📍 Checkpoint for {source}: last_id={last_id}")
        return last_id
    
    def update_checkpoint(self, source: str, last_id: int):
        """Update checkpoint after successful batch"""
        checkpoint = self.db.query(ETLCheckpoint).filter_by(source=source).first()
        
        if checkpoint:
            checkpoint.last_processed_id = last_id
            checkpoint.last_processed_timestamp = datetime.now(timezone.utc)
            checkpoint.updated_at = datetime.now(timezone.utc)
        else:
            checkpoint = ETLCheckpoint(
                source=source,
                last_processed_id=last_id,
                last_processed_timestamp=datetime.now(timezone.utc)
            )
            self.db.add(checkpoint)
        
        self.db.commit()
        print(f"✓ Updated checkpoint for {source}: last_id={last_id}")
    
    # ==================== ETL RUN TRACKING ====================
    
    def start_run(self, source: str = "all"):
        """Start tracking an ETL run"""
        self.start_time = datetime.now(timezone.utc)
        run = ETLRun(
            started_at=self.start_time,
            source=source,
            success=False
        )
        self.db.add(run)
        self.db.commit()
        self.run_id = run.id
        print(f"🚀 Started ETL run #{self.run_id} for source: {source}")
    
    def end_run(self, records_processed: int, success: bool, error_message: str = None):
        """End tracking an ETL run"""
        if not self.run_id:
            return
        
        run = self.db.query(ETLRun).filter_by(id=self.run_id).first()
        if run:
            run.ended_at = datetime.now(timezone.utc)
            run.records_processed = records_processed
            run.success = success
            run.error_message = error_message
            self.db.commit()
            
            duration = (run.ended_at - run.started_at).total_seconds()
            status = "✅ SUCCESS" if success else "❌ FAILED"
            print(f"{status} ETL run #{self.run_id}: {records_processed} records in {duration:.2f}s")
    
    # ==================== INGESTION METHODS ====================
    
    def ingest_from_coinpaprika(self, limit: int = 50) -> int:
        """Extract from CoinPaprika API"""
        print("\n" + "="*60)
        print("COINPAPRIKA INGESTION")
        print("="*60)
        
        try:
            # Extract
            client = CoinPaprikaClient()
            raw_data = client.get_tickers(limit=limit)
            
            # Store raw data
            for item in raw_data:
                raw = RawAPIData(source="coinpaprika", raw_data=item)
                self.db.add(raw)
            self.db.commit()
            print(f"✓ Stored {len(raw_data)} raw records")
            
            # Transform
            transformed: List[CryptoData] = DataTransformer.transform_coinpaprika(raw_data)
            
            # Load
            loaded_count = 0
            for crypto in transformed:
                # Convert Pydantic model to dict properly
                crypto_dict = crypto.model_dump()  # Use model_dump() instead of dict()
                
                # Convert datetime to string for JSON storage
                if 'timestamp' in crypto_dict and crypto_dict['timestamp']:
                    crypto_dict['timestamp'] = crypto_dict['timestamp'].isoformat()
                
                cleaned = CleanedData(
                    data_source=crypto.source,
                    crypto_id=crypto.crypto_id,
                    crypto_name=crypto.crypto_name,
                    price_usd=crypto.price_usd,
                    market_cap_usd=crypto.market_cap_usd,
                    volume_24h_usd=crypto.volume_24h_usd,
                    change_24h_percent=crypto.change_24h_percent,
                    normalized_data=crypto_dict  # Use the dict with serialized datetime
                )
                self.db.add(cleaned)
                loaded_count += 1
            
            self.db.commit()
            print(f"✓ Loaded {loaded_count} cleaned records")
            
            # Update checkpoint
            self.update_checkpoint("coinpaprika", loaded_count)
            
            return loaded_count
        
        except Exception as e:
            print(f"❌ CoinPaprika ingestion failed: {e}")
            self.db.rollback()
            raise
    
    def ingest_from_coingecko(self, limit: int = 50) -> int:
        """Extract from CoinGecko API"""
        print("\n" + "="*60)
        print("COINGECKO INGESTION")
        print("="*60)
        
        try:
            # Extract
            client = CoinGeckoClient()
            raw_data = client.get_coins_markets(limit=limit)
            
            # Store raw data
            for item in raw_data:
                raw = RawAPIData(source="coingecko", raw_data=item)
                self.db.add(raw)
            self.db.commit()
            print(f"✓ Stored {len(raw_data)} raw records")
            
            # Transform
            transformed: List[CryptoData] = DataTransformer.transform_coingecko(raw_data)
            
            # Load
            loaded_count = 0
            for crypto in transformed:
                # Convert Pydantic model to dict properly
                crypto_dict = crypto.model_dump()  # Use model_dump() instead of dict()
                
                # Convert datetime to string for JSON storage
                if 'timestamp' in crypto_dict and crypto_dict['timestamp']:
                    crypto_dict['timestamp'] = crypto_dict['timestamp'].isoformat()
                
                cleaned = CleanedData(
                    data_source=crypto.source,
                    crypto_id=crypto.crypto_id,
                    crypto_name=crypto.crypto_name,
                    price_usd=crypto.price_usd,
                    market_cap_usd=crypto.market_cap_usd,
                    volume_24h_usd=crypto.volume_24h_usd,
                    change_24h_percent=crypto.change_24h_percent,
                    normalized_data=crypto_dict
                )
                self.db.add(cleaned)
                loaded_count += 1
            
            self.db.commit()
            print(f"✓ Loaded {loaded_count} cleaned records")
            
            # Update checkpoint
            self.update_checkpoint("coingecko", loaded_count)
            
            return loaded_count
        
        except Exception as e:
            print(f"❌ CoinGecko ingestion failed: {e}")
            self.db.rollback()
            raise
    
    def ingest_from_csv(self) -> int:
        """Extract from CSV file"""
        print("\n" + "="*60)
        print("CSV INGESTION")
        print("="*60)
        
        try:
            # Extract
            reader = CSVReader()
            raw_data = reader.read()
            
            # Store raw data
            for item in raw_data:
                raw = RawCSVData(raw_data=item)
                self.db.add(raw)
            self.db.commit()
            print(f"✓ Stored {len(raw_data)} raw records")
            
            # Transform
            transformed: List[CryptoData] = DataTransformer.transform_csv(raw_data)
            
            # Load
            loaded_count = 0
            for crypto in transformed:
                # Convert Pydantic model to dict properly
                crypto_dict = crypto.model_dump()  # Use model_dump() instead of dict()
                
                # Convert datetime to string for JSON storage
                if 'timestamp' in crypto_dict and crypto_dict['timestamp']:
                    crypto_dict['timestamp'] = crypto_dict['timestamp'].isoformat()
                
                cleaned = CleanedData(
                    data_source=crypto.source,
                    crypto_id=crypto.crypto_id,
                    crypto_name=crypto.crypto_name,
                    price_usd=crypto.price_usd,
                    market_cap_usd=crypto.market_cap_usd,
                    volume_24h_usd=crypto.volume_24h_usd,
                    change_24h_percent=crypto.change_24h_percent,
                    normalized_data=crypto_dict
                )
                self.db.add(cleaned)
                loaded_count += 1
            
            self.db.commit()
            print(f"✓ Loaded {loaded_count} cleaned records")
            
            # Update checkpoint
            self.update_checkpoint("csv", loaded_count)
            
            return loaded_count
        
        except Exception as e:
            print(f"❌ CSV ingestion failed: {e}")
            self.db.rollback()
            raise
    
    # ==================== MAIN ETL RUN ====================
    
    def run(self, sources: List[str] = None):
        """
        Execute complete ETL pipeline
        
        Args:
            sources: List of sources to ingest from
                     Default: ['coinpaprika', 'coingecko', 'csv']
        """
        if sources is None:
            sources = ['coinpaprika', 'coingecko', 'csv']
        
        print("\n" + "="*60)
        print(f"🚀 STARTING ETL PIPELINE")
        print(f"Sources: {', '.join(sources)}")
        print("="*60)
        
        self.start_run(source=",".join(sources))
        total_records = 0
        
        try:
            # Ingest from each source
            if 'coinpaprika' in sources:
                count = self.ingest_from_coinpaprika(limit=10)  # Start with 10
                total_records += count
            
            if 'coingecko' in sources:
                count = self.ingest_from_coingecko(limit=10)  # Start with 10
                total_records += count
            
            if 'csv' in sources:
                count = self.ingest_from_csv()
                total_records += count
            
            # Mark run as successful
            self.end_run(records_processed=total_records, success=True)
            
            print("\n" + "="*60)
            print(f"✅ ETL PIPELINE COMPLETED SUCCESSFULLY!")
            print(f"Total records processed: {total_records}")
            print("="*60 + "\n")
            
            return True
        
        except Exception as e:
            # Mark run as failed
            self.end_run(records_processed=total_records, success=False, error_message=str(e))
            
            print("\n" + "="*60)
            print(f"❌ ETL PIPELINE FAILED!")
            print(f"Error: {e}")
            print("="*60 + "\n")
            
            return False


# Run ETL pipeline
if __name__ == "__main__":
    pipeline = ETLPipeline()
    success = pipeline.run()
    exit(0 if success else 1)
