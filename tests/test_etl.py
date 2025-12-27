# tests/test_etl.py

import pytest
from unittest.mock import Mock, patch, MagicMock
from ingestion.etl import ETLPipeline
from services.database import CleanedData, ETLRun, ETLCheckpoint


@pytest.mark.etl
class TestETLPipeline:
    """Test ETL pipeline operations"""

    def test_etl_pipeline_initialization(self):
        """Test ETL pipeline can be initialized"""
        pipeline = ETLPipeline()
        assert pipeline.db is not None
        assert pipeline.run_id is None

    def test_start_run(self, test_db):
        """Test starting an ETL run"""
        pipeline = ETLPipeline()
        pipeline.db = test_db

        pipeline.start_run(source="test")

        assert pipeline.run_id is not None
        run = test_db.query(ETLRun).filter_by(id=pipeline.run_id).first()
        assert run is not None
        assert run.source == "test"
        assert run.success is False

    def test_end_run_success(self, test_db):
        """Test ending an ETL run successfully"""
        pipeline = ETLPipeline()
        pipeline.db = test_db

        pipeline.start_run(source="test")
        pipeline.end_run(records_processed=10, success=True)

        run = test_db.query(ETLRun).filter_by(id=pipeline.run_id).first()
        assert run.success is True
        assert run.records_processed == 10
        assert run.ended_at is not None

    def test_end_run_failure(self, test_db):
        """Test ending an ETL run with failure"""
        pipeline = ETLPipeline()
        pipeline.db = test_db

        pipeline.start_run(source="test")
        pipeline.end_run(
            records_processed=0,
            success=False,
            error_message="Test error"
        )

        run = test_db.query(ETLRun).filter_by(id=pipeline.run_id).first()
        assert run.success is False
        assert run.error_message == "Test error"

    def test_checkpoint_creation(self, test_db):
        """Test checkpoint creation"""
        pipeline = ETLPipeline()
        pipeline.db = test_db

        pipeline.update_checkpoint("test_source", 100)

        checkpoint = test_db.query(ETLCheckpoint).filter_by(
            source="test_source"
        ).first()

        assert checkpoint is not None
        assert checkpoint.last_processed_id == 100

    def test_checkpoint_update(self, test_db):
        """Test checkpoint update (idempotency)"""
        pipeline = ETLPipeline()
        pipeline.db = test_db

        # Create initial checkpoint
        pipeline.update_checkpoint("test_source", 100)

        # Update checkpoint
        pipeline.update_checkpoint("test_source", 200)

        # Should have only one checkpoint record
        checkpoints = test_db.query(ETLCheckpoint).filter_by(
            source="test_source"
        ).all()

        assert len(checkpoints) == 1
        assert checkpoints[0].last_processed_id == 200

    def test_get_checkpoint(self, test_db):
        """Test retrieving checkpoint"""
        pipeline = ETLPipeline()
        pipeline.db = test_db
        
        # Use unique source name to avoid conflicts
        import uuid
        source_name = f"test_source_{uuid.uuid4().hex[:8]}"
        
        # Create checkpoint
        checkpoint = ETLCheckpoint(
            source=source_name,
            last_processed_id=50
        )
        test_db.add(checkpoint)
        test_db.commit()
        
        # Get checkpoint
        last_id = pipeline.get_checkpoint(source_name)
        
        assert last_id == 50


    def test_get_checkpoint_not_exists(self, test_db):
        """Test getting checkpoint that doesn't exist"""
        pipeline = ETLPipeline()
        pipeline.db = test_db

        last_id = pipeline.get_checkpoint("nonexistent")

        assert last_id == 0

    @patch('ingestion.etl.CoinPaprikaClient')
    def test_ingest_coinpaprika_mock(self, mock_client, test_db):
        """Test CoinPaprika ingestion with mocked API"""
        # Mock API response
        mock_instance = mock_client.return_value
        mock_instance.get_tickers.return_value = [{
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
        }]

        pipeline = ETLPipeline()
        pipeline.db = test_db

        count = pipeline.ingest_from_coinpaprika(limit=1)

        assert count == 1

        # Verify data was stored
        records = test_db.query(CleanedData).filter_by(
            data_source="coinpaprika"
        ).all()
        assert len(records) == 1
