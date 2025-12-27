# api/routes/health.py

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import datetime, timezone
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from services.database import get_db, ETLRun

router = APIRouter()


@router.get("/health")
async def health_check(db: Session = Depends(get_db)):
    """
    Health check endpoint
    
    Returns:
        - status: System status (healthy/unhealthy)
        - database: Database connection status
        - timestamp: Current server time
        - last_etl_run: Information about the last ETL run
    """
    try:
        # Check database connection
        db.execute(text("SELECT 1"))
        db_status = "connected"
        
        # Get last ETL run
        last_run = db.query(ETLRun).order_by(ETLRun.started_at.desc()).first()
        
        last_etl_info = None
        if last_run:
            last_etl_info = {
                "run_id": last_run.id,
                "source": last_run.source,
                "started_at": last_run.started_at.isoformat() if last_run.started_at else None,
                "ended_at": last_run.ended_at.isoformat() if last_run.ended_at else None,
                "records_processed": last_run.records_processed,
                "success": last_run.success,
                "error_message": last_run.error_message
            }
        
        return {
            "status": "healthy",
            "database": db_status,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "last_etl_run": last_etl_info
        }
    
    except Exception as e:
        return {
            "status": "unhealthy",
            "database": "disconnected",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "error": str(e)
        }
