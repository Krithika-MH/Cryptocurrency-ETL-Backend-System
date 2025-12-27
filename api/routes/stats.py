# api/routes/stats.py

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func, desc
from datetime import datetime, timezone
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from services.database import (
    get_db, 
    CleanedData, 
    RawAPIData, 
    RawCSVData, 
    ETLRun,
    ETLCheckpoint
)
from pydantic import BaseModel
from typing import List, Optional


router = APIRouter()


# Response models
class SourceStats(BaseModel):
    source: str
    total_records: int
    last_checkpoint: Optional[int]
    last_update: Optional[datetime]


class ETLRunInfo(BaseModel):
    run_id: int
    source: str
    started_at: datetime
    ended_at: Optional[datetime]
    records_processed: int
    success: bool
    duration_seconds: Optional[float]


class StatsResponse(BaseModel):
    total_cleaned_records: int
    total_raw_api_records: int
    total_raw_csv_records: int
    sources: List[SourceStats]
    recent_etl_runs: List[ETLRunInfo]
    system_uptime: str


@router.get("/stats", response_model=StatsResponse)
async def get_statistics(db: Session = Depends(get_db)):
    """
    Get ETL pipeline statistics
    
    Returns:
        - total_cleaned_records: Total number of cleaned records in database
        - total_raw_api_records: Total number of raw API records
        - total_raw_csv_records: Total number of raw CSV records
        - sources: Statistics per data source
        - recent_etl_runs: Information about recent ETL runs
        - system_uptime: Time since first ETL run
    """
    try:
        # Get total counts
        total_cleaned = db.query(func.count(CleanedData.id)).scalar()
        total_raw_api = db.query(func.count(RawAPIData.id)).scalar()
        total_raw_csv = db.query(func.count(RawCSVData.id)).scalar()
        
        # Get statistics per source
        sources_data = db.query(
            CleanedData.data_source,
            func.count(CleanedData.id).label('count')
        ).group_by(CleanedData.data_source).all()
        
        sources = []
        for source_name, count in sources_data:
            # Get checkpoint info
            checkpoint = db.query(ETLCheckpoint).filter_by(source=source_name).first()
            
            sources.append({
                "source": source_name,
                "total_records": count,
                "last_checkpoint": checkpoint.last_processed_id if checkpoint else None,
                "last_update": checkpoint.last_processed_timestamp if checkpoint else None
            })
        
        # Get recent ETL runs (last 10)
        recent_runs = db.query(ETLRun).order_by(desc(ETLRun.started_at)).limit(10).all()
        
        etl_runs = []
        for run in recent_runs:
            duration = None
            if run.started_at and run.ended_at:
                duration = (run.ended_at - run.started_at).total_seconds()
            
            etl_runs.append({
                "run_id": run.id,
                "source": run.source,
                "started_at": run.started_at,
                "ended_at": run.ended_at,
                "records_processed": run.records_processed,
                "success": run.success,
                "duration_seconds": duration
            })
        
        # Calculate system uptime (time since first ETL run)
        first_run = db.query(ETLRun).order_by(ETLRun.started_at).first()
        uptime = "N/A"
        if first_run and first_run.started_at:
            delta = datetime.now(timezone.utc) - first_run.started_at.replace(tzinfo=timezone.utc)
            days = delta.days
            hours = delta.seconds // 3600
            minutes = (delta.seconds % 3600) // 60
            uptime = f"{days}d {hours}h {minutes}m"
        
        return {
            "total_cleaned_records": total_cleaned,
            "total_raw_api_records": total_raw_api,
            "total_raw_csv_records": total_raw_csv,
            "sources": sources,
            "recent_etl_runs": etl_runs,
            "system_uptime": uptime
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving statistics: {str(e)}")


@router.get("/stats/summary")
async def get_summary_stats(db: Session = Depends(get_db)):
    """
    Get quick summary statistics
    
    Returns condensed statistics for dashboard view
    """
    try:
        total_records = db.query(func.count(CleanedData.id)).scalar()
        
        # Get last successful ETL run
        last_run = db.query(ETLRun).filter_by(success=True).order_by(desc(ETLRun.ended_at)).first()
        
        # Count by source
        by_source = db.query(
            CleanedData.data_source,
            func.count(CleanedData.id)
        ).group_by(CleanedData.data_source).all()
        
        return {
            "total_records": total_records,
            "last_successful_run": last_run.ended_at.isoformat() if last_run and last_run.ended_at else None,
            "records_by_source": {source: count for source, count in by_source}
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving summary: {str(e)}")
