# api/routes/data.py

from fastapi import APIRouter, Depends, Query, HTTPException, Request
from sqlalchemy.orm import Session
from sqlalchemy import desc
from typing import Optional, List
from datetime import datetime
import sys
import os
import time
import uuid

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from services.database import get_db, CleanedData
from pydantic import BaseModel


router = APIRouter()


# Response model
class CryptoDataResponse(BaseModel):
    id: int
    crypto_id: str
    crypto_name: str
    price_usd: float
    market_cap_usd: Optional[float]
    volume_24h_usd: Optional[float]
    change_24h_percent: Optional[float]
    data_source: str
    created_at: datetime
    
    class Config:
        from_attributes = True


class DataListResponse(BaseModel):
    request_id: str
    api_latency_ms: float
    total: int
    page: int
    page_size: int
    data: List[CryptoDataResponse]


@router.get("/data", response_model=DataListResponse)
async def get_crypto_data(
    request: Request,
    source: Optional[str] = Query(None, description="Filter by data source (coinpaprika, coingecko, csv)"),
    crypto_id: Optional[str] = Query(None, description="Filter by crypto ID (e.g., btc-bitcoin)"),
    limit: int = Query(10, ge=1, le=100, description="Number of records to return"),
    offset: int = Query(0, ge=0, description="Number of records to skip"),
    sort_by: str = Query("created_at", description="Sort by field (created_at, price_usd, market_cap_usd)"),
    order: str = Query("desc", description="Sort order (asc, desc)"),
    db: Session = Depends(get_db)
):
    """
    Retrieve cryptocurrency data with filtering and pagination
    
    Returns:
        - request_id: Unique identifier for this request
        - api_latency_ms: Request processing time in milliseconds
        - total: Total number of records matching the filter
        - page: Current page number
        - page_size: Number of records per page
        - data: List of cryptocurrency records
    """
    # Start timing
    start_time = time.time()
    
    # Generate unique request ID
    request_id = str(uuid.uuid4())
    
    try:
        # Build query
        query = db.query(CleanedData)
        
        # Apply filters
        if source:
            query = query.filter(CleanedData.data_source == source.lower())
        
        if crypto_id:
            query = query.filter(CleanedData.crypto_id == crypto_id.lower())
        
        # Get total count
        total = query.count()
        
        # Apply sorting
        sort_column = getattr(CleanedData, sort_by, CleanedData.created_at)
        if order.lower() == "desc":
            query = query.order_by(desc(sort_column))
        else:
            query = query.order_by(sort_column)
        
        # Apply pagination
        query = query.offset(offset).limit(limit)
        
        # Execute query
        results = query.all()
        
        # Calculate page number
        page = (offset // limit) + 1 if limit > 0 else 1
        
        # Calculate latency
        end_time = time.time()
        latency_ms = round((end_time - start_time) * 1000, 2)
        
        return {
            "request_id": request_id,
            "api_latency_ms": latency_ms,
            "total": total,
            "page": page,
            "page_size": limit,
            "data": results
        }
    
    except Exception as e:
        end_time = time.time()
        latency_ms = round((end_time - start_time) * 1000, 2)
        
        raise HTTPException(
            status_code=500, 
            detail={
                "request_id": request_id,
                "api_latency_ms": latency_ms,
                "error": f"Error retrieving data: {str(e)}"
            }
        )


@router.get("/data/{crypto_id}", response_model=CryptoDataResponse)
async def get_crypto_by_id(
    crypto_id: str,
    source: Optional[str] = Query(None, description="Filter by data source"),
    db: Session = Depends(get_db)
):
    """Get the latest data for a specific cryptocurrency"""
    try:
        query = db.query(CleanedData).filter(CleanedData.crypto_id == crypto_id.lower())
        
        if source:
            query = query.filter(CleanedData.data_source == source.lower())
        
        result = query.order_by(desc(CleanedData.created_at)).first()
        
        if not result:
            raise HTTPException(
                status_code=404,
                detail=f"Cryptocurrency '{crypto_id}' not found"
            )
        
        return result
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving data: {str(e)}")
