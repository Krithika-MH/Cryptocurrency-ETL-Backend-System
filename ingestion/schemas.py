# ingestion/schemas.py

from pydantic import BaseModel, Field, validator
from typing import Optional
from datetime import datetime


class CryptoData(BaseModel):
    """
    Unified schema for cryptocurrency data from all sources
    Pydantic automatically validates data types
    """
    
    # Required fields
    crypto_id: str = Field(..., description="Unique ID like 'bitcoin'")
    crypto_name: str = Field(..., description="Full name like 'Bitcoin'")
    price_usd: float = Field(..., gt=0, description="Price in USD, must be positive")
    
    # Optional fields
    market_cap_usd: Optional[float] = Field(None, ge=0)
    volume_24h_usd: Optional[float] = Field(None, ge=0)
    change_24h_percent: Optional[float] = None
    
    # Metadata
    source: str = Field(..., description="Data source: coinpaprika, coingecko, csv")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    
    @validator('price_usd', 'market_cap_usd', 'volume_24h_usd')
    def validate_positive_numbers(cls, v):
        """Ensure financial values are positive"""
        if v is not None and v < 0:
            raise ValueError("Value must be positive")
        return v
    
    @validator('crypto_id', 'crypto_name')
    def validate_not_empty(cls, v):
        """Ensure strings are not empty"""
        if not v or not v.strip():
            raise ValueError("Value cannot be empty")
        return v.strip()
    
    class Config:
        extra = "allow"  # Allow extra fields not defined here
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


# Test the schema
if __name__ == "__main__":
    # Valid data
    try:
        crypto = CryptoData(
            crypto_id="bitcoin",
            crypto_name="Bitcoin",
            price_usd=42000.50,
            market_cap_usd=800000000000,
            source="test"
        )
        print("✓ Valid data:", crypto)
    except Exception as e:
        print("✗ Validation failed:", e)
    
    # Invalid data (negative price)
    try:
        crypto = CryptoData(
            crypto_id="bitcoin",
            crypto_name="Bitcoin",
            price_usd=-100,  # Invalid!
            source="test"
        )
        print("✓ Valid data:", crypto)
    except Exception as e:
        print("✓ Correctly rejected invalid data:", e)
