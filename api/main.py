# api/main.py

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from api.routes import data, health, stats

# Create FastAPI app
app = FastAPI(
    title="Kasparro Backend - Crypto ETL API",
    description="RESTful API for cryptocurrency data ingestion and retrieval",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS middleware (allow all origins for development)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Change in production to specific domains
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Include routers
app.include_router(health.router, tags=["Health"])
app.include_router(data.router, tags=["Data"])
app.include_router(stats.router, tags=["Statistics"])


# Root endpoint
@app.get("/")
async def root():
    """
    Root endpoint with API information
    """
    return {
        "message": "Welcome to Kasparro Backend API",
        "version": "1.0.0",
        "docs": "/docs",
        "endpoints": {
            "health": "/health",
            "data": "/data",
            "stats": "/stats"
        }
    }


# Run with: uvicorn api.main:app --reload
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api.main:app", host="0.0.0.0", port=8000, reload=True)
