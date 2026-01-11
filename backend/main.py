from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import logging
from datetime import datetime

from database import engine, Base, get_db
from api.endpoints import router as api_router
from api.monitoring import router as monitoring_router
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Single lifespan function
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Starting Stock Data API...")

    # Create database tables
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables created/verified")
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")

    # Test database connection
    try:
        db = next(get_db())
        db.execute("SELECT 1")
        db.close()
        logger.info("Database connection successful")
    except Exception as e:
        logger.error(f"Database connection failed: {e}")

    # Optional: test HDFS connection if needed
    try:
        from services.hdfs_service import HDFSService
        hdfs_service = HDFSService()
        if hdfs_service.test_connection():
            logger.info("✅ HDFS connection successful")
        else:
            logger.warning("⚠️ HDFS connection failed")
    except Exception as e:
        logger.error(f"❌ HDFS initialization failed: {e}")

    yield  # Control returns here when app shuts down

    # Shutdown
    logger.info("Shutting down Stock Data API...")

# Create FastAPI app
app = FastAPI(
    title="Stock Data Pipeline API",
    description="API for stock data collection, processing, and analysis",
    version="1.0.0",
    lifespan=lifespan
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Cho phép tất cả
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# Include routers
app.include_router(api_router, prefix="/api/v1", tags=["api"])
app.include_router(monitoring_router, prefix="/api/v1", tags=["monitoring"])
# Health check endpoints
@app.get("/")
async def root():
    return {
        "service": "Stock Data Pipeline API",
        "status": "running",
        "timestamp": datetime.now().isoformat(),
        "version": "1.0.0"
    }

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "service": "stock-api",
        "version": "1.0.0"
    }

@app.get("/api/v1/health")
async def api_health():
    return {"status": "ok", "service": "stock-api"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)