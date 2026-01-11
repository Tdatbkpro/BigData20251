import logging
from fastapi import APIRouter, HTTPException, Depends, Query
from sqlalchemy.orm import Session
from typing import List, Dict, Any

from datetime import datetime, date
import json
from services.hdfs_service import HDFSService
from database import get_db
from models import StockMetadata, StockPrice, AnalysisResult
from services.stock_service import StockService

router = APIRouter()
logger = logging.getLogger(__name__)

@router.get("/hdfs/files", response_model=List[Dict[str, Any]])
async def list_hdfs_files(path: str = Query("/stock_data", description="HDFS path to list")):
    """List files in HDFS directory"""
    try:
        logger.info(f"API: Listing HDFS files in {path}")
        
        hdfs_service = HDFSService()
        
        # Test connection first
        if not hdfs_service.client:
            logger.error("HDFS client not available")
            return []
        
        # List files
        files = hdfs_service.list_files(path)
        logger.info(f"API: Found {len(files)} files")
        
        return files
        
    except Exception as e:
        logger.error(f"Error listing HDFS files: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/hdfs/read")
async def read_hdfs_file(
    file_path: str = Query(..., description="HDFS file path"),
    limit: int = Query(50, description="Maximum lines to return")
):
    """Read file from HDFS"""
    try:
        logger.info(f"API: Reading HDFS file {file_path}")
        
        hdfs_service = HDFSService()
        content = hdfs_service.read_file(file_path, limit)
        
        return {
            "file_path": file_path,
            "lines": len(content),
            "content": content
        }
        
    except Exception as e:
        logger.error(f"Error reading HDFS file: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/hdfs/dir-info")
async def get_directory_info(path: str = Query("/stock_data", description="HDFS directory path")):
    """Get directory information"""
    try:
        logger.info(f"API: Getting directory info for {path}")
        
        hdfs_service = HDFSService()
        info = hdfs_service.get_directory_info(path)
        
        return info
        
    except Exception as e:
        logger.error(f"Error getting directory info: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/hdfs/test")
async def test_hdfs_connection():
    """Test HDFS connection"""
    try:
        logger.info("API: Testing HDFS connection")
        
        hdfs_service = HDFSService()
        success = hdfs_service.test_connection()
        
        return {
            "success": success,
            "namenode": hdfs_service.hdfs_namenode,
            "user": hdfs_service.hdfs_user
        }
        
    except Exception as e:
        logger.error(f"Error testing HDFS connection: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/hdfs/explore")
async def explore_hdfs(path: str = Query("/", description="Starting path")):
    """Explore HDFS with recursive listing (limited depth)"""
    try:
        logger.info(f"API: Exploring HDFS from {path}")
        
        hdfs_service = HDFSService()
        
        def explore_dir(current_path: str, depth: int = 0, max_depth: int = 3):
            if depth >= max_depth:
                return []
            
            items = hdfs_service.list_files(current_path)
            result = []
            
            for item in items:
                item_info = {
                    "name": item["name"],
                    "path": item["path"],
                    "type": item["type"],
                    "size": item["size"],
                    "modification_time": item["modification_time"]
                }
                
                if item["type"] == "directory":
                    # Add children for directories
                    item_info["children"] = explore_dir(item["path"], depth + 1, max_depth)
                
                result.append(item_info)
            
            return result
        
        tree = explore_dir(path)
        return {
            "path": path,
            "tree": tree
        }
        
    except Exception as e:
        logger.error(f"Error exploring HDFS: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))@router.get("/hdfs/files", response_model=List[Dict[str, Any]])
async def list_hdfs_files(path: str = Query("/stock_data", description="HDFS path to list")):
    """List files in HDFS directory"""
    try:
        logger.info(f"API: Listing HDFS files in {path}")
        
        hdfs_service = HDFSService()
        
        # Test connection first
        if not hdfs_service.client:
            logger.error("HDFS client not available")
            return []
        
        # List files
        files = hdfs_service.list_files(path)
        logger.info(f"API: Found {len(files)} files")
        
        return files
        
    except Exception as e:
        logger.error(f"Error listing HDFS files: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/hdfs/read")
async def read_hdfs_file(
    file_path: str = Query(..., description="HDFS file path"),
    limit: int = Query(50, description="Maximum lines to return")
):
    """Read file from HDFS"""
    try:
        logger.info(f"API: Reading HDFS file {file_path}")
        
        hdfs_service = HDFSService()
        content = hdfs_service.read_file(file_path, limit)
        
        return {
            "file_path": file_path,
            "lines": len(content),
            "content": content
        }
        
    except Exception as e:
        logger.error(f"Error reading HDFS file: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/hdfs/dir-info")
async def get_directory_info(path: str = Query("/stock_data", description="HDFS directory path")):
    """Get directory information"""
    try:
        logger.info(f"API: Getting directory info for {path}")
        
        hdfs_service = HDFSService()
        info = hdfs_service.get_directory_info(path)
        
        return info
        
    except Exception as e:
        logger.error(f"Error getting directory info: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/hdfs/test")
async def test_hdfs_connection():
    """Test HDFS connection"""
    try:
        logger.info("API: Testing HDFS connection")
        
        hdfs_service = HDFSService()
        success = hdfs_service.test_connection()
        
        return {
            "success": success,
            "namenode": hdfs_service.hdfs_namenode,
            "user": hdfs_service.hdfs_user
        }
        
    except Exception as e:
        logger.error(f"Error testing HDFS connection: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/hdfs/explore")
async def explore_hdfs(path: str = Query("/", description="Starting path")):
    """Explore HDFS with recursive listing (limited depth)"""
    try:
        logger.info(f"API: Exploring HDFS from {path}")
        
        hdfs_service = HDFSService()
        
        def explore_dir(current_path: str, depth: int = 0, max_depth: int = 3):
            if depth >= max_depth:
                return []
            
            items = hdfs_service.list_files(current_path)
            result = []
            
            for item in items:
                item_info = {
                    "name": item["name"],
                    "path": item["path"],
                    "type": item["type"],
                    "size": item["size"],
                    "modification_time": item["modification_time"]
                }
                
                if item["type"] == "directory":
                    # Add children for directories
                    item_info["children"] = explore_dir(item["path"], depth + 1, max_depth)
                
                result.append(item_info)
            
            return result
        
        tree = explore_dir(path)
        return {
            "path": path,
            "tree": tree
        }
        
    except Exception as e:
        logger.error(f"Error exploring HDFS: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
@router.get("/stocks")
async def get_stocks(db: Session = Depends(get_db)):
    """Get list of stocks"""
    try:
        stocks = db.query(StockMetadata).limit(100).all()
        return [
            {
                "symbol": stock.symbol,
                "name": stock.company_name,
                "exchange": stock.exchange,
                "sector": stock.sector,
                "industry": stock.industry
            }
            for stock in stocks
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/stocks/{symbol}")
async def get_stock(symbol: str, db: Session = Depends(get_db)):
    """Get stock details and latest price"""
    try:
        # Get stock metadata
        stock = db.query(StockMetadata).filter(StockMetadata.symbol == symbol).first()
        if not stock:
            raise HTTPException(status_code=404, detail="Stock not found")
        
        # Get latest price
        latest_price = db.query(StockPrice).filter(
            StockPrice.symbol == symbol
        ).order_by(StockPrice.date.desc()).first()
        
        response = {
            "symbol": stock.symbol,
            "name": stock.company_name,
            "exchange": stock.exchange,
            "sector": stock.sector,
            "industry": stock.industry
        }
        
        if latest_price:
            response.update({
                "price": latest_price.close,
                "date": latest_price.date,
                "change": latest_price.close - latest_price.open,
                "volume": latest_price.volume
            })
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/stocks/{symbol}/prices")
async def get_stock_prices(
    symbol: str,
    start_date: date = None,
    end_date: date = None,
    limit: int = 100,
    db: Session = Depends(get_db)
):
    """Get historical prices for a stock"""
    try:
        query = db.query(StockPrice).filter(StockPrice.symbol == symbol)
        
        if start_date:
            query = query.filter(StockPrice.date >= start_date)
        if end_date:
            query = query.filter(StockPrice.date <= end_date)
        
        prices = query.order_by(StockPrice.date.desc()).limit(limit).all()
        
        return [
            {
                "date": price.date,
                "open": price.open,
                "high": price.high,
                "low": price.low,
                "close": price.close,
                "volume": price.volume
            }
            for price in prices
        ]
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/analytics/summary")
async def get_analytics_summary(db: Session = Depends(get_db)):
    """Get analytics summary"""
    try:
        stock_service = StockService(db)
        return stock_service.get_analytics_summary()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/analytics/top-gainers")
async def get_top_gainers(days: int = 7, db: Session = Depends(get_db)):
    """Get top gaining stocks"""
    try:
        stock_service = StockService(db)
        return stock_service.get_top_gainers(days)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/analytics/high-volume")
async def get_high_volume_stocks(days: int = 7, db: Session = Depends(get_db)):
    """Get high volume stocks"""
    try:
        stock_service = StockService(db)
        return stock_service.get_high_volume_stocks(days)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/hdfs/files")
async def list_hdfs_files(path: str = "/stock_data"):
    """List files in HDFS"""
    try:
        from services.hdfs_service import HDFSService
        hdfs_service = HDFSService()
        return hdfs_service.list_files(path)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/spark/jobs/run-batch")
async def run_spark_batch():
    """Trigger Spark batch processing"""
    try:
        import subprocess
        result = subprocess.run(
            ["python", "/opt/spark-apps/data_processor.py"],
            capture_output=True,
            text=True
        )
        
        return {
            "success": result.returncode == 0,
            "stdout": result.stdout,
            "stderr": result.stderr
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))