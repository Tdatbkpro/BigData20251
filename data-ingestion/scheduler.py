import time
import logging
import sys
import os
from datetime import datetime

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join('/data/logs', f'scheduler_{datetime.now().strftime("%Y%m%d")}.log'))
    ]
)
logger = logging.getLogger(__name__)

def run_data_collection():
    """Run the data collection process"""
    try:
        from data_collector import main as collect_data
        
        logger.info("=" * 60)
        logger.info("STARTING DATA COLLECTION CYCLE")
        logger.info("=" * 60)
        
        # Step 1: Collect stock data (automatically uploads each exchange to HDFS)
        logger.info("📊 STEP 1: Collecting stock data and uploading to HDFS...")
        collection_results = collect_data()
        
        if collection_results:
            # Step 2: Upload reports to HDFS
            logger.info("📋 STEP 2: Uploading reports to HDFS...")
            from hdfs_uploader import upload_reports
            report_count = upload_reports()
            logger.info(f"✓ Reports uploaded to HDFS: {report_count} files")
            
            # Step 3: Log summary
            logger.info("📊 STEP 3: Job summary:")
            logger.info(f"   - Successful symbols: {collection_results.get('total_successful', 0)}")
            logger.info(f"   - Failed symbols: {collection_results.get('total_failed', 0)}")
            logger.info(f"   - Total data points: {collection_results.get('total_data_points', 0)}")
            logger.info(f"   - HDFS uploads: {collection_results.get('total_hdfs_uploads', 0)}")
            logger.info(f"   - Reports uploaded: {report_count}")
        else:
            logger.error("✗ Data collection failed")
            
        return True
        
    except Exception as e:
        logger.error(f"💥 Error in data collection cycle: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

def main():
    """Main scheduler function"""
    from config import settings
    
    logger.info("🚀 STARTING DATA INGESTION SCHEDULER")
    logger.info(f"Configuration:")
    logger.info(f"  - Interval: {settings.COLLECTION_INTERVAL_HOURS} hours")
    logger.info(f"  - Symbols per exchange: {settings.SYMBOLS_PER_EXCHANGE}")
    logger.info(f"  - Days of data: {settings.DAYS_OF_DATA}")
    logger.info(f"  - Data directory: {settings.LOCAL_DATA_DIR}")
    logger.info("=" * 60)
    
    interval_seconds = settings.COLLECTION_INTERVAL_HOURS * 60 * 60
    cycle_count = 0
    
    while True:
        try:
            cycle_count += 1
            logger.info(f"\n🔄 CYCLE #{cycle_count} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            success = run_data_collection()
            
            if success:
                logger.info(f"✅ Cycle #{cycle_count} completed successfully")
            else:
                logger.warning(f"⚠️ Cycle #{cycle_count} completed with errors")
            
            # Calculate next run time
            next_run = datetime.now().timestamp() + interval_seconds
            next_run_str = datetime.fromtimestamp(next_run).strftime('%Y-%m-%d %H:%M:%S')
            
            logger.info(f"⏰ Next run at: {next_run_str}")
            logger.info(f"😴 Sleeping {settings.COLLECTION_INTERVAL_HOURS} hours...")
            logger.info("=" * 60)
            
            time.sleep(interval_seconds)

        except KeyboardInterrupt:
            logger.info("\n👋 Scheduler stopped by user")
            break
        except Exception as e:
            logger.exception(f"💥 Unexpected scheduler error: {e}")
            logger.info("🔄 Retrying in 5 minutes...")
            time.sleep(300)  # Wait 5 minutes before retrying

if __name__ == "__main__":
    main()