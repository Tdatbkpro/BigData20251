import os
import logging
import time
import sys
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class HDFSUploader:
    def __init__(self, max_retries=5, retry_delay=10):
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.client = None
        
        from config import settings
        self.settings = settings
        self.base_path = settings.HDFS_BASE_PATH
        
        # Fixed WebHDFS URL for Hadoop 3.x
        # Hadoop 3.x WebHDFS runs on port 9870 (Web UI port)
        self.webhdfs_url = "http://namenode:9870"
    
    def _connect(self):
        """Connect to HDFS with retry mechanism"""
        for attempt in range(self.max_retries):
            try:
                logger.info(f"Attempting to connect to HDFS (attempt {attempt + 1}/{self.max_retries})")
                logger.info(f"Using WebHDFS URL: {self.webhdfs_url}")
                logger.info(f"HDFS User: {self.settings.HDFS_USER}")
                logger.info(f"HDFS Base Path: {self.base_path}")
                
                from hdfs import InsecureClient
                
                self.client = InsecureClient(
                    self.webhdfs_url,
                    user=self.settings.HDFS_USER,
                    timeout=60
                )
                
                # Test connection by listing root
                try:
                    status = self.client.status('/')
                    logger.info(f"✓ Successfully connected to HDFS")
                    logger.info(f"HDFS root status: type={status['type']}, permission={status['permission']}")
                    return True
                except Exception as e:
                    # Try to create root if it doesn't exist (unlikely)
                    logger.warning(f"Could not get root status: {e}")
                    
                    # Try to create a test directory
                    test_path = f"/tmp/hdfs_test_{int(time.time())}"
                    try:
                        self.client.makedirs(test_path)
                        logger.info(f"✓ HDFS connection successful (created test dir: {test_path})")
                        self.client.delete(test_path, recursive=True)
                        return True
                    except Exception as e2:
                        logger.error(f"Failed to create test directory: {e2}")
                        raise
                
            except Exception as e:
                logger.warning(f"HDFS connection failed: {str(e)}")
                
                # Try alternative port if 9870 fails
                if attempt == 1:
                    logger.info("Trying alternative port 50070...")
                    self.webhdfs_url = "http://namenode:50070"
                elif attempt == 2:
                    logger.info("Trying alternative port 50075...")
                    self.webhdfs_url = "http://namenode:50075"
                
                if attempt < self.max_retries - 1:
                    logger.info(f"Retrying in {self.retry_delay} seconds...")
                    time.sleep(self.retry_delay)
                else:
                    logger.error("✗ Failed to connect to HDFS after all retries")
                    logger.error("Please check:")
                    logger.error("1. Is namenode container running?")
                    logger.error("2. Is WebHDFS enabled on namenode?")
                    logger.error("3. Can you access http://namenode:9870 from data-ingestion container?")
                    return False
    
    def create_hdfs_structure(self):
        """Create HDFS directory structure"""
        if not self.client:
            return False
        
        directories = [
            self.base_path,
            f"{self.base_path}/raw",
            f"{self.base_path}/raw/daily",
            f"{self.base_path}/processed",
            f"{self.base_path}/reports"
        ]
        
        for directory in directories:
            try:
                if not self.client.status(directory, strict=False):
                    # Tạo thư mục với quyền 755
                    self.client.makedirs(directory, permission=755)
                    logger.info(f"Created HDFS directory: {directory}")
                else:
                    logger.debug(f"HDFS directory already exists: {directory}")
            except Exception as e:
                logger.error(f"Failed to create directory {directory}: {str(e)}")
                # Thử tạo với superuser permission nếu cần
                return False
        
        return True
    
    def upload_file(self, local_path: str, hdfs_path: str) -> bool:
        """Upload a file to HDFS"""
        if not self.client:
            logger.error("Not connected to HDFS")
            return False
        
        try:
            # Check if file exists locally
            if not os.path.exists(local_path):
                logger.error(f"Local file not found: {local_path}")
                return False
            
            file_size = os.path.getsize(local_path)
            logger.info(f"Uploading {local_path} ({file_size:,} bytes) to {hdfs_path}")
            
            # Ensure parent directory exists in HDFS
            hdfs_dir = os.path.dirname(hdfs_path)
            if not self.client.status(hdfs_dir, strict=False):
                logger.info(f"Creating HDFS directory: {hdfs_dir}")
                self.client.makedirs(hdfs_dir)
            
            # Upload file
            self.client.upload(hdfs_path, local_path, overwrite=True)
            
            # Verify upload
            status = self.client.status(hdfs_path)
            logger.info(f"✓ Successfully uploaded to HDFS. File size: {status['length']:,} bytes")
            return True
            
        except Exception as e:
            logger.error(f"✗ Upload failed: {str(e)}")
            return False
    
    def upload_processed_files(self):
        """Upload all processed files to HDFS"""
        logger.info(f"Local data directory: {self.settings.LOCAL_DATA_DIR}")
        
        if not self._connect():
            logger.error("Cannot connect to HDFS")
            return 0
        
        if not self.create_hdfs_structure():
            logger.error("Failed to create HDFS structure")
            return 0
        
        upload_count = 0
        failed_count = 0
        
        # Upload raw data from all exchanges
        raw_dir = os.path.join(self.settings.LOCAL_DATA_DIR, "raw")
        
        if os.path.exists(raw_dir):
            logger.info(f"📁 Scanning for files in {raw_dir}")
            
            for exchange in os.listdir(raw_dir):
                exchange_dir = os.path.join(raw_dir, exchange)
                if os.path.isdir(exchange_dir):
                    logger.info(f"Found exchange: {exchange}")
                    
                    for date_dir in os.listdir(exchange_dir):
                        date_path = os.path.join(exchange_dir, date_dir)
                        if os.path.isdir(date_path):
                            logger.info(f"  Processing date: {date_dir}")
                            
                            for file in os.listdir(date_path):
                                if file.endswith('.csv'):
                                    local_path = os.path.join(date_path, file)
                                    
                                    # Check if file has data
                                    try:
                                        import pandas as pd
                                        df = pd.read_csv(local_path)
                                        if df.empty:
                                            logger.warning(f"    Skipping empty file: {file}")
                                            continue
                                    except:
                                        pass
                                    
                                    # Create HDFS path
                                    hdfs_path = f"{self.base_path}/raw/daily/{exchange}/{date_dir}/{file}"
                                    
                                    logger.info(f"    Uploading: {file}")
                                    
                                    if self.upload_file(local_path, hdfs_path):
                                        upload_count += 1
                                        logger.info(f"    ✓ Uploaded {file}")
                                    else:
                                        failed_count += 1
                                        logger.error(f"    ✗ Failed to upload {file}")
        
        # Upload reports
        reports_dir = os.path.join(self.settings.LOCAL_DATA_DIR, "reports")
        if os.path.exists(reports_dir):
            logger.info(f"📁 Scanning for reports in {reports_dir}")
            
            for file in os.listdir(reports_dir):
                if file.endswith(('.json', '.csv')):
                    local_path = os.path.join(reports_dir, file)
                    hdfs_path = f"{self.base_path}/reports/{file}"
                    
                    logger.info(f"  Uploading report: {file}")
                    
                    if self.upload_file(local_path, hdfs_path):
                        upload_count += 1
                        logger.info(f"  ✓ Uploaded report {file}")
                    else:
                        failed_count += 1
                        logger.error(f"  ✗ Failed to upload report {file}")
        
        logger.info(f"📊 Upload summary: {upload_count} successful, {failed_count} failed")
        
        # List files in HDFS to verify
        try:
            logger.info(f"📁 Listing HDFS directory: {self.base_path}")
            files = self.client.list(self.base_path, status=False)
            logger.info(f"Files in HDFS {self.base_path}:")
            for file in files:
                logger.info(f"  - {file}")
        except Exception as e:
            logger.warning(f"Could not list HDFS directory: {e}")
        
        return upload_count


# ========== CÁC HÀM ĐỘC LẬP ==========

def upload_exchange_files(exchange_code: str) -> int:
    """Upload files for a specific exchange to HDFS"""
    logger.info(f"📤 Uploading files for exchange: {exchange_code}")
    
    try:
        uploader = HDFSUploader(max_retries=3, retry_delay=5)
        
        if not uploader._connect():
            logger.error(f"❌ Cannot connect to HDFS for exchange {exchange_code}")
            return 0
        
        if not uploader.create_hdfs_structure():
            logger.error(f"❌ Failed to create HDFS structure for {exchange_code}")
            return 0
        
        upload_count = 0
        failed_count = 0
        
        # Upload raw data for this exchange
        raw_dir = os.path.join(uploader.settings.LOCAL_DATA_DIR, "raw", exchange_code)
        
        if os.path.exists(raw_dir):
            logger.info(f"📁 Scanning for files in {raw_dir}")
            
            for date_dir in os.listdir(raw_dir):
                date_path = os.path.join(raw_dir, date_dir)
                if os.path.isdir(date_path):
                    logger.info(f"  Processing date: {date_dir}")
                    
                    for file in os.listdir(date_path):
                        if file.endswith('.csv'):
                            local_path = os.path.join(date_path, file)
                            
                            # Check if file has data
                            try:
                                import pandas as pd
                                df = pd.read_csv(local_path)
                                if df.empty:
                                    logger.warning(f"    Skipping empty file: {file}")
                                    continue
                            except:
                                pass
                            
                            # Create HDFS path
                            hdfs_path = f"{uploader.base_path}/raw/daily/{exchange_code}/{date_dir}/{file}"
                            
                            logger.info(f"    Uploading: {file}")
                            
                            if uploader.upload_file(local_path, hdfs_path):
                                upload_count += 1
                                logger.info(f"    ✓ Uploaded {file}")
                            else:
                                failed_count += 1
                                logger.error(f"    ✗ Failed to upload {file}")
        else:
            logger.warning(f"⚠️ No data directory found for exchange {exchange_code}: {raw_dir}")
        
        logger.info(f"📊 Exchange {exchange_code} upload summary: {upload_count} successful, {failed_count} failed")
        
        return upload_count
        
    except Exception as e:
        logger.error(f"❌ Error uploading exchange {exchange_code}: {str(e)}")
        return 0


def upload_reports() -> int:
    """Upload report files to HDFS"""
    logger.info("📋 Uploading reports to HDFS...")
    
    try:
        uploader = HDFSUploader(max_retries=3, retry_delay=5)
        
        if not uploader._connect():
            logger.error("❌ Cannot connect to HDFS for reports")
            return 0
        
        upload_count = 0
        
        # Upload reports
        reports_dir = os.path.join(uploader.settings.LOCAL_DATA_DIR, "reports")
        if os.path.exists(reports_dir):
            logger.info(f"📁 Scanning for reports in {reports_dir}")
            
            for file in os.listdir(reports_dir):
                if file.endswith(('.json', '.csv')):
                    local_path = os.path.join(reports_dir, file)
                    hdfs_path = f"{uploader.base_path}/reports/{file}"
                    
                    logger.info(f"  Uploading report: {file}")
                    
                    if uploader.upload_file(local_path, hdfs_path):
                        upload_count += 1
                        logger.info(f"  ✓ Uploaded report {file}")
                    else:
                        logger.error(f"  ✗ Failed to upload report {file}")
        
        logger.info(f"📋 Reports upload summary: {upload_count} files uploaded")
        return upload_count
        
    except Exception as e:
        logger.error(f"❌ Error uploading reports: {str(e)}")
        return 0


def upload_to_hdfs():
    """Main upload function"""
    logger.info("=" * 60)
    logger.info("STARTING HDFS UPLOAD")
    logger.info("=" * 60)
    
    try:
        uploader = HDFSUploader(max_retries=5, retry_delay=10)
        count = uploader.upload_processed_files()
        
        if count > 0:
            logger.info(f"✅ HDFS upload completed successfully: {count} files uploaded")
        else:
            logger.warning(f"⚠️ HDFS upload completed but no files were uploaded")
        
        return count
        
    except Exception as e:
        logger.error(f"❌ HDFS upload failed: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return 0


def main():
    """Main function with command line arguments"""
    if len(sys.argv) > 1:
        if sys.argv[1] == "--exchange":
            if len(sys.argv) > 2:
                exchange_code = sys.argv[2]
                upload_exchange_files(exchange_code)
            else:
                logger.error("Please provide exchange code: python hdfs_uploader.py --exchange NASDAQ")
        elif sys.argv[1] == "--reports":
            upload_reports()
        elif sys.argv[1] == "--all":
            upload_to_hdfs()
        elif sys.argv[1] == "--help":
            print("""
Usage:
  python hdfs_uploader.py --exchange <exchange_code>  # Upload single exchange
  python hdfs_uploader.py --reports                   # Upload reports only
  python hdfs_uploader.py --all                       # Upload all files
  python hdfs_uploader.py --help                      # Show this help
            """)
        else:
            logger.error(f"Unknown argument: {sys.argv[1]}")
            print("Use --help for usage information")
    else:
        # Mặc định upload tất cả
        upload_to_hdfs()


if __name__ == "__main__":
    main()