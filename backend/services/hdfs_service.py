import json
import logging
from typing import List, Dict, Any
from hdfs import InsecureClient
from datetime import datetime
import os

logger = logging.getLogger(__name__)

class HDFSService:
    def __init__(self):
        # Sửa: Dùng port 9870 (WebHDFS) thay vì port mặc định
        self.hdfs_namenode = os.getenv('HDFS_NAMENODE', 'namenode:9870')
        self.hdfs_user = os.getenv('HDFS_USER', 'root')  # Sử dụng root để có quyền
        
        # URL WebHDFS đúng
        webhdfs_url = f"http://{self.hdfs_namenode}"
        
        logger.info(f"Connecting to HDFS: {webhdfs_url} as user: {self.hdfs_user}")
        
        try:
            self.client = InsecureClient(
                webhdfs_url,
                user=self.hdfs_user,
                timeout=30
            )
            # Test connection
            if self.test_connection():
                logger.info("✅ HDFS service initialized successfully")
            else:
                logger.error("❌ HDFS connection failed")
        except Exception as e:
            logger.error(f"❌ Failed to initialize HDFS client: {e}")
            self.client = None
    
    def test_connection(self) -> bool:
        """Test HDFS connection"""
        try:
            if not self.client:
                return False
            
            status = self.client.status("/", strict=False)
            logger.info(f"HDFS connection successful")
            return True
        except Exception as e:
            logger.error(f"HDFS connection failed: {str(e)}")
            return False
    
    def list_files(self, path: str = "/stock_data") -> List[Dict[str, Any]]:
        """List files in HDFS directory"""
        try:
            if not self.client:
                logger.error("HDFS client not initialized")
                return []
            
            files = []
            
            logger.info(f"Listing files in: {path}")
            
            try:
                # Check if path exists
                if not self.client.status(path, strict=False):
                    logger.warning(f"Path does not exist: {path}")
                    return files
                
                # List files
                items = self.client.list(path, status=True)
                logger.info(f"Found {len(items)} items in {path}")
                
                for item in items:
                    try:
                        file_name = item[0]
                        file_status = item[1]
                        
                        file_info = {
                            "path": f"{path}/{file_name}" if path != "/" else f"/{file_name}",
                            "name": file_name,
                            "size": file_status.get("length", 0),
                            "type": "directory" if file_status.get("type") == "DIRECTORY" else "file",
                            "modification_time": datetime.fromtimestamp(
                                file_status.get("modificationTime", 0) / 1000
                            ).isoformat() if file_status.get("modificationTime") else None,
                            "permission": file_status.get("permission", ""),
                            "owner": file_status.get("owner", ""),
                            "group": file_status.get("group", "")
                        }
                        files.append(file_info)
                        
                    except Exception as e:
                        logger.error(f"Error processing item {item[0]}: {e}")
                        continue
                        
            except Exception as e:
                logger.error(f"Error accessing path {path}: {e}")
                # Return empty list instead of raising
                return files
            
            logger.info(f"Successfully listed {len(files)} files")
            return files
            
        except Exception as e:
            logger.error(f"Error listing files in {path}: {str(e)}")
            # Return empty list instead of raising exception
            return []
    
    def read_file(self, file_path: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Read file from HDFS (CSV format)"""
        try:
            if not self.client:
                logger.error("HDFS client not initialized")
                return []
            
            logger.info(f"Reading file: {file_path}")
            
            with self.client.read(file_path, encoding='utf-8') as reader:
                lines = []
                headers = []
                
                for i, line in enumerate(reader):
                    if i >= limit:
                        break
                    
                    if i == 0:
                        # Try to parse headers
                        try:
                            headers = line.strip().split(',')
                            logger.info(f"CSV headers: {headers}")
                        except:
                            headers = [f"col_{j}" for j in range(len(line.split(',')))]
                    else:
                        try:
                            values = line.strip().split(',')
                            if len(values) == len(headers):
                                lines.append(dict(zip(headers, values)))
                            else:
                                logger.warning(f"Line {i} has {len(values)} values, expected {len(headers)}")
                        except:
                            logger.warning(f"Failed to parse line {i}")
                
                logger.info(f"Read {len(lines)} lines from {file_path}")
                return lines
                
        except Exception as e:
            logger.error(f"Error reading file {file_path}: {str(e)}")
            return []
    
    def get_directory_info(self, path: str = "/stock_data") -> Dict[str, Any]:
        """Get directory information"""
        try:
            if not self.client:
                return {"error": "HDFS client not initialized"}
            
            logger.info(f"Getting directory info for: {path}")
            
            # Check if path exists
            try:
                status = self.client.status(path, strict=False)
                if not status:
                    return {
                        "path": path,
                        "exists": False,
                        "error": "Path does not exist"
                    }
            except:
                return {
                    "path": path,
                    "exists": False,
                    "error": "Error checking path existence"
                }
            
            # Get basic info
            dir_info = {
                "path": path,
                "exists": True,
                "type": "directory",
                "permission": status.get("permission", ""),
                "owner": status.get("owner", ""),
                "group": status.get("group", ""),
                "modification_time": datetime.fromtimestamp(
                    status.get("modificationTime", 0) / 1000
                ).isoformat() if status.get("modificationTime") else None,
            }
            
            # Count files and directories
            try:
                items = self.client.list(path, status=True)
                dir_count = 0
                file_count = 0
                total_size = 0
                
                for item in items:
                    item_status = item[1]
                    if item_status.get("type") == "DIRECTORY":
                        dir_count += 1
                    else:
                        file_count += 1
                        total_size += item_status.get("length", 0)
                
                dir_info.update({
                    "dir_count": dir_count,
                    "file_count": file_count,
                    "total_size": total_size,
                    "total_items": len(items)
                })
                
            except Exception as e:
                logger.error(f"Error counting items: {e}")
                dir_info["error"] = f"Error counting items: {e}"
            
            return dir_info
            
        except Exception as e:
            logger.error(f"Error getting directory info: {str(e)}")
            return {
                "path": path,
                "exists": False,
                "error": str(e)
            }