import os
import sys
import logging
from hdfs import InsecureClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_hdfs_connection():
    """Test HDFS connection"""
    try:
        # Try different ports
        ports_to_try = ['9000', '9870', '50070']
        
        for port in ports_to_try:
            try:
                logger.info(f"Trying to connect to namenode:{port}")
                client = InsecureClient(
                    f'http://namenode:{port}',
                    user='hadoop',
                    timeout=10
                )
                
                # Test connection
                status = client.status('/')
                logger.info(f"✓ Connected successfully on port {port}")
                logger.info(f"HDFS Status: {status}")
                
                # List root directory
                files = client.list('/')
                logger.info(f"Root directory contents: {files}")
                
                return True
                
            except Exception as e:
                logger.warning(f"Failed on port {port}: {str(e)}")
        
        logger.error("Failed to connect on all ports")
        return False
        
    except Exception as e:
        logger.error(f"Connection test failed: {str(e)}")
        return False

def test_hadoop_containers():
    """Test if Hadoop containers are running"""
    import subprocess
    
    containers = ['namenode', 'datanode1', 'datanode2', 'datanode3', 'datanode4']
    
    for container in containers:
        try:
            result = subprocess.run(
                ['docker', 'exec', container, 'hostname'],
                capture_output=True,
                text=True
            )
            if result.returncode == 0:
                logger.info(f"✓ Container {container} is running")
            else:
                logger.error(f"✗ Container {container} is not running")
                
        except Exception as e:
            logger.error(f"✗ Cannot access container {container}: {str(e)}")

if __name__ == "__main__":
    print("=" * 60)
    print("HDFS CONNECTION TEST")
    print("=" * 60)
    
    # Test containers
    test_hadoop_containers()
    
    # Test HDFS connection
    if test_hdfs_connection():
        print("\n✓ HDFS connection test PASSED")
    else:
        print("\n✗ HDFS connection test FAILED")
        
        # Additional debugging info
        print("\nDebugging info:")
        print("1. Check if Hadoop services are running:")
        print("   docker exec namenode jps")
        print("\n2. Check HDFS status:")
        print("   docker exec namenode hdfs dfsadmin -report")
        print("\n3. Check HDFS web UI:")
        print("   Open browser to: http://localhost:9870")