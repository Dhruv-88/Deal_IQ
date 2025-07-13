#!/usr/bin/env python3
"""
CI/CD Test Script for GCS Data Operations
This script tests the authentication and basic operations in GitHub Actions
"""

import os
import sys
import pandas as pd
from datetime import datetime
import logging

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cloud.gcs_storage_operations import GCSDataOperations

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('ci_test.log')
    ]
)
logger = logging.getLogger(__name__)

def test_authentication():
    """Test if we can authenticate with Google Cloud"""
    try:
        project_id = os.getenv('GCP_PROJECT_ID')
        bucket_name = os.getenv('GCS_BUCKET_NAME')
        
        if not project_id:
            raise ValueError("GCP_PROJECT_ID environment variable not set")
        if not bucket_name:
            raise ValueError("GCS_BUCKET_NAME environment variable not set")
        
        logger.info(f"Testing authentication for project: {project_id}")
        logger.info(f"Using bucket: {bucket_name}")
        
        # Initialize GCS operations
        gcs = GCSDataOperations(project_id)
        
        logger.info("✅ Authentication successful!")
        return gcs, project_id, bucket_name
        
    except Exception as e:
        logger.error(f"❌ Authentication failed: {str(e)}")
        return None, None, None

def test_basic_operations(gcs, bucket_name):
    """Test basic read/write operations"""
    try:
        logger.info("Starting basic operations test...")
        
        # Create test data
        test_data = pd.DataFrame({
            'timestamp': [datetime.now()],
            'test_id': ['ci_cd_test'],
            'status': ['success'],
            'value': [42]
        })
        
        # Test CSV operations
        csv_path = f"ci_tests/test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        logger.info(f"Testing CSV upload to: {csv_path}")
        
        success = gcs.upload_csv(bucket_name, csv_path, test_data)
        if not success:
            raise Exception("CSV upload failed")
        
        # Test reading back
        df_read = gcs.read_csv(bucket_name, csv_path)
        if df_read is None or len(df_read) == 0:
            raise Exception("CSV read failed")
        
        logger.info(f"✅ CSV operations successful! Read {len(df_read)} rows")
        
        # Test Parquet operations
        parquet_path = csv_path.replace('.csv', '.parquet')
        logger.info(f"Testing Parquet conversion to: {parquet_path}")
        
        success = gcs.convert_csv_to_parquet(bucket_name, csv_path, parquet_path)
        if not success:
            raise Exception("CSV to Parquet conversion failed")
        
        # Test reading Parquet
        df_parquet = gcs.read_parquet(bucket_name, parquet_path)
        if df_parquet is None or len(df_parquet) == 0:
            raise Exception("Parquet read failed")
        
        logger.info(f"✅ Parquet operations successful! Read {len(df_parquet)} rows")
        
        # Test file listing
        files = gcs.list_data_files(bucket_name, prefix="ci_tests/")
        logger.info(f"✅ Found {len(files['csv']) + len(files['parquet'])} test files")
        
        # Clean up test files
        gcs.delete_file(bucket_name, csv_path)
        gcs.delete_file(bucket_name, parquet_path)
        logger.info("✅ Cleanup completed")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Basic operations failed: {str(e)}")
        return False

def test_file_info_operations(gcs, bucket_name):
    """Test file information and listing operations"""
    try:
        logger.info("Testing file info operations...")
        
        # Create a small test file for info testing
        test_data = pd.DataFrame({'test': [1, 2, 3]})
        info_test_path = "ci_tests/info_test.parquet"
        
        gcs.upload_parquet(bucket_name, info_test_path, test_data)
        
        # Test file info
        file_info = gcs.get_file_info(bucket_name, info_test_path)
        if file_info:
            logger.info(f"✅ File info retrieved: {file_info['size_mb']} MB")
        else:
            raise Exception("Could not retrieve file info")
        
        # Test file existence
        exists = gcs.file_exists(bucket_name, info_test_path)
        if not exists:
            raise Exception("File existence check failed")
        
        logger.info("✅ File existence check passed")
        
        # Cleanup
        gcs.delete_file(bucket_name, info_test_path)
        
        return True
        
    except Exception as e:
        logger.error(f"❌ File info operations failed: {str(e)}")
        return False

def main():
    """Main test function"""
    logger.info("=" * 50)
    logger.info("Starting CI/CD Tests for GCS Data Operations")
    logger.info("=" * 50)
    
    # Test 1: Authentication
    gcs, project_id, bucket_name = test_authentication()
    if not gcs:
        logger.error("❌ Authentication test failed - stopping tests")
        sys.exit(1)
    
    # Test 2: Basic operations
    if not test_basic_operations(gcs, bucket_name):
        logger.error("❌ Basic operations test failed")
        sys.exit(1)
    
    # Test 3: File info operations
    if not test_file_info_operations(gcs, bucket_name):
        logger.error("❌ File info operations test failed")
        sys.exit(1)
    
    logger.info("=" * 50)
    logger.info("🎉 All tests passed successfully!")
    logger.info("CI/CD pipeline is working correctly with GCS")
    logger.info("=" * 50)

if __name__ == "__main__":
    main()