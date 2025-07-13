#!/usr/bin/env python3
"""
Authentication Test Script for Stage 2
This script specifically tests Google Cloud authentication and basic GCS connectivity
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
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_environment_variables():
    """Test if required environment variables are set"""
    logger.info("Checking environment variables...")
    
    project_id = os.getenv('GCP_PROJECT_ID')
    bucket_name = os.getenv('GCS_BUCKET_NAME')
    
    if not project_id:
        raise ValueError("✗ GCP_PROJECT_ID environment variable not set")
    if not bucket_name:
        raise ValueError("✗ GCS_BUCKET_NAME environment variable not set")
    
    logger.info(f"✓ Project ID: {project_id}")
    logger.info(f"✓ Bucket Name: {bucket_name}")
    
    return project_id, bucket_name

def test_gcs_initialization(project_id):
    """Test GCS client initialization"""
    logger.info("Testing GCS client initialization...")
    
    try:
        gcs = GCSDataOperations(project_id)
        logger.info("✓ GCS client initialized successfully")
        return gcs
    except Exception as e:
        logger.error(f"✗ GCS client initialization failed: {str(e)}")
        raise

def test_bucket_access(gcs, bucket_name):
    """Test basic bucket access"""
    logger.info("Testing bucket access...")
    
    try:
        # Try to list files in bucket (this tests read access)
        files = gcs.list_data_files(bucket_name, prefix="")
        total_files = len(files['csv']) + len(files['parquet'])
        logger.info(f"✓ Bucket access successful - found {total_files} data files")
        return True
    except Exception as e:
        logger.error(f"✗ Bucket access failed: {str(e)}")
        return False

def test_basic_write_permission(gcs, bucket_name):
    """Test basic write permissions with a tiny test file"""
    logger.info("Testing write permissions...")
    
    try:
        # Create a minimal test file
        test_data = pd.DataFrame({
            'test_timestamp': [datetime.now()],
            'test_stage': ['auth_test'],
            'status': ['success']
        })
        
        test_path = f"auth_tests/auth_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        # Upload test file
        success = gcs.upload_csv(bucket_name, test_path, test_data)
        if not success:
            raise Exception("Failed to upload test file")
        
        # Verify file exists
        exists = gcs.file_exists(bucket_name, test_path)
        if not exists:
            raise Exception("Test file was not created")
        
        # Clean up
        gcs.delete_file(bucket_name, test_path)
        
        logger.info("✓ Write permissions verified")
        return True
        
    except Exception as e:
        logger.error(f"✗ Write permission test failed: {str(e)}")
        return False

def main():
    """Main authentication test function"""
    logger.info("=" * 60)
    logger.info("STAGE 2: GOOGLE CLOUD AUTHENTICATION TEST")
    logger.info("=" * 60)
    
    try:
        # Test 1: Environment variables
        project_id, bucket_name = test_environment_variables()
        
        # Test 2: GCS client initialization
        gcs = test_gcs_initialization(project_id)
        
        # Test 3: Bucket access
        bucket_accessible = test_bucket_access(gcs, bucket_name)
        
        # Test 4: Write permissions
        write_permissions = test_basic_write_permission(gcs, bucket_name)
        
        # Summary
        logger.info("=" * 60)
        if bucket_accessible and write_permissions:
            logger.info("STAGE 2 AUTHENTICATION: ALL TESTS PASSED!")
            logger.info("✓ Google Cloud authentication is working correctly")
            logger.info("✓ GCS read/write permissions verified")
            logger.info("Ready to proceed to Stage 3 (Data Cleaning)")
        else:
            logger.error("✗ STAGE 2 AUTHENTICATION: SOME TESTS FAILED!")
            logger.error("Please check your service account permissions")
            sys.exit(1)
            
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error("=" * 60)
        logger.error(f"✗ STAGE 2 AUTHENTICATION FAILED: {str(e)}")
        logger.error("=" * 60)
        sys.exit(1)

if __name__ == "__main__":
    main()