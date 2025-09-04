"""
Google Cloud Storage CSV and Parquet Operations
Focused on read/write operations for CSV and Parquet datasets
"""

import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage
from google.cloud.exceptions import NotFound
from typing import Optional, List, Dict, Any
import logging
from io import BytesIO, StringIO
import json
import numpy as np

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class NumpyEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle NumPy and Pandas data types"""
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        elif isinstance(obj, pd.NaType):
            return None
        elif hasattr(obj, 'item'):  # Handle other numpy scalar types
            return obj.item()
        return super(NumpyEncoder, self).default(obj)

class GCSDataOperations:
    """
    A class to handle CSV and Parquet operations with Google Cloud Storage
    """
    
    def __init__(self, project_id: str, credentials_path: Optional[str] = None):
        """
        Initialize GCS client for data operations
        
        Args:
            project_id: Your Google Cloud Project ID
            credentials_path: Path to service account JSON file (optional)
        """
        self.project_id = project_id
        
        if credentials_path:
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
        
        self.client = storage.Client(project=project_id)
        logger.info("GCS Data client initialized successfully")
    
    # ==================== CSV OPERATIONS ====================
    
    def upload_csv(self, bucket_name: str, blob_name: str, df: pd.DataFrame, **kwargs) -> bool:
        """
        Upload pandas DataFrame as CSV to GCS
        
        Args:
            bucket_name: Name of the bucket
            blob_name: Name of the CSV file in GCS (e.g., 'data/file.csv')
            df: Pandas DataFrame to upload
            **kwargs: Additional parameters for pd.to_csv()
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            
            # Default CSV parameters
            csv_params = {
                'index': False,
                'encoding': 'utf-8'
            }
            csv_params.update(kwargs)
            
            csv_string = df.to_csv(**csv_params)
            blob.upload_from_string(csv_string, content_type='text/csv')
            
            logger.info(f"CSV uploaded successfully ({len(df)} rows)")
            return True
        except Exception as e:
            logger.error(f"Error uploading CSV: {str(e)}")
            return False
    
    def read_csv(self, bucket_name: str, blob_name: str, **kwargs) -> Optional[pd.DataFrame]:
        """
        Read CSV file from GCS as pandas DataFrame
        
        Args:
            bucket_name: Name of the bucket
            blob_name: Name of the CSV file in GCS
            **kwargs: Additional parameters for pd.read_csv()
        
        Returns:
            pd.DataFrame: DataFrame object, None if error
        """
        try:
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            
            csv_string = blob.download_as_text()
            df = pd.read_csv(StringIO(csv_string), **kwargs)
            
            logger.info(f"CSV read successfully ({len(df)} rows, {len(df.columns)} columns)")
            return df
        except Exception as e:
            logger.error(f"Error reading CSV: {str(e)}")
            return None
    
    def append_to_csv(self, bucket_name: str, blob_name: str, new_df: pd.DataFrame) -> bool:
        """
        Append data to existing CSV file in GCS
        
        Args:
            bucket_name: Name of the bucket
            blob_name: Name of the CSV file in GCS
            new_df: DataFrame to append
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Read existing data
            existing_df = self.read_csv(bucket_name, blob_name)
            
            if existing_df is not None:
                # Combine data
                combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            else:
                combined_df = new_df
            
            # Upload combined data
            return self.upload_csv(bucket_name, blob_name, combined_df)
            
        except Exception as e:
            logger.error(f"Error appending to CSV: {str(e)}")
            return False
    
    # ==================== PARQUET OPERATIONS ====================
    
    def upload_parquet(self, bucket_name: str, blob_name: str, df: pd.DataFrame, **kwargs) -> bool:
        """
        Upload pandas DataFrame as Parquet to GCS
        
        Args:
            bucket_name: Name of the bucket
            blob_name: Name of the Parquet file in GCS (e.g., 'data/file.parquet')
            df: Pandas DataFrame to upload
            **kwargs: Additional parameters for pd.to_parquet()
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            
            # Default Parquet parameters
            parquet_params = {
                'engine': 'pyarrow',
                'compression': 'snappy'
            }
            parquet_params.update(kwargs)
            
            # Convert DataFrame to Parquet bytes
            buffer = BytesIO()
            df.to_parquet(buffer, **parquet_params)
            
            blob.upload_from_string(buffer.getvalue(), content_type='application/octet-stream')
            
            logger.info(f"Parquet uploaded successfully ({len(df)} rows)")
            return True
        except Exception as e:
            logger.error(f"Error uploading Parquet: {str(e)}")
            return False
    
    def read_parquet(self, bucket_name: str, blob_name: str, **kwargs) -> Optional[pd.DataFrame]:
        """
        Read Parquet file from GCS as pandas DataFrame
        
        Args:
            bucket_name: Name of the bucket
            blob_name: Name of the Parquet file in GCS
            **kwargs: Additional parameters for pd.read_parquet()
        
        Returns:
            pd.DataFrame: DataFrame object, None if error
        """
        try:
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            
            parquet_bytes = blob.download_as_bytes()
            df = pd.read_parquet(BytesIO(parquet_bytes), **kwargs)
            
            logger.info(f"Parquet read successfully ({len(df)} rows, {len(df.columns)} columns)")
            return df
        except Exception as e:
            logger.error(f"Error reading Parquet: {str(e)}")
            return None
    
    def append_to_parquet(self, bucket_name: str, blob_name: str, new_df: pd.DataFrame) -> bool:
        """
        Append data to existing Parquet file in GCS
        
        Args:
            bucket_name: Name of the bucket
            blob_name: Name of the Parquet file in GCS
            new_df: DataFrame to append
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Read existing data
            existing_df = self.read_parquet(bucket_name, blob_name)
            
            if existing_df is not None:
                # Combine data
                combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            else:
                combined_df = new_df
            
            # Upload combined data
            return self.upload_parquet(bucket_name, blob_name, combined_df)
            
        except Exception as e:
            logger.error(f"Error appending to Parquet: {str(e)}")
            return False
    
    # ==================== JSON OPERATIONS ====================
    
    def upload_json(self, bucket_name: str, blob_name: str, data: Any, **kwargs) -> bool:
        """
        Upload data as JSON to GCS
        
        Args:
            bucket_name: Name of the bucket
            blob_name: Name of the JSON file in GCS (e.g., 'data/file.json')
            data: Data to upload (dict, list, or any JSON-serializable object)
            **kwargs: Additional parameters for json.dumps() (e.g., indent, ensure_ascii)
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            
            # Default JSON parameters
            json_params = {
                'indent': 2,
                'ensure_ascii': False,
                'cls': NumpyEncoder  # Use custom encoder for NumPy/Pandas types
            }
            json_params.update(kwargs)
            
            json_string = json.dumps(data, **json_params)
            blob.upload_from_string(json_string, content_type='application/json')
            
            logger.info(f"JSON uploaded successfully")
            return True
        except Exception as e:
            logger.error(f"Error uploading JSON: {str(e)}")
            return False
    
    def read_json(self, bucket_name: str, blob_name: str, **kwargs) -> Optional[Any]:
        """
        Read JSON file from GCS
        
        Args:
            bucket_name: Name of the bucket
            blob_name: Name of the JSON file in GCS
            **kwargs: Additional parameters for json.loads()
        
        Returns:
            Any: Parsed JSON data, None if error
        """
        try:
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            
            json_string = blob.download_as_text()
            data = json.loads(json_string, **kwargs)
            
            logger.info(f"JSON read successfully")
            return data
        except Exception as e:
            logger.error(f"Error reading JSON: {str(e)}")
            return None

    # ==================== CONVERSION OPERATIONS ====================
    
    def convert_csv_to_parquet(self, bucket_name: str, csv_blob: str, parquet_blob: str, 
                              delete_csv: bool = False) -> bool:
        """
        Convert CSV file to Parquet format in GCS
        
        Args:
            bucket_name: Name of the bucket
            csv_blob: Path to CSV file in GCS
            parquet_blob: Path for new Parquet file in GCS
            delete_csv: Whether to delete original CSV file
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Read CSV
            df = self.read_csv(bucket_name, csv_blob)
            if df is None:
                return False
            
            # Upload as Parquet
            success = self.upload_parquet(bucket_name, parquet_blob, df)
            
            if success and delete_csv:
                self.delete_file(bucket_name, csv_blob)
                logger.info("Original CSV file deleted successfully")
            
            logger.info("CSV to Parquet conversion completed successfully")
            return success
            
        except Exception as e:
            logger.error(f"Error converting CSV to Parquet: {str(e)}")
            return False
    
    def convert_parquet_to_csv(self, bucket_name: str, parquet_blob: str, csv_blob: str, 
                              delete_parquet: bool = False) -> bool:
        """
        Convert Parquet file to CSV format in GCS
        
        Args:
            bucket_name: Name of the bucket
            parquet_blob: Path to Parquet file in GCS
            csv_blob: Path for new CSV file in GCS
            delete_parquet: Whether to delete original Parquet file
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Read Parquet
            df = self.read_parquet(bucket_name, parquet_blob)
            if df is None:
                return False
            
            # Upload as CSV
            success = self.upload_csv(bucket_name, csv_blob, df)
            
            if success and delete_parquet:
                self.delete_file(bucket_name, parquet_blob)
                logger.info("Original Parquet file deleted successfully")
            
            logger.info("Parquet to CSV conversion completed successfully")
            return success
            
        except Exception as e:
            logger.error(f"Error converting Parquet to CSV: {str(e)}")
            return False
    
    # ==================== UTILITY OPERATIONS ====================
    
    def get_file_info(self, bucket_name: str, blob_name: str) -> Optional[Dict[str, Any]]:
        """
        Get information about a file in GCS
        
        Args:
            bucket_name: Name of the bucket
            blob_name: Name of the file
        
        Returns:
            dict: File information, None if error
        """
        try:
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            
            if not blob.exists():
                return None
            
            blob.reload()
            
            info = {
                'name': blob.name,
                'size_bytes': blob.size,
                'size_mb': round(blob.size / (1024 * 1024), 2),
                'content_type': blob.content_type,
                'created': blob.time_created,
                'updated': blob.updated,
                'md5_hash': blob.md5_hash
            }
            
            return info
        except Exception as e:
            logger.error(f"Error getting file info: {str(e)}")
            return None
    
    def list_data_files(self, bucket_name: str, prefix: str = "", 
                       file_types: List[str] = ['csv', 'parquet']) -> Dict[str, List[str]]:
        """
        List CSV and Parquet files in a bucket
        
        Args:
            bucket_name: Name of the bucket
            prefix: Prefix to filter files
            file_types: List of file extensions to include
        
        Returns:
            dict: Dictionary with file types as keys and file lists as values
        """
        try:
            bucket = self.client.bucket(bucket_name)
            blobs = bucket.list_blobs(prefix=prefix)
            
            files_by_type = {ft: [] for ft in file_types}
            
            for blob in blobs:
                for file_type in file_types:
                    if blob.name.lower().endswith(f'.{file_type}'):
                        files_by_type[file_type].append(blob.name)
            
            total_files = sum(len(files) for files in files_by_type.values())
            logger.info(f"Found {total_files} data files successfully")
            
            return files_by_type
        except Exception as e:
            logger.error(f"Error listing files: {str(e)}")
            return {ft: [] for ft in file_types}
    
    def delete_file(self, bucket_name: str, blob_name: str) -> bool:
        """
        Delete a file from GCS
        
        Args:
            bucket_name: Name of the bucket
            blob_name: Name of the file to delete
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            blob.delete()
            
            logger.info("File deleted successfully")
            return True
        except Exception as e:
            logger.error(f"Error deleting file: {str(e)}")
            return False
    
    def file_exists(self, bucket_name: str, blob_name: str) -> bool:
        """
        Check if a file exists in GCS
        
        Args:
            bucket_name: Name of the bucket
            blob_name: Name of the file to check
        
        Returns:
            bool: True if file exists, False otherwise
        """
        try:
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            return blob.exists()
        except Exception as e:
            logger.error(f"Error checking file existence: {str(e)}")
            return False


