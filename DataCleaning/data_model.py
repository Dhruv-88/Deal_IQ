import pandas as pd
import re
import os
import importlib.util
from tqdm.auto import tqdm
import numpy as np 
from typing import Dict, Optional, Union


def extract_car_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract and organize car data from the 'model' and 'description' columns
    """
    # Make a copy to avoid modifying the original dataframe
    df_clean = df.copy()
    
    # Define possible values for validation
    # drive_options = [np.nan, 'rwd', '4wd', 'fwd']
    # type_options = ['pickup', 'truck', 'other', np.nan, 'coupe', 'SUV', 'hatchback', 
    #                'mini-van', 'sedan', 'offroad', 'bus', 'van', 'convertible', 'wagon']
    # cylinder_options = ['8 cylinders', '6 cylinders', np.nan, '4 cylinders', 
    #                    '5 cylinders', 'other', '3 cylinders', '10 cylinders', '12 cylinders']
    
    # Create mapping dictionaries for standardization
    drive_mapping = {
        '4d': '4wd', '4wd': '4wd', 'awd': '4wd','4x4': '4wd',
        '2d': 'rwd', 'rwd': 'rwd', 'rear': 'rwd',
        'fwd': 'fwd', 'front': 'fwd'
    }
    
    type_mapping = {
        'sedan': 'sedan', 'coupe': 'coupe', 'suv': 'SUV',
        'hatchback': 'hatchback', 'wagon': 'wagon', 'convertible': 'convertible',
        'pickup': 'pickup', 'truck': 'truck', 'van': 'van',
        'mini-van': 'mini-van', 'minivan': 'mini-van',
        'offroad': 'offroad', 'bus': 'bus'
    }
    
    # Read manufacturers list from Google Cloud Storage
    try:
        from cloud.gcs_storage_operations import GCSDataOperations
        
        # Get environment variables for GCS operations
        gcp_project_id = os.getenv('PROJECT_ID') or os.getenv('GCP_PROJECT_ID')
        gcs_bucket_name = os.getenv('BUCKET_NAME') or os.getenv('GCS_BUCKET_NAME')
        
        if not gcp_project_id or not gcs_bucket_name:
            raise ValueError("GCP_PROJECT_ID and GCS_BUCKET_NAME environment variables must be set")
        
        # Initialize GCS operations
        gcs = GCSDataOperations(gcp_project_id)
        
        # Read manufacturers list from GCS
        manufacturers_df = gcs.read_csv(gcs_bucket_name, "manufacturers_list.csv")
        manufacturers = manufacturers_df['manufacturer'].tolist()
        
        print(f"✓ Successfully loaded {len(manufacturers)} manufacturers from GCS")
        
    except Exception as e:
        print(f"Warning: Could not load manufacturers from GCS: {e}")
        print("Using fallback manufacturers list...")
        # Fallback to a basic manufacturers list if GCS read fails
        manufacturers = [
            'toyota', 'honda', 'ford', 'chevrolet', 'nissan', 'bmw', 'mercedes-benz',
            'audi', 'volkswagen', 'hyundai', 'kia', 'mazda', 'subaru', 'lexus',
            'acura', 'infiniti', 'cadillac', 'buick', 'gmc', 'jeep', 'ram', 'dodge',
            'chrysler', 'lincoln', 'volvo', 'jaguar', 'land rover', 'porsche',
            'tesla', 'mitsubishi', 'suzuki', 'isuzu', 'saab', 'pontiac', 'saturn',
            'oldsmobile', 'mercury', 'plymouth', 'geo', 'eagle', 'daewoo', 'scion'
        ]
    
    def parse_string(text_to_parse: str) -> Dict[str, Optional[Union[str, int]]]:
        """Parse individual string and extract components"""
        if pd.isna(text_to_parse) or text_to_parse == '':
            return {'manufacturer': None, 'type': None, 'drive': None, 'cylinders': None, 'year': None}
        
        text_str = str(text_to_parse).strip()
        extracted: Dict[str, Optional[Union[str, int]]] = {
            'manufacturer': None, 
            'type': None, 
            'drive': None, 
            'cylinders': None, 
            'year': None
        }
        
        # Extract year (4-digit number, typically 1900-2030)
        year_match = re.search(r'\b(19|20)\d{2}\b', text_str)
        if year_match:
            extracted['year'] = int(year_match.group())
        
        # Extract cylinders (number followed by 'cyl', 'cylinder', or 'cylinders')
        cyl_patterns = [
            r'(\d+)\s*(?:cyl|cylinder|cylinders?)\b',
            r'\b(\d+)(?:\s*|-)?(?:cyl|cylinder|cylinders?)\b'
        ]
        for pattern in cyl_patterns:
            cyl_match = re.search(pattern, text_str, re.IGNORECASE)
            if cyl_match:
                cyl_count = cyl_match.group(1)
                extracted['cylinders'] = f"{cyl_count} cylinders"
                break
        
        # Extract drive type
        drive_patterns = [
            r'\b(4d|4wd|awd|all.?wheel.?drive|4x4)\b',
            r'\b(2d|rwd|rear.?wheel.?drive)\b',
            r'\b(fwd|front.?wheel.?drive)\b'
        ]
        for pattern in drive_patterns:
            drive_match = re.search(pattern, text_str, re.IGNORECASE)
            if drive_match:
                drive_found = drive_match.group(1).lower().replace('-', '').replace(' ', '')
                extracted['drive'] = drive_mapping.get(drive_found, drive_found)
                break
        
        # Extract type
        type_patterns = [
            r'\b(sedan|coupe|suv|hatchback|wagon|convertible|pickup|truck|van|mini.?van|minivan|offroad|bus)\b'
        ]
        for pattern in type_patterns:
            type_match = re.search(pattern, text_str, re.IGNORECASE)
            if type_match:
                type_found = type_match.group(1).lower().replace('-', '')
                extracted['type'] = type_mapping.get(type_found, type_found)
                break
        
        # Extract manufacturer (check for multi-word manufacturers first, then single words)
        model_lower = text_str.lower()
        
        # First check for multi-word manufacturers (longer names first to avoid partial matches)
        multi_word_manufacturers = [m for m in manufacturers if ' ' in m or '-' in m]
        # Sort by length (longest first) to avoid partial matches
        multi_word_manufacturers.sort(key=len, reverse=True)
        
        for manufacturer in multi_word_manufacturers:
            # Handle both space and hyphen separators
            manufacturer_pattern = manufacturer.replace('-', '[-\\s]')
            if re.search(rf'\b{manufacturer_pattern}\b', model_lower, re.IGNORECASE):
                extracted['manufacturer'] = manufacturer
                break
        
        # If no multi-word manufacturer found, check single words
        if not extracted['manufacturer']:
            words = text_str.split()
            for word in words:
                word_clean = word.lower().strip()
                for manufacturer in manufacturers:
                    # Only check single-word manufacturers
                    if ' ' not in manufacturer and '-' not in manufacturer:
                        if word_clean == manufacturer.lower():
                            extracted['manufacturer'] = manufacturer
                            break
                if extracted['manufacturer']:
                    break
        
        return extracted
    
    # Initialize columns if they don't exist
    required_columns = ['manufacturer', 'type', 'drive', 'cylinders', 'year']
    for col in required_columns:
        if col not in df_clean.columns:
            df_clean[col] = np.nan
    
    # Process each row
    for idx, row in df_clean.iterrows():
        # Process 'model' then 'description' to fill in missing details
        for source_col in ['model', 'description']:
            if source_col in df_clean.columns and pd.notna(row[source_col]):
                extracted_data = parse_string(row[source_col])
                
                # Fill missing data only if the current value is NaN or empty
                for field, value in extracted_data.items():
                    if value is not None:
                        current_value = df_clean.at[idx, field]
                        if pd.isna(current_value) or str(current_value).strip() == '' or str(current_value).lower() == 'nan':
                            df_clean.at[idx, field] = value
    
    return df_clean