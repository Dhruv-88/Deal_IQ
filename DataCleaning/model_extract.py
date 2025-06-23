import pandas as pd
import re
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
    
    # Common car manufacturers for identification
    manufacturers = [
        'gmc', 'chevrolet', 'toyota', 'ford', 'jeep', 'nissan', 'ram',
       'mazda', 'cadillac', 'honda', 'dodge', 'lexus', 'jaguar', 'buick',
       'chrysler', 'volvo', 'audi', 'infiniti', 'lincoln', 'alfa-romeo',
       'subaru', 'acura', 'hyundai', 'mercedes-benz', 'bmw',
       'mitsubishi', 'volkswagen', 'porsche', 'kia', 'rover', 'ferrari',
       'mini', 'pontiac', 'fiat', 'tesla', 'saturn', 'mercury',
       'harley-davidson', 'datsun', 'aston-martin', 'land rover',
       'morgan','genesis','Freightliner','International','Scion','smart','Isuzu','Maserati'
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

def clean_and_validate_data(df: pd.DataFrame) -> pd.DataFrame:
    """Additional cleaning and validation"""
    df_clean = df.copy()
    
    # Standardize drive values
    drive_mapping = {'4d': '4wd', '2d': 'rwd'}
    if 'drive' in df_clean.columns:
        df_clean['drive'] = df_clean['drive'].replace(drive_mapping)
    
    # Ensure cylinders format is consistent
    if 'cylinders' in df_clean.columns:
        def format_cylinders(val: Union[str, float, int, None]) -> Union[str, float, None]:
            if pd.isna(val):
                return val
            val_str = str(val).lower()
            # Extract number from various formats
            match = re.search(r'(\d+)', val_str)
            if match:
                num = match.group(1)
                return f"{num} cylinders"
            return val
        
        df_clean['cylinders'] = df_clean['cylinders'].apply(format_cylinders)
    
    return df_clean

# Example usage function
def process_car_dataset(df: pd.DataFrame) -> pd.DataFrame:
    """
    Main function to process the car dataset
    
    Parameters:
    df (pandas.DataFrame): DataFrame with 'model' column
    
    Returns:
    pandas.DataFrame: Cleaned DataFrame with extracted data
    """
    print("Starting data extraction and cleaning...")
    
    # Extract data from model column
    df_processed = extract_car_data(df)
    
    # Additional cleaning and validation
    df_final = clean_and_validate_data(df_processed)
    
    print("Data extraction and cleaning completed!")
    
    return df_final

# # Example of how to use the script
# if __name__ == "__main__":
#     # Create sample data for testing
#     sample_data = {
#         'model': [
#             'Genesis G80 3.8 Sedan 4D',
#             'Toyota Camry 2022 4 Cylinder FWD',
#             'Ford F-150 Pickup Truck 8 Cylinders RWD',
#             'Honda Civic Coupe',
#             'BMW X5 SUV AWD 6 Cylinders 2021'
#         ],
#         'manufacturer': [np.nan, 'Toyota', np.nan, np.nan, 'BMW'],
#         'type': [np.nan, np.nan, 'pickup', 'coupe', np.nan],
#         'drive': [np.nan, np.nan, np.nan, np.nan, np.nan],
#         'cylinders': [np.nan, np.nan, np.nan, np.nan, np.nan],
#         'year': [np.nan, np.nan, np.nan, np.nan, np.nan]
#     }
    
#     df_sample = pd.DataFrame(sample_data)
#     print("Original data:")
#     print(df_sample)
#     print("\n" + "="*50 + "\n")
    
#     # Process the data
#     df_result = process_car_dataset(df_sample)
    
#     print("Processed data:")
#     print(df_result)
    
#     # Show what was extracted
#     print("\n" + "="*50 + "\n")
#     print("Extraction summary:")
#     for idx, row in df_result.iterrows():
#         print(f"Row {idx}: {row['model']}")
#         print(f"  Manufacturer: {row['manufacturer']}")
#         print(f"  Type: {row['type']}")
#         print(f"  Drive: {row['drive']}")
#         print(f"  Cylinders: {row['cylinders']}")
#         print(f"  Year: {row['year']}")
#         print()