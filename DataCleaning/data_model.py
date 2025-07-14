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
    # Common car manufacturers for identification
    
    
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



def validate_model_frequency(df, model_column='model', min_count=10):
    """
    Keep only models that appear at least min_count times in the dataset
    
    Parameters:
    df (pd.DataFrame): Input DataFrame
    model_column (str): Name of model column
    min_count (int): Minimum number of occurrences required (default: 10)
    
    Returns:
    pd.DataFrame: DataFrame with only frequently occurring models
    dict: Simple summary
    """
    
    original_rows = len(df)
    
    # Get value counts for models
    model_counts = df[model_column].value_counts()
    
    # Find models that appear at least min_count times
    frequent_models = model_counts[model_counts >= min_count].index
    
    # Count models that don't meet the threshold
    infrequent_models = model_counts[model_counts < min_count]
    infrequent_count = infrequent_models.sum()
    
    # Keep only rows with frequent models (including NaN)
    df_clean = df[df[model_column].isin(frequent_models) | df[model_column].isna()]
    
    final_rows = len(df_clean)
    rows_dropped = original_rows - final_rows
    
    # Create summary
    summary = {
        'original_rows': original_rows,
        'final_rows': final_rows,
        'rows_dropped': rows_dropped,
        'min_count_threshold': min_count,
        'models_kept': len(frequent_models),
        'models_dropped': len(infrequent_models),
        'infrequent_model_rows': infrequent_count
    }
    
    return df_clean, summary 



# Register tqdm for pandas
tqdm.pandas(desc="Cleaning Models")

def remove_numerical_models(df: pd.DataFrame, model_column: str = 'model'):
    """
    Remove rows where the model column contains only numerical values
    or where the model name length is more than 40 characters
    
    Parameters:
    df (pandas.DataFrame): DataFrame containing the model column
    model_column (str): Name of the model column (default: 'model')
    
    Returns:
    pd.DataFrame: DataFrame with problematic rows removed
    dict: Simple summary
    """
    df_clean = df.copy()
    
    if model_column not in df_clean.columns:
        print(f"Warning: Column '{model_column}' not found in DataFrame")
        summary = {
            'total_rows': len(df_clean),
            'rows_removed': 0,
            'error': f"Column '{model_column}' not found in DataFrame"
        }
        return df_clean, summary
    
    # Get the total number of rows before cleaning
    total_rows_before = len(df_clean)
    
    # Create masks to identify rows to remove (vectorized operations)
    # 1. Rows with only numerical values in model column
    numerical_mask = df_clean[model_column].astype(str).str.match(r'^\d+$', na=False)
    
    # 2. Rows with length more than 40 characters
    length_mask = df_clean[model_column].astype(str).str.len() > 40
    
    # Combine masks to get rows to remove
    rows_to_remove = numerical_mask | length_mask
    
    # Remove the problematic rows
    df_clean = df_clean[~rows_to_remove]
    
    # Get the total number of rows after cleaning
    total_rows_after = len(df_clean)
    rows_removed = total_rows_before - total_rows_after
    
    print(f"Debug: DataFrame shape after cleaning: {df_clean.shape}")
    
    # Log the results
    print(f"Step: remove_numerical_models")
    print(f"Rows removed due to numerical only: {numerical_mask.sum()}")
    print(f"Rows removed due to length > 40: {length_mask.sum()}")
    print(f"Total rows removed: {rows_removed}")
    
    # Create summary
    summary = {
        'total_rows_before': total_rows_before,
        'total_rows_after': total_rows_after,
        'rows_removed': rows_removed,
        'numerical_removed': numerical_mask.sum(),
        'length_removed': length_mask.sum()
    }
    
    return df_clean, summary
def _load_models_by_manufacturer():
    """
    Load model lists from data/models.py and organize them by manufacturer.
    
    Returns:
        dict: A dictionary where keys are manufacturers and values are lists of models.
    """
    models_by_manufacturer = {}
    
    # Construct path to data/models.py relative to this script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    models_file_path = os.path.join(project_root, 'data', 'models.py')

    if not os.path.exists(models_file_path):
        print(f"Warning: Model file not found at {models_file_path}")
        return models_by_manufacturer

    spec = importlib.util.spec_from_file_location("models", models_file_path)
    if spec is None or spec.loader is None:
        print(f"Could not get spec from {models_file_path}")
        return models_by_manufacturer
    models_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(models_module)
    
    for name in dir(models_module):
        if name.endswith('_models'):
            manufacturer = name.replace('_models', '').replace('_', '-')
            models = getattr(models_module, name)
            if isinstance(models, list):
                if manufacturer in models_by_manufacturer:
                    models_by_manufacturer[manufacturer].extend(models)
                else:
                    models_by_manufacturer[manufacturer] = models
    
    # Deduplicate models in each list
    for manufacturer, models in models_by_manufacturer.items():
        models_by_manufacturer[manufacturer] = sorted(list(set(models)))
        
    return models_by_manufacturer

def _normalize_text(text):
    """
    Normalize text for better matching by:
    - Converting to lowercase
    - Removing extra spaces, hyphens, underscores
    - Keeping only alphanumeric characters and single spaces
    """
    if pd.isna(text):
        return ""
    
    # Convert to string and lowercase
    text = str(text).lower()
    
    # Replace multiple spaces, hyphens, underscores with single space
    text = re.sub(r'[\s\-_]+', ' ', text)
    
    # Remove special characters except letters, numbers, and spaces
    text = re.sub(r'[^a-z0-9\s]', '', text)
    
    # Remove extra spaces
    text = ' '.join(text.split())
    
    return text

def _create_model_variations(model):
    """
    Create variations of a model name to handle different formats:
    - Original: f150
    - With hyphen: f-150
    - With space: f 150
    - Without spaces/hyphens: f150
    """
    variations = set()
    
    # Original model
    variations.add(model)
    
    # Create normalized version (no spaces, hyphens, underscores)
    normalized = re.sub(r'[\s\-_]+', '', model)
    variations.add(normalized)
    
    # Add version with hyphens between letters and numbers
    hyphen_version = re.sub(r'([a-z])(\d)', r'\1-\2', model)
    variations.add(hyphen_version)
    
    # Add version with spaces between letters and numbers
    space_version = re.sub(r'([a-z])(\d)', r'\1 \2', model)
    variations.add(space_version)
    
    return variations

def filter_by_value_counts(df, column, min_count=1):
    """
    Filter DataFrame to keep only rows where the specified column value 
    appears at least min_count times.
    
    Parameters:
    df (pd.DataFrame): Input DataFrame
    column (str): Column name to check value counts for
    min_count (int): Minimum number of occurrences to keep (default: 1)
    
    Returns:
    pd.DataFrame: Filtered DataFrame
    """
    # Get values that appear at least min_count times
    frequent_values = df[column].value_counts()
    frequent_values = frequent_values[frequent_values >= min_count].index
    
    # Keep only rows with frequently occurring values
    filtered_df = df[df[column].isin(frequent_values)]
    
    return filtered_df

def clean_models_with_list_optimized(df: pd.DataFrame, model_column: str = 'model', manufacturer_column: str = 'manufacturer'):
    """
    Optimized version of clean_models_with_list that uses vectorized operations and caching
    for much faster performance.
    
    Args:
        df (pd.DataFrame): DataFrame with model and manufacturer columns.
        model_column (str, optional): Name of the model column. Defaults to 'model'.
        manufacturer_column (str, optional): Name of the manufacturer column. Defaults to 'manufacturer'.

    Returns:
        pd.DataFrame: DataFrame with cleaned model and manufacturer names.
        dict: Simple summary
    """
    print("Starting optimized model cleaning...")
    df_clean = df.copy()
    models_by_manufacturer = _load_models_by_manufacturer()

    if not models_by_manufacturer:
        print("Warning: No models loaded, returning original DataFrame.")
        summary = {
            'total_rows': len(df_clean),
            'models_updated': 0,
            'manufacturers_updated': 0,
            'error': 'No models loaded'
        }
        return df_clean, summary

    # Create comprehensive lookup dictionaries - this is done once
    print("Creating optimized lookup tables...")
    
    # Direct lookup for exact matches
    exact_match_dict = {}
    contains_match_dict = {}
    prefix_match_dict = {}
    
    for manufacturer, models in models_by_manufacturer.items():
        for model in models:
            # Create variations for this model
            variations = _create_model_variations(model)
            
            for variation in variations:
                # Normalize the variation
                normalized_variation = _normalize_text(variation)
                if normalized_variation:
                    # Store for exact matches
                    exact_match_dict[normalized_variation] = (model, manufacturer)
                    
                    # Store for contains matches (sorted by length desc for priority)
                    if normalized_variation not in contains_match_dict:
                        contains_match_dict[normalized_variation] = []
                    contains_match_dict[normalized_variation].append((model, manufacturer))
    
    # Sort contains matches by length (longer matches first)
    for key in contains_match_dict:
        contains_match_dict[key] = sorted(contains_match_dict[key], key=lambda x: len(x[0]), reverse=True)
    
    # Get unique model texts to process (avoid duplicate processing)
    print("Processing unique model values...")
    unique_models = df_clean[model_column].dropna().unique()
    
    # Create a mapping from original text to cleaned result
    text_to_result = {}
    
    # Process unique models with progress bar
    for original_text in tqdm(unique_models, desc="Processing unique models"):
        normalized_text = _normalize_text(original_text)
        matched_model, matched_manufacturer = None, None
        
        # Strategy 1: Direct exact match (fastest)
        if normalized_text in exact_match_dict:
            matched_model, matched_manufacturer = exact_match_dict[normalized_text]
        
        # Strategy 2: Contains match (if no exact match)
        elif normalized_text:
            # Check if any known model is contained in the text
            for model_variation in sorted(contains_match_dict.keys(), key=len, reverse=True):
                if re.search(r'\b' + re.escape(model_variation) + r'\b', normalized_text):
                    matched_model, matched_manufacturer = contains_match_dict[model_variation][0]
                    break
            
            # Strategy 3: Manufacturer prefix match (if no contains match)
            if not matched_model:
                words = normalized_text.split()
                if len(words) >= 2:
                    remaining_text = ' '.join(words[1:])
                    if remaining_text in exact_match_dict:
                        matched_model, matched_manufacturer = exact_match_dict[remaining_text]
            
            # Strategy 4: Starts with match (if no prefix match)
            if not matched_model:
                for model_variation in sorted(contains_match_dict.keys(), key=len, reverse=True):
                    if normalized_text.startswith(model_variation + ' '):
                        matched_model, matched_manufacturer = contains_match_dict[model_variation][0]
                        break
        
        # Store the result
        text_to_result[original_text] = (matched_model, matched_manufacturer)
    
    # Apply the mapping to the DataFrame (vectorized operation)
    print("Applying results to DataFrame...")
    
    # Create a function to map the results
    def map_result(text):
        if pd.isna(text):
            return None, None
        return text_to_result.get(text, (None, None))
    
    # Apply the mapping
    results = df_clean[model_column].map(map_result)
    
    # Extract matched models and manufacturers
    matched_models = [result[0] if result else None for result in results]
    matched_manufacturers = [result[1] if result else None for result in results]
    
    # Create boolean mask for rows with valid matches
    mask = pd.Series(matched_models, index=df_clean.index).notna()
    
    # Track original values for comparison
    original_models = df_clean[model_column].copy()
    original_manufacturers = df_clean[manufacturer_column].copy()
    
    # Count changes
    models_updated = 0
    manufacturers_updated = 0
    
    if mask.any():
        print(f"Found {mask.sum()} models to update.")
        
        # Update using vectorized operations - fix the indexing issue
        matched_models_series = pd.Series(matched_models, index=df_clean.index)
        matched_manufacturers_series = pd.Series(matched_manufacturers, index=df_clean.index)
        
        # Count actual changes
        models_updated = (original_models != matched_models_series).sum()
        manufacturers_updated = (original_manufacturers != matched_manufacturers_series).sum()
        
        df_clean.loc[mask, model_column] = matched_models_series[mask]
        df_clean.loc[mask, manufacturer_column] = matched_manufacturers_series[mask]
    else:
        print("No models were updated.")

    # Calculate total changes
    changed_rows = (original_models != df_clean[model_column]) | \
                   (original_manufacturers != df_clean[manufacturer_column])
    
    print(f"Step: clean_models_with_list_optimized")
    print(f"Total rows modified: {changed_rows.sum()}")
    
    # Show some examples of changes
    if changed_rows.any():
        print("\nSample transformations:")
        sample_changes = df_clean[changed_rows].head(10)
        for idx in sample_changes.index:
            old_model = original_models.loc[idx]
            new_model = df_clean.loc[idx, model_column]
            old_mfg = original_manufacturers.loc[idx]
            new_mfg = df_clean.loc[idx, manufacturer_column]
            print(f"  '{old_model}' ({old_mfg}) -> '{new_model}' ({new_mfg})")

    # Create summary
    summary = {
        'total_rows': len(df_clean),
        'rows_modified': changed_rows.sum(),
        'models_updated': models_updated,
        'manufacturers_updated': manufacturers_updated,
        'unique_models_processed': len(unique_models),
        'lookup_tables_created': len(exact_match_dict)
    }

    return df_clean, summary