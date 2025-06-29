import pandas as pd
import numpy as np
from typing import Dict, Optional

def clean_drive_column(df: pd.DataFrame, drive_column: str = 'drive') -> pd.DataFrame:
    """
    Clean and standardize the drive column by mapping various formats to standard values
    
    Parameters:
    df (pandas.DataFrame): DataFrame containing the drive column
    drive_column (str): Name of the drive column (default: 'drive')
    
    Returns:
    pandas.DataFrame: DataFrame with cleaned drive column
    """
    df_clean = df.copy()
    
    # Define the mapping for drive types
    drive_mapping = {
        'allwheeldrive': '4wd',
        'frontwheeldrive': 'fwd',
        'rearwheeldrive': 'rwd',
        'front wheel drive': 'fwd',
        'all wheel drive': '4wd',
        'front-wheel drive': 'fwd',
        'rear-wheel drive': 'rwd',
        'all-wheel drive': '4wd',
        '4x4': '4wd',
        'awd': '4wd',
        '4d': '4wd',
        '2d': 'rwd',
        'fwd': 'fwd',
        'rwd': 'rwd',
        '4wd': '4wd'
    }
    
    if drive_column in df_clean.columns:
        # Convert to lowercase and strip whitespace for consistent processing
        df_clean[drive_column] = df_clean[drive_column].astype(str).str.lower().str.strip()
        
        # Apply the mapping
        df_clean[drive_column] = df_clean[drive_column].replace(drive_mapping)
        
        # Handle any remaining variations or edge cases
        df_clean[drive_column] = df_clean[drive_column].apply(standardize_drive_value)
        
        print(f"Drive column cleaning completed!")
        print(f"Updated distribution:")
        print(df_clean[drive_column].value_counts())
        
    else:
        print(f"Warning: Column '{drive_column}' not found in DataFrame")
    
    return df_clean

def standardize_drive_value(value: str) -> str:
    """
    Standardize individual drive values to handle edge cases
    
    Parameters:
    value (str): Drive value to standardize
    
    Returns:
    str: Standardized drive value
    """
    if pd.isna(value) or value == 'nan' or value == '':
        return np.nan
    
    value = str(value).lower().strip()
    
    # Handle common variations
    if 'all' in value and 'wheel' in value and 'drive' in value:
        return '4wd'
    elif 'front' in value and 'wheel' in value and 'drive' in value:
        return 'fwd'
    elif 'rear' in value and 'wheel' in value and 'drive' in value:
        return 'rwd'
    elif '4wd' in value or '4x4' in value or 'awd' in value:
        return '4wd'
    elif 'fwd' in value:
        return 'fwd'
    elif 'rwd' in value:
        return 'rwd'
    else:
        # If no clear match, return the original value
        return value

def validate_drive_values(df: pd.DataFrame, drive_column: str = 'drive') -> Dict:
    """
    Validate the drive column and return statistics
    
    Parameters:
    df (pandas.DataFrame): DataFrame containing the drive column
    drive_column (str): Name of the drive column (default: 'drive')
    
    Returns:
    Dict: Dictionary containing validation statistics
    """
    if drive_column not in df.columns:
        return {"error": f"Column '{drive_column}' not found"}
    
    # Get value counts
    value_counts = df[drive_column].value_counts()
    
    # Check for unexpected values
    expected_values = {'4wd', 'fwd', 'rwd'}
    actual_values = set(value_counts.index)
    unexpected_values = actual_values - expected_values
    
    # Calculate statistics
    total_rows = len(df)
    null_count = df[drive_column].isna().sum()
    
    stats = {
        "total_rows": total_rows,
        "null_count": null_count,
        "non_null_count": total_rows - null_count,
        "value_counts": value_counts.to_dict(),
        "expected_values": list(expected_values),
        "unexpected_values": list(unexpected_values),
        "is_clean": len(unexpected_values) == 0
    }
    
    return stats

def print_drive_cleaning_report(df_before: pd.DataFrame, df_after: pd.DataFrame, 
                              drive_column: str = 'drive') -> None:
    """
    Print a detailed report comparing drive column before and after cleaning
    
    Parameters:
    df_before (pandas.DataFrame): DataFrame before cleaning
    df_after (pandas.DataFrame): DataFrame after cleaning
    drive_column (str): Name of the drive column (default: 'drive')
    """
    print("=" * 60)
    print("DRIVE COLUMN CLEANING REPORT")
    print("=" * 60)
    
    if drive_column not in df_before.columns or drive_column not in df_after.columns:
        print(f"Error: Column '{drive_column}' not found in one or both DataFrames")
        return
    
    print(f"\nBEFORE CLEANING:")
    print("-" * 30)
    print(df_before[drive_column].value_counts())
    
    print(f"\nAFTER CLEANING:")
    print("-" * 30)
    print(df_after[drive_column].value_counts())
    
    # Calculate changes
    before_counts = df_before[drive_column].value_counts()
    after_counts = df_after[drive_column].value_counts()
    
    print(f"\nCHANGES:")
    print("-" * 30)
    for value in set(before_counts.index) | set(after_counts.index):
        before_count = before_counts.get(value, 0)
        after_count = after_counts.get(value, 0)
        if before_count != after_count:
            change = after_count - before_count
            print(f"{value}: {before_count} → {after_count} ({change:+d})")
    
    # Validation
    validation_stats = validate_drive_values(df_after, drive_column)
    print(f"\nVALIDATION:")
    print("-" * 30)
    print(f"Total rows: {validation_stats['total_rows']}")
    print(f"Null values: {validation_stats['null_count']}")
    print(f"Unexpected values: {validation_stats['unexpected_values']}")
    print(f"Column is clean: {validation_stats['is_clean']}")

# Example usage function
def process_drive_column(df: pd.DataFrame, drive_column: str = 'drive') -> pd.DataFrame:
    """
    Main function to process and clean the drive column
    
    Parameters:
    df (pandas.DataFrame): DataFrame with drive column to clean
    drive_column (str): Name of the drive column (default: 'drive')
    
    Returns:
    pandas.DataFrame: DataFrame with cleaned drive column
    """
    print("Starting drive column cleaning...")
    
    # Store original for comparison
    df_original = df.copy()
    
    # Clean the drive column
    df_cleaned = clean_drive_column(df, drive_column)
    
    # Print detailed report
    print_drive_cleaning_report(df_original, df_cleaned, drive_column)
    
    print("\nDrive column cleaning completed!")
    
    return df_cleaned

def fill_missing_drive_from_reference(df: pd.DataFrame, 
                                    reference_file: str = 'data/models_with_drive.csv',
                                    model_column: str = 'model',
                                    drive_column: str = 'drive') -> pd.DataFrame:
    """
    Fill missing drive values by matching model names with a reference CSV file
    
    Parameters:
    df (pandas.DataFrame): DataFrame containing model and drive columns
    reference_file (str): Path to the reference CSV file with model-drive mappings
    model_column (str): Name of the model column (default: 'model')
    drive_column (str): Name of the drive column (default: 'drive')
    
    Returns:
    pandas.DataFrame: DataFrame with filled drive values
    """
    df_clean = df.copy()
    
    try:
        # Load the reference data
        print(f"Loading reference data from: {reference_file}")
        reference_df = pd.read_csv(reference_file)
        
        # Check if required columns exist in reference file
        if 'model' not in reference_df.columns or 'drive' not in reference_df.columns:
            print("Error: Reference file must contain 'model' and 'drive' columns")
            return df_clean
        
        # Clean and standardize the reference data
        reference_df['model'] = reference_df['model'].astype(str).str.lower().str.strip()
        reference_df['drive'] = reference_df['drive'].astype(str).str.lower().str.strip()
        
        # Remove duplicates from reference data (keep first occurrence)
        reference_df = reference_df.drop_duplicates(subset=['model'], keep='first')
        
        # Create a mapping dictionary for faster lookup
        model_drive_mapping = dict(zip(reference_df['model'], reference_df['drive']))
        
        print(f"Loaded {len(model_drive_mapping)} model-drive mappings from reference file")
        
        # Find rows with missing drive values
        missing_drive_mask = df_clean[drive_column].isna()
        missing_count = missing_drive_mask.sum()
        
        print(f"Found {missing_count} rows with missing drive values")
        
        if missing_count == 0:
            print("No missing drive values to fill")
            return df_clean
        
        # Process rows with missing drive values
        filled_count = 0
        not_found_models = set()
        
        for idx in df_clean[missing_drive_mask].index:
            model_value = df_clean.at[idx, model_column]
            
            if pd.notna(model_value):
                # Clean the model value for matching
                model_clean = str(model_value).lower().strip()
                
                # Try exact match
                if model_clean in model_drive_mapping:
                    df_clean.at[idx, drive_column] = model_drive_mapping[model_clean]
                    filled_count += 1
                    print(f"✓ Filled drive for model '{model_value}' → '{model_drive_mapping[model_clean]}'")
                else:
                    not_found_models.add(model_value)
        
        # Print summary
        print(f"\n=== FILLING SUMMARY ===")
        print(f"Total missing drive values: {missing_count}")
        print(f"Successfully filled: {filled_count}")
        print(f"Remaining missing: {missing_count - filled_count}")
        
        if not_found_models:
            print(f"\nModels not found in reference file (first 10):")
            for model in list(not_found_models)[:10]:
                print(f"  - {model}")
            if len(not_found_models) > 10:
                print(f"  ... and {len(not_found_models) - 10} more")
        
        # Show updated distribution
        print(f"\nUpdated drive distribution:")
        print(df_clean[drive_column].value_counts())
        
    except FileNotFoundError:
        print(f"Error: Reference file '{reference_file}' not found")
    except Exception as e:
        print(f"Error loading reference file: {e}")
    
    return df_clean
