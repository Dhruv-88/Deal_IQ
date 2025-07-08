import pandas as pd 
import numpy as np
from typing import Dict


def impute_drive_from_type(df, type_column='type', drive_column='drive'):
    """
    Impute missing drive values based on vehicle type
    
    Decision based on crosstab analysis:
    - SUV, offroad, pickup, truck, other, wagon -> 4wd
    - hatchback, minivan, sedan, van -> fwd  
    - bus, convertible, coupe -> rwd
    
    Parameters:
    df (pd.DataFrame): Input DataFrame
    type_column (str): Name of type column
    drive_column (str): Name of drive column
    
    Returns:
    pd.DataFrame: DataFrame with imputed drive values
    dict: Simple summary
    """
    
    df_clean = df.copy()
    
    # Count missing values before imputation
    missing_before = df_clean[drive_column].isnull().sum()
    
    # Define conditions and corresponding drive types
    condition_4wd = df_clean[type_column].isin(['SUV', 'offroad', 'pickup', 'truck', 'other', 'wagon'])
    condition_fwd = df_clean[type_column].isin(['hatchback', 'minivan', 'sedan', 'van'])
    condition_rwd = df_clean[type_column].isin(['bus', 'convertible', 'coupe'])
    
    # Apply imputation only to missing values
    df_clean.loc[(df_clean[drive_column].isnull()) & condition_4wd, drive_column] = '4wd'
    df_clean.loc[(df_clean[drive_column].isnull()) & condition_fwd, drive_column] = 'fwd'
    df_clean.loc[(df_clean[drive_column].isnull()) & condition_rwd, drive_column] = 'rwd'
    
    # Count missing values after imputation
    missing_after = df_clean[drive_column].isnull().sum()
    values_imputed = missing_before - missing_after
    
    # Create summary
    summary = {
        'total_rows': len(df_clean),
        'missing_before': missing_before,
        'missing_after': missing_after,
        'values_imputed': values_imputed
    }
    
    return df_clean, summary 


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


def clean_drive_column(df: pd.DataFrame, drive_column: str = 'drive'):
    """
    Clean and standardize the drive column by mapping various formats to standard values
    
    Parameters:
    df (pandas.DataFrame): DataFrame containing the drive column
    drive_column (str): Name of the drive column (default: 'drive')
    
    Returns:
    pd.DataFrame: DataFrame with cleaned drive column
    dict: Simple summary
    """
    df_clean = df.copy()
    
    # Count original values
    original_counts = df_clean[drive_column].value_counts().to_dict()
    
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
        
        # Count new values after cleaning
        new_counts = df_clean[drive_column].value_counts().to_dict()
        
        # Create summary
        summary = {
            'total_rows': len(df_clean),
            'original_unique_values': len(original_counts),
            'new_unique_values': len(new_counts),
            'mappings_applied': drive_mapping
        }
        
    else:
        # If column not found, return original with error summary
        summary = {
            'total_rows': len(df_clean),
            'error': f"Column '{drive_column}' not found in DataFrame"
        }
    
    return df_clean, summary


def validate_drive_values(df: pd.DataFrame, drive_column: str = 'drive'):
    """
    Validate drive column to keep only valid values (4wd, fwd, rwd)
    
    Parameters:
    df (pd.DataFrame): Input DataFrame
    drive_column (str): Name of drive column
    
    Returns:
    pd.DataFrame: DataFrame with only valid drive values
    dict: Simple summary
    """
    
    # Valid drive values
    valid_drives = ['4wd', 'fwd', 'rwd']
    
    original_rows = len(df)
    
    # Find invalid values
    invalid_mask = ~df[drive_column].isin(valid_drives)
    invalid_count = invalid_mask.sum()
    
    # Keep only valid values (including NaN)
    df_clean = df[df[drive_column].isin(valid_drives) | df[drive_column].isna()]
    
    final_rows = len(df_clean)
    rows_dropped = original_rows - final_rows
    
    # Get value counts for summary
    value_counts = df_clean[drive_column].value_counts().to_dict()
    
    # Create summary
    summary = {
        'original_rows': original_rows,
        'final_rows': final_rows,
        'rows_dropped': rows_dropped,
        'invalid_values': invalid_count,
        'valid_drives': valid_drives,
        'value_counts': value_counts
    }
    
    return df_clean, summary


def fill_missing_drive_from_reference(df: pd.DataFrame, 
                                    reference_file: str = 'data/models_with_drive.csv',
                                    model_column: str = 'model',
                                    drive_column: str = 'drive'):
    """
    Fill missing drive values by matching model names with a reference CSV file
    
    Parameters:
    df (pd.DataFrame): DataFrame containing model and drive columns
    reference_file (str): Path to the reference CSV file with model-drive mappings
    model_column (str): Name of the model column (default: 'model')
    drive_column (str): Name of the drive column (default: 'drive')
    
    Returns:
    pd.DataFrame: DataFrame with filled drive values
    dict: Simple summary
    """
    df_clean = df.copy()
    
    # Count missing values before filling
    missing_before = df_clean[drive_column].isna().sum()
    
    try:
        # Load the reference data
        reference_df = pd.read_csv(reference_file)
        
        # Check if required columns exist in reference file
        if 'model' not in reference_df.columns or 'drive' not in reference_df.columns:
            summary = {
                'total_rows': len(df_clean),
                'missing_before': missing_before,
                'missing_after': missing_before,
                'values_filled': 0,
                'error': "Reference file must contain 'model' and 'drive' columns"
            }
            return df_clean, summary
        
        # Clean and standardize the reference data
        reference_df['model'] = reference_df['model'].astype(str).str.lower().str.strip()
        reference_df['drive'] = reference_df['drive'].astype(str).str.lower().str.strip()
        
        # Remove duplicates from reference data (keep first occurrence)
        reference_df = reference_df.drop_duplicates(subset=['model'], keep='first')
        
        # Create a mapping dictionary for faster lookup
        model_drive_mapping = dict(zip(reference_df['model'], reference_df['drive']))
        
        # Find rows with missing drive values
        missing_drive_mask = df_clean[drive_column].isna()
        
        if missing_before == 0:
            summary = {
                'total_rows': len(df_clean),
                'missing_before': missing_before,
                'missing_after': missing_before,
                'values_filled': 0,
                'mappings_loaded': len(model_drive_mapping)
            }
            return df_clean, summary
        
        # Process rows with missing drive values
        filled_count = 0
        not_found_count = 0
        
        for idx in df_clean[missing_drive_mask].index:
            model_value = df_clean.at[idx, model_column]
            
            if pd.notna(model_value):
                # Clean the model value for matching
                model_clean = str(model_value).lower().strip()
                
                # Try exact match
                if model_clean in model_drive_mapping:
                    df_clean.at[idx, drive_column] = model_drive_mapping[model_clean]
                    filled_count += 1
                else:
                    not_found_count += 1
        
        # Count missing values after filling
        missing_after = df_clean[drive_column].isna().sum()
        
        # Create summary
        summary = {
            'total_rows': len(df_clean),
            'missing_before': missing_before,
            'missing_after': missing_after,
            'values_filled': filled_count,
            'models_not_found': not_found_count,
            'mappings_loaded': len(model_drive_mapping)
        }
        
    except FileNotFoundError:
        summary = {
            'total_rows': len(df_clean),
            'missing_before': missing_before,
            'missing_after': missing_before,
            'values_filled': 0,
            'error': f"Reference file '{reference_file}' not found"
        }
    except Exception as e:
        summary = {
            'total_rows': len(df_clean),
            'missing_before': missing_before,
            'missing_after': missing_before,
            'values_filled': 0,
            'error': str(e)
        }
    
    return df_clean, summary