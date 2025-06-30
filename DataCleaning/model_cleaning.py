import pandas as pd
import re

def remove_numerical_models(df: pd.DataFrame, model_column: str = 'model') -> pd.DataFrame:
    """
    Remove rows where the model column contains only numerical values
    or where the model name length is more than 40 characters
    
    Parameters:
    df (pandas.DataFrame): DataFrame containing the model column
    model_column (str): Name of the model column (default: 'model')
    
    Returns:
    pd.DataFrame: DataFrame with problematic rows removed
    """
    df_clean = df.copy()
    
    if model_column not in df_clean.columns:
        print(f"Warning: Column '{model_column}' not found in DataFrame")
        return df_clean
    
    # Get the total number of rows before cleaning
    total_rows_before = len(df_clean)
    
    # Create masks to identify rows to remove
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
    
    return df_clean
