from typing import Dict
import pandas as pd
def drop_unnecessary_columns(df, columns_to_drop=None):
    """
    Drop columns that are not needed for analysis
    
    Parameters:
    df (pd.DataFrame): Input DataFrame
    columns_to_drop (list): List of column names to drop. If None, uses default columns.
    
    Returns:
    pd.DataFrame: DataFrame with specified columns dropped
    dict: Summary of dropped columns
    """
    
    # Default columns to drop if none specified
    if columns_to_drop is None:
        columns_to_drop = [
            'url', 'image_url', 'county', 'VIN', 'size', 
            'condition', 'posting_date', 'cylinders','region','region_url'
        ]
    
    # Create a copy to avoid modifying the original DataFrame
    df_cleaned = df.copy()
    
    # Check which columns actually exist in the DataFrame
    existing_columns = [col for col in columns_to_drop if col in df_cleaned.columns]
    missing_columns = [col for col in columns_to_drop if col not in df_cleaned.columns]
    
    # Drop the existing columns
    if existing_columns:
        df_cleaned = df_cleaned.drop(existing_columns, axis=1)
    
    # Create summary
    summary = {
        'original_columns': len(df.columns),
        'final_columns': len(df_cleaned.columns),
        'dropped_columns': existing_columns,
        'missing_columns': missing_columns,
        'columns_dropped_count': len(existing_columns),
        'columns_remaining': list(df_cleaned.columns)
    }
    
    return df_cleaned, summary 



def drop_rows_with_few_missing_values(df, columns_with_few_missing=None):
    """
    Drop rows where columns with very few missing values have NaN/null values
    
    This function targets columns that have very few missing values in the dataset.
    Since these columns have minimal missing data, dropping the few rows with 
    missing values is more efficient than imputation or other missing data handling methods.
    This approach preserves data quality while maintaining most of the dataset.
    
    Parameters:
    df (pd.DataFrame): Input DataFrame
    columns_with_few_missing (list): List of column names that have very few missing values.
                                    If None, uses default columns known to have few missing values.
    
    Returns:
    pd.DataFrame: DataFrame with rows containing missing values in specified columns removed
    dict: Summary of rows dropped and remaining
    """
    
    # Default columns with few missing values if none specified
    if columns_with_few_missing is None:
        columns_with_few_missing = [
            'year', 'description', 'fuel', 'odometer', 
            'lat', 'long', 'transmission', 'model','manufacturer'
        ]
    
    # Create a copy to avoid modifying the original DataFrame
    df_cleaned = df.copy()
    
    # Store original info
    original_count = len(df_cleaned)
    
    # Check which columns actually exist in the DataFrame
    existing_columns = [col for col in columns_with_few_missing if col in df_cleaned.columns]
    missing_columns = [col for col in columns_with_few_missing if col not in df_cleaned.columns]
    
    # Count missing values per column before dropping
    missing_counts = {}
    for col in existing_columns:
        missing_counts[col] = df_cleaned[col].isnull().sum()
    
    # Drop rows with missing values in columns with few missing values
    if existing_columns:
        df_cleaned = df_cleaned.dropna(subset=existing_columns)
    
    # Create summary
    summary = {
        'original_rows': original_count,
        'final_rows': len(df_cleaned),
        'dropped_rows': original_count - len(df_cleaned),
        'drop_percentage': round((original_count - len(df_cleaned)) / original_count * 100, 2),
        'columns_with_few_missing': existing_columns,
        'missing_columns': missing_columns,
        'missing_counts_before': missing_counts,
        'total_missing_values': sum(missing_counts.values())
    }
    
    return df_cleaned, summary
