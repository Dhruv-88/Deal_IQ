def validate_title_status_values(df, title_col='title_status'):
    """
    Validate that title_status column contains only valid values and return filtered DataFrame
    
    Parameters:
    df (pd.DataFrame): Input DataFrame
    title_col (str): Name of the title_status column
    
    Returns:
    pd.DataFrame: Filtered DataFrame with only valid title_status values
    dict: Summary of validation results
    """
    
    # Valid title_status values
    valid_values = ['clean', 'rebuilt', 'missing', 'salvage', 'lien', 'parts only']
    
    # Store original info
    original_count = len(df)
    original_values = df[title_col].value_counts().to_dict()
    
    # Find invalid values
    invalid_mask = ~df[title_col].isin(valid_values)
    invalid_values = df[invalid_mask][title_col].value_counts().to_dict()
    
    # Check for null values
    null_count = df[title_col].isnull().sum()
    
    # Filter DataFrame to keep only valid values
    valid_df = df[df[title_col].isin(valid_values)].copy()
    
    # Remove null values as well
    valid_df = valid_df[valid_df[title_col].notna()].copy()
    
    # Create summary
    validation_summary = {
        'total_rows': original_count,
        'valid_rows': len(valid_df),
        'dropped_rows': original_count - len(valid_df),
        'drop_percentage': round((original_count - len(valid_df)) / original_count * 100, 2),
        'valid_values': valid_values,
        'original_value_counts': original_values,
        'invalid_values': invalid_values,
        'null_values': null_count,
        'validation_passed': len(invalid_values) == 0 and null_count == 0
    }
    
    return valid_df, validation_summary


def fill_missing_values(df, column_name="title_status", fill_value='missing'):
    """
    Fill missing values (NaN/null) in a specific column with a specified value
    
    Parameters:
    df (pd.DataFrame): Input DataFrame
    column_name (str): Name of the column to fill missing values
    fill_value (str/int/float): Value to use for filling missing values (default: 'missing')
    
    Returns:
    pd.DataFrame: DataFrame with missing values filled in the specified column
    dict: Summary of missing values before and after filling
    """
    
    # Create a copy to avoid modifying the original DataFrame
    df_filled = df.copy()
    
    # Check if column exists
    if column_name not in df_filled.columns:
        raise ValueError(f"Column '{column_name}' not found in DataFrame")
    
    # Count missing values before filling
    missing_before = df_filled[column_name].isnull().sum()
    
    # Fill missing values
    df_filled[column_name] = df_filled[column_name].fillna(fill_value)
    
    # Count missing values after filling
    missing_after = df_filled[column_name].isnull().sum()
    
    # Create summary
    summary = {
        'column_name': column_name,
        'fill_value': fill_value,
        'missing_before': missing_before,
        'missing_after': missing_after,
        'values_filled': missing_before - missing_after,
        'total_rows': len(df_filled)
    }
    
    return df_filled, summary