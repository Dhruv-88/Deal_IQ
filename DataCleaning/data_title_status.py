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