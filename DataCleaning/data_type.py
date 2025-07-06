def validate_type_values(df, type_col='type', standardize_case=True):
    """
    Validate that type column contains only valid vehicle types and return filtered DataFrame
    
    Parameters:
    df (pd.DataFrame): Input DataFrame
    type_col (str): Name of the type column
    standardize_case (bool): Whether to convert all values to lowercase for standardization
    
    Returns:
    pd.DataFrame: Filtered DataFrame with only valid type values
    dict: Summary of validation results
    """
    
    # Valid type values based on your dataset (all lowercase for standardization)
    valid_values = [
        'sedan', 'suv', 'pickup', 'truck', 'other', 'coupe', 
        'hatchback', 'wagon', 'van', 'convertible', 'minivan', 
        'bus', 'offroad'
    ]
    
    # Store original info
    original_count = len(df)
    original_values = df[type_col].value_counts().to_dict()
    
    # Create working copy to avoid modifying original
    work_df = df.copy()
    
    # Standardize case if requested
    if standardize_case:
        work_df[type_col] = work_df[type_col].str.lower()
    
    # Find invalid values (after case standardization)
    invalid_mask = ~work_df[type_col].isin(valid_values)
    invalid_values = work_df[invalid_mask][type_col].value_counts().to_dict()
    
    # Check for null values
    null_count = work_df[type_col].isnull().sum()
    
    # Filter DataFrame to keep only valid values
    valid_df = work_df[work_df[type_col].isin(valid_values)].copy()
    
    # Remove null values as well
    valid_df = valid_df[valid_df[type_col].notna()].copy()
    
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
        'validation_passed': len(invalid_values) == 0 and null_count == 0,
        'case_standardized': standardize_case
    }
    
    return valid_df, validation_summary