def convert_transmission_to_automatic(df, transmission_col='transmission'):
    """
    Convert all transmission values except 'manual' to 'automatic'
    
    Parameters:
    df (pd.DataFrame): Input DataFrame
    transmission_col (str): Name of the transmission column
    
    Returns:
    pd.DataFrame: DataFrame with standardized transmission values
    dict: Summary of conversion results
    """
    
    # Make a copy to avoid modifying original
    df_converted = df.copy()
    
    # Store original values for summary
    original_values = df_converted[transmission_col].value_counts().to_dict()
    
    # Convert all non-manual values to automatic
    df_converted[transmission_col] = df_converted[transmission_col].apply(
        lambda x: 'manual' if str(x).lower().strip() == 'manual' else 'automatic'
    )
    
    # Handle null values - convert to automatic
    df_converted[transmission_col] = df_converted[transmission_col].fillna('automatic')
    
    # New values for summary
    new_values = df_converted[transmission_col].value_counts().to_dict()
    
    # Create summary
    conversion_summary = {
        'total_rows': len(df_converted),
        'original_unique_values': len(original_values),
        'new_unique_values': len(new_values),
        'original_value_counts': original_values,
        'new_value_counts': new_values,
        'converted_to_automatic': sum(count for value, count in original_values.items() 
                                    if str(value).lower().strip() != 'manual')
    }
    
    return df_converted, conversion_summary

def validate_transmission_values(df, transmission_col='transmission'):
    """
    Validate that transmission column contains only 'automatic' and 'manual' values
    
    Parameters:
    df (pd.DataFrame): Input DataFrame
    transmission_col (str): Name of the transmission column
    
    Returns:
    pd.DataFrame: Filtered DataFrame with only valid transmission values
    dict: Summary of validation results
    """
    
    # Valid transmission values
    valid_values = ['automatic', 'manual']
    
    # Store original info
    original_count = len(df)
    original_values = df[transmission_col].value_counts().to_dict()
    
    # Find invalid values
    invalid_mask = ~df[transmission_col].isin(valid_values)
    invalid_values = df[invalid_mask][transmission_col].value_counts().to_dict()
    
    # Check for null values
    null_count = df[transmission_col].isnull().sum()
    
    # Filter DataFrame to keep only valid values
    valid_df = df[df[transmission_col].isin(valid_values)].copy()
    
    # Remove null values as well
    valid_df = valid_df[valid_df[transmission_col].notna()].copy()
    
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

