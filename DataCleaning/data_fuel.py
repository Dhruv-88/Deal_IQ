def convert_fuel_to_gas(df, fuel_col='fuel'):
    """
    Convert all fuel values except 'diesel', 'hybrid', 'electric' to 'gas'
    
    Parameters:
    df (pd.DataFrame): Input DataFrame
    fuel_col (str): Name of the fuel column
    
    Returns:
    pd.DataFrame: DataFrame with standardized fuel values
    dict: Summary of conversion results
    """
    
    # Make a copy to avoid modifying original
    df_converted = df.copy()
    
    # Store original values for summary
    original_values = df_converted[fuel_col].value_counts().to_dict()
    
    # Valid fuel types (except gas, which is the default)
    valid_non_gas = ['diesel', 'hybrid', 'electric']
    
    # Convert values: keep valid ones, convert others to 'gas'
    def standardize_fuel(value):
        if pd.isna(value):
            return 'gas'
        
        clean_value = str(value).lower().strip()
        
        if clean_value in valid_non_gas:
            return clean_value
        else:
            return 'gas'
    
    df_converted[fuel_col] = df_converted[fuel_col].apply(standardize_fuel)
    
    # New values for summary
    new_values = df_converted[fuel_col].value_counts().to_dict()
    
    # Create summary
    conversion_summary = {
        'total_rows': len(df_converted),
        'original_unique_values': len(original_values),
        'new_unique_values': len(new_values),
        'original_value_counts': original_values,
        'new_value_counts': new_values,
        'converted_to_gas': sum(count for value, count in original_values.items() 
                               if str(value).lower().strip() not in valid_non_gas + ['gas'])
    }
    
    return df_converted, conversion_summary


def validate_fuel_values(df, fuel_col='fuel'):
    """
    Validate that fuel column contains only 'gas', 'diesel', 'hybrid', 'electric'
    
    Parameters:
    df (pd.DataFrame): Input DataFrame
    fuel_col (str): Name of the fuel column
    
    Returns:
    pd.DataFrame: Filtered DataFrame with only valid fuel values
    dict: Summary of validation results
    """
    
    # Valid fuel values
    valid_values = ['gas', 'diesel', 'hybrid', 'electric']
    
    # Store original info
    original_count = len(df)
    original_values = df[fuel_col].value_counts().to_dict()
    
    # Find invalid values
    invalid_mask = ~df[fuel_col].isin(valid_values)
    invalid_values = df[invalid_mask][fuel_col].value_counts().to_dict()
    
    # Check for null values
    null_count = df[fuel_col].isnull().sum()
    
    # Filter DataFrame to keep only valid values
    valid_df = df[df[fuel_col].isin(valid_values)].copy()
    
    # Remove null values as well
    valid_df = valid_df[valid_df[fuel_col].notna()].copy()
    
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

