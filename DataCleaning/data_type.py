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



def replace_values(df, column_name, replacement_dict):
    """
    Replace values in a column and return summary
    
    Parameters:
    df (pd.DataFrame): Input DataFrame
    column_name (str): Column name
    replacement_dict (dict): Values to replace {old: new}
    
    Returns:
    pd.DataFrame: DataFrame with replaced values
    dict: Simple summary
    """
    
    df_clean = df.copy()
    
    # Count affected rows before replacement
    affected_rows = 0
    for old_value in replacement_dict.keys():
        affected_rows += (df_clean[column_name] == old_value).sum()
    
    # Do the replacement
    df_clean[column_name] = df_clean[column_name].replace(replacement_dict)
    
    # Create simple summary
    summary = {
        'total_rows': len(df_clean),
        'rows_changed': affected_rows,
        'replacements': replacement_dict
    }
    
    return df_clean, summary





def fill_type_from_model(df, model_column='model', type_column='type'):
    """
    Fill missing type values based on most common type for each model
    
    Uses existing data to create model->type mapping based on mode (most frequent type)
    for each model, then applies this mapping to fill missing type values.
    
    Parameters:
    df (pd.DataFrame): Input DataFrame
    model_column (str): Name of model column
    type_column (str): Name of type column
    
    Returns:
    pd.DataFrame: DataFrame with filled type values
    dict: Simple summary
    """
    
    df_clean = df.copy()
    
    # Count missing values before filling
    missing_before = df_clean[type_column].isna().sum()
    
    # Create mapping of model to most common type
    model_type_mapping = df_clean.groupby(model_column)[type_column].agg(
        lambda x: x.mode()[0] if not x.mode().empty else None
    )
    
    # Fill missing values using the mapping
    df_clean[type_column] = df_clean[type_column].fillna(df_clean[model_column].map(model_type_mapping))
    
    # Count missing values after filling
    missing_after = df_clean[type_column].isna().sum()
    values_filled = missing_before - missing_after
    
    # Create summary
    summary = {
        'total_rows': len(df_clean),
        'missing_before': missing_before,
        'missing_after': missing_after,
        'values_filled': values_filled,
        'mapping_created': len(model_type_mapping.dropna())
    }
    
    return df_clean, summary 


def drop_na_type(df, type_column='type'):
    """
    Drop rows with missing values in type column
    
    Parameters:
    df (pd.DataFrame): Input DataFrame
    type_column (str): Name of type column
    
    Returns:
    pd.DataFrame: DataFrame with NA type rows removed
    dict: Simple summary
    """
    
    # Count missing values before dropping
    missing_count = df[type_column].isna().sum()
    original_rows = len(df)
    
    # Drop rows with missing type values
    df_clean = df.dropna(subset=[type_column])
    
    final_rows = len(df_clean)
    rows_dropped = original_rows - final_rows
    
    # Create summary
    summary = {
        'original_rows': original_rows,
        'final_rows': final_rows,
        'rows_dropped': rows_dropped,
        'missing_values': missing_count
    }
    
    return df_clean, summary