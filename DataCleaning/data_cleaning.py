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
            'condition', 'posting_date', 'cylinders'
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