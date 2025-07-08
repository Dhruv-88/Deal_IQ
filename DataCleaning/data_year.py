import pandas as pd
def validate_years(df, year_column='year', min_year=1990):
    """
    Validate and filter DataFrame to keep only rows with years >= min_year
    
    Parameters:
    df (pd.DataFrame): Input DataFrame
    year_column (str): Name of the year column
    min_year (int): Minimum year to keep (default: 1990)
    
    Returns:
    pd.DataFrame: Filtered DataFrame with only valid years
    dict: Summary of validation results
    """
    
    # Store original info
    original_count = len(df)
    original_year_range = (df[year_column].min(), df[year_column].max())
    
    # Find invalid years (older than min_year)
    invalid_years_mask = df[year_column] < min_year
    invalid_years = df[invalid_years_mask][year_column].value_counts().sort_index()
    
    # Also check for null values
    null_years = df[year_column].isnull().sum()
    
    # Filter DataFrame to keep only valid years
    filtered_df = df[df[year_column] >= min_year].copy()
    
    # Remove null values as well
    filtered_df = filtered_df[filtered_df[year_column].notna()].copy()
    
    # Calculate new year range
    if len(filtered_df) > 0:
        new_year_range = (filtered_df[year_column].min(), filtered_df[year_column].max())
    else:
        new_year_range = (None, None)
    
    # Create summary
    validation_summary = {
        'original_rows': original_count,
        'filtered_rows': len(filtered_df),
        'dropped_rows': original_count - len(filtered_df),
        'drop_percentage': round((original_count - len(filtered_df)) / original_count * 100, 2),
        'min_year_threshold': min_year,
        'original_year_range': original_year_range,
        'new_year_range': new_year_range,
        'null_years': null_years,
        'invalid_years_count': len(invalid_years),
        'invalid_years_breakdown': dict(invalid_years)
    }
    
    return filtered_df, validation_summary

