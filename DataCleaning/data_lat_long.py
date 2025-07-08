import pandas as pd


def validate_usa_coordinates(df, lat_column='lat', long_column='long'):
    """
    Validate latitude and longitude to keep only USA coordinates
    
    USA bounds (including Alaska and Hawaii):
    - Latitude: 18.0 to 72.0 (Hawaii to Alaska)
    - Longitude: -180.0 to -66.0 (Alaska to Maine)
    
    Parameters:
    df (pd.DataFrame): Input DataFrame
    lat_column (str): Name of latitude column
    long_column (str): Name of longitude column
    
    Returns:
    pd.DataFrame: DataFrame with only valid USA coordinates
    dict: Simple summary
    """
    
    # USA coordinate bounds (including Alaska and Hawaii)
    min_lat, max_lat = 18.0, 72.0
    min_long, max_long = -180.0, -66.0
    
    original_rows = len(df)
    
    # Find invalid coordinates (outside USA bounds)
    invalid_lat = (df[lat_column] < min_lat) | (df[lat_column] > max_lat)
    invalid_long = (df[long_column] < min_long) | (df[long_column] > max_long)
    invalid_mask = invalid_lat | invalid_long
    invalid_count = invalid_mask.sum()
    
    # Keep only valid coordinates (within USA bounds or NaN)
    valid_coords = (
        ((df[lat_column] >= min_lat) & (df[lat_column] <= max_lat)) &
        ((df[long_column] >= min_long) & (df[long_column] <= max_long))
    )
    
    # Also keep rows where both are NaN
    both_nan = df[lat_column].isna() & df[long_column].isna()
    
    df_clean = df[valid_coords | both_nan]
    
    final_rows = len(df_clean)
    rows_dropped = original_rows - final_rows
    
    # Create summary
    summary = {
        'original_rows': original_rows,
        'final_rows': final_rows,
        'rows_dropped': rows_dropped,
        'invalid_coordinates': invalid_count,
        'lat_bounds': (min_lat, max_lat),
        'long_bounds': (min_long, max_long)
    }
    
    return df_clean, summary