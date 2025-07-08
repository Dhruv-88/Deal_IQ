import pandas as pd 
def fill_paint_color_nulls(df, paint_color_col='paint_color', manufacturer_col='manufacturer', state_col='state'):
    """
    Fill null values in paint_color column based on most common color 
    for each manufacturer-state combination
    
    Parameters:
    df (pd.DataFrame): Input DataFrame
    paint_color_col (str): Name of the paint color column
    manufacturer_col (str): Name of the manufacturer column  
    state_col (str): Name of the state column
    
    Returns:
    pd.DataFrame: DataFrame with filled paint_color values
    dict: Summary of filling operation
    """
    
    # Make a copy to avoid modifying original
    df_filled = df.copy()
    
    # Count nulls before filling
    nulls_before = df_filled[paint_color_col].isnull().sum()
    total_rows = len(df_filled)
    
    # Step 1: Fill based on manufacturer + state combination
    manufacturer_state_mode = df_filled.groupby([manufacturer_col, state_col])[paint_color_col].agg(
        lambda x: x.mode().iloc[0] if len(x.mode()) > 0 and not x.mode().empty else None
    ).to_dict()
    
    # Create mask for nulls
    null_mask = df_filled[paint_color_col].isnull()
    
    # Fill nulls with manufacturer-state mode
    for idx in df_filled[null_mask].index:
        manufacturer = df_filled.loc[idx, manufacturer_col]
        state = df_filled.loc[idx, state_col]
        
        if (manufacturer, state) in manufacturer_state_mode:
            mode_color = manufacturer_state_mode[(manufacturer, state)]
            if pd.notna(mode_color):
                df_filled.loc[idx, paint_color_col] = mode_color
    
    # Step 2: Fill remaining nulls with manufacturer-only mode
    still_null_mask = df_filled[paint_color_col].isnull()
    manufacturer_mode = df_filled.groupby(manufacturer_col)[paint_color_col].agg(
        lambda x: x.mode().iloc[0] if len(x.mode()) > 0 and not x.mode().empty else None
    ).to_dict()
    
    for idx in df_filled[still_null_mask].index:
        manufacturer = df_filled.loc[idx, manufacturer_col]
        
        if manufacturer in manufacturer_mode:
            mode_color = manufacturer_mode[manufacturer]
            if pd.notna(mode_color):
                df_filled.loc[idx, paint_color_col] = mode_color
    
    # Step 3: Fill any remaining nulls with overall most common color
    remaining_null_mask = df_filled[paint_color_col].isnull()
    if remaining_null_mask.sum() > 0:
        overall_mode = df_filled[paint_color_col].mode()
        if len(overall_mode) > 0:
            df_filled.loc[remaining_null_mask, paint_color_col] = overall_mode.iloc[0]
    
    # Count nulls after filling
    nulls_after = df_filled[paint_color_col].isnull().sum()
    filled_count = nulls_before - nulls_after
    
    # Create summary
    filling_summary = {
        'total_rows': total_rows,
        'nulls_before': nulls_before,
        'nulls_after': nulls_after,
        'filled_count': filled_count,
        'fill_percentage': round((filled_count / nulls_before * 100), 2) if nulls_before > 0 else 0,
        'manufacturer_state_combinations': len(manufacturer_state_mode),
        'successful_combinations': sum(1 for v in manufacturer_state_mode.values() if pd.notna(v))
    }
    
    return df_filled, filling_summary 

def validate_paint_color(df, paint_color_column='paint_color'):
    """
    Validate paint_color column to keep only allowed values, drop others
    
    Allowed values: white, black, silver, blue, red, grey, green, brown, 
                   custom, orange, yellow, purple
    
    Parameters:
    df (pd.DataFrame): Input DataFrame
    paint_color_column (str): Name of paint_color column
    
    Returns:
    pd.DataFrame: DataFrame with only valid paint_color values
    dict: Simple summary
    """
    
    # Valid paint color values
    valid_colors = [
        'white', 'black', 'silver', 'blue', 'red', 'grey', 
        'green', 'brown', 'custom', 'orange', 'yellow', 'purple'
    ]
    
    original_rows = len(df)
    
    # Find invalid values
    invalid_mask = ~df[paint_color_column].isin(valid_colors)
    invalid_count = invalid_mask.sum()
    
    # Keep only valid values (including NaN)
    df_clean = df[df[paint_color_column].isin(valid_colors) | df[paint_color_column].isna()]
    
    final_rows = len(df_clean)
    rows_dropped = original_rows - final_rows
    
    # Create summary
    summary = {
        'original_rows': original_rows,
        'final_rows': final_rows,
        'rows_dropped': rows_dropped,
        'invalid_values': invalid_count,
        'valid_colors': valid_colors
    }
    
    return df_clean, summary