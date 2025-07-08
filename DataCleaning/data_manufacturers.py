import pandas as pd


def standardize_manufacturer(df, manufacturer_column='manufacturer'):
    """
    Standardize manufacturer names (land rover variations to land-rover)
    
    Parameters:
    df (pd.DataFrame): Input DataFrame
    manufacturer_column (str): Name of manufacturer column
    
    Returns:
    pd.DataFrame: DataFrame with standardized manufacturer values
    dict: Simple summary
    """
    
    df_clean = df.copy()
    
    # Define replacements
    replacements = {
        'land rover': 'land-rover',
        'rover': 'land-rover',
        'land rover': 'land-rover'  
    }
    
    # Count affected rows before replacement
    affected_rows = 0
    for old_value in replacements.keys():
        affected_rows += (df_clean[manufacturer_column] == old_value).sum()
    
    # Do the replacement
    df_clean[manufacturer_column] = df_clean[manufacturer_column].replace(replacements)
    
    # Create summary
    summary = {
        'total_rows': len(df_clean),
        'rows_changed': affected_rows,
        'replacements': replacements
    }
    
    return df_clean, summary 



def validate_manufacturers(df, manufacturer_column='manufacturer'):
    """
    Validate and filter DataFrame to keep only rows with approved manufacturers
    
    Parameters:
    df (pd.DataFrame): Input DataFrame
    manufacturer_column (str): Name of the manufacturer column
    
    Returns:
    pd.DataFrame: Filtered DataFrame with only valid manufacturers
    dict: Summary of validation results
    """
    
    # Valid manufacturers list
    valid_manufacturers = [
        'acura', 'alfa-romeo', 'am-general', 'amc', 'audi', 'bentley', 'bmw', 
        'buick', 'cadillac', 'chevrolet', 'chrysler', 'dodge', 'eagle', 'ferrari', 
        'fiat', 'ford', 'freightliner', 'geo', 'gmc', 'hino', 'honda', 'hyundai', 
        'infiniti', 'international', 'isuzu', 'jaguar', 'jeep', 'kaiser', 'kenworth', 
        'kia', 'lamborghini', 'land-rover', 'lexus', 'lincoln', 'lotus', 'maserati', 
        'mazda', 'mclaren', 'mercedes-benz', 'mercury', 'mg', 'mini', 'mitsubishi', 
        'nash', 'nissan', 'oldsmobile', 'packard', 'peterbilt', 'plymouth', 'polaris', 
        'pontiac', 'porsche', 'ram', 'rolls-royce', 'saab', 'saturn', 'smart', 
        'sterling', 'studebaker', 'subaru', 'suzuki', 'tesla', 'toyota', 'triumph', 
        'volkswagen', 'volvo', 'vpg', 'western-star', 'willys','edsel','genesis','datsun'
    ]
    
    # Store original info
    original_count = len(df)
    original_manufacturers = df[manufacturer_column].value_counts()
    
    # Find invalid manufacturers
    invalid_manufacturers = df[~df[manufacturer_column].isin(valid_manufacturers)][manufacturer_column].value_counts()
    
    # Filter DataFrame
    filtered_df = df[df[manufacturer_column].isin(valid_manufacturers)].copy()
    
    # Create summary
    validation_summary = {
        'original_rows': original_count,
        'filtered_rows': len(filtered_df),
        'dropped_rows': original_count - len(filtered_df),
        'drop_percentage': round((original_count - len(filtered_df)) / original_count * 100, 2),
        'valid_manufacturers_count': len(valid_manufacturers),
        'found_manufacturers_count': len(original_manufacturers),
        'invalid_manufacturers': dict(invalid_manufacturers)
    }
    
    return filtered_df, validation_summary