import pandas as pd
def add_census_divisions_abbrev(df, state_col='state', new_col='census_region'):
    """
    Add U.S. Census Bureau Regional Divisions based on state abbreviations
    
    Parameters:
    df (pd.DataFrame): Input DataFrame
    state_col (str): Name of the state column (with abbreviations)
    new_col (str): Name of the new census division column
    
    Returns:
    pd.DataFrame: DataFrame with new census division column
    """
    
    # U.S. Census Bureau Regional Divisions mapping (state abbreviations)
    census_divisions = {
        # New England
        'ct': 'New England',  # Connecticut
        'me': 'New England',  # Maine
        'ma': 'New England',  # Massachusetts
        'nh': 'New England',  # New Hampshire
        'ri': 'New England',  # Rhode Island
        'vt': 'New England',  # Vermont
        
        # Middle Atlantic
        'nj': 'Middle Atlantic',  # New Jersey
        'ny': 'Middle Atlantic',  # New York
        'pa': 'Middle Atlantic',  # Pennsylvania
        
        # East North Central
        'il': 'East North Central',  # Illinois
        'in': 'East North Central',  # Indiana
        'mi': 'East North Central',  # Michigan
        'oh': 'East North Central',  # Ohio
        'wi': 'East North Central',  # Wisconsin
        
        # West North Central
        'ia': 'West North Central',  # Iowa
        'ks': 'West North Central',  # Kansas
        'mn': 'West North Central',  # Minnesota
        'mo': 'West North Central',  # Missouri
        'ne': 'West North Central',  # Nebraska
        'nd': 'West North Central',  # North Dakota
        'sd': 'West North Central',  # South Dakota
        
        # South Atlantic
        'de': 'South Atlantic',  # Delaware
        'fl': 'South Atlantic',  # Florida
        'ga': 'South Atlantic',  # Georgia
        'md': 'South Atlantic',  # Maryland
        'nc': 'South Atlantic',  # North Carolina
        'sc': 'South Atlantic',  # South Carolina
        'va': 'South Atlantic',  # Virginia
        'wv': 'South Atlantic',  # West Virginia
        'dc': 'South Atlantic',  # Washington DC
        
        # East South Central
        'al': 'East South Central',  # Alabama
        'ky': 'East South Central',  # Kentucky
        'ms': 'East South Central',  # Mississippi
        'tn': 'East South Central',  # Tennessee
        
        # West South Central
        'ar': 'West South Central',  # Arkansas
        'la': 'West South Central',  # Louisiana
        'ok': 'West South Central',  # Oklahoma
        'tx': 'West South Central',  # Texas
        
        # Mountain
        'az': 'Mountain',  # Arizona
        'co': 'Mountain',  # Colorado
        'id': 'Mountain',  # Idaho
        'mt': 'Mountain',  # Montana
        'nv': 'Mountain',  # Nevada
        'nm': 'Mountain',  # New Mexico
        'ut': 'Mountain',  # Utah
        'wy': 'Mountain',  # Wyoming
        
        # Pacific
        'ak': 'Pacific',  # Alaska
        'ca': 'Pacific',  # California
        'hi': 'Pacific',  # Hawaii
        'or': 'Pacific',  # Oregon
        'wa': 'Pacific'   # Washington
    }
    
    # Create a copy to avoid modifying original
    df_with_divisions = df.copy()
    
    # Map state abbreviations to census divisions
    df_with_divisions[new_col] = df_with_divisions[state_col].map(census_divisions)
    
    # Check for unmapped states
    unmapped_states = df_with_divisions[df_with_divisions[new_col].isnull()][state_col].unique()
    
    # Summary
    mapping_summary = {
        'total_rows': len(df_with_divisions),
        'mapped_rows': df_with_divisions[new_col].notna().sum(),
        'unmapped_rows': df_with_divisions[new_col].isna().sum(),
        'unmapped_states': list(unmapped_states),
        'divisions_found': df_with_divisions[new_col].nunique()
    }
    
    return df_with_divisions, mapping_summary

def validate_regions(df, column_name='census_region'):
    """
    Validate region column to ensure only the 9 US census divisions exist.
    Drop rows with invalid regions.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        DataFrame containing the region column
    column_name : str
        Name of the region column (default: 'region')
    
    Returns:
    --------
    tuple: (pandas.DataFrame, dict)
        Filtered DataFrame and summary dictionary
    """
    
    # Define the 9 allowed census divisions
    allowed_regions = {
        'New England',
        'Middle Atlantic', 
        'East North Central',
        'West North Central',
        'South Atlantic',
        'East South Central',
        'West South Central',
        'Mountain',
        'Pacific'
    }
    
    original_count = len(df)
    
    # Create a copy to avoid modifying original dataframe
    df_clean = df.copy()
    
    # Handle missing values and standardize
    df_clean[column_name] = (df_clean[column_name]
                            .fillna('unknown')
                            .astype(str)
                            .str.strip())
    
    # Find valid regions
    valid_mask = df_clean[column_name].isin(allowed_regions)
    invalid_regions = df_clean.loc[~valid_mask, column_name].unique().tolist()
    
    # Drop rows with invalid regions
    df_filtered = df_clean[valid_mask].copy()
    
    # Create summary
    summary = {
        'original_rows': original_count,
        'rows_after_filtering': len(df_filtered),
        'rows_dropped': original_count - len(df_filtered),
        'invalid_regions_found': invalid_regions,
        'drop_rate_percent': round((original_count - len(df_filtered)) / original_count * 100, 2),
        'valid_regions': list(allowed_regions)
    }
    
    return df_filtered, summary