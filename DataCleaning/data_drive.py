import pandas as pd 
def impute_drive_from_type(df, type_column='type', drive_column='drive'):
    """
    Impute missing drive values based on vehicle type
    
    Decision based on crosstab analysis:
    - SUV, offroad, pickup, truck, other, wagon -> 4wd
    - hatchback, minivan, sedan, van -> fwd  
    - bus, convertible, coupe -> rwd
    
    Parameters:
    df (pd.DataFrame): Input DataFrame
    type_column (str): Name of type column
    drive_column (str): Name of drive column
    
    Returns:
    pd.DataFrame: DataFrame with imputed drive values
    dict: Simple summary
    """
    
    df_clean = df.copy()
    
    # Count missing values before imputation
    missing_before = df_clean[drive_column].isnull().sum()
    
    # Define conditions and corresponding drive types
    condition_4wd = df_clean[type_column].isin(['SUV', 'offroad', 'pickup', 'truck', 'other', 'wagon'])
    condition_fwd = df_clean[type_column].isin(['hatchback', 'minivan', 'sedan', 'van'])
    condition_rwd = df_clean[type_column].isin(['bus', 'convertible', 'coupe'])
    
    # Apply imputation only to missing values
    df_clean.loc[(df_clean[drive_column].isnull()) & condition_4wd, drive_column] = '4wd'
    df_clean.loc[(df_clean[drive_column].isnull()) & condition_fwd, drive_column] = 'fwd'
    df_clean.loc[(df_clean[drive_column].isnull()) & condition_rwd, drive_column] = 'rwd'
    
    # Count missing values after imputation
    missing_after = df_clean[drive_column].isnull().sum()
    values_imputed = missing_before - missing_after
    
    # Create summary
    summary = {
        'total_rows': len(df_clean),
        'missing_before': missing_before,
        'missing_after': missing_after,
        'values_imputed': values_imputed
    }
    
    return df_clean, summary 
