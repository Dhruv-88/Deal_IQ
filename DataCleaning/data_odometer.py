"""
Odometer Data Validation Functions
=================================

This module contains functions for validating and cleaning odometer data
in car pricing datasets. It includes functions for:
- Removing extreme odometer values
- Validating odometer readings within reasonable ranges
- Using IQR method for outlier detection
- Comprehensive processing pipeline

Author: Data Validation Module
Date: 2025
"""

import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')


def remove_extreme_odometer(df, odometer_col='odometer'):
    """
    Remove extreme values from odometer column
    
    Parameters:
    df (pd.DataFrame): Input DataFrame
    odometer_col (str): Name of the odometer column
    
    Returns:
    pd.DataFrame: DataFrame with extreme odometer values removed
    dict: Summary of removal results
    """
    
    # Make a copy to avoid modifying original
    df_cleaned = df.copy()
    
    # Store original info
    original_count = len(df_cleaned)
    original_stats = df_cleaned[odometer_col].describe()
    
    # Remove null values first
    df_cleaned = df_cleaned[df_cleaned[odometer_col].notna()].copy()
    
    # Set reasonable odometer limits
    min_odometer = 0        # Can't have negative mileage
    max_odometer = 500000   # Maximum reasonable odometer reading (500k miles)
    
    # Find extreme values
    extreme_low = df_cleaned[df_cleaned[odometer_col] < min_odometer]
    extreme_high = df_cleaned[df_cleaned[odometer_col] > max_odometer]
    
    # Remove extreme values
    df_cleaned = df_cleaned[
        (df_cleaned[odometer_col] >= min_odometer) & 
        (df_cleaned[odometer_col] <= max_odometer)
    ].copy()
    
    # Calculate statistics after cleaning
    cleaned_stats = df_cleaned[odometer_col].describe()
    
    # Create summary
    removal_summary = {
        'original_rows': original_count,
        'cleaned_rows': len(df_cleaned),
        'removed_rows': original_count - len(df_cleaned),
        'removal_percentage': round((original_count - len(df_cleaned)) / original_count * 100, 2),
        'min_threshold': min_odometer,
        'max_threshold': max_odometer,
        'extreme_low_count': len(extreme_low),
        'extreme_high_count': len(extreme_high),
        'null_count': df[odometer_col].isnull().sum(),
        'original_stats': original_stats.to_dict(),
        'cleaned_stats': cleaned_stats.to_dict()
    }
    
    return df_cleaned, removal_summary


def validate_odometer_values(df, odometer_col='odometer', min_miles=0, max_miles=500000):
    """
    Validate odometer values are within reasonable range
    
    Parameters:
    df (pd.DataFrame): Input DataFrame
    odometer_col (str): Name of the odometer column
    min_miles (int): Minimum acceptable mileage
    max_miles (int): Maximum acceptable mileage
    
    Returns:
    pd.DataFrame: Validated DataFrame
    dict: Summary of validation results
    """
    
    # Store original info
    original_count = len(df)
    original_range = (df[odometer_col].min(), df[odometer_col].max())
    
    # Find invalid values
    null_mask = df[odometer_col].isnull()
    below_min_mask = df[odometer_col] < min_miles
    above_max_mask = df[odometer_col] > max_miles
    
    invalid_values = {
        'null_values': null_mask.sum(),
        'below_minimum': below_min_mask.sum(),
        'above_maximum': above_max_mask.sum()
    }
    
    # Filter to keep only valid values
    valid_df = df[
        (df[odometer_col].notna()) & 
        (df[odometer_col] >= min_miles) & 
        (df[odometer_col] <= max_miles)
    ].copy()
    
    # Calculate new range
    if len(valid_df) > 0:
        new_range = (valid_df[odometer_col].min(), valid_df[odometer_col].max())
    else:
        new_range = (None, None)
    
    # Create summary
    validation_summary = {
        'total_rows': original_count,
        'valid_rows': len(valid_df),
        'dropped_rows': original_count - len(valid_df),
        'drop_percentage': round((original_count - len(valid_df)) / original_count * 100, 2),
        'validation_range': (min_miles, max_miles),
        'original_range': original_range,
        'new_range': new_range,
        'invalid_values': invalid_values,
        'validation_passed': sum(invalid_values.values()) == 0
    }
    
    return valid_df, validation_summary


def remove_odometer_outliers_iqr(df, odometer_col='odometer', multiplier=1.5):
    """
    Remove odometer outliers using IQR method
    
    Parameters:
    df (pd.DataFrame): Input DataFrame
    odometer_col (str): Name of the odometer column
    multiplier (float): IQR multiplier for outlier detection
    
    Returns:
    pd.DataFrame: DataFrame with outliers removed
    dict: Summary of IQR outlier removal
    """
    
    # Remove null values first
    df_clean = df[df[odometer_col].notna()].copy()
    original_count = len(df_clean)
    
    # Calculate IQR
    Q1 = df_clean[odometer_col].quantile(0.25)
    Q3 = df_clean[odometer_col].quantile(0.75)
    IQR = Q3 - Q1
    
    # Define outlier bounds
    lower_bound = Q1 - multiplier * IQR
    upper_bound = Q3 + multiplier * IQR
    
    # Ensure lower bound is not negative for odometer
    lower_bound = max(0, lower_bound)
    
    # Count outliers
    outliers_below = (df_clean[odometer_col] < lower_bound).sum()
    outliers_above = (df_clean[odometer_col] > upper_bound).sum()
    
    # Remove outliers
    df_clean = df_clean[
        (df_clean[odometer_col] >= lower_bound) & 
        (df_clean[odometer_col] <= upper_bound)
    ].copy()
    
    # Create summary
    iqr_summary = {
        'original_rows': original_count,
        'cleaned_rows': len(df_clean),
        'removed_rows': original_count - len(df_clean),
        'removal_percentage': round((original_count - len(df_clean)) / original_count * 100, 2),
        'Q1': Q1,
        'Q3': Q3,
        'IQR': IQR,
        'lower_bound': lower_bound,
        'upper_bound': upper_bound,
        'outliers_below': outliers_below,
        'outliers_above': outliers_above,
        'multiplier': multiplier
    }
    
    return df_clean, iqr_summary


def process_odometer_column(df, odometer_col='odometer', min_miles=0, max_miles=500000):
    """
    Complete processing: remove extremes, then validate
    
    Parameters:
    df (pd.DataFrame): Input DataFrame
    odometer_col (str): Name of the odometer column
    min_miles (int): Minimum acceptable mileage
    max_miles (int): Maximum acceptable mileage
    
    Returns:
    pd.DataFrame: Processed DataFrame with clean odometer values
    dict: Summary of all operations
    """
    
    print("Step 1: Removing extreme odometer values...")
    df_cleaned, removal_summary = remove_extreme_odometer(df, odometer_col)
    
    print("\nStep 2: Validating odometer values...")
    df_final, validation_summary = validate_odometer_values(df_cleaned, odometer_col, min_miles, max_miles)
    
    # Combined summary
    combined_summary = {
        'original_rows': len(df),
        'final_rows': len(df_final),
        'total_removed': len(df) - len(df_final),
        'total_removal_percentage': round((len(df) - len(df_final)) / len(df) * 100, 2),
        'removal_summary': removal_summary,
        'validation_summary': validation_summary
    }
    
    print(f"\nFinal Results:")
    print(f"Original rows: {len(df):,}")
    print(f"Final rows: {len(df_final):,}")
    print(f"Total removed: {len(df) - len(df_final):,} ({combined_summary['total_removal_percentage']}%)")
    
    if len(df_final) > 0:
        print(f"Final odometer range: {df_final[odometer_col].min():,.0f} - {df_final[odometer_col].max():,.0f}")
        print(f"Final odometer mean: {df_final[odometer_col].mean():,.0f}")
    
    return df_final, combined_summary


def preview_odometer_cleaning(df, odometer_col='odometer'):
    """
    Preview what odometer values will be removed
    
    Parameters:
    df (pd.DataFrame): Input DataFrame
    odometer_col (str): Name of the odometer column
    
    Returns:
    None: Prints preview information
    """
    
    print("Current odometer statistics:")
    print(df[odometer_col].describe())
    
    print(f"\nNull values: {df[odometer_col].isnull().sum():,}")
    
    # Check for extreme values
    extreme_high = df[df[odometer_col] > 500000][odometer_col].count()
    extreme_low = df[df[odometer_col] < 0][odometer_col].count()
    
    print(f"Values > 500,000: {extreme_high:,}")
    print(f"Values < 0: {extreme_low:,}")
    
    # Show distribution
    print(f"\nPercentile distribution:")
    for p in [1, 5, 10, 25, 50, 75, 90, 95, 99]:
        value = df[odometer_col].quantile(p/100)
        print(f"  {p}th percentile: {value:,.0f}")
    
    # Show potential outliers using IQR
    if df[odometer_col].notna().sum() > 0:
        Q1 = df[odometer_col].quantile(0.25)
        Q3 = df[odometer_col].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = max(0, Q1 - 1.5 * IQR)
        upper_bound = Q3 + 1.5 * IQR
        
        print(f"\nIQR outlier bounds:")
        print(f"  Lower bound: {lower_bound:,.0f}")
        print(f"  Upper bound: {upper_bound:,.0f}")
        
        outliers_count = ((df[odometer_col] < lower_bound) | (df[odometer_col] > upper_bound)).sum()
        print(f"  Potential outliers: {outliers_count:,}")







def validate_odometer(df, odometer_column='odometer', min_miles=0, max_miles=500000):
    """
    Validate odometer column to keep only values within specified range
    
    Parameters:
    df (pd.DataFrame): Input DataFrame
    odometer_column (str): Name of odometer column
    min_miles (int): Minimum allowed miles (default: 0)
    max_miles (int): Maximum allowed miles (default: 500000)
    
    Returns:
    pd.DataFrame: DataFrame with only valid odometer values
    dict: Simple summary
    """
    
    original_rows = len(df)
    
    # Find invalid values (outside range)
    invalid_mask = (df[odometer_column] < min_miles) | (df[odometer_column] > max_miles)
    invalid_count = invalid_mask.sum()
    
    # Keep only valid values (within range or NaN)
    df_clean = df[((df[odometer_column] >= min_miles) & (df[odometer_column] <= max_miles)) | 
                  df[odometer_column].isna()]
    
    final_rows = len(df_clean)
    rows_dropped = original_rows - final_rows
    
    # Create summary
    summary = {
        'original_rows': original_rows,
        'final_rows': final_rows,
        'rows_dropped': rows_dropped,
        'invalid_values': invalid_count,
        'min_miles': min_miles,
        'max_miles': max_miles
    }
    
    return df_clean, summary
