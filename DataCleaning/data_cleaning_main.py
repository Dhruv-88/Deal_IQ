import sys
import os
import pandas as pd
import numpy as np

# Add utility path and import print_summary
sys.path.append(os.path.join(os.getcwd(), '../'))
from utility.print_summary import print_summary

def main_data_cleaning_pipeline():
    """
    Main data cleaning pipeline that processes car dataset from GCS.
    Follows the exact sequence from the Jupyter notebook.
    """
    
    # Step 1: Get environment variables for GCP configuration
    GCP_PROJECT_ID = os.getenv('PROJECT_ID')
    GCS_BUCKET_NAME = os.getenv('BUCKET_NAME')
    
    # Step 2: Initialize GCS operations and read raw data
    from cloud.gcs_storage_operations import GCSDataOperations
    gcs = GCSDataOperations(GCP_PROJECT_ID)
    df = gcs.read_parquet(GCS_BUCKET_NAME, "raw_data.parquet")
    
    # Step 3: Standardization and extracting info from model and description
    from DataCleaning.data_model import extract_car_data
    df = extract_car_data(df)
    
    # Step 4: Drop unnecessary columns
    from DataCleaning.data_cleaning import drop_unnecessary_columns, drop_rows_with_few_missing_values
    df, summary = drop_unnecessary_columns(df)
    print_summary(summary)
    
    # Step 5: Drop rows due to high NAs
    df, summary = drop_rows_with_few_missing_values(df)
    print_summary(summary)
    
    # Step 6: Fill missing values in title_status with 'missing'
    from DataCleaning.data_title_status import fill_missing_values
    df, summary = fill_missing_values(df)
    print_summary(summary)
    
    # Step 7: Fill missing values in transmission
    from DataCleaning.data_transmission import fill_missing_values_transmission, convert_transmission_to_automatic
    df, summary = fill_missing_values_transmission(df)
    print_summary(summary)
    
    # Step 8: Convert transmission to automatic format
    df, summary = convert_transmission_to_automatic(df)
    print_summary(summary)
    
    # Step 9: Drive column standardization
    from DataCleaning.data_drive import clean_drive_column
    df, summary = clean_drive_column(df, 'drive')
    
    # Step 10: Fill missing drive values from reference file
    from DataCleaning.data_drive import fill_missing_drive_from_reference
    df, summary = fill_missing_drive_from_reference(df,
                                                   reference_file='/Users/dhruvpatel/Desktop/projects/DealPredection/data/models_with_drive.csv')
    print_summary(summary)
    
    # Step 11: Remove numerical models (Stage 1 of model cleaning)
    from DataCleaning.data_model import remove_numerical_models
    df, summary = remove_numerical_models(df)
    print_summary(summary)
    
    # Step 12: Clean models with optimized list (Stage 2 of model cleaning)
    from DataCleaning.data_model import clean_models_with_list_optimized
    df, summary = clean_models_with_list_optimized(df)
    print_summary(summary)
    
    # Step 13: Filter models by value counts (minimum 10 occurrences)
    from DataCleaning.data_model import filter_by_value_counts
    df = filter_by_value_counts(df, 'model', min_count=10)
    
    # Step 14: Drop NA values in drive type
    from DataCleaning.data_type import drop_na_drive_type
    df, summary = drop_na_drive_type(df)
    print_summary(summary)
    
    # Step 15: Replace and standardize type values (mini van -> minivan)
    from DataCleaning.data_type import replace_values
    df, summary = replace_values(df, 'type', {'mini van': 'minivan', 'mini-van': 'minivan'})
    print_summary(summary)
    
    # Step 16: Fill type from model information
    from DataCleaning.data_type import fill_type_from_model
    df, summary = fill_type_from_model(df)
    print_summary(summary)
    
    # Step 17: Drop remaining NA values in type
    from DataCleaning.data_type import drop_na_type
    df_clean, summary = drop_na_type(df)
    print_summary(summary)
    df = df_clean  # Update df with cleaned version
    
    # Step 18: Impute drive values based on type cross-tabulation
    from DataCleaning.data_drive import impute_drive_from_type
    df, summary = impute_drive_from_type(df)
    print_summary(summary)
    
    # Step 19: Standardize manufacturer names
    from DataCleaning.data_manufacturers import standardize_manufacturer
    df, summary = standardize_manufacturer(df)
    print_summary(summary)
    
    # Step 20: Fill paint color null values
    from DataCleaning.data_paint_color import fill_paint_color_nulls
    df, summary = fill_paint_color_nulls(df)
    print_summary(summary)
    
    # Step 21: Add census divisions and regions
    from DataCleaning.data_census_region import add_census_divisions_abbrev, validate_regions
    df, summary = add_census_divisions_abbrev(df)
    print_summary(summary)
    
    # Step 22: Clean price data
    from DataCleaning.data_price import clean_price_data
    df, summary = clean_price_data(df, 'price')
    print_summary(summary)
    
    # Step 23: Convert fuel values to gas format
    from DataCleaning.data_fuel import convert_fuel_to_gas
    df, summary = convert_fuel_to_gas(df)
    print_summary(summary)
    
    # Step 24: Process odometer column
    from DataCleaning.data_odometer import process_odometer_column
    df, summary = process_odometer_column(df, 'odometer')
    print_summary(summary)
    
    # VALIDATION STEPS - Validate all columns
    
    # Step 25: Validate census regions
    df, summary = validate_regions(df)
    print_summary(summary)
    
    # Step 26: Validate years (minimum year 1990)
    from DataCleaning.data_year import validate_years
    df, summary = validate_years(df, year_column='year', min_year=1990)
    print_summary(summary)
    
    # Step 27: Validate transmission values
    from DataCleaning.data_transmission import validate_transmission_values
    df, validation_summary = validate_transmission_values(df)
    print_summary(validation_summary)
    
    # Step 28: Validate fuel values
    from DataCleaning.data_fuel import validate_fuel_values
    df, summary = validate_fuel_values(df)
    print_summary(summary)
    
    # Step 29: Validate title status values
    from DataCleaning.data_title_status import validate_title_status_values
    df, summary = validate_title_status_values(df)
    print_summary(summary)
    
    # Step 30: Validate type values
    from DataCleaning.data_type import validate_type_values
    df, summary = validate_type_values(df, standardize_case=True)
    print_summary(summary)
    
    # Step 31: Validate manufacturers
    from DataCleaning.data_manufacturers import validate_manufacturers
    df, summary = validate_manufacturers(df)
    print_summary(summary)
    
    # Step 32: Validate paint color
    from DataCleaning.data_paint_color import validate_paint_color
    df, summary = validate_paint_color(df)
    print_summary(summary)
    
    # Step 33: Validate state values
    from DataCleaning.data_state import validate_state
    df, summary = validate_state(df)
    print_summary(summary)
    
    # Step 34: Validate model frequency (minimum 10 occurrences)
    from DataCleaning.data_model import validate_model_frequency
    df_clean, summary = validate_model_frequency(df, min_count=10)
    print_summary(summary)
    df = df_clean  # Update df with cleaned version
    
    # Step 35: Validate drive values
    from DataCleaning.data_drive import validate_drive_values
    df, summary = validate_drive_values(df)
    print_summary(summary)
    
    # Step 36: Validate odometer values (0 to 500,000 miles)
    from DataCleaning.data_odometer import validate_odometer
    df, summary = validate_odometer(df, min_miles=0, max_miles=500000)
    print_summary(summary)
    
    # Step 37: Validate USA coordinates (latitude and longitude)
    from DataCleaning.data_lat_long import validate_usa_coordinates
    df, summary = validate_usa_coordinates(df)
    print_summary(summary)
    
    # Final step: Print remaining null values summary
    print("\nFinal null values summary:")
    print(df.isna().sum()) 
    

    # writing clean file to the cloud storage
    gcs.upload_parquet(bucket_name=GCS_BUCKET_NAME,blob_name='clean_data.parquet',df=df)
    
    return df

if __name__ == "__main__":
    # Execute the main data cleaning pipeline
    cleaned_df = main_data_cleaning_pipeline()
    print(f"\nData cleaning completed. Final dataset shape: {cleaned_df.shape}")