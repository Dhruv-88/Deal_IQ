import pandas as pd
import re
import os
import importlib.util
from tqdm.auto import tqdm
import numpy as np

# Register tqdm for pandas
tqdm.pandas(desc="Cleaning Models")

def remove_numerical_models(df: pd.DataFrame, model_column: str = 'model') -> pd.DataFrame:
    """
    Remove rows where the model column contains only numerical values
    or where the model name length is more than 40 characters
    
    Parameters:
    df (pandas.DataFrame): DataFrame containing the model column
    model_column (str): Name of the model column (default: 'model')
    
    Returns:
    pd.DataFrame: DataFrame with problematic rows removed
    """
    df_clean = df.copy()
    
    if model_column not in df_clean.columns:
        print(f"Warning: Column '{model_column}' not found in DataFrame")
        return df_clean
    
    # Get the total number of rows before cleaning
    total_rows_before = len(df_clean)
    
    # Create masks to identify rows to remove (vectorized operations)
    # 1. Rows with only numerical values in model column
    numerical_mask = df_clean[model_column].astype(str).str.match(r'^\d+$', na=False)
    
    # 2. Rows with length more than 40 characters
    length_mask = df_clean[model_column].astype(str).str.len() > 40
    
    # Combine masks to get rows to remove
    rows_to_remove = numerical_mask | length_mask
    
    # Remove the problematic rows
    df_clean = df_clean[~rows_to_remove]
    
    # Get the total number of rows after cleaning
    total_rows_after = len(df_clean)
    rows_removed = total_rows_before - total_rows_after
    
    print(f"Debug: DataFrame shape after cleaning: {df_clean.shape}")
    
    # Log the results
    print(f"Step: remove_numerical_models")
    print(f"Rows removed due to numerical only: {numerical_mask.sum()}")
    print(f"Rows removed due to length > 40: {length_mask.sum()}")
    print(f"Total rows removed: {rows_removed}")
    
    return df_clean

def _load_models_by_manufacturer():
    """
    Load model lists from data/models.py and organize them by manufacturer.
    
    Returns:
        dict: A dictionary where keys are manufacturers and values are lists of models.
    """
    models_by_manufacturer = {}
    
    # Construct path to data/models.py relative to this script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    models_file_path = os.path.join(project_root, 'data', 'models.py')

    if not os.path.exists(models_file_path):
        print(f"Warning: Model file not found at {models_file_path}")
        return models_by_manufacturer

    spec = importlib.util.spec_from_file_location("models", models_file_path)
    if spec is None or spec.loader is None:
        print(f"Could not get spec from {models_file_path}")
        return models_by_manufacturer
    models_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(models_module)
    
    for name in dir(models_module):
        if name.endswith('_models'):
            manufacturer = name.replace('_models', '').replace('_', '-')
            models = getattr(models_module, name)
            if isinstance(models, list):
                if manufacturer in models_by_manufacturer:
                    models_by_manufacturer[manufacturer].extend(models)
                else:
                    models_by_manufacturer[manufacturer] = models
    
    # Deduplicate models in each list
    for manufacturer, models in models_by_manufacturer.items():
        models_by_manufacturer[manufacturer] = sorted(list(set(models)))
        
    return models_by_manufacturer

def _normalize_text(text):
    """
    Normalize text for better matching by:
    - Converting to lowercase
    - Removing extra spaces, hyphens, underscores
    - Keeping only alphanumeric characters and single spaces
    """
    if pd.isna(text):
        return ""
    
    # Convert to string and lowercase
    text = str(text).lower()
    
    # Replace multiple spaces, hyphens, underscores with single space
    text = re.sub(r'[\s\-_]+', ' ', text)
    
    # Remove special characters except letters, numbers, and spaces
    text = re.sub(r'[^a-z0-9\s]', '', text)
    
    # Remove extra spaces
    text = ' '.join(text.split())
    
    return text

def _create_model_variations(model):
    """
    Create variations of a model name to handle different formats:
    - Original: f150
    - With hyphen: f-150
    - With space: f 150
    - Without spaces/hyphens: f150
    """
    variations = set()
    
    # Original model
    variations.add(model)
    
    # Create normalized version (no spaces, hyphens, underscores)
    normalized = re.sub(r'[\s\-_]+', '', model)
    variations.add(normalized)
    
    # Add version with hyphens between letters and numbers
    hyphen_version = re.sub(r'([a-z])(\d)', r'\1-\2', model)
    variations.add(hyphen_version)
    
    # Add version with spaces between letters and numbers
    space_version = re.sub(r'([a-z])(\d)', r'\1 \2', model)
    variations.add(space_version)
    
    return variations

def filter_by_value_counts(df, column, min_count=1):
    """
    Filter DataFrame to keep only rows where the specified column value 
    appears at least min_count times.
    
    Parameters:
    df (pd.DataFrame): Input DataFrame
    column (str): Column name to check value counts for
    min_count (int): Minimum number of occurrences to keep (default: 1)
    
    Returns:
    pd.DataFrame: Filtered DataFrame
    """
    # Get values that appear at least min_count times
    frequent_values = df[column].value_counts()
    frequent_values = frequent_values[frequent_values >= min_count].index
    
    # Keep only rows with frequently occurring values
    filtered_df = df[df[column].isin(frequent_values)]
    
    return filtered_df

def clean_models_with_list_optimized(df: pd.DataFrame, model_column: str = 'model', manufacturer_column: str = 'manufacturer') -> pd.DataFrame:
    """
    Optimized version of clean_models_with_list that uses vectorized operations and caching
    for much faster performance.
    
    Args:
        df (pd.DataFrame): DataFrame with model and manufacturer columns.
        model_column (str, optional): Name of the model column. Defaults to 'model'.
        manufacturer_column (str, optional): Name of the manufacturer column. Defaults to 'manufacturer'.

    Returns:
        pd.DataFrame: DataFrame with cleaned model and manufacturer names.
    """
    print("Starting optimized model cleaning...")
    df_clean = df.copy()
    models_by_manufacturer = _load_models_by_manufacturer()

    if not models_by_manufacturer:
        print("Warning: No models loaded, returning original DataFrame.")
        return df_clean

    # Create comprehensive lookup dictionaries - this is done once
    print("Creating optimized lookup tables...")
    
    # Direct lookup for exact matches
    exact_match_dict = {}
    contains_match_dict = {}
    prefix_match_dict = {}
    
    for manufacturer, models in models_by_manufacturer.items():
        for model in models:
            # Create variations for this model
            variations = _create_model_variations(model)
            
            for variation in variations:
                # Normalize the variation
                normalized_variation = _normalize_text(variation)
                if normalized_variation:
                    # Store for exact matches
                    exact_match_dict[normalized_variation] = (model, manufacturer)
                    
                    # Store for contains matches (sorted by length desc for priority)
                    if normalized_variation not in contains_match_dict:
                        contains_match_dict[normalized_variation] = []
                    contains_match_dict[normalized_variation].append((model, manufacturer))
    
    # Sort contains matches by length (longer matches first)
    for key in contains_match_dict:
        contains_match_dict[key] = sorted(contains_match_dict[key], key=lambda x: len(x[0]), reverse=True)
    
    # Get unique model texts to process (avoid duplicate processing)
    print("Processing unique model values...")
    unique_models = df_clean[model_column].dropna().unique()
    
    # Create a mapping from original text to cleaned result
    text_to_result = {}
    
    # Process unique models with progress bar
    for original_text in tqdm(unique_models, desc="Processing unique models"):
        normalized_text = _normalize_text(original_text)
        matched_model, matched_manufacturer = None, None
        
        # Strategy 1: Direct exact match (fastest)
        if normalized_text in exact_match_dict:
            matched_model, matched_manufacturer = exact_match_dict[normalized_text]
        
        # Strategy 2: Contains match (if no exact match)
        elif normalized_text:
            # Check if any known model is contained in the text
            for model_variation in sorted(contains_match_dict.keys(), key=len, reverse=True):
                if re.search(r'\b' + re.escape(model_variation) + r'\b', normalized_text):
                    matched_model, matched_manufacturer = contains_match_dict[model_variation][0]
                    break
            
            # Strategy 3: Manufacturer prefix match (if no contains match)
            if not matched_model:
                words = normalized_text.split()
                if len(words) >= 2:
                    remaining_text = ' '.join(words[1:])
                    if remaining_text in exact_match_dict:
                        matched_model, matched_manufacturer = exact_match_dict[remaining_text]
            
            # Strategy 4: Starts with match (if no prefix match)
            if not matched_model:
                for model_variation in sorted(contains_match_dict.keys(), key=len, reverse=True):
                    if normalized_text.startswith(model_variation + ' '):
                        matched_model, matched_manufacturer = contains_match_dict[model_variation][0]
                        break
        
        # Store the result
        text_to_result[original_text] = (matched_model, matched_manufacturer)
    
    # Apply the mapping to the DataFrame (vectorized operation)
    print("Applying results to DataFrame...")
    
    # Create a function to map the results
    def map_result(text):
        if pd.isna(text):
            return None, None
        return text_to_result.get(text, (None, None))
    
    # Apply the mapping
    results = df_clean[model_column].map(map_result)
    
    # Extract matched models and manufacturers
    matched_models = [result[0] if result else None for result in results]
    matched_manufacturers = [result[1] if result else None for result in results]
    
    # Create boolean mask for rows with valid matches
    mask = pd.Series(matched_models, index=df_clean.index).notna()
    
    # Track original values for comparison
    original_models = df_clean[model_column].copy()
    original_manufacturers = df_clean[manufacturer_column].copy()
    
    if mask.any():
        print(f"Found {mask.sum()} models to update.")
        
        # Update using vectorized operations - fix the indexing issue
        matched_models_series = pd.Series(matched_models, index=df_clean.index)
        matched_manufacturers_series = pd.Series(matched_manufacturers, index=df_clean.index)
        
        df_clean.loc[mask, model_column] = matched_models_series[mask]
        df_clean.loc[mask, manufacturer_column] = matched_manufacturers_series[mask]
    else:
        print("No models were updated.")

    # Calculate changes
    changed_rows = (original_models != df_clean[model_column]) | \
                   (original_manufacturers != df_clean[manufacturer_column])
    
    print(f"Step: clean_models_with_list_optimized")
    print(f"Total rows modified: {changed_rows.sum()}")
    
    # Show some examples of changes
    if changed_rows.any():
        print("\nSample transformations:")
        sample_changes = df_clean[changed_rows].head(10)
        for idx in sample_changes.index:
            old_model = original_models.loc[idx]
            new_model = df_clean.loc[idx, model_column]
            old_mfg = original_manufacturers.loc[idx]
            new_mfg = df_clean.loc[idx, manufacturer_column]
            print(f"  '{old_model}' ({old_mfg}) -> '{new_model}' ({new_mfg})")

    return df_clean

# Keep the original function for backward compatibility
def clean_models_with_list(df: pd.DataFrame, model_column: str = 'model', manufacturer_column: str = 'manufacturer') -> pd.DataFrame:
    """
    Wrapper function that calls the optimized version
    """
    return clean_models_with_list_optimized(df, model_column, manufacturer_column)