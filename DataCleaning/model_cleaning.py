import pandas as pd
import re
import os
import importlib.util
from tqdm.auto import tqdm

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
    
    # Create masks to identify rows to remove
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
    
def clean_models_with_list(df: pd.DataFrame, model_column: str = 'model', manufacturer_column: str = 'manufacturer') -> pd.DataFrame:
    """
    Cleans model and manufacturer names based on a predefined list from data/models.py.
    Handles edge cases like hyphen/space variations and manufacturer prefixes.

    Args:
        df (pd.DataFrame): DataFrame with model and manufacturer columns.
        model_column (str, optional): Name of the model column. Defaults to 'model'.
        manufacturer_column (str, optional): Name of the manufacturer column. Defaults to 'manufacturer'.

    Returns:
        pd.DataFrame: DataFrame with cleaned model and manufacturer names.
    """
    df_clean = df.copy()
    models_by_manufacturer = _load_models_by_manufacturer()

    if not models_by_manufacturer:
        print("Warning: No models loaded, returning original DataFrame.")
        return df_clean

    # Create comprehensive lookup dictionaries
    model_to_manufacturer = {}
    normalized_to_original = {}
    
    print("Creating model variations and lookup tables...")
    
    for manufacturer, models in models_by_manufacturer.items():
        for model in models:
            # Store original mapping
            model_to_manufacturer[model] = manufacturer
            
            # Create variations for this model
            variations = _create_model_variations(model)
            
            for variation in variations:
                # Normalize the variation
                normalized_variation = _normalize_text(variation)
                if normalized_variation:
                    normalized_to_original[normalized_variation] = model
                    model_to_manufacturer[model] = manufacturer

    print("Starting model and manufacturer cleaning...")
    
    # Track changes
    original_models = df_clean[[model_column, manufacturer_column]].copy()
    
    def find_best_match(text):
        """
        Find the best model match for a given text using multiple strategies.
        """
        if pd.isna(text):
            return None, None
            
        original_text = str(text).lower()
        normalized_text = _normalize_text(text)
        
        # Strategy 1: Direct normalized match
        if normalized_text in normalized_to_original:
            matched_model = normalized_to_original[normalized_text]
            return matched_model, model_to_manufacturer[matched_model]
        
        # Strategy 2: Check if any known model is contained in the text
        # Sort models by length (desc) to match more specific models first
        sorted_models = sorted(normalized_to_original.keys(), key=len, reverse=True)
        
        for model_variation in sorted_models:
            # Check if model is a word boundary match in the text
            if re.search(r'\b' + re.escape(model_variation) + r'\b', normalized_text):
                matched_model = normalized_to_original[model_variation]
                return matched_model, model_to_manufacturer[matched_model]
        
        # Strategy 3: Check for manufacturer prefix matches
        # e.g., "toyota sequoia" -> "sequoia"
        words = normalized_text.split()
        if len(words) >= 2:
            # Check if first word is a manufacturer and second+ words form a model
            first_word = words[0]
            remaining_text = ' '.join(words[1:])
            
            # Check if remaining text matches any model
            if remaining_text in normalized_to_original:
                matched_model = normalized_to_original[remaining_text]
                matched_manufacturer = model_to_manufacturer[matched_model]
                
                # Verify the manufacturer matches or is similar
                if (first_word == matched_manufacturer or 
                    first_word.replace('-', '') == matched_manufacturer.replace('-', '') or
                    first_word in matched_manufacturer or 
                    matched_manufacturer in first_word):
                    return matched_model, matched_manufacturer
        
        # Strategy 4: Partial matching for models with additional text
        # e.g., "escape r4529" -> "escape"
        for model_variation in sorted_models:
            if normalized_text.startswith(model_variation + ' '):
                matched_model = normalized_to_original[model_variation]
                return matched_model, model_to_manufacturer[matched_model]
        
        return None, None
    
    # Apply the matching function
    print("Applying model matching...")
    results = df_clean[model_column].progress_apply(find_best_match)
    
    # Extract matched models and manufacturers
    matched_models = [result[0] for result in results]
    matched_manufacturers = [result[1] for result in results]
    
    # Create boolean mask for rows with valid matches
    mask = pd.Series(matched_models).notna()
    
    if mask.any():
        print(f"Found {mask.sum()} models to update.")
        
        # Get the indices where we have valid matches
        valid_indices = df_clean.index[mask]
        
        # Extract only the non-null values
        valid_models = [model for model in matched_models if model is not None]
        valid_manufacturers = [mfg for mfg in matched_manufacturers if mfg is not None]
        
        # Update the DataFrame
        df_clean.loc[valid_indices, model_column] = valid_models
        df_clean.loc[valid_indices, manufacturer_column] = valid_manufacturers
    else:
        print("No models were updated.")

    # Calculate changes
    changed_rows = (original_models[model_column] != df_clean[model_column]) | \
                   (original_models[manufacturer_column] != df_clean[manufacturer_column])
    
    print(f"Step: clean_models_with_list")
    print(f"Total rows modified: {changed_rows.sum()}")
    
    # Show some examples of changes
    if changed_rows.any():
        print("\nSample transformations:")
        sample_changes = df_clean[changed_rows].head(10)
        for idx in sample_changes.index:
            old_model = original_models.loc[idx, model_column]
            new_model = df_clean.loc[idx, model_column]
            old_mfg = original_models.loc[idx, manufacturer_column]
            new_mfg = df_clean.loc[idx, manufacturer_column]
            print(f"  '{old_model}' ({old_mfg}) -> '{new_model}' ({new_mfg})")

    return df_clean