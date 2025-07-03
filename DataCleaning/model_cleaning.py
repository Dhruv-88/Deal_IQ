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

def clean_models_with_list(df: pd.DataFrame, model_column: str = 'model', manufacturer_column: str = 'manufacturer') -> pd.DataFrame:
    """
    Cleans model and manufacturer names based on a predefined list from data/models.py.

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

    model_to_manufacturer = {
        model: mfg
        for mfg, models in models_by_manufacturer.items()
        for model in models
    }
    
    # Sort models by length (desc) to match more specific models first 
    # e.g., 'jetta-gli' before 'jetta'
    sorted_models = sorted(model_to_manufacturer.keys(), key=len, reverse=True)

    print("Starting model and manufacturer cleaning...")
    
    # Vectorized approach for performance
    original_models = df_clean[[model_column, manufacturer_column]].copy()
    
    # Standardize the model column for matching
    standardized_col = df_clean[model_column].astype(str).str.lower().str.replace(r'[\s_]+', '-', regex=True)
    
    # Create one large regex for fast matching
    master_regex = r'\b(' + '|'.join(re.escape(m) for m in sorted_models) + r')\b'
    
    # Extract matched models using the master regex
    print("Extracting valid models...")
    
    def get_match(text):
        match = re.search(master_regex, text)
        return match.group(1) if match else None

    matched_models = standardized_col.progress_apply(get_match)

    # Update rows where a valid model was found
    mask = matched_models.notna()
    
    if mask.any():
        print(f"Found {mask.sum()} models to update.")
        df_clean.loc[mask, model_column] = matched_models[mask]
        df_clean.loc[mask, manufacturer_column] = df_clean.loc[mask, model_column].map(model_to_manufacturer)
    else:
        print("No models were updated.")

    # Calculate changes
    changed_rows = (original_models[model_column] != df_clean[model_column]) | \
                   (original_models[manufacturer_column] != df_clean[manufacturer_column])
    
    print(f"Step: clean_models_with_list")
    print(f"Total rows modified: {changed_rows.sum()}")

    return df_clean


