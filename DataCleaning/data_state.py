import pandas as pd


def validate_state(df, state_column='state'):
    """
    Validate state column to keep only allowed values, drop others
    
    Allowed values: ca, fl, tx, ny, oh, mi, pa, or, wa, nc, wi, tn, co, il, id, va, nj, 
                   az, ma, mn, ia, ga, ks, mt, in, ok, sc, ct, md, al, ky, mo, ak, nm, 
                   nv, ar, dc, nh, la, me, vt, hi, ri, ut, sd, wv, ms, ne, de, wy, nd
    
    Parameters:
    df (pd.DataFrame): Input DataFrame
    state_column (str): Name of state column
    
    Returns:
    pd.DataFrame: DataFrame with only valid state values
    dict: Simple summary
    """
    
    # Valid state values
    valid_states = [
        'ca', 'fl', 'tx', 'ny', 'oh', 'mi', 'pa', 'or', 'wa', 'nc', 'wi', 'tn', 'co', 'il', 
        'id', 'va', 'nj', 'az', 'ma', 'mn', 'ia', 'ga', 'ks', 'mt', 'in', 'ok', 'sc', 'ct', 
        'md', 'al', 'ky', 'mo', 'ak', 'nm', 'nv', 'ar', 'dc', 'nh', 'la', 'me', 'vt', 'hi', 
        'ri', 'ut', 'sd', 'wv', 'ms', 'ne', 'de', 'wy', 'nd'
    ]
    
    original_rows = len(df)
    
    # Find invalid values
    invalid_mask = ~df[state_column].isin(valid_states)
    invalid_count = invalid_mask.sum()
    
    # Keep only valid values (including NaN)
    df_clean = df[df[state_column].isin(valid_states) | df[state_column].isna()]
    
    final_rows = len(df_clean)
    rows_dropped = original_rows - final_rows
    
    # Create summary
    summary = {
        'original_rows': original_rows,
        'final_rows': final_rows,
        'rows_dropped': rows_dropped,
        'invalid_values': invalid_count,
        'valid_states': valid_states
    }
    
    return df_clean, summary