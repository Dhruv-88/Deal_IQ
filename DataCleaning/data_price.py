def clean_price_data(df, price_col='price'):
   """
   Clean price data by removing invalid and extreme outliers
   
   Parameters:
   -----------
   df : pandas.DataFrame
       DataFrame containing the price column
   price_col : str
       Name of the price column (default: 'price')
   
   Returns:
   --------
   tuple: (pandas.DataFrame, dict)
       Cleaned DataFrame and summary dictionary
   """
   
   original_count = len(df)
   
   # Remove invalid prices
   df_clean = df[df[price_col] > 0].copy()  # Remove $0 prices
   zero_price_dropped = original_count - len(df_clean)
   
   # Set reasonable price limits for used cars
   min_price = 500      # Minimum reasonable car price
   max_price = 300000   # Maximum reasonable used car price
   
   # Apply price range filtering
   df_clean = df_clean[(df_clean[price_col] >= min_price) & 
                       (df_clean[price_col] <= max_price)]
   
   # Calculate statistics
   final_count = len(df_clean)
   total_dropped = original_count - final_count
   range_dropped = total_dropped - zero_price_dropped
   
   # Create summary
   summary = {
       'original_rows': original_count,
       'rows_after_cleaning': final_count,
       'total_rows_dropped': total_dropped,
       'zero_price_dropped': zero_price_dropped,
       'out_of_range_dropped': range_dropped,
       'drop_rate_percent': round(total_dropped / original_count * 100, 2),
       'price_range_applied': f"${min_price:,} - ${max_price:,}",
       'final_price_min': df_clean[price_col].min() if not df_clean.empty else 0,
       'final_price_max': df_clean[price_col].max() if not df_clean.empty else 0,
       'final_price_mean': round(df_clean[price_col].mean(), 2) if not df_clean.empty else 0,
       'final_price_std': round(df_clean[price_col].std(), 2) if not df_clean.empty else 0
   }
   
   return df_clean, summary

