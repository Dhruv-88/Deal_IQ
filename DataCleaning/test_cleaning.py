import pandas as pd
from model_cleaning import remove_numerical_models

# Create a test DataFrame with the problematic model
test_data = {
    'model': [
        "sequoia sr5 trd build out*1-owner*full new build*bilstein lift*35\"master craft mxt tires*new 18\"black rhino wheels*chrome delete pkg*custom painted 2-tone bumpers*8-pass seating*",
        "normal_model",
        "12345",
        "another_normal_model"
    ]
}

df = pd.DataFrame(test_data)

print("Original DataFrame:")
print(df)
print(f"Column names: {df.columns.tolist()}")
print(f"Model column dtype: {df['model'].dtype}")
print(f"Model column has NaN: {df['model'].isna().any()}")

# Test the length check manually
print("\nManual length check:")
for i, model in enumerate(df['model']):
    length = len(str(model))
    print(f"Row {i}: '{model}' -> Length: {length}, Should remove: {length > 40}")

# Apply the cleaning function
print("\nApplying cleaning function:")
cleaned_df = remove_numerical_models(df, model_column='model')

print("\nCleaned DataFrame:")
print(cleaned_df)

# Check if the long model was removed
long_model_removed = "sequoia sr5 trd build out*1-owner*full new build*bilstein lift*35\"master craft mxt tires*new 18\"black rhino wheels*chrome delete pkg*custom painted 2-tone bumpers*8-pass seating*" not in cleaned_df['model'].values
print(f"\nLong model removed: {long_model_removed}") 