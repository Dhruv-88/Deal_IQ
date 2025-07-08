def print_summary(summary, title="Summary"):
    """
    Print all key-value pairs from a summary dictionary
    
    Parameters:
    summary (dict): Summary dictionary from validation functions
    title (str): Title for the summary output
    """
    
    print(f"{title}")
    print("=" * len(title))
    
    for key, value in summary.items():
        # Format the key (replace underscores with spaces, capitalize)
        formatted_key = key.replace('_', ' ').title()
        
        # Format the value based on its type
        if isinstance(value, (list, tuple)):
            # For lists/tuples, join with commas or show range
            if len(value) == 2 and all(isinstance(x, (int, float)) for x in value):
                # Looks like a range (min, max)
                formatted_value = f"{value[0]} - {value[1]}"
            else:
                # Regular list
                formatted_value = str(value)
        elif isinstance(value, dict):
            # For dictionaries, show in a readable format
            formatted_value = str(value)
        else:
            # For numbers, add comma formatting if it's a large number
            if isinstance(value, int) and value >= 1000:
                formatted_value = f"{value:,}"
            else:
                formatted_value = str(value)
        
        print(f"{formatted_key}: {formatted_value}")


