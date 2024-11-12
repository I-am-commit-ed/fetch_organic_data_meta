import json
import pandas as pd
from pathlib import Path
import os

def flatten_json(nested_json: dict, parent_key: str = '', sep: str = '_') -> dict:
    """
    Flatten nested JSON structure to a single level dictionary
    """
    items = []
    for k, v in nested_json.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_json(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            if v and isinstance(v[0], dict):  # If list contains dictionaries
                for i, item in enumerate(v):
                    items.extend(flatten_json(item, f"{new_key}{sep}{i}", sep=sep).items())
            else:
                items.append((new_key, str(v)))
        else:
            items.append((new_key, v))
    return dict(items)

def convert_facebook_page_data(json_path: str, output_dir: Path) -> None:
    """
    Convert Facebook page data JSON to CSV
    """
    print(f"\nProcessing Facebook page data from: {json_path}")
    
    # Read JSON file
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Flatten the JSON structure
    flattened_data = flatten_json(data)
    
    # Convert to DataFrame
    df = pd.DataFrame([flattened_data])
    
    # Save to CSV
    output_path = output_dir / 'facebook_page_data_processed.csv'
    df.to_csv(output_path, index=False, encoding='utf-8')
    print(f"Facebook data saved to: {output_path}")
    print(f"Columns in Facebook data: {', '.join(df.columns)}")

def convert_instagram_insights(json_path: str, output_dir: Path) -> None:
    """
    Convert Instagram insights JSON to CSV
    """
    print(f"\nProcessing Instagram insights from: {json_path}")
    
    # Read JSON file
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    if 'data' not in data:
        print("No 'data' field found in Instagram JSON")
        return
    
    # Process each metric
    all_metrics = []
    for metric in data['data']:
        metric_name = metric['name']
        for value in metric['values']:
            row = {
                'metric': metric_name,
                'end_time': value['end_time'],
                'value': value['value']
            }
            all_metrics.append(row)
    
    # Convert to DataFrame
    df = pd.DataFrame(all_metrics)
    
    # Save to CSV
    output_path = output_dir / 'instagram_insights_processed.csv'
    df.to_csv(output_path, index=False, encoding='utf-8')
    print(f"Instagram data saved to: {output_path}")
    print(f"Columns in Instagram data: {', '.join(df.columns)}")
    
    # Create pivot table for better visualization
    pivot_df = df.pivot(index='end_time', columns='metric', values='value').reset_index()
    pivot_output_path = output_dir / 'instagram_insights_pivot_processed.csv'
    pivot_df.to_csv(pivot_output_path, index=False, encoding='utf-8')
    print(f"Instagram pivot data saved to: {pivot_output_path}")

def main():
    # Define paths
    facebook_json = "/Users/manuel/Documents/GitHub/JeanPierreWeill/Data_Extract/data/20241111_230216/facebook_page_data.json"
    instagram_json = "/Users/manuel/Documents/GitHub/JeanPierreWeill/Data_Extract/data/20241111_230223/instagram_insights.json"
    
    # Create output directory
    output_dir = Path("/Users/manuel/Documents/GitHub/JeanPierreWeill/Data_Extract/data/processed")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        # Process Facebook data
        if os.path.exists(facebook_json):
            convert_facebook_page_data(facebook_json, output_dir)
        else:
            print(f"Facebook JSON file not found: {facebook_json}")
        
        # Process Instagram data
        if os.path.exists(instagram_json):
            convert_instagram_insights(instagram_json, output_dir)
        else:
            print(f"Instagram JSON file not found: {instagram_json}")
            
    except Exception as e:
        print(f"An error occurred: {str(e)}")
    
    print("\nProcessing complete! Check the 'processed' directory for the CSV files.")

if __name__ == "__main__":
    main()
