import pandas as pd
import json
import os
import re

def convert_memory_to_bytes(memory_str):
    if pd.isna(memory_str):
        return 0
    
    memory_str = str(memory_str).upper()
    match = re.search(r'(\d+(?:\.\d+)?)\s*([GT]B)', memory_str)
    if not match:
        return 0
        
    number = float(match.group(1))
    unit = match.group(2)
    
    if unit == 'GB':
        return int(number * 1024 * 1024 * 1024)
    if unit == 'TB':
        return int(number * 1024 * 1024 * 1024 * 1024)
    return 0

def clean_number(value):
    if pd.isna(value):
        return 0
    # Convert to string, replace comma with period, and convert to float
    str_value = str(value).replace(',', '.')
    try:
        return int(float(str_value))
    except (ValueError, TypeError):
        return 0

def csv_to_json(csv_file, sheet_name="Citi Hardware"):
    try:
        # Try reading with openpyxl engine
        df = pd.read_excel(csv_file, sheet_name=sheet_name, engine='openpyxl')
    except Exception as e:
        print(f"Error reading Excel file: {str(e)}")
        return None
    
    cluster = {
        "clusters": [
            {
                "name": "NJ-RUTHERFORD DATA CENTER",
                "hosts": []
            }
        ]
    }
    
    host_counter = 1
    storage_counter = 1
    
    # Print column names to help with debugging
    print("Available columns:", df.columns.tolist())
    
    for index, row in df.iterrows():
        try:
            # Skip rows without CPU info
            if pd.isna(row['CPU name']):
                continue
                
            # Determine if it's a storage server (this logic may need adjustment based on your data)
            is_storage = False
            if 'storage_server' in df.columns:
                is_storage = row.get('storage_server', 0) == 1
            
            name_prefix = 'S' if is_storage else 'H'
            counter = storage_counter if is_storage else host_counter
            
            # Get core count from the new columns
            core_count = clean_number(row['CPU Total Cores'])
            
            # Get core speed from base frequency (convert GHz to MHz)
            core_speed = int(float(row['CPU Base Frequency (in GHz)']) * 1000) if pd.notna(row['CPU Base Frequency (in GHz)']) else 0
            
            # Set idle power to 0 and get max power from CPU TDP L1
            idle_power = 75  # Fixed to 0 as requested
            max_power = clean_number(row.get('CPU TDP L1 (in Watts)', 100))  # Default to 100 if column not found
            
            # For memory, adapt as needed
            memory_size = convert_memory_to_bytes(row.get('Memory Spec', '8GB'))  # Default value
            
            host = {
                "name": f"{name_prefix}{str(counter).zfill(2)}",
                "count": int(row.get('count', 1)) if pd.notna(row.get('count')) else 1,
                "cpu": {
                    "coreCount": core_count,
                    "coreSpeed": core_speed
                },
                "memory": {
                    "memorySize": memory_size
                },
                "powerModel": {
                    "modelType": "linear",
                    "maxPower": max_power,
                    "idlePower": idle_power
                }
            }
            
            cluster['clusters'][0]['hosts'].append(host)
            if is_storage:
                storage_counter += 1
            else:
                host_counter += 1
        except Exception as e:
            print(f"Error processing row {index}: {str(e)}")
            continue
    
    return cluster

def save_json(data, output_file):
    # Ensure the directory exists
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, 'w') as f:
        json.dump(data, f, indent=4)

if __name__ == "__main__":
    try:
        # Use os.path.join for cross-platform compatibility
        base_dir = os.path.join("C:", os.sep, "Users", "Joshua", "Dev", "citi_reserach")
        csv_file = os.path.join(base_dir, "citi_data", "CSV_DT_Data.xlsx")
        output_file = os.path.join(base_dir, "toplogies", "datacenter_config.json")
        
        if not os.path.exists(csv_file):
            print(f"Error: Excel file not found at {csv_file}")
            exit(1)
            
        cluster_data = csv_to_json(csv_file)
        if cluster_data:
            save_json(cluster_data, output_file)
            print(f"JSON file has been created at: {output_file}")
        else:
            print("Failed to create cluster data.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")