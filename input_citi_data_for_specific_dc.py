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

def csv_to_json(csv_file):
    try:
        # Try reading with UTF-8 encoding first
        df = pd.read_csv(csv_file, encoding='utf-8')
    except UnicodeDecodeError:
        try:
            # If UTF-8 fails, try with Latin-1 encoding
            df = pd.read_csv(csv_file, encoding='latin1')
        except Exception as e:
            # Fall back to a more lenient encoding
            df = pd.read_csv(csv_file, encoding='cp1252')
    
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
            if pd.notna(row['CPU Spec']) and str(row['CPU Spec']).strip().upper() == 'NO CPU':
                continue
                
            is_storage = row.get('storage_server', 0) == 1
            name_prefix = 'S' if is_storage else 'H'
            counter = storage_counter if is_storage else host_counter
            
            # Clean power values and ensure max_power >= idle_power
            idle_power = clean_number(row['Idle Power Consumption'])
            max_power = clean_number(row['Max Power Consumption'])
            max_power = max(max_power, idle_power)
            
            host = {
                "name": f"{name_prefix}{str(counter).zfill(2)}",
                "count": int(row['count']) if pd.notna(row['count']) else 1,
                "cpu": {
                    "coreCount": int(row['core count']) if pd.notna(row['core count']) else 0,
                    "coreSpeed": int(row['core speed']) if pd.notna(row['core speed']) else 0
                },
                "memory": {
                    "memorySize": convert_memory_to_bytes(row['Memory Spec'])
                },
                "powerModel": {
                    "modelType": "square",
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
        # Use the absolute path to the CSV file
        csv_file = "/home/joshua/dev/CitiReserach/python_files/citi_data/citi_rutherford_hardware_sheet.csv"
        output_file = "/home/joshua/dev/CitiReserach/python_files/toplogies/datacenter_config.json"
        
        if not os.path.exists(csv_file):
            print(f"Error: CSV file not found at {csv_file}")
            exit(1)
            
        cluster_data = csv_to_json(csv_file)
        save_json(cluster_data, output_file)
        print(f"JSON file has been created at: {output_file}")
    except Exception as e:
        print(f"An error occurred: {str(e)}")