import pandas as pd
import json
import os
import re

def convert_memory_to_bytes(memory_str):
    if pd.isna(memory_str):
        return 0
    
    memory_str = str(memory_str).upper()
    # Extract numbers and unit using regex
    match = re.search(r'(\d+(?:\.\d+)?)\s*([GT]B)', memory_str)
    if not match:
        return 0
        
    number = float(match.group(1))
    unit = match.group(2)
    
    if unit == 'GB':
        return int(number * 1024 * 1024 * 1024)  # Convert GB to bytes
    if unit == 'TB':
        return int(number * 1024 * 1024 * 1024 * 1024)  # Convert TB to bytes
    return 0

def excel_to_json(excel_file):
    # Read Excel file
    df = pd.read_excel(excel_file)
    
    # Initialize the cluster structure
    cluster = {
        "clusters": [
            {
                "name": "NJ-RUTHERFORD DATA CENTER",
                "hosts": []
            }
        ]
    }
    
    host_counter = 1
    
    # Process each row in the Excel file
    for index, row in df.iterrows():
        # Skip if CPU Spec is 'NO CPU'
        if pd.notna(row['CPU Spec']) and str(row['CPU Spec']).strip().upper() == 'NO CPU':
            continue
            
        # Create host object following schema requirements
        host = {
            "name": f"H{str(host_counter).zfill(2)}",
            "count": int(row['count']),
            "cpu": {
                "coreCount": int(row['core count']) if pd.notna(row['core count']) else 0,
                "coreSpeed": int(row['core speed']) if pd.notna(row['core speed']) else 0
            },
            "memory": {
                "memorySize": convert_memory_to_bytes(row['Memory Spec'])
            },
            "powerModel": {
                "modelType": "square",
                "maxPower": int(row['Power Consumption (Avg)']) if pd.notna(row['Power Consumption (Avg)']) else 0,
                "idlePower": int(row['Power Consumption (Avg)']) if pd.notna(row['Power Consumption (Avg)']) else 0
            }
        }
        
        cluster['clusters'][0]['hosts'].append(host)
        host_counter += 1
    
    return cluster

def save_json(data, output_file):
    with open(output_file, 'w') as f:
        json.dump(data, f, indent=4)

if __name__ == "__main__":
    excel_file = os.path.join("citi_data","citi_rutherford_hardware_sheet.xlsx")
    output_file = os.path.join("output_folder","datacenter_config.json")
    
    cluster_data = excel_to_json(excel_file)
    save_json(cluster_data, output_file)
    print(f"JSON file has been created at: {output_file}")