import pandas as pd
import numpy as np
from datetime import datetime
import os

def convert_csv_to_parquet(csv_file_path, output_parquet_path):
    # Read the CSV file with low_memory=False to handle mixed types
    df = pd.read_csv(csv_file_path, low_memory=False)
    
    # Filter for RUTH datacenter
    df = df[df['vCenter DC'].str.upper() == 'RUTH']  # Change this when necessary!!!!!!!!
    
    # Create unique IDs for each VM
    df = df.reset_index(drop=True)
    vm_ids = df.index + 1  # Start IDs from 1
    
    # Create the new dataframe with required format
    task_df = pd.DataFrame({
        'id': vm_ids,
        'submission_time': '10/24/2024 2:30',  # Fixed timestamp as specified, should change to make flexible
        'duration': 2592252000,  # Fixed value as specified
        'cpu_count': df['CPU'],
        'cpu_capacity': 2500.0,  # Fixed value as specified
        'mem_capacity': df['Mem (MB)']
    })
    
    # Sort by id
    task_df = task_df.sort_values('id')
    
    # Save as parquet file
    task_df.to_parquet(output_parquet_path, index=False)
    
    print(f"Created parquet file at: {output_parquet_path}")
    print(f"Number of records processed: {len(task_df)}")
    print("\nFirst few records:")
    print(task_df.head())
    
    return task_df

if __name__ == "__main__":
    # Define base paths
    csv_file = os.path.join("citi_data", "UF_VirtualMachines_expanded_102424.csv")
    output_file = os.path.join("output_folder", "task.parquet")
    
    df = convert_csv_to_parquet(csv_file, output_file)