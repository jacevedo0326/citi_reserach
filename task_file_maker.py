import pandas as pd
import numpy as np
from datetime import datetime
import os
import pyarrow as pa
import pyarrow.parquet as pq
import random
import math

def convert_excel_to_parquet(excel_file_path, output_parquet_path, sheet_name="VMs"):
    """
    Converts data from an Excel file to a Parquet file with the specified columns.
    
    Args:
        excel_file_path: Path to the input Excel file
        output_parquet_path: Path where the Parquet file will be saved
        sheet_name: Name of the sheet to read from the Excel file
    
    Returns:
        DataFrame with the processed data or None if an error occurred
    """
    os.makedirs(os.path.dirname(output_parquet_path), exist_ok=True)
    
    try:
        print(f"Reading Excel file from: {excel_file_path}")
        # Read the Excel file
        df = pd.read_excel(excel_file_path, sheet_name=sheet_name)
        print(f"Successfully read Excel file. Found {len(df)} rows.")
        
        # Print column names to verify
        print(f"\nColumns in the Excel file: {df.columns.tolist()}")
        
        # Save the original row numbers before any filtering
        df['original_row'] = df.index + 1
    
        # Use the full dataset
        vms_data = df.copy()
        print(f"Processing all {len(vms_data)} VMs from the VMs page")

        # Check if required columns exist based on the actual column names in the file
        required_columns = ['CPUs VM is using', 'Memory', 'Total CPU Capacity (MHz)']
        missing_columns = [col for col in required_columns if col not in vms_data.columns]
        
        if missing_columns:
            print(f"ERROR: Missing required columns: {missing_columns}")
            print("Cannot proceed without required columns. Aborting.")
            return None
        
        # Process all VMs (no subset)
        print(f"Using all {len(vms_data)} VMs from the dataset")
        
        # Use the original row numbers as VM IDs
        vm_ids = vms_data['original_row'].astype(str)
        
        # Convert submission_time to timestamp in milliseconds
        timestamp = int(datetime.strptime("2013-08-12 13:35:46", "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
        
        # Use the column names as seen in the Excel file and round up the CPU values
        try:
            # Get CPU values and round up using math.ceil
            cpu_values = pd.to_numeric(vms_data['CPUs VM is using'], errors='coerce').fillna(1)
            cpu_count = cpu_values.apply(lambda x: math.ceil(x)).astype(np.int32)
        except Exception as e:
            print(f"Error converting CPU values: {e}")
            return None
            
        try:
            mem_capacity = pd.to_numeric(vms_data['Memory'], errors='coerce').fillna(4).astype(np.float64)
            # Convert from GB to MB (multiply by 1024)
            mem_capacity = (mem_capacity * 1024).astype(np.int64)
        except Exception as e:
            print(f"Error converting Memory values: {e}")
            return None
        
        # Get total CPU capacity from the new column
        try:
            total_cpu_capacity = pd.to_numeric(vms_data['Total CPU Capacity (MHz)'], errors='coerce').fillna(2500.0).astype(np.float64)
        except Exception as e:
            print(f"Error converting Total CPU Capacity values: {e}")
            # Fall back to the fixed value if there's an error
            total_cpu_capacity = pd.Series([2500.0] * len(vms_data))
        
        # Create task dataframe with exact fields needed
        task_df = pd.DataFrame({
            'id': vm_ids,
            'submission_time': timestamp,
            'duration': 2592252000,  # Keep duration unchanged
            'cpu_count': cpu_count,
            'cpu_capacity': total_cpu_capacity,
            'mem_capacity': mem_capacity
        })
        
        task_df = task_df.sort_values('id')
        
        schema = pa.schema([
            ('id', pa.string(), False),
            ('submission_time', pa.int64(), False),
            ('duration', pa.int64(), False),
            ('cpu_count', pa.int32(), False),
            ('cpu_capacity', pa.float64(), False),
            ('mem_capacity', pa.int64(), False)
        ])
        
        # Print statistics before saving
        print("\nStatistics before saving:")
        print(f"Total VMs: {len(task_df)}")
        print(f"CPU count - min: {task_df['cpu_count'].min()}, max: {task_df['cpu_count'].max()}, mean: {task_df['cpu_count'].mean():.2f}")
        print(f"CPU capacity - min: {task_df['cpu_capacity'].min()}, max: {task_df['cpu_capacity'].max()}, mean: {task_df['cpu_capacity'].mean():.2f}")
        print(f"Memory capacity (MB) - min: {task_df['mem_capacity'].min()}, max: {task_df['mem_capacity'].max()}, mean: {task_df['mem_capacity'].mean():.2f}")
        
        # Create the table and write to parquet
        table = pa.Table.from_pandas(task_df, schema=schema)
        pq.write_table(table, output_parquet_path)
        
        print(f"\nCreated parquet file at: {output_parquet_path}")
        print(f"Number of VMs processed: {len(task_df)}")
        print("\nFirst few records:")
        print(task_df.head())
        
        return task_df
        
    except Exception as e:
        print(f"Error reading or processing Excel file: {e}")
        return None

if __name__ == "__main__":
    # Use os.path.join for cross-platform compatibility
    excel_file = os.path.join("C:", os.sep, "Users", "Joshua", "Dev", "citi_reserach", "citi_data", "CSV_DT_Data.xlsx")
    
    # Create tasks_file directory and put tasks.parquet in it
    output_dir = os.path.join("C:", os.sep, "Users", "Joshua", "Dev", "citi_reserach", "tasks_file")
    output_file = os.path.join(output_dir, "tasks.parquet")
    
    # Make sure the output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"Input Excel: {excel_file}")
    print(f"Output Parquet: {output_file}")
    print(f"Processing all VMs in the dataset")
    
    try:
        # Process all VMs (no subset)
        df = convert_excel_to_parquet(excel_file, output_file)
        
        if df is not None:
            print("\nTask file creation successful!")
            print(f"Created tasks for {len(df)} total VMs")
        else:
            print("\nTask file creation failed!")
    except Exception as e:
        print(f"Error: {e}")
        print("\nTroubleshooting tips:")
        print("1. Check if the Excel file exists and is readable")
        print("2. Verify the Excel file has the expected columns: 'CPUs VM is using', 'Memory', 'Total CPU Capacity (MHz)'")
        print("3. Make sure the 'VMs' sheet exists in the Excel file")