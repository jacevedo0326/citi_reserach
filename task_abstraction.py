import pandas as pd
import numpy as np
from datetime import datetime
import os
import pyarrow as pa
import pyarrow.parquet as pq
import random

def detect_encoding(file_path):
    """Detect the encoding of a file using chardet if available"""
    try:
        import chardet
        with open(file_path, 'rb') as f:
            # Read a sample of the file to detect encoding
            sample = f.read(10000)
            result = chardet.detect(sample)
            print(f"Detected encoding: {result['encoding']} with confidence {result['confidence']}")
            return result['encoding']
    except ImportError:
        print("chardet module not available. Will try common encodings instead.")
        return None

def convert_csv_to_parquet(csv_file_path, output_parquet_path, subset_fraction=0.25):
    os.makedirs(os.path.dirname(output_parquet_path), exist_ok=True)
    
    # Try to detect the file encoding
    encoding = detect_encoding(csv_file_path)
    if encoding:
        print(f"Using detected encoding: {encoding}")
    else:
        print("Will try common encodings...")
    
    # Try different encodings if needed
    encodings_to_try = [encoding] if encoding else [] 
    encodings_to_try.extend(['latin1', 'ISO-8859-1', 'cp1252', 'utf-8-sig'])
    
    # Try each encoding until one works
    for enc in encodings_to_try:
        try:
            print(f"Attempting to read with encoding: {enc}")
            df = pd.read_csv(csv_file_path, encoding=enc, low_memory=False)
            print(f"Successfully read file with encoding: {enc}")
            break
        except UnicodeDecodeError as e:
            print(f"Failed with encoding {enc}: {e}")
        except Exception as e:
            print(f"Other error with encoding {enc}: {e}")
    else:
        raise ValueError("Failed to read CSV with any of the attempted encodings")
    
    # Print column names to verify
    print(f"\nColumns in the CSV file: {df.columns.tolist()}")
    
    # Save the original row numbers before any filtering
    df['original_row'] = df.index + 1
    
    # Filter for RUTH datacenter specifically
    if 'vCenter DC' not in df.columns:
        print(f"ERROR: 'vCenter DC' column not found in CSV file.")
        print("Cannot filter for RUTH VMs. Aborting.")
        return None
    
    # Filter only for RUTH VMs, preserving the original row numbers
    ruth_vms = df[df['vCenter DC'].str.upper() == 'RUTH'].copy()
    print(f"Filtered to {len(ruth_vms)} VMs with 'vCenter DC' = 'RUTH'")
    
    if len(ruth_vms) == 0:
        print("No VMs found with 'vCenter DC' = 'RUTH'. Please check the CSV file.")
        return None
    
    # Check if required columns exist
    required_columns = ['CPU', 'Mem (MB)']
    missing_columns = [col for col in required_columns if col not in ruth_vms.columns]
    
    if missing_columns:
        print(f"ERROR: Missing required columns: {missing_columns}")
        print("Cannot proceed without required columns. Aborting.")
        return None
    
    # Decide if we're taking a subset or using all RUTH VMs
    if subset_fraction < 1.0:
        original_row_count = len(ruth_vms)
        subset_size = max(1, int(original_row_count * subset_fraction))
        
        print(f"\nSelecting a random subset of {subset_size} VMs out of {original_row_count} total RUTH VMs ({subset_fraction*100:.0f}%)")
        
        # Get random indices for the subset
        random_indices = random.sample(range(original_row_count), subset_size)
        
        # Filter the dataframe to only include the random subset
        ruth_vms = ruth_vms.iloc[random_indices]
        print(f"Randomly selected {len(ruth_vms)} VMs")
    
    # Use the original row numbers as VM IDs
    vm_ids = ruth_vms['original_row'].astype(str)
    
    # Create task dataframe with original column names but with replacement data
    task_df = pd.DataFrame({
        'id': vm_ids,
        'submission_time': 0,  # Empty submission_time as requested
        'duration': 2592252000,
        'cpu_count': "Amount of CPUs it has access to",  # Replace data with the string
        'cpu_capacity': "2500.0, what we guessed as a good benchmark, this can be changed when we get more data from them",  # Updated as requested
        'mem_capacity': "Amount of memory VM has access to"  # Replace data with the string
    })
    
    task_df = task_df.sort_values('id')
    
    # Define schema with string type for all text fields
    schema = pa.schema([
        ('id', pa.string(), False),
        ('submission_time', pa.int64(), False),
        ('duration', pa.int64(), False),
        ('cpu_count', pa.string(), False),  
        ('cpu_capacity', pa.string(), False),  # Changed to string type
        ('mem_capacity', pa.string(), False)  
    ])
    
    # Print statistics before saving
    print("\nStatistics before saving:")
    print(f"Total VMs: {len(task_df)}")
    
    # Create the table and write to parquet
    table = pa.Table.from_pandas(task_df, schema=schema)
    pq.write_table(table, output_parquet_path)
    
    print(f"\nCreated parquet file at: {output_parquet_path}")
    print(f"Number of RUTH VMs processed: {len(task_df)}")
    print("\nFirst few records:")
    print(task_df.head())
    
    return task_df

if __name__ == "__main__":
    # Use the specific CSV file path provided
    csv_file = "/home/joshua/dev/CitiReserach/python_files/citi_data/UF_VirtualMachines_expanded_102424.csv"
    
    # Use the Simulations directory structure
    base_dir = "/home/joshua/dev/CitiReserach/python_files"
    output_dir = os.path.join(base_dir, "abstracted_files")
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, "tasks.parquet")
    
    # Set random seed for reproducibility
    random.seed(42)
    
    # Process all VMs (100%)
    subset_fraction = 1.0
    
    print(f"Input CSV: {csv_file}")
    print(f"Output Parquet: {output_file}")
    print(f"Will select 100% of RUTH VMs")
    
    try:
        # Use subset_fraction to process all VMs
        df = convert_csv_to_parquet(csv_file, output_file, subset_fraction=subset_fraction)
        
        if df is not None:
            print("\nTask file creation successful!")
            print(f"Created tasks for {len(df)} RUTH VMs (100% of total)")
        else:
            print("\nTask file creation failed!")
    except Exception as e:
        print(f"Error: {e}")
        print("\nTroubleshooting tips:")
        print("1. Check if the CSV file exists and is readable")
        print("2. Verify the CSV file has the expected columns: 'vCenter DC', 'CPU', 'Mem (MB)'")
        print("3. Confirm there are VMs with 'vCenter DC' value of 'RUTH'")