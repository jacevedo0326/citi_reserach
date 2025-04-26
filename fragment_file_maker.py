import pandas as pd
import numpy as np
import random
import os
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

def create_fragment_file(excel_file_path, output_parquet_path, sheet_name="VMs"):
    """
    Converts data from an Excel file to fragments Parquet file with only one fragment per VM.
    
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
        required_columns = ['CPUs VM is using', 'CPU Utilization(MHz)', 'Total CPU Capacity (MHz)']
        missing_columns = [col for col in required_columns if col not in vms_data.columns]
        
        if missing_columns:
            print(f"ERROR: Missing required columns: {missing_columns}")
            print("Cannot proceed without required columns. Aborting.")
            return None
        
        # Process all VMs
        print(f"Creating one fragment for each of the {len(vms_data)} VMs")
        
        # Use the original row numbers as VM IDs
        vm_ids = vms_data['original_row'].astype(str)
        
        # Extract data directly from the Excel sheet - exact values, no rounding
        try:
            # Use exact CPU values without rounding
            cpu_values = pd.to_numeric(
                vms_data['CPUs VM is using'], 
                errors='coerce'
            ).fillna(1).astype(np.int32)
            
            # Get CPU utilization values - exact values
            cpu_usage_values = pd.to_numeric(
                vms_data['CPU Utilization(MHz)'], 
                errors='coerce'
            ).fillna(0).astype(np.float64)
            
        except Exception as e:
            print(f"Error converting values: {e}")
            return None
        
        # Create one fragment per VM
        all_fragments = []
        
        for i in range(len(vms_data)):
            all_fragments.append({
                'id': vm_ids.iloc[i],
                'duration': random.choice([300000, 600000, 900000]),  # Random duration
                'cpu_count': int(cpu_values.iloc[i]),  # Exact CPU count value
                'cpu_usage': float(cpu_usage_values.iloc[i])  # Exact CPU usage value
            })
        
        # Create the fragments dataframe
        fragment_df = pd.DataFrame(all_fragments)
        
        # Ensure correct data types
        fragment_df['cpu_count'] = fragment_df['cpu_count'].astype(np.int32)
        
        # Define the schema
        schema = pa.schema([
            ('id', pa.string(), False),
            ('duration', pa.int64(), False),
            ('cpu_count', pa.int32(), False),
            ('cpu_usage', pa.float64(), False)
        ])
        
        # Print statistics before saving
        print("\nStatistics before saving:")
        print(f"Total fragments: {len(fragment_df)} (one per VM)")
        print(f"CPU count - min: {fragment_df['cpu_count'].min()}, max: {fragment_df['cpu_count'].max()}, mean: {fragment_df['cpu_count'].mean():.2f}")
        print(f"CPU usage - min: {fragment_df['cpu_usage'].min()}, max: {fragment_df['cpu_usage'].max()}, mean: {fragment_df['cpu_usage'].mean():.2f}")
        print(f"Duration - min: {fragment_df['duration'].min()}, max: {fragment_df['duration'].max()}, mean: {fragment_df['duration'].mean():.2f}")
        
        # Create the table and write to parquet
        table = pa.Table.from_pandas(fragment_df, schema=schema)
        pq.write_table(table, output_parquet_path)
        
        print(f"\nCreated fragment file at: {output_parquet_path}")
        print(f"Total number of fragments: {len(fragment_df)} (one per VM)")
        print("\nFirst few records:")
        print(fragment_df.head())
        
        return fragment_df
        
    except Exception as e:
        print(f"Error reading or processing Excel file: {e}")
        return None

if __name__ == "__main__":
    # Use os.path.join for cross-platform compatibility
    excel_file = os.path.join("C:", os.sep, "Users", "Joshua", "Dev", "citi_reserach", "citi_data", "CSV_DT_Data.xlsx")
    
    # Create fragment_files directory and put fragments.parquet in it
    output_dir = os.path.join("C:", os.sep, "Users", "Joshua", "Dev", "citi_reserach", "fragment_files")
    output_file = os.path.join(output_dir, "fragments.parquet")
    
    # Make sure the output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"Input Excel: {excel_file}")
    print(f"Output Parquet: {output_file}")
    print(f"Processing all VMs in the dataset to create fragments")
    
    try:
        # Process all VMs
        df = create_fragment_file(excel_file, output_file)
        
        if df is not None:
            print("\nFragment file creation successful!")
            print(f"Created one fragment for each of {len(df)} VMs")
        else:
            print("\nFragment file creation failed!")
    except Exception as e:
        print(f"Error: {e}")
        print("\nTroubleshooting tips:")
        print("1. Check if the Excel file exists and is readable")
        print("2. Verify the Excel file has the expected columns: 'CPUs VM is using', 'CPU Utilization(MHz)', 'Total CPU Capacity (MHz)'")
        print("3. Make sure the 'VMs' sheet exists in the Excel file")