import pandas as pd
import numpy as np
from datetime import datetime
import os
import pyarrow as pa
import pyarrow.parquet as pq

def convert_csv_to_parquet(csv_file_path, output_parquet_path):
    os.makedirs(os.path.dirname(output_parquet_path), exist_ok=True)
    
    df = pd.read_csv(csv_file_path, low_memory=False)
    df = df[df['vCenter DC'].str.upper() == 'RUTH']
    
    df = df.reset_index(drop=True)
    vm_ids = (df.index + 1).astype(str)
    
    # Convert submission_time to timestamp in milliseconds
    timestamp = int(datetime.strptime("2013-08-12 13:35:46", "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
    
    task_df = pd.DataFrame({
        'id': vm_ids,
        'submission_time': timestamp,
        'duration': 2592252000,
        'cpu_count': df['CPU'].astype(np.int32),
        'cpu_capacity': 2500.0,
        'mem_capacity': df['Mem (MB)'].astype(np.int64)  # Convert to int64
    })
    
    task_df = task_df.sort_values('id')
    
    schema = pa.schema([
        ('id', pa.string(), False),
        ('submission_time', pa.int64(), False),
        ('duration', pa.int64(), False),
        ('cpu_count', pa.int32(), False),
        ('cpu_capacity', pa.float64(), False),
        ('mem_capacity', pa.int64(), False)  # Changed to int64
    ])
    
    table = pa.Table.from_pandas(task_df, schema=schema)
    pq.write_table(table, output_parquet_path)
    
    print(f"Created parquet file at: {output_parquet_path}")
    print(f"Number of records processed: {len(task_df)}")
    print("\nFirst few records:")
    print(task_df.head())
    
    return task_df

if __name__ == "__main__":
    csv_file = os.path.join("citi_data", "UF_VirtualMachines_expanded_102424.csv")
    output_file = os.path.join("citi_simulation_iteration_1", "workloads", "bitbrains-small", "tasks.parquet")
    
    df = convert_csv_to_parquet(csv_file, output_file)