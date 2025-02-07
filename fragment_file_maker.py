import pandas as pd
import numpy as np
import random
import os
import pyarrow as pa
import pyarrow.parquet as pq
percentage = 0.1 #Here we change how hard the vm work the servers(percentage)
def create_fragment_file(task_parquet_path, output_parquet_path):
    os.makedirs(os.path.dirname(output_parquet_path), exist_ok=True)
    
    task_df = pd.read_parquet(task_parquet_path)
    all_fragments = []
    
    for _, vm in task_df.iterrows():
        num_fragments = random.randint(20, 50)
        num_zero = num_fragments // 5
        num_nonzero = num_fragments - num_zero
        
        # Changed from /2 (50%) to /3.333... (30%)
        target_cpu_sum = vm['cpu_capacity'] * percentage
        nonzero_cpu_values = np.random.random(num_nonzero)
        nonzero_cpu_values = (nonzero_cpu_values / np.sum(nonzero_cpu_values)) * target_cpu_sum
        
        vm_fragments = []
        
        for _ in range(num_zero):
            vm_fragments.append({
                'id': str(vm['id']),
                'duration': random.choice([300000, 600000, 900000]),
                'cpu_count': int(vm['cpu_count']),  # Ensure int32
                'cpu_usage': 0.0
            })
        
        for cpu_usage in nonzero_cpu_values:
            vm_fragments.append({
                'id': str(vm['id']),
                'duration': random.choice([300000, 600000, 900000]),
                'cpu_count': int(vm['cpu_count']),  # Ensure int32
                'cpu_usage': cpu_usage
            })
        
        all_fragments.extend(vm_fragments)
    
    fragment_df = pd.DataFrame(all_fragments)
    fragment_df['cpu_count'] = fragment_df['cpu_count'].astype(np.int32)  # Ensure int32
    
    schema = pa.schema([
        ('id', pa.string(), False),
        ('duration', pa.int64(), False),
        ('cpu_count', pa.int32(), False),  # Changed to int32
        ('cpu_usage', pa.float64(), False)
    ])
    
    table = pa.Table.from_pandas(fragment_df, schema=schema)
    pq.write_table(table, output_parquet_path)
    
    print(f"Created fragment file at: {output_parquet_path}")
    print(f"Total number of fragments created: {len(fragment_df)}")
    print("\nFirst few records:")
    print(fragment_df.head())
    
    return fragment_df

if __name__ == "__main__":
    task_file = os.path.join("citi_simulation_iteration_4", "workloads", "bitbrains-small", "tasks.parquet")
    output_file = os.path.join("citi_simulation_iteration_4", "workloads", "bitbrains-small", "fragments.parquet")
    
    df = create_fragment_file(task_file, output_file)