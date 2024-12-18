import pandas as pd
import numpy as np
import random
import os

def create_fragment_file(task_parquet_path, output_parquet_path):
    # Read the task parquet file
    task_df = pd.read_parquet(task_parquet_path)
    
    # List to store all fragments
    all_fragments = []
    
    # For each VM in the task file
    for _, vm in task_df.iterrows():
        # Random number of fragments between 20-50
        num_fragments = random.randint(20, 50)
        
        # Calculate number of zero and non-zero fragments based on 1:4 ratio
        num_zero = num_fragments // 5  # 20% of fragments
        num_nonzero = num_fragments - num_zero  # 80% of fragments
        
        # Calculate target sum for non-zero CPU usage (half of VM's capacity)
        target_cpu_sum = vm['cpu_capacity'] / 2
        
        # Create non-zero CPU usage values that sum to target
        nonzero_cpu_values = np.random.random(num_nonzero)
        nonzero_cpu_values = (nonzero_cpu_values / np.sum(nonzero_cpu_values)) * target_cpu_sum
        
        # Create fragments for this VM
        vm_fragments = []
        
        # Add zero CPU fragments
        for _ in range(num_zero):
            vm_fragments.append({
                'id': vm['id'],
                'duration': random.choice([300000, 600000, 900000]),
                'cpu_count': vm['cpu_count'],
                'cpu_usage': 0
            })
        
        # Add non-zero CPU fragments
        for cpu_usage in nonzero_cpu_values:
            vm_fragments.append({
                'id': vm['id'],
                'duration': random.choice([300000, 600000, 900000]),
                'cpu_count': vm['cpu_count'],
                'cpu_usage': cpu_usage
            })
        
        all_fragments.extend(vm_fragments)
    
    # Create dataframe from all fragments
    fragment_df = pd.DataFrame(all_fragments)
    
    # Save as parquet file
    fragment_df.to_parquet(output_parquet_path, index=False)
    
    print(f"Created fragment file at: {output_parquet_path}")
    print(f"Total number of fragments created: {len(fragment_df)}")
    print("\nFirst few records:")
    print(fragment_df.head())
    
    return fragment_df

if __name__ == "__main__":
    # Define paths using os.path.join
    task_file = os.path.join("output_folder", "task.parquet")
    output_file = os.path.join("output_folder", "fragment.parquet")
    
    df = create_fragment_file(task_file, output_file)