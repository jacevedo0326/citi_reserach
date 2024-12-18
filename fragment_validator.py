import pandas as pd
import numpy as np
import os
def validate_fragment_file(fragment_parquet_path, task_parquet_path):
    # Read both parquet files
    fragment_df = pd.read_parquet(fragment_parquet_path)
    task_df = pd.read_parquet(task_parquet_path)
    
    # Group fragments by id and calculate sums and counts
    fragment_stats = fragment_df.groupby('id').agg({
        'cpu_usage': ['sum', 'count'],
        'cpu_count': 'first'  # Get cpu_count for reference
    }).reset_index()
    
    # Rename columns for clarity
    fragment_stats.columns = ['id', 'total_cpu_usage', 'num_fragments', 'cpu_count']
    
    # Merge with task data to get cpu_capacity
    validation_df = pd.merge(fragment_stats, task_df[['id', 'cpu_capacity']], on='id')
    
    # Calculate expected values and percentages
    validation_df['expected_total'] = validation_df['cpu_capacity'] / 2
    validation_df['usage_percentage'] = (validation_df['total_cpu_usage'] / validation_df['cpu_capacity']) * 100
    
    # Count zero and non-zero fragments for each ID
    zero_nonzero_counts = fragment_df.groupby('id').agg({
        'cpu_usage': lambda x: (sum(x == 0), sum(x > 0))
    }).reset_index()
    zero_nonzero_counts.columns = ['id', 'zero_nonzero']
    validation_df['num_zero'] = zero_nonzero_counts['zero_nonzero'].apply(lambda x: x[0])
    validation_df['num_nonzero'] = zero_nonzero_counts['zero_nonzero'].apply(lambda x: x[1])
    
    # Print validation results
    print("\nValidation Results:")
    print("-" * 80)
    
    for _, row in validation_df.iterrows():
        print(f"\nVM ID: {int(row['id'])}")
        print(f"Total CPU Usage: {row['total_cpu_usage']:.2f}")
        print(f"Expected Usage (50% of capacity): {row['expected_total']:.2f}")
        print(f"Actual Usage Percentage: {row['usage_percentage']:.2f}%")
        print(f"Total Fragments: {int(row['num_fragments'])}")
        print(f"Zero CPU Fragments: {int(row['num_zero'])}")
        print(f"Non-zero CPU Fragments: {int(row['num_nonzero'])}")
        print("-" * 40)
    
    return validation_df

if __name__ == "__main__":
    fragment_file = os.path.join('output_folder',"fragment.parquet" )
    task_file = os.path.join('output_folder',"task.parquet" )
    
    validation_results = validate_fragment_file(fragment_file, task_file)