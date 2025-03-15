import pandas as pd
import numpy as np
import random
import os
import pyarrow as pa
import pyarrow.parquet as pq

# Customizable CPU usage percentage (default 10%)
CPU_USAGE_PERCENTAGE = 0.10

def create_fragment_file(task_parquet_path, output_parquet_path, usage_percentage=CPU_USAGE_PERCENTAGE, duration_hours=24):
    """
    Creates a fragment file with consistent CPU usage over the specified duration
    
    Parameters:
    - task_parquet_path: Path to input task parquet file (can be None to generate synthetic data)
    - output_parquet_path: Path where output parquet will be saved
    - usage_percentage: Percentage of VM's capacity to utilize (0.10 = 10%)
    - duration_hours: Duration in hours to generate data for (default 24 hours)
    
    Note: Fragment IDs are preserved from the task file's VM IDs to maintain relationship
    """
    os.makedirs(os.path.dirname(output_parquet_path), exist_ok=True)
    
    # Convert hours to milliseconds
    total_duration_ms = duration_hours * 60 * 60 * 1000
    
    # Define fragment durations in milliseconds (5 minutes = 300000 ms)
    fragment_duration_ms = 300000  # 5-minute fragments
    
    # Calculate how many fragments we need to cover the entire duration
    fragments_per_vm = total_duration_ms // fragment_duration_ms
    
    print(f"Each VM will have {fragments_per_vm} fragments of {fragment_duration_ms/1000/60} minutes each")
    print(f"Total duration per VM: {fragments_per_vm * fragment_duration_ms / 1000 / 60 / 60:.2f} hours")
    
    # Validate that we can exactly cover the requested duration
    if fragments_per_vm * fragment_duration_ms != total_duration_ms:
        print(f"WARNING: Fragment size doesn't divide evenly into {duration_hours} hours.")
        print(f"Actual duration will be {fragments_per_vm * fragment_duration_ms / 1000 / 60 / 60:.2f} hours")
    
    # If we have an input file, read it; otherwise, generate synthetic VMs
    if task_parquet_path and os.path.exists(task_parquet_path):
        try:
            task_df = pd.read_parquet(task_parquet_path)
            print(f"Successfully read task file with {len(task_df)} VMs")
            # Verify required columns are present
            required_columns = ['id', 'cpu_capacity', 'cpu_count']
            missing_columns = [col for col in required_columns if col not in task_df.columns]
            if missing_columns:
                raise ValueError(f"Input file is missing required columns: {missing_columns}")
        except Exception as e:
            print(f"Error reading input parquet file: {e}")
            print("Generating synthetic VM data instead.")
            task_df = generate_synthetic_vms(10)  # Generate 10 synthetic VMs
    else:
        print("Input file not provided or doesn't exist. Generating synthetic VM data.")
        task_df = generate_synthetic_vms(10)  # Generate 10 synthetic VMs
    
    all_fragments = []
    
    print(f"Generating fragments for {len(task_df)} VMs across {duration_hours} hours ({fragments_per_vm} fragments per VM)...")
    
    for _, vm in task_df.iterrows():
        # Ensure we preserve the exact VM ID from the task file
        vm_id = str(vm['id'])
        cpu_count = int(vm['cpu_count'])
        cpu_capacity = float(vm['cpu_capacity'])
        
        # Calculate fixed CPU usage based on the specified percentage of THIS VM's capacity
        # This ensures each VM runs at exactly the specified percentage of ITS OWN capacity
        cpu_usage = cpu_capacity * usage_percentage
        
        # Create fragments for the entire duration
        for i in range(int(fragments_per_vm)):
            all_fragments.append({
                'id': vm_id,  # Preserve VM ID as fragment ID to maintain relationship
                'duration': fragment_duration_ms,
                'cpu_count': cpu_count,
                'cpu_usage': cpu_usage
            })
    
    fragment_df = pd.DataFrame(all_fragments)
    fragment_df['cpu_count'] = fragment_df['cpu_count'].astype(np.int32)
    
    schema = pa.schema([
        ('id', pa.string(), False),
        ('duration', pa.int64(), False),
        ('cpu_count', pa.int32(), False),
        ('cpu_usage', pa.float64(), False)
    ])
    
    table = pa.Table.from_pandas(fragment_df, schema=schema)
    pq.write_table(table, output_parquet_path)
    
    # Now create a validation and summary section
    print("\nPerforming validation checks...")
    
    # Group fragments by VM ID to validate each VM individually
    vm_groups = fragment_df.groupby('id')
    
    validation_results = []
    for vm_id, vm_fragments in vm_groups:
        # Get the original VM data
        vm_data = task_df[task_df['id'] == vm_id].iloc[0]
        
        # Calculate expected values
        expected_fragments = fragments_per_vm
        expected_duration_ms = fragment_duration_ms * expected_fragments
        expected_cpu_usage = vm_data['cpu_capacity'] * usage_percentage
        
        # Calculate actual values
        actual_fragments = len(vm_fragments)
        actual_duration_ms = vm_fragments['duration'].sum()
        actual_cpu_usage = vm_fragments['cpu_usage'].mean()
        
        # Validate each VM
        validation_results.append({
            'vm_id': vm_id,
            'expected_fragments': expected_fragments,
            'actual_fragments': actual_fragments,
            'fragments_match': expected_fragments == actual_fragments,
            'expected_duration_hours': expected_duration_ms / 1000 / 60 / 60,
            'actual_duration_hours': actual_duration_ms / 1000 / 60 / 60,
            'duration_match': expected_duration_ms == actual_duration_ms,
            'expected_cpu_usage': expected_cpu_usage,
            'actual_cpu_usage': actual_cpu_usage,
            'cpu_usage_match': abs(expected_cpu_usage - actual_cpu_usage) < 0.001  # Allow small floating point differences
        })
    
    # Create validation DataFrame for easy analysis
    validation_df = pd.DataFrame(validation_results)
    
    # Summary statistics
    all_pass = (
        validation_df['fragments_match'].all() and 
        validation_df['duration_match'].all() and 
        validation_df['cpu_usage_match'].all()
    )
    
    print("\nValidation Results:")
    print(f"✓ All VMs have {fragments_per_vm} fragments: {'PASS' if validation_df['fragments_match'].all() else 'FAIL'}")
    print(f"✓ All VMs run for exactly {duration_hours} hours: {'PASS' if validation_df['duration_match'].all() else 'FAIL'}")
    print(f"✓ All VMs use exactly {usage_percentage*100}% of their capacity: {'PASS' if validation_df['cpu_usage_match'].all() else 'FAIL'}")
    print(f"Overall validation: {'PASS' if all_pass else 'FAIL'}")
    
    # If there are any failures, show details
    if not all_pass:
        print("\nDetailed validation results for failed checks:")
        failed_validations = validation_df[
            ~validation_df['fragments_match'] | 
            ~validation_df['duration_match'] | 
            ~validation_df['cpu_usage_match']
        ]
        if not failed_validations.empty:
            print(failed_validations)
    
    return fragment_df

def generate_synthetic_vms(num_vms):
    """Generate synthetic VM data if no input file is provided"""
    vms = []
    for i in range(num_vms):
        cpu_count = random.randint(2, 16)
        vms.append({
            'id': f"vm-{1000 + i}",
            'cpu_count': cpu_count,
            'cpu_capacity': cpu_count * 1000.0  # Assuming capacity scales with CPU count
        })
    return pd.DataFrame(vms)

if __name__ == "__main__":
    # Set your desired CPU usage percentage here (0.10 = 10%)
    CPU_USAGE_PERCENTAGE = 0.10
    
    # Set the duration in hours (default 24 hours)
    DURATION_HOURS = 24
    
    # Use the specific base directory path
    base_dir = "/home/joshua/dev/CitiReserach/python_files/Simulations"
    task_file = os.path.join(base_dir, "citi_simulation_iteration_6", "workloads", "bitbrains-small", "tasks.parquet")
    output_file = os.path.join(base_dir, "citi_simulation_iteration_6", "workloads", "bitbrains-small", "fragments.parquet")
    
    # Print paths for verification
    print(f"Input task file: {task_file}")
    print(f"Output fragment file: {output_file}")
    
    # Create output directory if it doesn't exist
    output_dir = os.path.dirname(output_file)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
        print(f"Created output directory: {output_dir}")
    
    # Allow customization via command line arguments
    import argparse
    parser = argparse.ArgumentParser(description='Generate fragment parquet file with consistent CPU usage')
    parser.add_argument('--usage', type=float, default=CPU_USAGE_PERCENTAGE,
                        help='CPU usage percentage (0.10 = 10%%)')
    parser.add_argument('--hours', type=int, default=DURATION_HOURS,
                        help='Duration in hours to generate data for')
    parser.add_argument('--input', type=str, default=task_file,
                        help='Input task parquet file path')
    parser.add_argument('--output', type=str, default=output_file,
                        help='Output fragment parquet file path')
    
    args = parser.parse_args()
    
    # Generate the fragment file with the specified CPU usage percentage
    df = create_fragment_file(
        args.input, 
        args.output, 
        usage_percentage=args.usage,
        duration_hours=args.hours
    )
    
    print("\nSummary:")
    print(f"- Each VM is running at exactly {args.usage*100}% of its capacity")
    print(f"- Fragment IDs match exactly with VM IDs from the task file")
    print(f"- Fragments cover a {args.hours}-hour period with 5-minute intervals")
    print(f"- Output file created at: {args.output}")