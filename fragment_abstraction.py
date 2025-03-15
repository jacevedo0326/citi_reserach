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
    
    # Convert hours to milliseconds (for 24-hour coverage)
    total_duration_ms = duration_hours * 60 * 60 * 1000
    
    # Define fragment durations in milliseconds (5 minutes = 300000 ms)
    fragment_duration_ms = 300000  # 5-minute fragments
    
    # Calculate how many fragments we need to cover the entire duration
    fragments_per_vm = total_duration_ms // fragment_duration_ms
    
    # If we have an input file, read it; otherwise, generate synthetic VMs
    if task_parquet_path and os.path.exists(task_parquet_path):
        try:
            task_df = pd.read_parquet(task_parquet_path)
            print(f"Successfully read task file with {len(task_df)} VMs")
            
            # Verify required columns are present
            required_columns = ['id', 'cpu_capacity']
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
        cpu_capacity = float(vm['cpu_capacity'])
        
        # Calculate fixed CPU usage based on the specified percentage of THIS VM's capacity
        # This ensures each VM runs at exactly the specified percentage of ITS OWN capacity
        cpu_usage = cpu_capacity * usage_percentage
        
        # Create fragments for the entire duration
        for i in range(int(fragments_per_vm)):
            all_fragments.append({
                'id': vm_id,  # Preserve VM ID as fragment ID to maintain relationship
                'duration': fragment_duration_ms,
                'cpu_count': "Same as the amount of CPUs of the VM it is running on",  # Use constant text value
                'cpu_usage': cpu_usage
            })
    
    fragment_df = pd.DataFrame(all_fragments)
    
    schema = pa.schema([
        ('id', pa.string(), False),
        ('duration', pa.int64(), False),
        ('cpu_count', pa.string(), False),  # Changed to string type to hold text
        ('cpu_usage', pa.float64(), False)
    ])
    
    table = pa.Table.from_pandas(fragment_df, schema=schema)
    pq.write_table(table, output_parquet_path)
    
    print(f"Created fragment file at: {output_parquet_path}")
    print(f"Total number of fragments created: {len(fragment_df)}")
    print(f"CPU usage percentage: {usage_percentage * 100:.1f}%")
    print(f"Duration covered: {duration_hours} hours")
    print("\nFirst few records:")
    print(fragment_df.head())
    
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
    base_dir = "/home/joshua/dev/CitiReserach/python_files/abstracted_files"
    task_file = os.path.join(base_dir, "tasks.parquet")
    output_file = os.path.join(base_dir,"fragments.parquet")
    
    # Create output directory if it doesn't exist
    output_dir = os.path.dirname(output_file)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
    
    # Generate the fragment file with the specified CPU usage percentage
    df = create_fragment_file(
        task_file, 
        output_file, 
        usage_percentage=CPU_USAGE_PERCENTAGE,
        duration_hours=DURATION_HOURS
    )
    
    print("\nSummary:")
    print(f"- Each VM is running at exactly {CPU_USAGE_PERCENTAGE*100}% of its capacity")
    print(f"- Fragment IDs match exactly with VM IDs from the task file")
    print(f"- Fragments cover a {DURATION_HOURS}-hour period with 5-minute intervals")