import pandas as pd
import numpy as np
from collections import Counter
import re
import os

# Set your input and output file paths here
# Use raw string prefix (r) to avoid escape character issues in Windows paths
input_file = r"C:\Users\Joshua\Dev\citi_reserach\citi_data\CSV_DT_Data.xlsx"  # <-- CHANGE THIS TO YOUR EXCEL FILE PATH
sheet_name = "VMs"  # <-- SPECIFY THE SHEET NAME HERE
# Use os.path.dirname to get the current directory for output
current_dir = os.path.dirname(os.path.abspath(__file__))
output_file = os.path.join(current_dir, "vm_classification.csv")  # Output in same directory as script

def classify_vms(input_file, sheet_name, output_file):
    """
    Classify VMs based on their specifications and count each type.
    
    Parameters:
    - input_file: Path to Excel file containing VM data
    - sheet_name: Name of the sheet containing VM data
    - output_file: Path to output CSV file with VM classifications
    
    Returns:
    - DataFrame with VM classifications and counts
    """
    # Read the Excel file with specific sheet name
    print(f"Reading VM data from {input_file}, sheet '{sheet_name}'...")
    try:
        df = pd.read_excel(input_file, sheet_name=sheet_name)
    except ValueError as e:
        # If sheet name is not found, list available sheets
        xls = pd.ExcelFile(input_file)
        available_sheets = xls.sheet_names
        raise ValueError(f"Sheet '{sheet_name}' not found. Available sheets: {', '.join(available_sheets)}")
    
    # Clean up column names (remove extra spaces, standardize names)
    df.columns = [re.sub(r'\s+', ' ', col).strip() for col in df.columns]
    
    # Print all column names to help debug
    print(f"Available columns: {', '.join(df.columns)}")
    
    # Identify the exact column names from the file
    memory_col = next((col for col in df.columns if 'Memory' in col), None)
    cpu_capacity_col = next((col for col in df.columns if 'Total CPU Capacity' in col), None)
    cpu_access_col = next((col for col in df.columns if 'CPUs VM has access' in col), None)
    
    if not all([memory_col, cpu_capacity_col, cpu_access_col]):
        available_cols = ", ".join(df.columns)
        raise ValueError(f"Could not find required columns. Available columns: {available_cols}")
    
    print(f"Found columns: {memory_col}, {cpu_capacity_col}, {cpu_access_col}")
    
    # Create a new column that combines the key specifications
    df['vm_spec'] = df.apply(
        lambda row: (
            row[memory_col],
            row[cpu_capacity_col],
            row[cpu_access_col]
        ),
        axis=1
    )
    
    # Count occurrences of each unique VM spec
    vm_counts = Counter(df['vm_spec'])
    
    # Create a DataFrame with unique VM specs and their counts
    vm_types_df = pd.DataFrame({
        'Memory': [spec[0] for spec in vm_counts.keys()],
        'Total_CPU_Capacity_MHz': [spec[1] for spec in vm_counts.keys()],
        'CPUs_VM_Access': [spec[2] for spec in vm_counts.keys()],
        'Count': list(vm_counts.values())
    })
    
    # Sort by resource capacity (memory, then CPU capacity, then CPU access)
    vm_types_df = vm_types_df.sort_values(
        by=['Memory', 'Total_CPU_Capacity_MHz', 'CPUs_VM_Access'],
        ascending=[True, True, True]
    )
    
    # Generate simple numbered category labels
    category_labels = [f"Category {i+1}" for i in range(len(vm_types_df))]
    vm_types_df['Category'] = category_labels
    
    # Save to CSV
    print(f"Saving VM classification to {output_file}...")
    vm_types_df.to_csv(output_file, index=False)
    
    return vm_types_df

# Main execution
if __name__ == "__main__":
    try:
        # First check if the Excel file exists
        if not os.path.exists(input_file):
            raise FileNotFoundError(f"Excel file not found at path: {input_file}")
            
        # List all available sheets to help with debugging
        xls = pd.ExcelFile(input_file)
        print(f"Available sheets in the Excel file: {', '.join(xls.sheet_names)}")
        
        result_df = classify_vms(input_file, sheet_name, output_file)
        print("\nVM Classification Summary:")
        print(result_df)
        print(f"\nTotal VMs classified: {result_df['Count'].sum()}")
        print(f"Total unique VM types: {len(result_df)}")
    except Exception as e:
        print(f"Error: {e}")