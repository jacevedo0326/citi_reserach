#!/usr/bin/env python3
"""
Parquet to CSV Converter - Optimized for Large Files

This script converts a Parquet file to a CSV file, with special handling for large files.
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import sys
import os
from datetime import datetime
import gc

# ====== CONFIGURATION VARIABLES - EDIT THESE ======
inputFilePath = "/home/joshua/dev/CitiReserach/python_files/Simulations/citi_simulation_iteration_5/workloads/bitbrains-small/tasks.parquet"  # Path to your input Parquet file
outputFilePath = "/home/joshua/dev/CitiReserach/python_files/CSVs/TASK5.csv"  # Path for the output CSV file
chunksize = 100000  # Process this many rows at a time (recommended for large files)
separator = ','  # Delimiter for the CSV file (e.g., ',' or ';' or '\t')
naRepresentation = ''  # How to represent missing values (e.g., '' or 'NULL' or 'NA')
# =================================================

def get_parquet_row_count(parquet_file):
    """
    Get the total number of rows in a parquet file without loading the whole file.
    """
    try:
        parquet_file = pq.ParquetFile(parquet_file)
        return parquet_file.metadata.num_rows
    except Exception as e:
        print(f"Error getting row count: {str(e)}")
        return None

def parquet_to_csv_chunked(input_path, output_path, chunksize=100000, sep=',', na_rep=''):
    """
    Convert a Parquet file to CSV format in chunks to handle large files.
    
    Parameters:
    -----------
    input_path : str
        Path to the input Parquet file
    output_path : str
        Path to save the output CSV file
    chunksize : int, default 100000
        Number of rows to process at a time
    sep : str, default ','
        Delimiter to use in the output CSV
    na_rep : str, default ''
        String representation of NaN values in the output
    
    Returns:
    --------
    bool
        True if conversion is successful
    """
    try:
        start_time = datetime.now()
        print(f"Starting conversion: {input_path} -> {output_path}")
        
        # Validate input file
        if not os.path.exists(input_path):
            print(f"Error: Input file '{input_path}' does not exist.")
            return False
        
        # Get total row count if possible
        total_rows = get_parquet_row_count(input_path)
        if total_rows:
            print(f"Total rows to process: {total_rows:,}")
        
        # Create output directory if it doesn't exist
        output_dir = os.path.dirname(output_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
            print(f"Created output directory: {output_dir}")
        
        # Open the parquet file
        parquet_file = pq.ParquetFile(input_path)
        
        # Process the file in chunks
        total_processed = 0
        for i, batch in enumerate(parquet_file.iter_batches(batch_size=chunksize)):
            # Convert PyArrow Table to pandas DataFrame
            chunk = batch.to_pandas()
            
            # Write to CSV
            mode = 'w' if i == 0 else 'a'
            header = i == 0
            
            chunk.to_csv(
                output_path, 
                index=False, 
                sep=sep, 
                na_rep=na_rep, 
                mode=mode, 
                header=header
            )
            
            total_processed += len(chunk)
            
            # Report progress
            if total_rows:
                progress = (total_processed / total_rows) * 100
                print(f"Processed chunk {i+1}: {len(chunk):,} rows | Total: {total_processed:,} of {total_rows:,} ({progress:.1f}%)")
            else:
                print(f"Processed chunk {i+1}: {len(chunk):,} rows | Total: {total_processed:,} rows")
            
            # Clean up to free memory
            del chunk
            gc.collect()
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        print(f"Conversion completed in {duration:.2f} seconds")
        print(f"Successfully processed {total_processed:,} rows total")
        return True
        
    except Exception as e:
        print(f"Error during conversion: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def main():
    # Use the global variables
    # Check if the configuration variables have been set
    if not inputFilePath:
        print("Error: Please set the inputFilePath variable at the top of the script.")
        sys.exit(1)
    
    # Create a local variable for the output path
    output_path = outputFilePath
    if not output_path:
        # Default to the same name as input but with .csv extension
        output_path = os.path.splitext(inputFilePath)[0] + '.csv'
        print(f"No output path specified. Using: {output_path}")
    
    # Force chunking for large files
    chunk_size = chunksize if chunksize else 100000
    print(f"Processing with chunk size: {chunk_size:,}")
    
    # Call the conversion function with the configured parameters
    success = parquet_to_csv_chunked(
        input_path=inputFilePath, 
        output_path=output_path, 
        chunksize=chunk_size, 
        sep=separator, 
        na_rep=naRepresentation
    )
    
    # Exit with appropriate status code
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()