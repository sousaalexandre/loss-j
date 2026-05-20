import pandas as pd
import glob
import os
import sys
import argparse

def analyze_performance(batch_name):
    """
    Analyzes the aggregated timings for a specific batch and prints a summary.
    """
    timings_dir = 'outputs/batch_results/timings'
    search_pattern = os.path.join(timings_dir, f"{batch_name}_timings_*.csv")
    matching_files = glob.glob(search_pattern)
    
    if not matching_files:
        print(f"Error: No timing results found for batch '{batch_name}' in {timings_dir}")
        return

    # Pick the most recent one if multiple exist
    latest_file = max(matching_files, key=os.path.getctime)
    
    df = pd.read_csv(latest_file)
    
    # Identify timing columns (columns ending in '(s)')
    timing_cols = [c for c in df.columns if '(s)' in c]
    
    if not timing_cols:
        print("Error: No timing columns found in the CSV.")
        return

    # Calculate average scores across all runs
    averages = df[timing_cols].mean()
    
    print(f"Analyzing timings file: {latest_file}")
    print("-" * 40)
    print(f"BATCH PERFORMANCE STATISTICS: {batch_name.upper()}")
    print("-" * 40)
    
    for col in timing_cols:
        print(f"Average {col:20} {averages[col]:.5f}")
    
    print("-" * 40)
    print(f"Total Runs: {len(df)}")
    print("-" * 40)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Analyze Batch Performance Timings')
    parser.add_argument('name', type=str, help='Name of the batch to analyze')
    
    args = parser.parse_args()
    analyze_performance(args.name)
