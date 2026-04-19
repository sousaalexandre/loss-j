import os
import pandas as pd
import glob
import sys
import argparse

def analyze_batch(name):
    """
    Finds the most recent matrix for a given batch name and displays statistics.
    """
    directory = "outputs/batch_results"
    pattern = os.path.join(directory, f"{name}_matrix_*.csv")
    files = glob.glob(pattern)
    
    if not files:
        print(f"Error: No matrix files found for batch name '{name}' in {directory}")
        return

    # Get the most recent file
    latest_file = max(files, key=os.path.getmtime)
    print(f"Analyzing matrix: {latest_file}")
    
    # Load matrix (Index is 'Run', columns are 'Query ID')
    df = pd.read_csv(latest_file, index_col=0)
    
    # Total Average (Average of all scores)
    total_avg = df.values.mean()
    
    # Average per Query
    query_avgs = df.mean(axis=0)
    best_query = query_avgs.idxmax()
    worst_query = query_avgs.idxmin()
    
    # Average per Run
    run_avgs = df.mean(axis=1)
    best_run = run_avgs.idxmax()
    worst_run = run_avgs.idxmin()

    print("-" * 40)
    print(f"BATCH STATISTICS: {name.upper()}")
    print("-" * 40)
    print(f"Overall Average Score: {total_avg:.2f}%")
    print(f"Total Runs:           {len(df)}")
    print(f"Total Queries:        {len(df.columns)}")
    print("-" * 40)
    print(f"Best Query:           ID {best_query} (Avg: {query_avgs[best_query]:.2f}%)")
    print(f"Worst Query:          ID {worst_query} (Avg: {query_avgs[worst_query]:.2f}%)")
    print("-" * 40)
    print(f"Best Run:             Run {best_run} (Avg: {run_avgs[best_run]:.2f}%)")
    print(f"Worst Run:            Run {worst_run} (Avg: {run_avgs[worst_run]:.2f}%)")
    print("-" * 40)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Analyze Batch Evaluation Results')
    parser.add_argument('name', type=str, help='Name of the batch to analyze (e.g., v1)')
    
    args = parser.parse_args()
    analyze_batch(args.name)
