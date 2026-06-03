import os
import pandas as pd
import glob
import sys
import argparse

def analyze_batch(name):
    """
    Finds the most recent matrix and timings for a given batch name and displays combined statistics
    across all DeepEval metrics.
    """
    matrix_dir = "outputs/deepeval/batch_results/matrix"
    timings_dir = "outputs/deepeval/batch_results/timings"
    
    timings_pattern = os.path.join(timings_dir, f"{name}_timings_*.csv")
    timings_files = glob.glob(timings_pattern)
    
    print(f"==================================================")
    print(f" BATCH ANALYSIS REPORT: {name.upper()}")
    print(f"==================================================")

    # --- PERFORMANCE TIMINGS SECTION ---
    if not timings_files:
        print(f"\n[!] No timing files found for batch name '{name}' in {timings_dir}")
    else:
        latest_timings = max(timings_files, key=os.path.getmtime)
        print(f"\nAnalyzing Performance Timings: {latest_timings}")
        
        df_timings = pd.read_csv(latest_timings)
        timing_cols = [c for c in df_timings.columns if '(s)' in c]
        
        if not timing_cols:
            print("[!] No timing columns found in the CSV.")
        else:
            averages = df_timings[timing_cols].mean()
            
            print("-" * 45)
            print(" PERFORMANCE STATISTICS (AVERAGES PER RUN)")
            print("-" * 45)
            for col in timing_cols:
                print(f"Average {col:25} {averages[col]:.5f} s")
            print("-" * 45)
            print(f"Total Runs:           {len(df_timings)}")
            print("-" * 45)


    # --- ACCURACY RESULTS SECTION ---
    metrics_to_track = [
        'answer_relevancy', 'faithfulness', 'contextual_precision', 
        'contextual_recall', 'contextual_relevancy', 'correctness'
    ]
    
    print("\n" + "=" * 50)
    print(" DEEPEVAL METRICS STATISTICS")
    print("=" * 50)

    found_any = False
    for metric in metrics_to_track:
        matrix_pattern = os.path.join(matrix_dir, f"{name}_{metric}_matrix_*.csv")
        matrix_files = glob.glob(matrix_pattern)
        
        if not matrix_files:
            continue
            
        found_any = True
        latest_matrix = max(matrix_files, key=os.path.getmtime)
        df_matrix = pd.read_csv(latest_matrix, index_col=0)
        
        total_avg = df_matrix.values.mean()
        query_avgs = df_matrix.mean(axis=0)
        best_query = query_avgs.idxmax()
        worst_query = query_avgs.idxmin()
        run_avgs = df_matrix.mean(axis=1)
        best_run = run_avgs.idxmax()
        worst_run = run_avgs.idxmin()

        display_name = metric.replace('_', ' ').title()
        
        print(f"\nMetric: {display_name}")
        print("-" * 45)
        print(f"Overall Average Score: {total_avg:.2f}%")
        print(f"Total Runs:           {len(df_matrix)}")
        print(f"Total Queries:        {len(df_matrix.columns)}")
        print("-" * 45)
        print(f"Best Query:           ID {best_query} (Avg: {query_avgs[best_query]:.2f}%)")
        print(f"Worst Query:          ID {worst_query} (Avg: {query_avgs[worst_query]:.2f}%)")
        print("-" * 45)
        print(f"Best Run:             Run {best_run} (Avg: {run_avgs[best_run]:.2f}%)")
        print(f"Worst Run:            Run {worst_run} (Avg: {run_avgs[worst_run]:.2f}%)")
        print("-" * 45)

    if not found_any:
        print(f"\n[!] No matrix files found for batch name '{name}' in {matrix_dir}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Unified Batch Evaluation and Performance Analyzer for DeepEval')
    parser.add_argument('name', type=str, help='Name of the batch to analyze (e.g., gptoss_batch1)')
    
    args = parser.parse_args()
    analyze_batch(args.name)
