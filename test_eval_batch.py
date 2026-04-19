import os
import argparse
import pandas as pd
from datetime import datetime
import test_eval


from concurrent.futures import ThreadPoolExecutor

def run_batch_evaluation(name: str, num_runs: int, json_file: str):
    """
    Runs the RAG evaluation multiple times in parallel and aggregates results.
    """
    os.makedirs('outputs/results', exist_ok=True)
    os.makedirs('outputs/batch_results', exist_ok=True)
    
    queries = test_eval.load_queries_from_json(json_file)
    all_scores = {}
    timestamp_batch = datetime.now().strftime('%Y%m%d_%H%M%S')

    def run_single_evaluation(run_idx):
        print(f"--- Starting Run {run_idx}/{num_runs} for '{name}' ---")
        
        # Run the tests (this is already parallelized inside test_eval)
        results_df = test_eval.run_tests(queries)
        
        # Save individual run file
        timestamp_run = datetime.now().strftime('%Y%m%d_%H%M%S')
        run_filename = f"outputs/results/{name}_{run_idx}_{timestamp_run}.csv"
        results_df.to_csv(run_filename, index=False)
        
        # Return the scores for this run
        scores = results_df.set_index('Query ID')['Meaning Acc (%)'].to_dict()
        return f"Run {run_idx}", scores

    # We use a smaller worker count for runs to avoid overwhelming the system,
    # since each run itself uses 10 threads.
    # 5 concurrent runs * 10 threads per run = 50 concurrent API calls.
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(run_single_evaluation, i) for i in range(1, num_runs + 1)]
        for future in futures:
            run_name, scores = future.result()
            all_scores[run_name] = scores
            print(f"Finished {run_name}")

    # Create the matrix: Rows = Run Number, Columns = Query ID
    matrix_df = pd.DataFrame(all_scores).T
    
    # Sort the index numerically by run number
    run_cols = sorted(all_scores.keys(), key=lambda x: int(x.split(' ')[1]))
    matrix_df.index = pd.Categorical(matrix_df.index, categories=run_cols, ordered=True)
    matrix_df = matrix_df.sort_index()
    
    # Change index from "Run 1" to "1"
    matrix_df.index = matrix_df.index.str.replace('Run ', '', regex=False)
    matrix_df.index.name = 'Run'
    
    # Sort columns by Query ID
    matrix_df = matrix_df.reindex(columns=sorted(matrix_df.columns))
    
    matrix_output_path = f"outputs/batch_results/{name}_matrix_{timestamp_batch}.csv"
    matrix_df.to_csv(matrix_output_path)
    
    print(f"\nBatch evaluation complete!")
    print(f"Summary matrix saved to: {matrix_output_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Batch RAG Evaluation')
    parser.add_argument('--name', type=str, required=True, help='Name for this batch of runs')
    parser.add_argument('--runs', type=int, required=True, help='Number of times to run the evaluation')
    parser.add_argument('--json', type=str, default='query.json', help='Path to queries JSON file (default: query.json)')
    
    args = parser.parse_args()
    
    run_batch_evaluation(args.name, args.runs, args.json)
