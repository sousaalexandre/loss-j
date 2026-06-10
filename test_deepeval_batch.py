import os
import time
import argparse
import pandas as pd
from datetime import datetime
import test_deepeval

# ---------------------------------------------------------------------------
# Tunables
# ---------------------------------------------------------------------------
COOLDOWN_BETWEEN_RUNS_SEC = 0
RETRY_COOLDOWN_SEC = 1              # wait before re-running failed queries
MAX_QUERY_RETRIES = 5               # how many times to re-run queries that failed in a run
QUERY_WORKERS = 3                   # concurrent queries
METRIC_WORKERS = 5                  # global max concurrent metric evaluations


def _ensure_ids(queries):
    """Attach stable integer ids so retry runs can match rows back to originals."""
    out = []
    for i, q in enumerate(queries, start=1):
        qc = dict(q)
        qc.setdefault('id', i)
        out.append(qc)
    return out


def _failed_mask(df: pd.DataFrame) -> pd.Series:
    return df['Received Response'].astype(str).str.startswith('Error:')


def run_with_query_retries(
    queries,
    max_retries: int = MAX_QUERY_RETRIES,
    retry_cooldown: float = RETRY_COOLDOWN_SEC,
    query_workers: int = QUERY_WORKERS,
    metric_workers: int = METRIC_WORKERS,
) -> pd.DataFrame:
    """Run one full evaluation pass, then re-run only the queries that hit errors."""
    results_df = test_deepeval.run_tests(queries, query_workers=query_workers, metric_workers=metric_workers)

    for attempt in range(1, max_retries + 1):
        failed = results_df[_failed_mask(results_df)]
        n_failed = len(failed)
        if n_failed == 0:
            break

        failed_ids = set(failed['Query ID'].tolist())
        print(
            f"  [query-retry {attempt}/{max_retries}] "
            f"{n_failed} failed queries — cooling down {retry_cooldown:.0f}s, "
            f"then retrying ids={sorted(failed_ids)}"
        )
        time.sleep(retry_cooldown)

        to_retry = [q for q in queries if q['id'] in failed_ids]
        retry_df = test_deepeval.run_tests(to_retry, query_workers=query_workers, metric_workers=metric_workers)

        # Keep only successful retries, swap them in for the failed rows.
        successful = retry_df[~_failed_mask(retry_df)]
        if len(successful) > 0:
            succeeded_ids = set(successful['Query ID'].tolist())
            results_df = results_df[~results_df['Query ID'].isin(succeeded_ids)]
            results_df = pd.concat([results_df, successful], ignore_index=True)
            results_df = (
                results_df
                .sort_values(by='Query ID', ascending=True, na_position='last')
                .reset_index(drop=True)
            )

    still_failed = int(_failed_mask(results_df).sum())
    if still_failed:
        print(f"  ! {still_failed} query(ies) still failed after {max_retries} retries")

    return results_df


def run_batch_evaluation(
    name: str,
    num_runs: int,
    json_file: str,
    cooldown: float = COOLDOWN_BETWEEN_RUNS_SEC,
    query_retries: int = MAX_QUERY_RETRIES,
    retry_cooldown: float = RETRY_COOLDOWN_SEC,
    query_workers: int = QUERY_WORKERS,
    metric_workers: int = METRIC_WORKERS,
):
    """Runs the DeepEval RAG evaluation multiple times sequentially and aggregates results."""
    os.makedirs('outputs/deepeval/results', exist_ok=True)
    os.makedirs('outputs/deepeval/batch_results/matrix', exist_ok=True)
    os.makedirs('outputs/deepeval/batch_results/timings', exist_ok=True)

    queries = _ensure_ids(test_deepeval.load_queries_from_json(json_file))
    
    metrics_to_track = [
        'Answer Relevancy (%)', 'Faithfulness (%)', 'Contextual Precision (%)', 
        'Contextual Recall (%)', 'Contextual Relevancy (%)', 'Correctness (%)'
    ]
    
    all_scores = {m: {} for m in metrics_to_track}
    all_timings = []
    timestamp_batch = datetime.now().strftime('%Y%m%d_%H%M%S')

    def run_single_evaluation(run_idx):
        print(f"--- Starting Run {run_idx}/{num_runs} for '{name}' ---")

        results_df = run_with_query_retries(
            queries,
            max_retries=query_retries,
            retry_cooldown=retry_cooldown,
            query_workers=query_workers,
            metric_workers=metric_workers,
        )

        timestamp_run = datetime.now().strftime('%Y%m%d_%H%M%S')
        run_filename = f"outputs/deepeval/results/{name}_{run_idx}_{timestamp_run}.csv"
        
        timing_keys = {
            'Time Retrieval (s)': 'Retrieval Time (s)',
            'Time Generation (s)': 'Generation Time (s)',
            'Time Evaluation (s)': 'Evaluation Time (s)',
            'Time Total (s)': 'Total Time (s)'
        }
        
        run_timing = {'Run': run_idx}
        for col, label in timing_keys.items():
            if col in results_df.columns:
                total_stage_time = float(results_df[col].sum())
                run_timing[label] = round(total_stage_time, 5)
            else:
                run_timing[label] = 0.0

        run_timing_df = pd.DataFrame([run_timing])
        
        original_cols = ['Query ID', 'Query', 'Received Response', 'Expected Response'] + metrics_to_track
        save_df = results_df[[c for c in original_cols if c in results_df.columns]]
        save_df.to_csv(run_filename, index=False)

        # Extract scores for each metric
        scores_dict = {}
        for metric in metrics_to_track:
            if metric in results_df.columns:
                scores_dict[metric] = results_df.set_index('Query ID')[metric].to_dict()
                
        return f"Run {run_idx}", scores_dict, run_timing_df

    for i in range(1, num_runs + 1):
        run_name, scores_dict, timings_df = run_single_evaluation(i)
        
        for metric, scores in scores_dict.items():
            all_scores[metric][run_name] = scores
            
        if not timings_df.empty:
            all_timings.append(timings_df)
        print(f"Finished {run_name}")

        if i < num_runs and cooldown > 0:
            print(f"  ...cooling down {cooldown:.0f}s before next run")
            time.sleep(cooldown)

    # Matrix: rows = run number, columns = query id.
    # We generate one matrix CSV per metric
    for metric in metrics_to_track:
        if not all_scores[metric]:
            continue
            
        matrix_df = pd.DataFrame(all_scores[metric]).T

        run_cols = sorted(all_scores[metric].keys(), key=lambda x: int(x.split(' ')[1]))
        matrix_df.index = pd.Categorical(matrix_df.index, categories=run_cols, ordered=True)
        matrix_df = matrix_df.sort_index()

        matrix_df.index = matrix_df.index.str.replace('Run ', '', regex=False)
        matrix_df.index.name = 'Run'

        matrix_df = matrix_df.reindex(columns=sorted(matrix_df.columns))

        safe_metric_name = metric.replace(' (%)', '').replace(' ', '_').lower()
        matrix_output_path = f"outputs/deepeval/batch_results/matrix/{name}_{safe_metric_name}_matrix_{timestamp_batch}.csv"
        matrix_df.to_csv(matrix_output_path)

    print(f"\nBatch evaluation complete!")
    print(f"Summary matrices saved to: outputs/deepeval/batch_results/matrix/")

    # Save aggregated timings
    if all_timings:
        final_timings_df = pd.concat(all_timings, ignore_index=True)
        timings_output_path = f"outputs/deepeval/batch_results/timings/{name}_timings_{timestamp_batch}.csv"
        final_timings_df.to_csv(timings_output_path, index=False)
        print(f"Timings saved to: {timings_output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Batch DeepEval RAG Evaluation')
    parser.add_argument('--name', type=str, required=True, help='Name for this batch of runs')
    parser.add_argument('--runs', type=int, required=True, help='Number of times to run the evaluation')
    parser.add_argument('--json', type=str, default='query.json', help='Path to queries JSON file (default: query.json)')
    parser.add_argument('--cooldown', type=float, default=COOLDOWN_BETWEEN_RUNS_SEC,
                        help=f'Seconds to wait between runs (default: {COOLDOWN_BETWEEN_RUNS_SEC})')
    parser.add_argument('--query-retries', type=int, default=MAX_QUERY_RETRIES,
                        help=f'Max times to re-run failed queries within a run (default: {MAX_QUERY_RETRIES})')
    parser.add_argument('--retry-cooldown', type=float, default=RETRY_COOLDOWN_SEC,
                        help=f'Seconds to wait before retrying failed queries (default: {RETRY_COOLDOWN_SEC})')

    args = parser.parse_args()

    run_batch_evaluation(
        name=args.name,
        num_runs=args.runs,
        json_file=args.json,
        cooldown=args.cooldown,
        query_retries=args.query_retries,
        retry_cooldown=args.retry_cooldown,
        query_workers=QUERY_WORKERS,
        metric_workers=METRIC_WORKERS,
    )
