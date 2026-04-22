import os
import time
import argparse
import pandas as pd
from datetime import datetime
import test_eval


# ---------------------------------------------------------------------------
# Tunables
# ---------------------------------------------------------------------------
COOLDOWN_BETWEEN_RUNS_SEC = 5   # wait between full runs so TPM windows drain
RETRY_COOLDOWN_SEC = 5          # wait before re-running failed queries
MAX_QUERY_RETRIES = 5            # how many times to re-run queries that failed in a run


def _ensure_ids(queries):
    """Attach stable integer ids so retry runs can match rows back to originals.

    test_eval.run_tests assigns ids via `item.get('id', idx)` where idx comes
    from `enumerate(..., start=1)`. If we later pass only a subset of queries
    back in, their enumerate-index would collide with other ids. Injecting a
    stable 'id' up-front keeps Query IDs consistent across the original run
    and any retry passes.
    """
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
) -> pd.DataFrame:
    """Run one full evaluation pass, then re-run only the queries that hit
    transient errors (OpenAI TPM / Chroma tenant / connection errors).

    Re-running just the failed subset sends far fewer tokens, so the second
    pass is much less likely to hit the TPM ceiling. A cooldown between
    attempts lets the 60-second rate-limit window drain.
    """
    results_df = test_eval.run_tests(queries)

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
        retry_df = test_eval.run_tests(to_retry)

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
):
    """Runs the RAG evaluation multiple times sequentially and aggregates results."""
    os.makedirs('outputs/results', exist_ok=True)
    os.makedirs('outputs/batch_results', exist_ok=True)

    queries = _ensure_ids(test_eval.load_queries_from_json(json_file))
    all_scores = {}
    timestamp_batch = datetime.now().strftime('%Y%m%d_%H%M%S')

    def run_single_evaluation(run_idx):
        print(f"--- Starting Run {run_idx}/{num_runs} for '{name}' ---")

        results_df = run_with_query_retries(
            queries,
            max_retries=query_retries,
            retry_cooldown=retry_cooldown,
        )

        timestamp_run = datetime.now().strftime('%Y%m%d_%H%M%S')
        run_filename = f"outputs/results/{name}_{run_idx}_{timestamp_run}.csv"
        results_df.to_csv(run_filename, index=False)

        scores = results_df.set_index('Query ID')['Meaning Acc (%)'].to_dict()
        return f"Run {run_idx}", scores

    for i in range(1, num_runs + 1):
        run_name, scores = run_single_evaluation(i)
        all_scores[run_name] = scores
        print(f"Finished {run_name}")

        # Cooldown between runs (skip after the last one).
        if i < num_runs and cooldown > 0:
            print(f"  ...cooling down {cooldown:.0f}s before next run")
            time.sleep(cooldown)

    # Matrix: rows = run number, columns = query id.
    matrix_df = pd.DataFrame(all_scores).T

    run_cols = sorted(all_scores.keys(), key=lambda x: int(x.split(' ')[1]))
    matrix_df.index = pd.Categorical(matrix_df.index, categories=run_cols, ordered=True)
    matrix_df = matrix_df.sort_index()

    matrix_df.index = matrix_df.index.str.replace('Run ', '', regex=False)
    matrix_df.index.name = 'Run'

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
    )