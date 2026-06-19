import json
import pandas as pd
import os
import time
import nest_asyncio
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import threading

# Apply nest_asyncio to allow asyncio to run within ThreadPoolExecutor threads
nest_asyncio.apply()

from src.api.query_handler import query_handler

# Lock to ensure local RAG models are queried sequentially
rag_lock = threading.Lock()

# DeepEval imports
from deepeval.metrics import AnswerRelevancyMetric, FaithfulnessMetric, ContextualPrecisionMetric, GEval, ContextualRecallMetric, ContextualRelevancyMetric
from deepeval.test_case import LLMTestCase, SingleTurnParams

# Suppress telemetry warnings
os.environ["ANONYMIZED_TELEMETRY"] = "False"
os.environ["CHROMA_TELEMETRY"] = "False"
os.environ["DEEPEVAL_TELEMETRY_OPT_OUT"] = "YES"
os.environ["POSTHOG_DISABLED"] = "1"
os.environ["DO_NOT_TRACK"] = "1"


def load_queries_from_json(json_file_path: str) -> list:
    """Load test queries and expected responses from a JSON file."""
    with open(json_file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data


def run_tests(queries: list, query_workers: int = 3, metric_workers: int = 5) -> pd.DataFrame:
    """Execute complete RAG generation and DeepEval metrics on queries."""
    
    model_name = "gpt-5.4-nano"
    
    # Custom model wrapper to increase httpx timeout and HTTP-level retries
    from deepeval.models import DeepEvalBaseLLM
    from langchain_openai import ChatOpenAI

    class CustomGPTModel(DeepEvalBaseLLM):
        def __init__(self, m_name: str):
            self.m_name = m_name
            # Increase timeout to 300s to avoid asyncio.TimeoutError during heavy batch concurrency
            # Also enforce JSON object response format to avoid DeepEval invalid JSON errors
            self.chat_model = ChatOpenAI(
                model=m_name, 
                max_retries=10, 
                timeout=300,
                model_kwargs={"response_format": {"type": "json_object"}}
            )

        def load_model(self):
            return self.chat_model

        def generate(self, prompt: str, **kwargs) -> str:
            if "schema" in kwargs:
                # Natively enforce the schema using LangChain and the LLM's Structured Outputs API
                schema = kwargs["schema"]
                res = self.chat_model.with_structured_output(schema).invoke(prompt)
                return res.model_dump_json()
            return self.chat_model.invoke(prompt).content

        async def a_generate(self, prompt: str, **kwargs) -> str:
            if "schema" in kwargs:
                # Natively enforce the schema using LangChain and the LLM's Structured Outputs API
                schema = kwargs["schema"]
                res = await self.chat_model.with_structured_output(schema).ainvoke(prompt)
                return res.model_dump_json()
            res = await self.chat_model.ainvoke(prompt)
            return res.content

        def get_model_name(self):
            return self.m_name

    eval_model = CustomGPTModel(model_name)
    
    def process_single_query(item, idx, metric_executor):
        query_id = item.get('id', idx)
        query = item['query']
        expected = item['expected']

        print(f"\n🔍 Testing Query {query_id}: {query}")

        try:
            # 1. RAG GENERATION
            with rag_lock:
                # Local models cannot handle concurrent requests, so lock generation step
                received_data = query_handler(query)
                
            received = received_data["response"].replace('\n', ' ').replace('\r', ' ')
            
            timings = received_data.get("timings", {})
            time_retrieval = timings.get("time_retrieval", 0.0)
            time_reranking = timings.get("time_reranking", 0.0)
            time_generation = timings.get("time_generation", 0.0)
            
            retrieval_context = [doc.page_content for doc in received_data.get("documents", [])]

            # 2. DEEPEVAL EVALUATION
            test_case = LLMTestCase(
                input=query,
                actual_output=received,
                expected_output=expected,
                retrieval_context=retrieval_context
            )
            
            t0_eval = time.perf_counter()
            
            answer_relevancy = AnswerRelevancyMetric(threshold=0.5, model=eval_model, include_reason=False, async_mode=False, strict_mode=False)
            faithfulness = FaithfulnessMetric(threshold=0.5, model=eval_model, include_reason=False, async_mode=False, strict_mode=False)
            contextual_precision = ContextualPrecisionMetric(threshold=0.5, model=eval_model, include_reason=False, async_mode=False, strict_mode=False)
            contextual_recall = ContextualRecallMetric(threshold=0.5, model=eval_model, include_reason=False, async_mode=False, strict_mode=False)
            contextual_relevancy = ContextualRelevancyMetric(threshold=0.5, model=eval_model, include_reason=False, async_mode=False, strict_mode=False)
            correctness = GEval(
                name="Correctness",
                criteria=(
                    "Avalia a exatidão da resposta recebida (Actual Output) em relação à resposta esperada (Expected Output), "
                    "tendo em conta a pergunta do utilizador (Input). "
                    "Foca-te em dois aspetos: "
                    "1. Precisão Factual: A resposta recebida contém informação factualmente correta tal como definido na resposta esperada? "
                    "2. Completude: Cobre todos os pontos-chave e o significado central presentes na resposta esperada? "
                    "Não penalizes por diferenças de estilo ou formulação, desde que o significado central seja o mesmo."
                ),
                evaluation_params=[
                    SingleTurnParams.INPUT, 
                    SingleTurnParams.ACTUAL_OUTPUT, 
                    SingleTurnParams.EXPECTED_OUTPUT
                ],
                threshold=0.5,
                model=eval_model,
                async_mode=False
            )
            
            metrics_to_run = [answer_relevancy, faithfulness, contextual_precision, contextual_recall, contextual_relevancy, correctness]
            
            import random
            
            def measure_staggered(m):
                # Add a tiny random delay (50-250ms) to prevent hitting the API with perfectly simultaneous request bursts
                time.sleep(random.uniform(0.05, 0.25))
                return m.measure(test_case)
            
            # Run metrics concurrently, but bound global concurrency using the shared metric_executor
            max_eval_retries = 3
            for attempt in range(max_eval_retries):
                try:
                    list(metric_executor.map(measure_staggered, metrics_to_run))
                    break  # Success, exit the retry loop
                except Exception as eval_e:
                    if attempt < max_eval_retries - 1:
                        sleep_time = (2 ** attempt) + 1  # exponential backoff
                        print(f"⚠️ Evaluation API error for Query {query_id}: {str(eval_e)}. Retrying {attempt+1}/{max_eval_retries} in {sleep_time}s...")
                        time.sleep(sleep_time)
                    else:
                        raise eval_e  # Failed all retries, raise to the main error handler
            
            time_evaluation = time.perf_counter() - t0_eval
            time_total = time_retrieval + time_reranking + time_generation + time_evaluation
            
            ar_score = (answer_relevancy.score * 100) if answer_relevancy.score is not None else 0.0
            f_score = (faithfulness.score * 100) if faithfulness.score is not None else 0.0
            cp_score = (contextual_precision.score * 100) if contextual_precision.score is not None else 0.0
            cr_score = (contextual_recall.score * 100) if contextual_recall.score is not None else 0.0
            crel_score = (contextual_relevancy.score * 100) if contextual_relevancy.score is not None else 0.0
            c_score = (correctness.score * 100) if correctness.score is not None else 0.0
            
            print(f"✅ Evaluation {query_id} finished in {time_total:.1f}s")
            
        except Exception as e:
            print(f"❌ Error processing query {query_id}: {str(e)}")
            return {
                'Query ID': query_id,
                'Query': query,
                'Received Response': f"Error: {str(e)}",
                'Expected Response': expected,
                'Answer Relevancy (%)': 0.0,
                'Faithfulness (%)': 0.0,
                'Contextual Precision (%)': 0.0,
                'Contextual Recall (%)': 0.0,
                'Contextual Relevancy (%)': 0.0,
                'Correctness (%)': 0.0,
                'Time Retrieval (s)': 0.0,
                'Time Reranking (s)': 0.0,
                'Time Generation (s)': 0.0,
                'Time Evaluation (s)': 0.0,
                'Time Total (s)': 0.0
            }

        return {
            'Query ID': query_id,
            'Query': query,
            'Received Response': received,
            'Expected Response': expected,
            'Answer Relevancy (%)': ar_score,
            'Faithfulness (%)': f_score,
            'Contextual Precision (%)': cp_score,
            'Contextual Recall (%)': cr_score,
            'Contextual Relevancy (%)': crel_score,
            'Correctness (%)': c_score,
            'Time Retrieval (s)': time_retrieval,
            'Time Reranking (s)': time_reranking,
            'Time Generation (s)': time_generation,
            'Time Evaluation (s)': time_evaluation,
            'Time Total (s)': time_total
        }

    results = []
    # metric_executor bounds the total number of concurrent API calls for metrics evaluation
    with ThreadPoolExecutor(max_workers=metric_workers) as metric_executor:
        with ThreadPoolExecutor(max_workers=query_workers) as query_executor:
            futures = [query_executor.submit(process_single_query, item, i, metric_executor) for i, item in enumerate(queries, start=1)]
            for future in futures:
                results.append(future.result())

    results_df = pd.DataFrame(results)
    if 'Query ID' in results_df.columns:
        results_df['Query ID'] = pd.to_numeric(results_df['Query ID'], errors='coerce')
        results_df = results_df.sort_values(by='Query ID', ascending=True, na_position='last')
    
    return results_df


def main() -> None:
    import sys
    
    run_name = "run"
    if len(sys.argv) > 1:
        run_name = sys.argv[1]
        
    json_file_path = 'query.json'
    print(f"Loading queries from: {json_file_path}")
    
    queries = load_queries_from_json(json_file_path)
    results_df = run_tests(queries)
    
    original_cols = [
        'Query ID', 'Query', 'Received Response', 'Expected Response', 
        'Answer Relevancy (%)', 'Faithfulness (%)', 'Contextual Precision (%)', 
        'Contextual Recall (%)', 'Contextual Relevancy (%)', 'Correctness (%)'
    ]
    save_df = results_df[[c for c in original_cols if c in results_df.columns]]
    
    output_dir = 'outputs/deepeval/results'
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_path = os.path.join(output_dir, f'{run_name}_eval_{timestamp}.csv')
    
    save_df.to_csv(output_path, index=False)
    print(f"\n📊 Evaluation Results saved to {output_path}")


if __name__ == "__main__":
    main()
