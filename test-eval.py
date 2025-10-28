import json
import pandas as pd
import os
from datetime import datetime
from src.chains.chain_retrieving_generating import query_handler
from src.services.llm_generator import get_llm
from pydantic import BaseModel, Field


class AccuracyScore(BaseModel):
    """A model to store the semantic accuracy score."""
    score: float = Field(
        ...,
        description="A pontuação de 0 a 100, comparando a Resposta RAG com a Resposta Esperada, com base na Query.",
        ge=0,
        le=100 
    )

def get_llm_comparison_score(query: str, received: str, expected: str) -> float:
    llm = get_llm()

    llm_with_schema = llm.with_structured_output(AccuracyScore)

    prompt_template = """
    ### Tarefa de Avaliação (Português Europeu)

    És um avaliador especialista de um sistema RAG (Retrieval-Augmented Generation).
    A tua tarefa é avaliar a 'Resposta RAG' com base na 'Query' do utilizador, usando a 'Resposta Esperada' como a fonte de "verdade" ou a resposta ideal.

    Compara a 'Resposta RAG' com a 'Resposta Esperada' com base nos seguintes critérios:
    1.  **Precisão Factual:** A 'Resposta RAG' contém informação factualmente correta, tal como definido pela 'Resposta Esperada'?
    2.  **Completude:** A 'Resposta RAG' cobre todos os pontos-chave e o significado central presentes na 'Resposta Esperada'?

    ### Critérios de Pontuação (0-100)

    * **95-100 (Perfeita):** A 'Resposta RAG' responde perfeitamente à 'Query' e é 100% precisa e completa. Contém toda a informação chave da 'Resposta Esperada'. Diferenças de estilo são aceitáveis.
    * **80-94 (Boa):** A 'Resposta RAG' é factualmente precisa e responde bem à 'Query', mas pode omitir pequenos detalhes ou ser ligeiramente menos completa que a 'Resposta Esperada'.
    * **50-79 (Razoável):** A 'Resposta RAG' responde à 'Query', mas é visivelmente incompleta ou omite factos importantes presentes na 'Resposta Esperada'.
    * **10-49 (Fraca):** A 'Resposta RAG' contém imprecisões factuais, "alucinações" (informação que contradiz a 'Resposta Esperada') ou é maioritariamente irrelevante para a 'Query'.
    * **0 (Terrível):** Completamente errada ou irrelevante.

    Foca-te apenas na precisão factual e na completude. Não penalizes por diferenças de estilo ou formulação, *desde que* o significado central seja o mesmo.

    ---
    ### Dados para Avaliar

    **Query:**
    "{query}"

    **Resposta Esperada (Ideal):**
    "{expected}"

    **Resposta RAG (Recebida):**
    "{received}"
    """
    
    response = llm_with_schema.invoke(
        prompt_template.format(
            query=query,
            expected=expected,
            received=received
        )
    )

    return response.score



def load_queries_from_json(json_file_path: str) -> list:
    with open(json_file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data


    
def run_tests(queries: list) -> pd.DataFrame:
    results = []
    for item in queries:
        query = item['query']
        expected = item['expected']

        try:
            received = query_handler(query)
        except Exception as e:
            received = f"Error: {str(e)}"

        score = get_llm_comparison_score(
            query=query,
            received=received,
            expected=expected
        )

        results.append({
            'Query': query,
            'Received Response': received,
            'Expected Response': expected,
            'Meaning Acc (%)': score
        })
    return pd.DataFrame(results)

def main(json_file_path: str):
    queries = load_queries_from_json(json_file_path)
    results_df = run_tests(queries)
    
    os.makedirs('outputs', exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_path = f'outputs/results/test_results_{timestamp}.csv'
    
    results_df.to_csv(output_path, index=False)
    print(f"Results saved to {output_path}")

if __name__ == "__main__":
    json_file = 'query.json'
    main(json_file)