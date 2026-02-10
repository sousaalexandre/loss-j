import re
from typing import List, Dict, Any
from langchain_core.runnables import RunnablePassthrough
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from src.services.vector_db import get_retriever
from src.services.llm_gateway import get_llm
from src.logger import log
import os

KNOWN_CATEGORIES = [
    "Praças",
    "Sargentos",
    "Oficiais",
    "RCE",
    "Remunerações",
    "Geral",
    "Recrutamento",
    "Manual de Utilizador",
    "Legislação",
    "Documentação",
    "Guião",
]

def _detect_categories_in_query(prompt: str) -> List[str]:
    """
    Returns the list of categories whose names appear in the query text.
    This is independent of the number of documents you add.
    """
    p = prompt.lower()
    found = []
    for cat in KNOWN_CATEGORIES:
        if cat.lower() in p:
            found.append(cat)
    return found


def _rerank_docs(docs, query: str) -> list:
    """
    Rerank retrieved docs using:
      - Soft boost if doc.category is explicitly mentioned in query.
      - Soft boost based on overlap between header_hierarchy and query tokens.
      
    We do NOT filter anything out – just reorder.
    """
    q = query.lower()
    q_tokens = set(re.findall(r"\w+", q))

    categories_in_query = _detect_categories_in_query(query)

    def score(doc, idx: int) -> float:
        s = 0.0
        md = doc.metadata or {}

        # 1) Category boost: generic and cheap
        doc_cat = (md.get("category") or "").strip()
        if doc_cat and doc_cat in categories_in_query:
            # small boost: enough to fix obvious mis-matches
            s += 2.0

        # 2) Header / query lexical overlap
        header = (md.get("header_hierarchy") or "").lower()
        if header:
            h_tokens = set(re.findall(r"\w+", header))
            overlap = len(q_tokens & h_tokens)
            # Each shared token gives a small bump
            s += 0.2 * overlap

        # 3) Keep original similarity order as a strong prior
        # (lower idx = more similar). Using negative idx so earlier docs are preferred.
        s += max(0.0, 5.0 - idx * 0.01)  # very small decay, mostly for tie-breaking

        return s

    scored = [(score(doc, i), i, doc) for i, doc in enumerate(docs)]
    scored.sort(key=lambda x: x[0], reverse=True)

    return [doc for _, _, doc in scored]


def _build_context(docs) -> str:
    """
    Join docs into a single context string, preserving reranked order.
    """
    parts = []
    for i, d in enumerate(docs):
        parts.append(d.page_content)
    return "\n\n-----\n\n".join(parts)


def query_handler(prompt: str) -> dict:
    """
    Processes a user query by retrieving relevant context and generating a response.
    """
    retriever = get_retriever()
    raw_docs = retriever.invoke(prompt)

    log(f"Retrieved {len(raw_docs)} documents for prompt: '{prompt}'", level="info")

    reranked_docs = _rerank_docs(raw_docs, prompt)
    for i, doc in enumerate(reranked_docs):
        file_name = os.path.basename(doc.metadata.get('source', 'Unknown Source'))
        log(
            f"Document {i+1} (from {file_name}):\n"
            f"Metadata: {doc.metadata}\n"
            f"Content: {doc.page_content[:200]}...\n",
            level="info"
        )

    context = _build_context(reranked_docs)

    llm = get_llm().bind(temperature=0)

    template = """
    Você é um assistente. Responda à pergunta do utilizador baseando-se APENAS no contexto a seguir.
    Se o contexto não contiver a resposta, diga "Não sei a resposta."
    Responda sempre em português de Portugal.
    Quando a pergunta se referir a uma categoria específica (Praças, Sargentos, Oficiais, RCE),
    dê mais importância à informação dessa categoria, mas NUNCA invente.

    Contexto:
    {context}

    Pergunta:
    {question}
    """
    prompt_template = ChatPromptTemplate.from_template(template)

    rag_chain = (
        prompt_template
        | llm
        | StrOutputParser()
    )

    response = rag_chain.invoke({"context": context, "question": prompt})

    return {"response": response, "documents": reranked_docs}
