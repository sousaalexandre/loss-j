from langchain_core.runnables import RunnablePassthrough
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from src.vector_store.retriever import get_retriever
from src.services.llm_generator import get_llm
import os

def query_handler(prompt: str) -> str:
    """
    Processes a user query by retrieving relevant context and generating a response.

    Args:
        prompt (str): The user's question.

    Returns:
        str: The generated response from the RAG pipeline.
    """
    retriever = get_retriever()


    retrieved_docs = retriever.invoke(prompt)
    print(f"Retrieved {len(retrieved_docs)} documents for prompt: '{prompt}'")
    for i, doc in enumerate(retrieved_docs):
        file_name = os.path.basename(doc.metadata.get('source', 'Unknown Source'))
        print(f"Document {i+1} (from {file_name}): {doc.page_content[:200]}...")  # Print file name and first 200 chars of each doc


    llm = get_llm()

    template = """
    Você é um assistente. Responda à pergunta do utilizador baseando-se APENAS no contexto a seguir.
    Se o contexto não contiver a resposta, diga que não sabe a resposta.
    Responda sempre em português de Portugal

    Contexto:
    {context}

    Pergunta:
    {question}
    """
    prompt_template = ChatPromptTemplate.from_template(template)

    rag_chain = (
        {"context": retriever, "question": RunnablePassthrough()}
        | prompt_template
        | llm
        | StrOutputParser()
    )

    return rag_chain.invoke(prompt)