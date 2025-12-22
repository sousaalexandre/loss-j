from langchain_core.runnables import RunnablePassthrough
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from src.rag.retrieval import retrieve_documents, get_retriever
from src.services.llm_gateway import get_llm
from src.logger import log
import os

def query_handler(prompt: str) -> dict:
    """
    Processes a user query by retrieving relevant context and generating a response.

    Args:
        prompt (str): The user's question.

    Returns:
        dict: A dictionary containing the generated response and the list of retrieved documents.
    """
    retrieved_docs = retrieve_documents(prompt)

    llm = get_llm()
    llm = llm.bind(temperature=0)

    template = """
    Você é um assistente. Responda à pergunta do utilizador baseando-se APENAS no contexto a seguir.
    Se o contexto não contiver a resposta, diga \"Não sei a resposta.\"
    Responda sempre em português de Portugal

    Contexto:
    {context}

    Pergunta:
    {question}
    """
    prompt_template = ChatPromptTemplate.from_template(template)

    retriever = get_retriever()
    rag_chain = (
        {"context": retriever, "question": RunnablePassthrough()}
        | prompt_template
        | llm
        | StrOutputParser()
    )

    response = rag_chain.invoke(prompt)
    return {"response": response, "documents": retrieved_docs}