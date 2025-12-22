from langchain_core.vectorstores import VectorStoreRetriever
from src.services.vector_db import get_vector_store
from src import settings
from src.logger import log
import os


def get_retriever() -> VectorStoreRetriever:
    """
    Creates and returns a retriever from the persisted ChromaDB vector store.

    This function connects to the existing database, initializes it with the
    correct embedding model, and returns an object capable of searching for
    relevant documents.

    Returns:
        VectorStoreRetriever: An object configured to search the vector database.
    
    Raises:
        FileNotFoundError: If the database path does not exist.
    """
    if not os.path.exists(settings.VECTOR_DB_PATH):
        raise FileNotFoundError(
            f"Vector database not found at path: {settings.VECTOR_DB_PATH}. "
            "Please run the ingestion script first."
        )

    vector_store = get_vector_store()
    return vector_store.as_retriever(search_kwargs={"k": settings.RETRIEVER_K})


def retrieve_documents(query: str) -> list:
    """
    Retrieve relevant documents for a given query.
    
    Args:
        query (str): The search query
        
    Returns:
        list: List of relevant Document objects
    """
    retriever = get_retriever()
    retrieved_docs = retriever.invoke(query)
    log(f"Retrieved {len(retrieved_docs)} documents for query: '{query}'", level="info")
    for i, doc in enumerate(retrieved_docs):
        file_name = os.path.basename(doc.metadata.get('source', 'Unknown Source'))
        log(f"Document {i+1} (from {file_name}): {doc.page_content[:200]}...", level="info")
    return retrieved_docs
