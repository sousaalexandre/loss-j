from langchain_chroma import Chroma
from langchain_core.vectorstores import VectorStoreRetriever
from src.services.embedding_model import get_embedding_model
from src import settings
from collections import Counter
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

    embedding_function = get_embedding_model()

    vector_store = Chroma(
        persist_directory=settings.VECTOR_DB_PATH,
        embedding_function=embedding_function
    )

    return vector_store.as_retriever(search_kwargs={"k": settings.RETRIEVER_K})



#### UTILS #####
def get_database_summary():
    """
    Connects to ChromaDB and returns a summary of its contents.

    Returns:
        dict: A dictionary containing database statistics.
              Returns None if the database doesn't exist.
    """
    if not os.path.exists(settings.VECTOR_DB_PATH):
        return None

    vector_store = Chroma(persist_directory=settings.VECTOR_DB_PATH)
    data = vector_store.get()

    if not data or not data.get('ids'):
        return {
            "total_chunks": 0,
            "num_files": 0,
            "file_details": {}
        }

    metadatas = data.get('metadatas', [])
    total_chunks = len(metadatas)
    
    sources = [meta.get('source', 'Unknown Source') for meta in metadatas]
    source_counts = Counter(sources)
    
    unique_files = list(source_counts.keys())
    num_files = len(unique_files)

    file_details = {os.path.basename(name): count for name, count in source_counts.items()}

    return {
        "total_chunks": total_chunks,
        "num_files": num_files,
        "file_details": file_details
    }