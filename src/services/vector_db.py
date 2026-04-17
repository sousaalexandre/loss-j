import os
from langchain_chroma import Chroma
from langchain_core.vectorstores import VectorStoreRetriever
from src.services.embedder import get_embedding_model
from src import settings
from src.utils import generate_file_hash
from src.logger import log
from collections import Counter


def get_vector_store() -> Chroma:
    """
    Connects to the ChromaDB vector store. Creates it if it doesn't exist.

    Returns:
        Chroma: An instance of the Chroma vector store.
    """
    vector_store = Chroma(
        persist_directory=settings.VECTOR_DB_PATH,
        embedding_function=get_embedding_model()
    )

    return vector_store


def store(chunks: list, embeddings) -> None:
    """
    Store document chunks in the vector database.
    
    Args:
        chunks: List of document chunks to store
        embeddings: Embedding model to use
    """
    log("Storing document chunks in vector store...", level="info")
    
    # Check if database exists
    if os.path.exists(settings.VECTOR_DB_PATH):
        vector_store = Chroma(
            persist_directory=settings.VECTOR_DB_PATH,
            embedding_function=embeddings
        )
        vector_store.add_documents(chunks)
    else:
        Chroma.from_documents(
            documents=chunks,
            embedding=embeddings,
            persist_directory=settings.VECTOR_DB_PATH
        )


def check_file_exists_vector_store(file_path: str) -> bool:
    """
    Check if a file already exists in the vector store.
    
    Args:
        file_path: Path to the file to check
        
    Returns:
        bool: True if file exists in vector store, False otherwise
    """
    vector_store_instance = get_vector_store()
    # Check by source path (normalized to forward slashes)
    normalized_path = file_path.replace("\\", "/")
    results = vector_store_instance.get(where={"source": normalized_path})
    return len(results.get('ids', [])) > 0


_RETRIEVER_INSTANCE = None

def get_retriever() -> VectorStoreRetriever:
    """
    Creates and returns a retriever from the persisted ChromaDB vector store.
    Uses a singleton pattern to ensure thread-safety and prevent connection errors
    during high-concurrency access.
    """
    global _RETRIEVER_INSTANCE
    
    if _RETRIEVER_INSTANCE is not None:
        return _RETRIEVER_INSTANCE

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

    _RETRIEVER_INSTANCE = vector_store.as_retriever(search_kwargs={"k": settings.RETRIEVER_K})
    return _RETRIEVER_INSTANCE


def is_file_already_indexed(file_hash: str) -> bool:
    """
    Check if a file (by hash) is already indexed in the vectorstore.
    
    Args:
        file_hash: SHA-256 hash of the file
        
    Returns:
        bool: True if file is already indexed, False otherwise
    """
    if not os.path.exists(settings.VECTOR_DB_PATH):
        return False
    
    vector_store = Chroma(persist_directory=settings.VECTOR_DB_PATH)
    data = vector_store.get()
    
    if not data or not data.get('ids'):
        return False
    
    metadatas = data.get('metadatas', [])
    for meta in metadatas:
        if meta.get('file_hash') == file_hash:
            return True
    
    return False


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
