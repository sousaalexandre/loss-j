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


def remove_document(file_name_or_hash: str) -> bool:
    """
    Remove a document from the vector store and the data lakehouse.
    """
    from pathlib import Path
    import json
    import shutil

    hash_name = Path(file_name_or_hash).stem.split('_')[0]

    log(f"Removing document with hash {hash_name}...", level="info")

    # 1. Remove from Vector Store
    if os.path.exists(settings.VECTOR_DB_PATH):
        vector_store = Chroma(persist_directory=settings.VECTOR_DB_PATH)
        data = vector_store.get()
        if data and data.get('ids'):
            ids_to_delete = []
            for idx, meta in zip(data['ids'], data.get('metadatas', [])):
                if meta.get('file_hash') == hash_name or hash_name in meta.get('source', ''):
                    ids_to_delete.append(idx)

            if ids_to_delete:
                vector_store.delete(ids_to_delete)
                log(f"Deleted {len(ids_to_delete)} chunks from vector store.", level="info")

    # 2. Remove from Bronze Layer
    bronze_dir = Path("data_lakehouse/01_bronze")
    if bronze_dir.exists():
        pdf_file = bronze_dir / f"{hash_name}.pdf"
        if pdf_file.exists():
            pdf_file.unlink()

        catalog_file = bronze_dir / "_catalog.json"
        if catalog_file.exists():
            with open(catalog_file, 'r', encoding='utf-8') as f:
                catalog = json.load(f)
            if hash_name in catalog:
                del catalog[hash_name]
                with open(catalog_file, 'w', encoding='utf-8') as f:
                    json.dump(catalog, f, indent=2, ensure_ascii=False)

    # 3. Remove from Gold Layer
    gold_dir = Path("data_lakehouse/03_gold")
    if gold_dir.exists():
        gold_file_dir = gold_dir / hash_name
        if gold_file_dir.exists() and gold_file_dir.is_dir():
            shutil.rmtree(gold_file_dir)

        for md_file in gold_dir.glob(f"{hash_name}_*.md"):
            md_file.unlink()

        gold_catalog_file = gold_dir / "_catalog.json"
        if gold_catalog_file.exists():
            with open(gold_catalog_file, 'r', encoding='utf-8') as f:
                catalog = json.load(f)
            if hash_name in catalog:
                del catalog[hash_name]
                with open(gold_catalog_file, 'w', encoding='utf-8') as f:
                    json.dump(catalog, f, indent=2, ensure_ascii=False)

    return True
