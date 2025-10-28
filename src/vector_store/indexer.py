import os
from src.services import embedding_model
from src import settings
from src.utils import generate_file_hash
from langchain_chroma import Chroma

def get_vector_store() -> Chroma:
    """
    Connects to the existing ChromaDB vector store.

    Returns:
        Chroma: An instance of the Chroma vector store.
    
    Raises:
        FileNotFoundError: If the database path does not exist.
    """
    if not os.path.exists(settings.VECTOR_DB_PATH):
        raise FileNotFoundError(
            f"Vector database not found at path: {settings.VECTOR_DB_PATH}. "
            "Please run the ingestion script first."
        )

    vector_store = Chroma(
        persist_directory=settings.VECTOR_DB_PATH,
        embedding_function=embedding_model.get_embedding_model()
    )

    return vector_store

def store(chunks:list, embeddings) -> None:
    Chroma.from_documents(
        documents=chunks,
        embedding=embeddings,
        persist_directory=settings.VECTOR_DB_PATH
    )


#### UTILS ######
def check_file_exists_vector_store(file_path: str) -> bool:
    file_hash = generate_file_hash(file_path)
    vector_store_instance = get_vector_store()
    results = vector_store_instance.get(where={"file_hash": file_hash})
    return len(results.get('ids', [])) > 0

