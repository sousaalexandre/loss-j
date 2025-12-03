import os
from src.data_processing import loader, splitter
from src.services import embedding_model
from src.vector_store.indexer import check_file_exists_vector_store, store
from src.logger import log


def index_file(file_path: str) -> None:
    """
    Indexes a single file into the vector store. Check for duplicates before ingestion.
    """
    if (check_file_exists_vector_store(file_path)):
        log(f"--- ⚠️ Document '{os.path.basename(file_path)}' already exists. Skipping ingestion. ---", level="warning")
        return

    log(f"PDF Name: {file_path}")

    documents = loader.load_document(file_path)
    chunks = splitter.split_documents(documents)

    embeddings = embedding_model.get_embedding_model()

    store(chunks, embeddings)

