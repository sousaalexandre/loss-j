import os
from src.data_processing import loader, splitter
from src.services import embedding_model
from src.vector_store.indexer import check_file_exists_vector_store, store
from src.utils import generate_file_hash

def index_file(file_path: str) -> None:
    """
    Indexes a single file into the vector store. Check for duplicates before ingestion.
    """
    if (check_file_exists_vector_store(file_path)):
        print(f"--- ⚠️ Document '{os.path.basename(file_path)}' already exists. Skipping ingestion. ---")
        return

    documents = loader.load_document(file_path)
    chunks = splitter.split_documents(documents)
    file_hash = generate_file_hash(file_path)

    for chunk in chunks:
        chunk.metadata["file_hash"] = file_hash

    # Step 6: Get the embedding model
    embeddings = embedding_model.get_embedding_model()

    store(chunks, embeddings)

