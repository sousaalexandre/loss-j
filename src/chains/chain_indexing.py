import os
from src.data_processing import loader, splitter
from src.services import embedding_model
from src.vector_store.indexer import check_file_exists_vector_store, store
from src.utils import generate_file_hash
from src.logger import log

import os
from pathlib import Path
import datetime

def index_file(file_path: str) -> None:
    """
    Indexes a single file into the vector store. Check for duplicates before ingestion.
    """
    if (check_file_exists_vector_store(file_path)):
        log(f"--- ⚠️ Document '{os.path.basename(file_path)}' already exists. Skipping ingestion. ---", level="warning")
        return

    documents = loader.load_document_mineru(file_path)
    chunks = splitter.split_documents_markdown(documents)
    file_hash = generate_file_hash(file_path)

    # debug: save chunks to file
    # output_debug_dir = "outputs/logs/chunks/"
    # base_filename = Path(file_path).stem
    # os.makedirs(output_debug_dir, exist_ok=True)
    # output_chunk_path = os.path.join(output_debug_dir, f"{base_filename}_chunks_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
    # with open(output_chunk_path, "w", encoding="utf-8") as f:
    #    for i, chunk in enumerate(chunks):
    #        chunk.metadata["file_hash"] = file_hash
    #        f.write(f"\n\n--- CHUNK {i:03d} ---\n\n")
    #        f.write(chunk.page_content)
    # log(f"Saved all {len(chunks)} chunks to: {output_chunk_path}", level="info")

    embeddings = embedding_model.get_embedding_model()

    store(chunks, embeddings)

