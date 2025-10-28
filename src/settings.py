import os
from dotenv import load_dotenv

load_dotenv()

LLM_MODEL_NAME = "gpt-4o-mini"
EMBEDDING_MODEL_NAME = "text-embedding-3-small"

# --- Vector Database Configuration ---
# The path where the ChromaDB vector store will be persisted
VECTOR_DB_PATH = "vectorstore_db"

# --- Text Splitting Configuration ---
# The target size for each text chunk in characters
CHUNK_SIZE = 1000
# The number of characters to overlap between adjacent chunks
CHUNK_OVERLAP = 200


# --- Retriever Configuration ---
RETRIEVER_K = 5  # Number of relevant documents to retrieve