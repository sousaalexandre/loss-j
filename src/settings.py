from dotenv import load_dotenv

load_dotenv()


# ============================================================================
# INGESTION & ENVIRONMENT SETTINGS
# ============================================================================

# Loader type determines the ingestion pipeline
# "mineru": PDF → Markdown (ETL) → Cleaning → Indexing 
# "pdfloader": PDF → Indexing directly (faster, no cleaning)
# "docling": PDF → Markdown via Docling (ETL) → Indexing
# "docling-images": PDF → Markdown with Images descriptions via Docling (ETL) → Indexing with image support
LOADER_TYPE = "docling"


# ============================================================================
# MINERU ETL CONFIGURATION
# ============================================================================
# Only applicable when LOADER_TYPE = "mineru"

# MinerU conversion backend
# "vlm-http-client": Use remote VLM server (faster, requires server running)
# "pipeline": Use local pipeline (slower but no dependencies)
MINERU_BACKEND = "pipeline"
MINERU_VLM_HTTP_URL = "http://192.168.103.9:30000"

# ETL Cleaning Options (applied after PDF→MD conversion)
ENABLE_HTML_CLEANING = False
ENABLE_LATEX_CLEANING = False
ENABLE_HIERARCHY_REBUILDING = False
HIERARCHY_REBUILDING_MODE = "font"  # "font" or "llm"


# ============================================================================
# RAG DOCUMENT SPLITTING CONFIGURATION
# ============================================================================
# Controls how documents are split for retrieval

# Splitting strategy
# "recursive": Basic recursive text splitter
# "markdown_recursive": Markdown-aware splitter (preserves structure)
# "hierarchical": Hierarchical splitter (sections, subsections, etc.)
SPLITTING_TYPE = "hierarchical"

# Chunk parameters
CHUNK_SIZE = 1000          # Characters per chunk
CHUNK_OVERLAP = 200        # Characters of overlap between chunks


# ============================================================================
# LLM & EMBEDDING MODELS
# ============================================================================

# Cloud Models (Default)
LLM_MODEL_NAME = "gpt-4o-mini"
EMBEDDING_MODEL_NAME = "text-embedding-3-small"


# Image description extraction model (local)
VLM_MODEL_NAME = "google/gemma-3-27b-it"


# Local Models (LM Studio)
USE_LOCAL_MODELS = True
LOCAL_API_BASE_URL = "http://127.0.0.1:1234/v1"
LOCAL_LLM_MODEL_NAME = "qwen/qwen3-30b-a3b-2507"
LOCAL_EMBEDDING_MODEL_NAME = "text-embedding-qwen3-embedding-8b"


# ============================================================================
# VECTOR DATABASE & RETRIEVAL
# ============================================================================

# Vector database storage path
VECTOR_DB_PATH = "vectorstore_db"

# Number of documents to retrieve for RAG
RETRIEVER_K = 50
