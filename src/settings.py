from dotenv import load_dotenv

load_dotenv()


# --- Loader Configuration ---
# options: "pdfloader", "mineru"
# default is "pdfloader"
LOADER_TYPE = "mineru"

# --- MinerU Configuration ---
# converted pdfs to markdown cache directory
MD_CACHE_DIR = "outputs/mineru/"
MD_CLEANED_DIR = "outputs/mineru_cleaned/"

# backend options: "pipeline", "vlm-http-client" (needs server running)
# default is "pipeline"
MINERU_BACKEND = "vlm-http-client"  
MINERU_VLM_HTTP_URL = "http://192.168.103.9:30000"
# --- End MinerU Configuration ---



# --- Cleaning Configuration ---
# Only applicable if using MinerU loader
# True/False to enable/disable each cleaning step
ENABLE_HTML_CLEANING = False
ENABLE_LATEX_CLEANING = False
ENABLE_HIERARCHY_REBUILDING = False

# Hierarchy rebuilding mode: "font" or "llm"
# default is "font"
HIERARCHY_REBUILDING_MODE = "font"
# --- End Cleaning Configuration ---



# --- Splitting Configuration ---
# options: "recursive", "markdown_recursive"
# default is "recursive"
SPLITTING_TYPE = "markdown_recursive" 


# --- Text Splitting Configuration ---
CHUNK_SIZE = 1000
CHUNK_OVERLAP = 200
# --- End Text Splitting Configuration ---





# --- Models Configuration ---
LLM_MODEL_NAME = "gpt-4o-mini"
EMBEDDING_MODEL_NAME = "text-embedding-3-small"
# --- End Models Configuration ---



# --- Vector Database Configuration ---
VECTOR_DB_PATH = "vectorstore_db"
# --- End Vector Database Configuration ---






# --- Retriever Configuration ---
RETRIEVER_K = 5
# --- End Retriever Configuration ---