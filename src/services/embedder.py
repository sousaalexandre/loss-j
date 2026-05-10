from langchain_openai import OpenAIEmbeddings
from langchain.embeddings.base import Embeddings
from src import settings

_EMBEDDING_MODEL_INSTANCE = None

def get_embedding_model(force_cloud: bool = False, reset: bool = False) -> Embeddings:
    """Get the embedding model instance for text vectorization.
    
    Uses a singleton pattern to ensure the model is only instantiated once.
    
    Args:
        force_cloud (bool): If True, bypasses local models and forces the use of cloud models.
        reset (bool): If True, clears the existing instance and creates a new one.
        
    Returns:
        Embeddings: OpenAI embedding model configured based on settings
    """
    global _EMBEDDING_MODEL_INSTANCE
    
    if reset:
        _EMBEDDING_MODEL_INSTANCE = None

    # If cloud is forced, we always return a new instance
    if force_cloud:
        return OpenAIEmbeddings(
            model=settings.EMBEDDING_MODEL_NAME,
        )

    # Return cached instance if available
    if _EMBEDDING_MODEL_INSTANCE is not None:
        return _EMBEDDING_MODEL_INSTANCE

    if getattr(settings, "USE_LOCAL_MODELS", False):
        # Increased timeout and reduced chunk size for local reliability
        _EMBEDDING_MODEL_INSTANCE = OpenAIEmbeddings(
            base_url=getattr(settings, "LOCAL_API_BASE_URL", "http://127.0.0.1:1234/v1"),
            model=settings.LOCAL_EMBEDDING_MODEL_NAME,
            api_key="lm-studio",    # dummy
            check_embedding_ctx_length=False,
            chunk_size=16,
            timeout=120             # 120 second timeout for local/remote servers
        )
    else:
        _EMBEDDING_MODEL_INSTANCE = OpenAIEmbeddings(
            model=settings.EMBEDDING_MODEL_NAME,
        )

    return _EMBEDDING_MODEL_INSTANCE
