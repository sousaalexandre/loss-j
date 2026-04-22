from langchain_openai import OpenAIEmbeddings
from langchain.embeddings.base import Embeddings
from src import settings

def get_embedding_model(force_cloud: bool = False) -> Embeddings:
    """Get the embedding model instance for text vectorization.
    
    Args:
        force_cloud (bool): If True, bypasses local models and forces the use of cloud models.
        
    Returns:
        Embeddings: OpenAI embedding model configured based on settings
    """
    if getattr(settings, "USE_LOCAL_MODELS", False) and not force_cloud:
        embeddings = OpenAIEmbeddings(
            base_url=getattr(settings, "LOCAL_API_BASE_URL", "http://127.0.0.1:1234/v1"),
            model=settings.LOCAL_EMBEDDING_MODEL_NAME,
            api_key="lm-studio",    # dummy
            check_embedding_ctx_length=False
        )
    else:
        embeddings = OpenAIEmbeddings(
            model=settings.EMBEDDING_MODEL_NAME,
        )

    return embeddings