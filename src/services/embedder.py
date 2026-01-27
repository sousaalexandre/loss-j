from langchain_openai import OpenAIEmbeddings
from langchain.embeddings.base import Embeddings
from src import settings

def get_embedding_model() -> Embeddings:
    """Get the embedding model instance for text vectorization.
    
    Returns:
        Embeddings: OpenAI embedding model configured with EMBEDDING_MODEL_NAME setting
    """
    embeddings = OpenAIEmbeddings(
        model=settings.EMBEDDING_MODEL_NAME,
    )

    return embeddings