from langchain_openai import OpenAIEmbeddings
from langchain.embeddings.base import Embeddings
from src import settings

def get_embedding_model() -> Embeddings:
    embeddings = OpenAIEmbeddings(
        model=settings.EMBEDDING_MODEL_NAME,
    )

    return embeddings