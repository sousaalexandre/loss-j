from langchain_openai import ChatOpenAI
from langchain_core.language_models.chat_models import BaseChatModel
from src import settings

def get_llm() -> BaseChatModel:
    """Get the language model instance for inference.
    
    Returns:
        BaseChatModel: OpenAI chat model configured with LLM_MODEL_NAME setting
    """
    llm = ChatOpenAI(
        model=settings.LLM_MODEL_NAME
    )
    
    return llm