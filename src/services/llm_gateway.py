from langchain_openai import ChatOpenAI
from langchain_core.language_models.chat_models import BaseChatModel
from src import settings

def get_llm(force_cloud: bool = False) -> BaseChatModel:
    """Get the language model instance for inference.
    
    Args:
        force_cloud (bool): If True, bypasses local models and forces the use of cloud models.
        
    Returns:
        BaseChatModel: OpenAI chat model configured based on settings
    """
    if getattr(settings, "USE_LOCAL_MODELS", False) and not force_cloud:
        llm = ChatOpenAI(
            base_url=getattr(settings, "LOCAL_API_BASE_URL", "http://127.0.0.1:1234/v1"),
            model=settings.LOCAL_LLM_MODEL_NAME,
            api_key="lm-studio"    # dummy
        )
    else:
        llm = ChatOpenAI(
            model=settings.LLM_MODEL_NAME
        )
    
    return llm