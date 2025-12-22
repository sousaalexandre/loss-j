from langchain_openai import ChatOpenAI
from langchain_core.language_models.chat_models import BaseChatModel
from src import settings

def get_llm() -> BaseChatModel:
    llm = ChatOpenAI(
        model=settings.LLM_MODEL_NAME
    )
    
    return llm