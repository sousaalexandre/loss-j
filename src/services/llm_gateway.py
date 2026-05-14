from langchain_openai import ChatOpenAI
from langchain_core.language_models.chat_models import BaseChatModel
from src import settings
import time
import random
from functools import wraps

def with_retry(max_retries=3, initial_delay=2, backoff_factor=2):
    """Decorator to retry LLM calls with exponential backoff and jitter."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            delay = initial_delay
            last_exception = None
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    # List of common transient error patterns
                    error_msg = str(e).lower()
                    is_transient = any(term in error_msg for term in [
                        "timeout", "timed out", "keepalive", "connection", "closed", "400", "500", "502", "503", "504"
                    ])

                    if not is_transient or attempt == max_retries:
                        break

                    # Add random jitter (0 to 2 seconds) to prevent thundering herd
                    jitter = random.uniform(0, 2)
                    sleep_time = delay + jitter
                    
                    print(f"  [retry {attempt+1}/{max_retries}] Transient error: {e}. Retrying in {sleep_time:.1f}s...")
                    time.sleep(sleep_time)
                    delay *= backoff_factor
            raise last_exception
        return wrapper
    return decorator

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
            api_key="lm-studio",    # dummy
            timeout=120,            # 120 second timeout for local/remote servers
            max_retries=0           # use custom wrapper at call sites for better control
        )
    else:
        llm = ChatOpenAI(
            model=settings.LLM_MODEL_NAME
        )

    return llm