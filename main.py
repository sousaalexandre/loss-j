import streamlit as st
from src.api.query_handler import query_handler
import os
import json
from pathlib import Path
from src import settings
import requests

def get_document_title(source_path: str) -> str:
    """
    Get the document title from the Bronze catalog JSON.
    Falls back to original_filename, then hash name if not found.
    """
    try:
        # Extract hash from source path (e.g., "hash.md" or path containing hash)
        source_file = Path(source_path)
        hash_name = source_file.stem  # Get filename without extension
        
        # Look for metadata in the Bronze catalog
        catalog_file = Path("data_lakehouse/01_bronze/_catalog.json")
        
        if catalog_file.exists():
            with open(catalog_file, 'r', encoding='utf-8') as f:
                catalog = json.load(f)
                if hash_name in catalog:
                    metadata = catalog[hash_name]
                    # Return title if available, otherwise original filename
                    if metadata.get("title"):
                        return metadata["title"]
                    if metadata.get("original_filename"):
                        return metadata["original_filename"]
    except Exception as e:
        # Silently fall back if any error
        pass
    
    # Fall back to hash or filename
    return os.path.basename(source_path)

def main():
    """Run the main Streamlit application for the LOSS-J RAG chatbot.
    
    Initializes the Streamlit page configuration and manages the chat interface,
    including message history, user input handling, and response generation with
    document sources.
    """
    st.set_page_config(
        page_title="LOSS-J",
        page_icon="💬",
        layout="wide",
        initial_sidebar_state="expanded"
    )

    # Model Status Bar
    llm_name = settings.LOCAL_LLM_MODEL_NAME if getattr(settings, "USE_LOCAL_MODELS", False) else settings.LLM_MODEL_NAME
    emb_name = settings.LOCAL_EMBEDDING_MODEL_NAME if getattr(settings, "USE_LOCAL_MODELS", False) else settings.EMBEDDING_MODEL_NAME
    
    connected = False
    if getattr(settings, "USE_LOCAL_MODELS", False):
        try:
            # Check if local API is reachable
            url = settings.LOCAL_API_BASE_URL.rstrip('/') + "/models"
            response = requests.get(url, timeout=1)
            connected = response.status_code == 200
        except:
            connected = False
    else:
        connected = os.getenv("OPENAI_API_KEY") is not None

    status_text = "CONNECTED" if connected else "DISCONNECTED"
    status_color = "#28a745" if connected else "#dc3545"
    
    st.markdown(
        f"""
        <div style="background-color: rgba(128, 128, 128, 0.1); padding: 4px 12px; border-radius: 4px; font-family: monospace; font-size: 11px; display: flex; align-items: center; gap: 20px; margin-bottom: 20px;">
            <span style="white-space: nowrap; color: #888;">LLM: <span style="color: inherit; font-weight: 600;">{llm_name}</span></span>
            <span style="white-space: nowrap; color: #888;">EMBEDDING: <span style="color: inherit; font-weight: 600;">{emb_name}</span></span>
            <div style="margin-left: auto; display: flex; align-items: center; gap: 6px;">
                <span style="width: 6px; height: 6px; background-color: {status_color}; border-radius: 50%;"></span>
                <span style="color: {status_color}; font-weight: 700; font-size: 9px; letter-spacing: 0.4px;">{status_text}</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )

    st.title("LOSS-J RAG (Beta)")

    if "messages" not in st.session_state:
        st.session_state.messages = [
            {"role": "assistant", "content": "Olá, pergunta-me qualquer coisa sobre os teus documentos!"}
        ]

    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    if prompt := st.chat_input("Pergunta algo sobre os teus documentos..."):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            with st.spinner("A pensar..."):
                try:
                    result = query_handler(prompt)
                    response = result["response"]
                    documents = result["documents"]
                    st.markdown(response)
                    full_content = response
                    if documents and not response.strip().startswith("Não sei"):
                        st.subheader("Fontes:")
                        sources_text = ""
                        unique_sources = set()
                        for doc in documents:
                            source_path = doc.metadata.get('source', 'Fonte Desconhecida')
                            file_title = get_document_title(source_path)
                            if file_title not in unique_sources:
                                unique_sources.add(file_title)
                                st.write(f"- **{file_title}**")
                                sources_text += f"- **{file_title}**\n"
                        full_content += f"\n\n**Fontes:**\n{sources_text}"
                    st.session_state.messages.append({"role": "assistant", "content": full_content})
                except FileNotFoundError:
                    st.error("Database not found. Please go to the 'Manage Knowledge Base' page to upload a document.")
                except Exception as e:
                    st.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()