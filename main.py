import streamlit as st
from src.pipelines.pipeline_inference import query_handler
import os
import json
from pathlib import Path

def get_document_title(source_path: str) -> str:
    """
    Get the document title from metadata JSON.
    Falls back to original_filename, then hash name if not found.
    """
    try:
        # Extract hash from source path (e.g., "hash.md" or path containing hash)
        source_file = Path(source_path)
        hash_name = source_file.stem  # Get filename without extension
        
        # Look for metadata JSON in landing zone
        landing_zone = Path("data_lakehouse/01_bronze")
        metadata_file = landing_zone / f"{hash_name}.json"
        
        if metadata_file.exists():
            with open(metadata_file, 'r', encoding='utf-8') as f:
                metadata = json.load(f)
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
    st.set_page_config(page_title="LOSS-J", page_icon="💬")
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
                        for doc in documents:
                            source_path = doc.metadata.get('source', 'Fonte Desconhecida')
                            file_title = get_document_title(source_path)
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