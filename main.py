import streamlit as st
from src.api.query_handler import query_handler
import os
import json
from pathlib import Path
from src import settings
import requests
import html as html_lib


def get_document_title(source_path: str) -> str:
    """
    Get the document title from the Gold or Bronze catalog JSON.
    Falls back to original_filename, then hash name if not found.
    """
    try:
        source_file = Path(source_path)
        hash_name = source_file.stem.split('_')[0]

        catalog_files = [
            Path("data_lakehouse/03_gold/_catalog.json"),
            Path("data_lakehouse/01_bronze/_catalog.json")
        ]

        for catalog_file in catalog_files:
            if catalog_file.exists():
                with open(catalog_file, 'r', encoding='utf-8') as f:
                    catalog = json.load(f)
                    if hash_name in catalog:
                        metadata = catalog[hash_name]
                        if metadata.get("title"):
                            return metadata["title"]
                        if metadata.get("original_filename"):
                            return metadata["original_filename"]
    except Exception:
        pass

    return os.path.basename(source_path)


def render_message(message: dict):
    """
    Render a message from session state.
    Plain text + sources list are stored; HTML is built HERE at render time.
    This means session state never contains HTML, so there is no duplication.
    """
    text = message.get("content", "")
    sources = message.get("sources", [])

    if sources:
        sources_items = "".join(
            f'<li style="padding: 2px 0;">{html_lib.escape(s)}</li>'
            for s in sources
        )
        sources_html = f"""
<style>
.lossjtooltip {{
  position: relative;
  display: inline-block;
  vertical-align: middle;
  margin-left: 6px;
}}
.lossjtooltip .lossjtooltip-box {{
  visibility: hidden;
  opacity: 0;
  transition: opacity 0.15s;
  position: absolute;
  bottom: calc(100% + 8px);
  left: 0;
    background: #ffffff;
    color: #1a1a1a;
    border: 1px solid #d0d0d0;
  border-radius: 8px;
  padding: 10px 14px;
  font-size: 12.5px;
  font-family: sans-serif;
  line-height: 1.6;
  z-index: 99999;
  box-shadow: 0 6px 20px rgba(0,0,0,0.15);
  width: max-content;
  max-width: 300px;
  max-height: 260px;
  overflow-y: auto;
  pointer-events: none;
}}
[data-theme="dark"] .lossjtooltip .lossjtooltip-box,
.stApp[data-theme="dark"] .lossjtooltip .lossjtooltip-box {{
    background: #1f2937 !important;
    color: #f3f4f6 !important;
    border-color: #4b5563 !important;
}}
[data-theme="dark"] .lossjtooltip-box .tooltip-header,
.stApp[data-theme="dark"] .lossjtooltip-box .tooltip-header {{
    color: #d1d5db !important;
}}
[data-theme="light"] .lossjtooltip .lossjtooltip-box,
.stApp[data-theme="light"] .lossjtooltip .lossjtooltip-box {{
    background: #ffffff !important;
    color: #1a1a1a !important;
    border-color: #d0d0d0 !important;
}}
[data-theme="light"] .lossjtooltip-box .tooltip-header,
.stApp[data-theme="light"] .lossjtooltip-box .tooltip-header {{
    color: #666666 !important;
}}
@media (prefers-color-scheme: dark) {{
    .lossjtooltip .lossjtooltip-box {{
        background: #1f2937 !important;
        color: #f3f4f6 !important;
        border-color: #4b5563 !important;
    }}
    .lossjtooltip-box .tooltip-header {{
        color: #d1d5db !important;
    }}
}}
@media (prefers-color-scheme: light) {{
    .lossjtooltip .lossjtooltip-box {{
        background: #ffffff !important;
        color: #1a1a1a !important;
        border-color: #d0d0d0 !important;
    }}
    .lossjtooltip-box .tooltip-header {{
        color: #666666 !important;
    }}
}}
.lossjtooltip:hover .lossjtooltip-box {{
  visibility: visible;
  opacity: 1;
}}
.lossjtooltip-box ul {{
  margin: 0;
  padding-left: 16px;
}}
.lossjtooltip-box .tooltip-header {{
  font-weight: 700;
  font-size: 11px;
  text-transform: uppercase;
  letter-spacing: 0.5px;
    color: #666666;
  margin-bottom: 5px;
}}
</style>
<span class="lossjtooltip">
  <svg xmlns="http://www.w3.org/2000/svg" width="15" height="15" viewBox="0 0 24 24"
       fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round"
       stroke-linejoin="round" style="cursor:pointer; opacity:0.55; vertical-align:middle;">
    <path d="m21.44 11.05-9.19 9.19a6 6 0 0 1-8.49-8.49l8.57-8.57A4 4 0 1 1 18 8.84l-8.59 8.57a2 2 0 0 1-2.83-2.83l8.49-8.48"/>
  </svg>
  <div class="lossjtooltip-box">
    <div class="tooltip-header">Fontes</div>
    <ul>{sources_items}</ul>
  </div>
</span>"""
        st.markdown(
            f'<span style="white-space: pre-wrap;">{html_lib.escape(text)}</span>{sources_html}',
            unsafe_allow_html=True
        )
    else:
        st.markdown(text)


def main():
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
            url = settings.LOCAL_API_BASE_URL.rstrip('/') + "/models"
            resp = requests.get(url, timeout=1)
            connected = resp.status_code == 200
        except Exception:
            connected = False
    else:
        connected = os.getenv("OPENAI_API_KEY") is not None

    status_text = "CONNECTED" if connected else "DISCONNECTED"
    status_color = "#28a745" if connected else "#dc3545"

    st.markdown(
        f"""
        <div style="background-color: rgba(128,128,128,0.1); padding: 4px 12px; border-radius: 4px;
                    font-family: monospace; font-size: 11px; display: flex; align-items: center;
                    gap: 20px; margin-bottom: 20px;">
            <span style="white-space: nowrap; color: #888;">LLM:
                <span style="font-weight: 600;">{llm_name}</span>
            </span>
            <span style="white-space: nowrap; color: #888;">EMBEDDING:
                <span style="font-weight: 600;">{emb_name}</span>
            </span>
            <div style="margin-left: auto; display: flex; align-items: center; gap: 6px;">
                <span style="width:6px; height:6px; background-color:{status_color};
                             border-radius:50%; display:inline-block;"></span>
                <span style="color:{status_color}; font-weight:700; font-size:9px;
                             letter-spacing:0.4px;">{status_text}</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )

    st.title("LOSS-J RAG (Beta)")

    # Session state schema:
    #   {"role": str, "content": str, "sources": list[str]}
    # IMPORTANT: Never store HTML in session state — build it at render time only.
    if "messages" not in st.session_state:
        st.session_state.messages = [
            {
                "role": "assistant",
                "content": "Olá, pergunta-me qualquer coisa sobre os teus documentos!",
                "sources": []
            }
        ]

    # Render history
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            render_message(message)

    # Handle new input
    if prompt := st.chat_input("Pergunta algo sobre os teus documentos..."):
        user_msg = {"role": "user", "content": prompt, "sources": []}
        st.session_state.messages.append(user_msg)
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            try:
                with st.spinner("A pensar..."):
                    result = query_handler(prompt)
                    response_text = result["response"]
                    documents = result["documents"]

                    sources_list = []
                    if documents and not response_text.strip().startswith("Não sei"):
                        seen = set()
                        for doc in documents:
                            source_path = doc.metadata.get('source', '')
                            title = get_document_title(source_path)
                            if title not in seen:
                                seen.add(title)
                                sources_list.append(title)

                    assistant_msg = {
                        "role": "assistant",
                        "content": response_text,
                        "sources": sources_list,
                    }

                    # Render first, then append — prevents double render on this turn
                    render_message(assistant_msg)
                    st.session_state.messages.append(assistant_msg)

            except FileNotFoundError:
                st.error("Base de dados não encontrada. Vai à página 'Gerir Base de Conhecimento' para carregar um documento.")
            except Exception as e:
                st.error(f"Ocorreu um erro: {e}")


if __name__ == "__main__":
    main()