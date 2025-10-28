import streamlit as st
from src.chains.chain_retrieving_generating import query_handler
from src.logger import init_logger
import os


init_logger()


def main():
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
                            file_name = os.path.basename(doc.metadata.get('source', 'Fonte Desconhecida'))
                            st.write(f"- **{file_name}**")
                            sources_text += f"- **{file_name}**\n"
                        full_content += f"\n\n**Fontes:**\n{sources_text}"
                    st.session_state.messages.append({"role": "assistant", "content": full_content})
                except FileNotFoundError:
                    st.error("Database not found. Please go to the 'Manage Knowledge Base' page to upload a document.")
                except Exception as e:
                    st.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()