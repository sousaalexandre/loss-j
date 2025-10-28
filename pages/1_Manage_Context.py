import streamlit as st
from src.chains.chain_indexing import index_file
from src.vector_store.retriever import get_database_summary
import os

st.title("Gerir Base de Conhecimento (Knowledge Base) 🗂️")
st.markdown("Adicione novos documentos à sua base de conhecimento e visualize seu estado atual.")

st.divider()

st.header("Adicionar Novo Documento 📄")
uploaded_files = st.file_uploader(
    "Carregar PDFs para adicioná-los à base de conhecimento (arraste e solte ou selecione múltiplos)",
    type=["pdf"],
    accept_multiple_files=True
)

if st.button("Indexar Documentos"):
    if uploaded_files:
        temp_dir = "temp_uploads"
        os.makedirs(temp_dir, exist_ok=True)
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        for i, uploaded_file in enumerate(uploaded_files):
            temp_file_path = os.path.join(temp_dir, uploaded_file.name)
            
            try:
                with open(temp_file_path, "wb") as f:
                    f.write(uploaded_file.getvalue())
                
                status_text.text(f"Indexing '{uploaded_file.name}'...")
                index_file(temp_file_path)
                
                st.success(f"Successfully indexed '{uploaded_file.name}'!")
            
            except Exception as e:
                st.error(f"Error indexing '{uploaded_file.name}': {e}")
            
            finally:
                if os.path.exists(temp_file_path):
                    os.remove(temp_file_path)
                
                progress_bar.progress((i + 1) / len(uploaded_files))
        
        status_text.text("All files processed.")
        progress_bar.empty()
    else:
        st.warning("Please upload at least one PDF file first.")

st.divider()

st.header("Estado da Base de Conhecimento 📊")
summary = get_database_summary()
if summary:
    st.markdown(f"**Total Chunks:** `{summary['total_chunks']}`")
    st.markdown(f"**Total Files:** `{summary['num_files']}`")

    file_details = summary.get('file_details', {})

    search_query = st.text_input("Pesquisar arquivos indexados por nome", key="file_search")

    if search_query:
        filtered_files = {
            name: count for name, count in file_details.items()
            if search_query.lower() in name.lower()
        }
    else:
        filtered_files = file_details

    with st.expander("Ver Arquivos Indexados"):
        if not filtered_files:
            st.info("Nenhum arquivo correspondente encontrado." if search_query else "Nenhum arquivo indexado ainda.")
        else:
            for name, count in filtered_files.items():
                st.markdown(f"- `{name}` (*{count} chunks*)")
else:
    st.warning("Database not found. Please upload your first document.")