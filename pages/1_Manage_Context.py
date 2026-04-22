import streamlit as st
import base64
import json
from src.pipelines.controller import run_ingestion
from src.services.vector_db import get_database_summary, remove_document
from pathlib import Path

def get_document_title(source_filename: str) -> str:
    """
    Get the document title from _catalog.json in gold or landing zone.
    Falls back to original_filename or hash name if not found.
    """
    try:
        # Extract hash from source filename (e.g., "hash.md" or "hash.pdf")
        source_file = Path(source_filename)
        hash_name = source_file.stem.split('_')[0]  # Get base hash part

        # Look for catalog in gold and landing zones
        catalog_files = [
            Path("data_lakehouse/03_gold/_catalog.json"),
            Path("data_lakehouse/01_bronze/_catalog.json")
        ]

        for catalog_file in catalog_files:
            if catalog_file.exists():
                with open(catalog_file, 'r', encoding='utf-8') as f:
                    catalog = json.load(f)
                    # Check if hash exists in catalog
                    if hash_name in catalog:
                        metadata = catalog[hash_name]
                        # Return title if available, otherwise original filename
                        if metadata.get("title"):
                            return metadata["title"]
                        if metadata.get("original_filename"):
                            return metadata["original_filename"].replace(".pdf", "")
    except Exception:
        # Silently fall back if any error
        pass

    # Fall back to hash or filename
    return Path(source_filename).stem
# Set page config FIRST
st.set_page_config(layout="wide", initial_sidebar_state="expanded")

# Add comprehensive CSS to force 100% zoom and prevent layout collapse
st.markdown("""
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
<style>
    * {
        margin: 0;
        padding: 0;
        box-sizing: border-box;
    }
    
    html {
        width: 100vw !important;
        height: 100vh !important;
        overflow-x: hidden !important;
        zoom: 100% !important;
        font-size: 16px !important;
    }
    
    body {
        width: 100vw !important;
        height: 100vh !important;
        margin: 0 !important;
        padding: 0 !important;
        overflow-x: hidden !important;
        zoom: 100% !important;
    }
    
    .main {
        width: 100vw !important;
        max-width: 100vw !important;
        min-width: 100vw !important;
        margin: 0 !important;
        padding: 1rem !important;
        zoom: 100% !important;
    }
    
    [data-testid="stAppViewContainer"] {
        width: 100vw !important;
        max-width: 100vw !important;
        min-width: 100vw !important;
        margin: 0 !important;
        padding: 0 !important;
        zoom: 100% !important;
    }
    
    [data-testid="stVerticalBlock"] {
        gap: 1rem !important;
        width: 100% !important;
        min-width: 100% !important;
    }
    
    [data-testid="stHorizontalBlock"] {
        width: 100% !important;
        min-width: 100% !important;
    }
    
    [data-testid="column"] {
        min-width: 0 !important;
        width: 100% !important;
        flex-basis: auto !important;
    }
    
    iframe {
        min-height: 600px !important;
        width: 100% !important;
        min-width: 100% !important;
    }
    
    .stForm {
        border: 1px solid #e0e0e0 !important;
        padding: 1.5rem !important;
        border-radius: 0.5rem !important;
        width: 100% !important;
        min-width: 100% !important;
    }
    
    .stButton {
        width: 100% !important;
    }
    x
    .stExpander {
        width: 100% !important;
    }
    
    .stMetric {
        width: 100% !important;
    }
</style>
""", unsafe_allow_html=True)

# --- Helper Function for PDF Display ---
def display_pdf(uploaded_file):
    """
    Display PDF using an iframe for a clear, full-document preview.
    """
    try:
        import base64
        # Read file as bytes
        bytes_data = uploaded_file.getvalue()

        # Base64 encode the bytes
        base64_pdf = base64.b64encode(bytes_data).decode('utf-8')

        # Embed PDF in HTML
        pdf_display = f'<iframe src="data:application/pdf;base64,{base64_pdf}" width="100%" height="600" type="application/pdf"></iframe>'

        # Display the HTML
        st.markdown(pdf_display, unsafe_allow_html=True)

    except Exception as e:
        st.warning(f"⚠️ Não consegui pré-visualizar este PDF")
        st.caption(f"Detalhes: {str(e)[:100]}")# ---------------------------------------

st.title("Gerir Base de Conhecimento 🗂️")
st.markdown("Adicione novos documentos à sua base de conhecimento e visualize seu estado atual.")

st.divider()

st.header("Adicionar Novo Documento 📄")
uploaded_files = st.file_uploader(
    "Carregar PDFs para adicioná-los à base de conhecimento (arraste e solte ou selecione múltiplos)",
    type=["pdf"],
    accept_multiple_files=True
)

def extract_pdf_title(filename: str) -> str:
    """Extract a clean title from PDF filename.
    
    Removes .pdf extension and replaces underscores and hyphens with spaces.
    """
    title = filename.replace(".pdf", "").replace("_", " ").replace("-", " ")
    return " ".join(title.split())

def parse_category_input(category_str: str) -> list:
    """Parse comma-separated category string into list.
    
    Returns ['Geral'] if input is empty, otherwise splits by comma and strips whitespace.
    """
    if not category_str.strip():
        return ["Geral"]
    return [cat.strip() for cat in category_str.split(",") if cat.strip()]

# Store uploaded files in session state to persist through reruns
if uploaded_files:
    st.session_state.temp_uploaded_files = uploaded_files
    st.session_state.in_metadata_form = True
elif "in_metadata_form" in st.session_state and st.session_state.in_metadata_form and "temp_uploaded_files" in st.session_state:
    uploaded_files = st.session_state.temp_uploaded_files

# Show button to enter metadata form or show form if already in it
if uploaded_files:
    if not st.session_state.get("in_metadata_form", False):
        if st.button("Configurar Metadados e Indexar"):
            st.session_state.in_metadata_form = True
            st.rerun()
    else:
        landing_zone = Path("data_lakehouse/01_bronze")
        landing_zone.mkdir(parents=True, exist_ok=True)
    # Initialize session state for metadata if not exists
    if "metadata_forms" not in st.session_state:
        st.session_state.metadata_forms = {}
        
        for uploaded_file in uploaded_files:
            filename = uploaded_file.name
            st.session_state.metadata_forms[filename] = {
                "title": extract_pdf_title(filename),
                "description": "",
                "category": ""
            }
    
    # Show metadata collection UI
    st.info("📝 Edite os metadados para cada documento antes de indexar")
    
    # --- UI Logic ---
    if len(uploaded_files) == 1:
        # Single file layout
        uploaded_file = uploaded_files[0]
        col1, col2 = st.columns([3, 2]) # 3:2 ratio gives more space to PDF
        
        with col1:
            st.subheader(f"📄 Pré-visualização: {uploaded_file.name}")
            display_pdf(uploaded_file) # <--- CALL THE PREVIEW HERE
        
        with col2:
            st.subheader("Metadados")
            form_key = uploaded_file.name
            metadata = st.session_state.metadata_forms[form_key]
            
            with st.form(key="single_file_form"):
                title_input = st.text_input(
                    "Título *",
                    value=metadata["title"]
                )
                
                category_input = st.text_input(
                    "Categoria (separadas por vírgula) *",
                    value=metadata["category"],
                    placeholder="ex: Decreto Lei, Remunerações, Oficiais"
                )
                
                description_input = st.text_area(
                    "Descrição (opcional)",
                    value=metadata["description"],
                    height=150
                )
                
                if st.form_submit_button("Guardar Metadados", use_container_width=True):
                    metadata["title"] = title_input
                    metadata["category"] = category_input
                    metadata["description"] = description_input
                    st.session_state.metadata_forms[form_key] = metadata
                    st.success("✓ Metadados guardados na sessão")
    
    else:
        # Multiple files: use optimized lazy-loading for many files
        if len(uploaded_files) > 5:
            # For many files, use selectbox to load one at a time
            st.info(f"📦 {len(uploaded_files)} ficheiros carregados. Selecione um para pré-visualizar e editar metadados.")
            
            # Initialize selected file index in session state
            if "selected_file_idx" not in st.session_state:
                st.session_state.selected_file_idx = 0
            
            # File selector
            file_names = [f.name for f in uploaded_files]
            selected_idx = st.selectbox(
                "Selecione o ficheiro:",
                range(len(uploaded_files)),
                format_func=lambda i: f"[{i+1}/{len(uploaded_files)}] {file_names[i]}"
            )
            st.session_state.selected_file_idx = selected_idx
            
            # Display selected file
            uploaded_file = uploaded_files[selected_idx]
            col1, col2 = st.columns([3, 2])
            
            with col1:
                st.subheader(f"📄 Pré-visualização ({selected_idx + 1}/{len(uploaded_files)})")
                display_pdf(uploaded_file)
            
            with col2:
                st.subheader("Metadados")
                form_key = uploaded_file.name
                metadata = st.session_state.metadata_forms[form_key]
                
                with st.form(key=f"file_form_{selected_idx}"):
                    title_input = st.text_input(
                        "Título *",
                        value=metadata["title"]
                    )
                    
                    category_input = st.text_input(
                        "Categoria (separadas por vírgula) *",
                        value=metadata["category"],
                        placeholder="ex: Decreto Lei, Remunerações, Oficiais"
                    )
                    
                    description_input = st.text_area(
                        "Descrição (opcional)",
                        value=metadata["description"],
                        height=150
                    )
                    
                    if st.form_submit_button("Guardar Metadados", width='stretch'):
                        metadata["title"] = title_input
                        metadata["category"] = category_input
                        metadata["description"] = description_input
                        st.session_state.metadata_forms[form_key] = metadata
                        st.success("✓ Metadados guardados na sessão")
        else:
            # For 2-5 files, use tabs (lightweight)
            tabs = st.tabs([f.name for f in uploaded_files])
            
            for tab_idx, (tab, uploaded_file) in enumerate(zip(tabs, uploaded_files)):
                with tab:
                    col1, col2 = st.columns([3, 2])
                    
                    with col1:
                        st.subheader(f"📄 Pré-visualização")
                        display_pdf(uploaded_file) # <--- CALL THE PREVIEW HERE
                    
                    with col2:
                        st.subheader("Metadados")
                        form_key = uploaded_file.name
                        metadata = st.session_state.metadata_forms[form_key]
                        
                        with st.form(key=f"file_form_{tab_idx}"):
                            title_input = st.text_input(
                                "Título *",
                                value=metadata["title"]
                            )
                            
                            category_input = st.text_input(
                                "Categoria (separadas por vírgula) *",
                                value=metadata["category"],
                                placeholder="ex: Decreto Lei, Remunerações, Oficiais"
                            )
                            
                            description_input = st.text_area(
                                "Descrição (opcional)",
                                value=metadata["description"],
                                height=150
                            )
                            
                            if st.form_submit_button("Guardar Metadados", width='stretch'):
                                metadata["title"] = title_input
                                metadata["category"] = category_input
                                metadata["description"] = description_input
                                st.session_state.metadata_forms[form_key] = metadata
                                st.success("✓ Metadados guardados na sessão")

    # Confirm and index
    st.divider()
    col_confirm, col_cancel = st.columns(2)
    
    with col_confirm:
        if st.button("✓ Confirmar e Indexar", width='stretch', type="primary"):
            # ... (Rest of your indexing logic remains exactly the same)
            landing_zone = Path("data_lakehouse/01_bronze")
            landing_zone.mkdir(parents=True, exist_ok=True)
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            phase_text = st.empty()
            
            file_paths = []
            file_metadata_dict = {}
            
            # Prepare file list and metadata dict
            for i, uploaded_file in enumerate(uploaded_files):
                try:
                    # Create temp file to pass to controller
                    temp_path = Path("temp_uploads") / uploaded_file.name
                    temp_path.parent.mkdir(parents=True, exist_ok=True)
                    
                    uploaded_file.seek(0)
                    with open(temp_path, "wb") as f:
                        f.write(uploaded_file.getvalue())
                    
                    file_paths.append(str(temp_path))
                    
                    # Collect metadata
                    form_data = st.session_state.metadata_forms[uploaded_file.name]
                    file_metadata_dict[uploaded_file.name] = {
                        "title": form_data["title"],
                        "category": form_data["category"],
                        "description": form_data["description"]
                    }
                    
                    progress_bar.progress((i + 1) / len(uploaded_files) * 0.1)
                
                except Exception as e:
                    st.error(f"Erro ao preparar '{uploaded_file.name}': {e}")
            
            # Run ingestion with controller (handles landing zone organization)
            if file_paths:
                def update_progress(phase, progress_pct, message):
                    progress_bar.progress(0.1 + (progress_pct / 100) * 0.9)
                    phase_text.text(f"**{phase}**: {message}")
                
                try:
                    update_progress("Inicialização", 0, "Preparando pipeline...")
                    results = run_ingestion(file_paths, file_metadata=file_metadata_dict, progress_callback=update_progress)
                    
                    # Check if duplicates were detected
                    if results.get("status") == "skipped":
                        st.warning(f"⚠️  {results.get('message')}")
                        st.info("Estes ficheiros já estão presentes na base de conhecimento.")
                    else:
                        st.success("✓ Ingestion concluída com sucesso!")
                        
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Ficheiros Processados", results.get("total_files", 0))
                        with col2:
                            extracted = results.get("etl", {}).get("successfully_extracted", 0)
                            st.metric("Extraídos", extracted)
                        with col3:
                            indexed = len(results.get("indexing", {}).get("indexed_documents", []))
                            st.metric("Indexados", indexed)
                    
                    if "metadata_forms" in st.session_state:
                        del st.session_state.metadata_forms
                    if "in_metadata_form" in st.session_state:
                        del st.session_state.in_metadata_form
                    if "temp_uploaded_files" in st.session_state:
                        del st.session_state.temp_uploaded_files
                    st.rerun()
                
                except Exception as e:
                    st.error(f"Erro ao processar documentos: {e}")
                
                finally:
                    progress_bar.empty()
                    phase_text.empty()
    
    with col_cancel:
        if st.button("✗ Cancelar", width='stretch'):
            if "metadata_forms" in st.session_state:
                del st.session_state.metadata_forms
            if "in_metadata_form" in st.session_state:
                del st.session_state.in_metadata_form
            if "temp_uploaded_files" in st.session_state:
                del st.session_state.temp_uploaded_files
            st.rerun()

elif uploaded_files:
    st.info("Carregue o formulário de metadados clicando em 'Configurar Metadados e Indexar'")
else:
    st.info("Aguardando upload de ficheiros PDF...")

st.divider()


@st.dialog("Visualizar Documento", width="large")
def visualize_document(pdf_path: Path):
    try:
        import base64
        with open(pdf_path, "rb") as f:
            bytes_data = f.read()
        base64_pdf = base64.b64encode(bytes_data).decode('utf-8')
        pdf_display = f'<iframe src="data:application/pdf;base64,{base64_pdf}" width="100%" height="800" type="application/pdf"></iframe>'
        st.markdown(pdf_display, unsafe_allow_html=True)
    except Exception as e:
        st.error(f"Erro ao abrir documento: {e}")

@st.dialog("Confirmar Remoção")
def confirm_removal(name: str, title: str):
    st.warning(f"Tem a certeza que pretende remover o documento **{title}**?\n\n⚠️ Esta ação **não é reversível** e irá apagar permanentemente o ficheiro do sistema.")
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Sim, Remover", type="primary", use_container_width=True):
            remove_document(name)
            st.rerun()
    with col2:
        if st.button("Cancelar", use_container_width=True):
            st.rerun()

st.header("Estado da Base de Conhecimento 📊")
summary = get_database_summary()
if summary:
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Total de Chunks", summary['total_chunks'])
    with col2:
        st.metric("Total de Ficheiros", summary['num_files'])

    file_details = summary.get('file_details', {})

    with st.expander("Ver Ficheiros Indexados"):
        if not file_details:
            st.info("Nenhum ficheiro indexado ainda.")
        else:
            for name, count in file_details.items():
                file_title = get_document_title(name)
                col_text, col_btn_vis, col_btn_rem = st.columns([4, 1, 1])
                with col_text:
                    st.markdown(f"- **{file_title}** (*{count} chunks*)")
                with col_btn_vis:
                    if st.button("Visualizar", key=f"vis_{name}"):
                        hash_name = Path(name).stem.split('_')[0]
                        pdf_path = Path(f"data_lakehouse/01_bronze/{hash_name}.pdf")
                        if pdf_path.exists():
                            visualize_document(pdf_path)
                        else:
                            st.error("PDF não encontrado.")
                with col_btn_rem:
                    if st.button("Remover", key=f"rem_{name}"):
                        confirm_removal(name, file_title)
else:
    st.warning("Base de conhecimento não encontrada. Por favor, carregue o seu primeiro documento.")