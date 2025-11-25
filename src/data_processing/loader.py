from langchain_community.document_loaders import PyPDFLoader
from langchain_core.documents import Document
from typing import List, Any
from src.logger import log
from pathlib import Path
from src.utils import generate_file_hash
from src.data_processing.mineru import pdf_to_md
from src.data_processing.cleaner import apply_cleaning

import src.settings as settings
import requests
import os
import traceback


def load_document(file_path: str) -> List[Document]:
    """
    Loads a document from the specified file path.
    Currently supports PDF files.

    Args:
        file_path (str): The path to the document file.

    Returns:
        List[Document]: A list of Document objects, where each object
                        represents a page in the PDF.
    """
    log(f"Loading document from: {file_path}", level="info")

    loader = PyPDFLoader(file_path)
    documents = loader.load()
    log(f"Successfully loaded {len(documents)} pages.", level="info")
    return documents


def load_document_mineru(file_path: str) -> List[Document]:
    """
    Loads a document using the Mineru pre-processor.
    """

    file_hash = generate_file_hash(file_path)
    os.makedirs(settings.MD_CACHE_DIR, exist_ok=True)
    cached_md_path = os.path.join(settings.MD_CACHE_DIR, f"{file_hash}.md")
    cache_cleaned_md_path = os.path.join(settings.MD_CLEANED_DIR, f"{file_hash}_cleaned.md")

    # check cache first
    if cached_md_path and os.path.exists(cached_md_path):
        log(f"Cache hit. Loading Markdown from: {cached_md_path}", level="info")
        with open(cached_md_path, "r", encoding="utf-8") as f:
            markdown_content = f.read()

            #### apply cleaning step
            markdown_content = apply_cleaning(file_path, markdown_content)
            _save_cache(cache_cleaned_md_path, markdown_content)

        return [Document(page_content=markdown_content, metadata={"source": file_path})]
    

    # if first time loading, use mineru to load
    markdown_content= pdf_to_md(file_path)
    _save_cache(cached_md_path, markdown_content)
    
    #### apply cleaning step
    markdown_content = apply_cleaning(file_path, markdown_content)
    _save_cache(cache_cleaned_md_path, markdown_content)

        
    return [Document(page_content=markdown_content, metadata={"source": file_path})]



def _save_cache(path, content):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
        log(f"Successfully saved new Markdown to cache: {path}", level="info")




# def _load_document_mineru_docker(file_path: str) -> List[Document]:
#     """
#     Loads a document by sending it to the local MinerU Docker
#     container API to extract structured Markdown.
#     """
#     log(f"Processing document with local MinerU Docker: {file_path}", level="info")

#     try:
#         with open(file_path, 'rb') as f:
#             files = {'file': (os.path.basename(file_path), f)}
#             data = {'language': 'latin'}
#             response = requests.post(settings.MINERU_LOCAL_API_URL, files=files, data=data)
#             response.raise_for_status()

#         result_json = response.json()
#         markdown_content = result_json.get("data", {}).get("md")
        
#         if not markdown_content:
#             log("Could not find 'md' in MinerU response", level="error")
#             return []
            
#         return [Document(page_content=markdown_content, metadata={"source": file_path})]

#     except Exception as e:
#         log(f"Failed to process with MinerU Docker: {e}", level="error")
#         return []


# def _load_document_mineru_local(file_path: str) -> List[Document]:
    
#     file_hash = generate_file_hash(file_path)
#     os.makedirs(settings.MD_CACHE_DIR, exist_ok=True)
#     cached_md_path = os.path.join(settings.MD_CACHE_DIR, f"{file_hash}.md")

#     if cached_md_path and os.path.exists(cached_md_path):
#         log(f"Cache hit. Loading Markdown from: {cached_md_path}", level="info")
#         with open(cached_md_path, "r", encoding="utf-8") as f:
#             markdown_content = f.read()
#         return [Document(page_content=markdown_content, metadata={"source": file_path})]


#     try:
#         from mineru.cli.common import convert_pdf_bytes_to_bytes_by_pypdfium2, prepare_env, read_fn
#         from mineru.data.data_reader_writer import FileBasedDataWriter
#         from mineru.utils.enum_class import MakeMode
#         from mineru.backend.vlm.vlm_analyze import doc_analyze as vlm_doc_analyze
#         from mineru.backend.vlm.vlm_middle_json_mkcontent import union_make as vlm_union_make
#     except ImportError:
#         log("Mineru library not found. Please run 'pip install mineru[mlx]'", level="critical")
#         return []
    
#     log(f"Processing document with local MinerU (MLX): {file_path}", level="info")

#     try:
#         output_dir = "mineru_temp_output" 
#         pdf_file_name = Path(file_path).stem
#         parse_method = "vlm" 
#         local_image_dir, local_md_dir = prepare_env(output_dir, pdf_file_name, parse_method)
#         image_writer = FileBasedDataWriter(local_image_dir)
        
#         pdf_bytes = read_fn(file_path)
#         pdf_bytes = convert_pdf_bytes_to_bytes_by_pypdfium2(pdf_bytes, 0, None)

#         middle_json, infer_result = vlm_doc_analyze(
#             pdf_bytes, 
#             image_writer=image_writer, 
#             backend="mlx-engine",
#             server_url=None
#         )

#         # log("Sanitizing JSON blueprint to prevent utf-8 errors...", level="info")
#         # cleaned_middle_json = _clean_json_strings(middle_json)
#         # pdf_info = cleaned_middle_json["pdf_info"]
#         pdf_info = middle_json["pdf_info"]


#         image_dir_name = str(os.path.basename(local_image_dir))

#         #log(f"Pdf info: {pdf_info}", level="info")
#         markdown_content = vlm_union_make(
#             pdf_info, 
#             MakeMode.MM_MD,
#             image_dir_name
#         )
#         log("success vlm union make")

#         if not isinstance(markdown_content, str):
#             markdown_content = "\n".join(markdown_content)

#         log(f"Successfully processed document with local MinerU.", level="info")

#         try:
#             with open(cached_md_path, "w", encoding="utf-8") as f:
#                 f.write(markdown_content)
#             log(f"Successfully saved new Markdown to cache: {cached_md_path}", level="info")
#         except Exception as e:
#             log(f"Warning: Failed to write to cache: {e}", level="warning")


#         return [Document(page_content=markdown_content, metadata={"source": file_path})]

#     except Exception as e:
#         log(f"Failed to process with local MinerU: {e}", level="error")
#         log(f"Full traceback:\n{traceback.format_exc()}", level="error")
#         return []
