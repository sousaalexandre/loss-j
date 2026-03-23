from langchain_community.document_loaders import PyPDFLoader
from langchain_core.documents import Document
from typing import List
from src.logger import log

import src.settings as settings
import os


def load_document(file_path: str, is_markdown: bool = False) -> List[Document]:
    """
    Loads a document from file path.
    
    Args:
        file_path: Path to the document (PDF or Markdown)
        is_markdown: If True, loads as Markdown. If False, loads as PDF.
    
    Returns:
        List[Document]: List of Document objects
    """
    if is_markdown:
        log("Using Markdown loader.", level="info")
        return load_document_markdown(file_path)
    elif settings.LOADER_TYPE == "mineru":
        log("Using MinerU loader.", level="info")
        return load_document_mineru(file_path)
    else:
        log("Using PDF Loader.", level="info")
        return load_document_pdfloader(file_path)


def load_document_pdfloader(file_path: str) -> List[Document]:
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


def load_document_markdown(file_path: str) -> List[Document]:
    """
    Loads a Markdown document from file.
    
    Args:
        file_path (str): The path to the Markdown file.
    
    Returns:
        List[Document]: A list containing a single Document object with the Markdown content.
    """
    log(f"Loading Markdown document from: {file_path}", level="info")
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        document = Document(
            page_content=content,
            metadata={
                "source": file_path,
                "file_type": "markdown"
            }
        )
        
        log(f"Successfully loaded Markdown document.", level="info")
        return [document]
    
    except Exception as e:
        log(f"Error loading Markdown document: {e}", level="error")
        return []


def load_document_mineru(file_path: str) -> List[Document]:
    """Load and process document using MinerU backend with caching.
    
    Converts PDF to Markdown using MinerU, applies cleaning operations,
    and caches results for future use. Supports both VLM HTTP Client and
    local pipeline backends based on settings.
    
    Args:
        file_path: Path to the PDF file to process
        
    Returns:
        List[Document]: A list containing a single Document object with processed Markdown content
    """

    from src.preprocessing.mineru import pdf_to_md
    from src.preprocessing.cleaners import apply_cleaning

    if cached_md_path and os.path.exists(cached_md_path):
        log(f"Cache hit. Loading Markdown from: {cached_md_path}", level="info")
        with open(cached_md_path, "r", encoding="utf-8") as f:
            markdown_content = f.read()

            #### apply cleaning step
            markdown_content = apply_cleaning(file_path, markdown_content)
            _save_cache(cache_cleaned_md_path, markdown_content)

        return [Document(page_content=markdown_content, metadata={"source": file_path})]
    

    if settings.MINERU_BACKEND == "vlm-http-client":
        log(f"Processing document with MinerU VLM HTTP Client: {file_path}", level="info")
        markdown_content= pdf_to_md(pdf_path=file_path, backend="vlm-http-client", server_url=settings.MINERU_VLM_HTTP_URL)

    else:
        log(f"Processing document with MinerU Pipeline: {file_path}", level="info")
        markdown_content= pdf_to_md(pdf_path=file_path)

    _save_cache(cached_md_path, markdown_content)
    
    #### apply cleaning step
    markdown_content = apply_cleaning(file_path, markdown_content)
    _save_cache(cache_cleaned_md_path, markdown_content)

    return [Document(page_content=markdown_content, metadata={"source": file_path})]



def _save_cache(path, content):
    """Save content to cache file.
    
    Creates parent directories if they don't exist and writes content to the specified path.
    
    Args:
        path: File path where content should be cached
        content: Content string to save
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
        log(f"Successfully saved new Markdown to cache: {path}", level="info")
