from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_core.documents import Document
from typing import List
from src import settings
from src.logger import log


def split_documents(documents: List[Document]) -> List[Document]:
    """
    Split documents based on SPLITTING_TYPE setting.
    """
    if settings.LOADER_TYPE == "pdfloader":
        log("Using basic recursive splitter", level="info")
        return _split_recursive(documents)
    
    if settings.SPLITTING_TYPE == "markdown_recursive":
        log("Using Markdown-aware splitter", level="info")
        return _split_markdown(documents)
    else:
        return _split_recursive(documents)


def _split_recursive(documents: List[Document]) -> List[Document]:
    """
    Basic recursive character text splitter.
    Works with any document format.
    """
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=settings.CHUNK_SIZE,
        chunk_overlap=settings.CHUNK_OVERLAP,
        length_function=len,
        is_separator_regex=False,
    )
    chunks = text_splitter.split_documents(documents)
    log(f"Successfully split documents into {len(chunks)} chunks (recursive).", level="info")
    return chunks


def _split_markdown(documents: List[Document]) -> List[Document]:
    """
    Markdown-aware recursive splitting for MinerU documents.
    Respects header hierarchy and semantic structure.
    """
    markdown_splitter = RecursiveCharacterTextSplitter.from_language(
        language="markdown",
        chunk_size=settings.CHUNK_SIZE,
        chunk_overlap=settings.CHUNK_OVERLAP
    )
    chunks = markdown_splitter.split_documents(documents)
    log(f"Successfully split documents into {len(chunks)} chunks (markdown-aware).", level="info")
    return chunks
