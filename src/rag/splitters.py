from langchain_text_splitters import RecursiveCharacterTextSplitter, MarkdownHeaderTextSplitter
from langchain_core.documents import Document
from typing import List, Dict, Tuple
import re
import html2text
from pylatexenc.latex2text import LatexNodes2Text
from src import settings
from src.logger import log
from pathlib import Path
import json

CHUNK_SIZE = settings.CHUNK_SIZE
CHUNK_OVERLAP = settings.CHUNK_OVERLAP


def split_documents(documents: List[Document]) -> List[Document]:
    """
    Split documents based on SPLITTING_TYPE setting.
    
    Supported strategies:
    - "recursive": Basic recursive character splitting
    - "markdown_recursive": Markdown-aware recursive splitting
    """
    if settings.SPLITTING_TYPE == "markdown_recursive":
        log("Using markdown-aware splitter", level="info")
        return _split_markdown(documents)
    else:
        log("Using basic recursive splitter", level="info")
        return _split_recursive(documents)


def _split_recursive(documents: List[Document]) -> List[Document]:
    """
    Basic recursive character text splitter.
    Works with any document format.
    """
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=CHUNK_SIZE,
        chunk_overlap=CHUNK_OVERLAP,
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
        chunk_size=CHUNK_SIZE,
        chunk_overlap=CHUNK_OVERLAP
    )
    chunks = markdown_splitter.split_documents(documents)
    log(f"Successfully split documents into {len(chunks)} chunks (markdown-aware).", level="info")
    return chunks
