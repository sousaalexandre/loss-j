from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_text_splitters import MarkdownHeaderTextSplitter
from langchain_core.documents import Document
from typing import List
from src import settings
from src.logger import log

import html2text
import re
from pylatexenc.latex2text import LatexNodes2Text

def split_documents(documents: List[Document]) -> List[Document]:
    """
    Splits a list of documents into smaller chunks based on configured settings.

    Args:
        documents (List[Document]): The list of documents to split.

    Returns:
        List[Document]: A list of smaller document chunks.
    """
    log("Splitting documents into smaller chunks...", level="info")
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=settings.CHUNK_SIZE,
        chunk_overlap=settings.CHUNK_OVERLAP,
        length_function=len,
        is_separator_regex=False,
    )
    chunks = text_splitter.split_documents(documents)
    log(f"Successfully split documents into {len(chunks)} chunks.", level="info")
    return chunks


def split_documents_markdown(documents: List[Document]) -> List[Document]:
    """
    Splits a list of documents (assumed to be Mineru Markdown) into
    chunks, after *surgically* cleaning HTML and LaTeX.
    
    Uses a Markdown-aware Recursive splitter to create
    semantically-aware, size-controlled chunks that respect headers.
    """
    log("Surgically cleaning and splitting Markdown documents...", level="info")
    
    # --- 1. Initialize Converters ---
    l2t = LatexNodes2Text() if LatexNodes2Text else None
    h = html2text.HTML2Text()
    h.body_width = 0
    h.single_line_break = True

    # --- 2. The "Smart" Recursive Markdown Splitter ---
    markdown_splitter = RecursiveCharacterTextSplitter.from_language(
        language="markdown",
        chunk_size=settings.CHUNK_SIZE,
        chunk_overlap=settings.CHUNK_OVERLAP
    )

    cleaned_documents = []
    
    for doc in documents:
        cleaned_content = doc.page_content

        # --- 3. Apply Chained *Surgical* Cleaning ---
        
        # Step A: Clean *only* HTML table blocks
        repl_html_func = lambda m: _clean_html_snippet(m, h)
        cleaned_content = re.sub(
            r'<table.*?</table>', 
            repl_html_func, 
            cleaned_content, 
            flags=re.DOTALL | re.IGNORECASE
        )
        
        # Step B: Clean *only* LaTeX $...$ blocks
        if l2t:
            repl_latex_func = lambda m: _clean_latex_snippet(m, l2t)
            cleaned_content = re.sub(r'\$(.*?)\$', repl_latex_func, cleaned_content)

        # Create a new, clean Document
        cleaned_documents.append(
            Document(
                page_content=cleaned_content,
                metadata=doc.metadata
            )
        )

    # --- 4. Split the Cleaned Documents ---
    # This splitter is smart. It will try to split on "#" first.
    # It will keep tables together (since they are now clean text).
    # And it will not discard empty headers.
    chunks = markdown_splitter.split_documents(cleaned_documents)

    log(f"Successfully split documents into {len(chunks)} chunks.", level="info")

    return chunks



# Helper functions for surgical cleaning (temporary)
def _clean_latex_snippet(match, latex_converter):
    latex_str = match.group(1) 
    if not latex_str or not latex_converter: return match.group(0) 
    try:
        plain_text = latex_converter.latex_to_text(latex_str) 
        return re.sub(r'\s+', ' ', plain_text).strip()
    except Exception as e:
        log(f"LaTeX snippet parsing failed ('{latex_str}'): {e}", level="warning")
        return re.sub(r'\\[a-zA-Z]+\s*', '', latex_str).strip()

def _clean_html_snippet(match, html_converter):
    html_str = match.group(0)
    if not html_converter: return html_str
    try:
        markdown_table = html_converter.handle(html_str)
        return f"\n{markdown_table}\n"
    except Exception as e:
        log(f"HTML snippet parsing failed: {e}", level="warning")
        return html_str

