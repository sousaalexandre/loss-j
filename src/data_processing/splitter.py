from langchain_text_splitters import RecursiveCharacterTextSplitter, MarkdownHeaderTextSplitter
from langchain_core.documents import Document
from typing import List, Dict, Tuple
import re
import html2text
from pylatexenc.latex2text import LatexNodes2Text
from src import settings
from src.logger import log

CHUNK_SIZE = settings.CHUNK_SIZE
CHUNK_OVERLAP = settings.CHUNK_OVERLAP


def split_documents_markdown(
    documents: List[Document],
    chunk_size: int = None,
    chunk_overlap: int = None,
    include_header_prefix: bool = True,
    min_chunk_size: int = 50
) -> List[Document]:
    """
    Splits markdown documents hierarchically, preserving header context.

    Args:
        documents: List of Document objects with markdown content
        chunk_size: Maximum chunk size (default: settings.CHUNK_SIZE)
        chunk_overlap: Overlap between chunks (default: settings.CHUNK_OVERLAP)
        include_header_prefix: Whether to prepend "[Contexto: ...]" to chunks
        min_chunk_size: Minimum chunk size (smaller chunks filtered out)
    
    Returns:
        List of Document chunks with hierarchical context preserved
    """
    chunk_size = chunk_size or CHUNK_SIZE
    chunk_overlap = chunk_overlap or CHUNK_OVERLAP
    
    log("Splitting documents with hierarchical context...", level="info")
    
    headers_to_split_on = [
        ("#", "h1"),
        ("##", "h2"),
        ("###", "h3"),
    ]
    
    md_header_splitter = MarkdownHeaderTextSplitter(
        headers_to_split_on=headers_to_split_on,
        strip_headers=False
    )
    
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
        length_function=len,
        separators=["\n\n", "\n", ". ", " ", ""]
    )
    
    all_chunks = []
    
    for doc in documents:
        base_metadata = _extract_base_metadata(doc)
        cleaned_content = _clean_content(doc.page_content)
        header_splits = md_header_splitter.split_text(cleaned_content)
        
        for split in header_splits:
            header_hierarchy = _build_header_hierarchy(split.metadata)

            header_prefix = ""
            if include_header_prefix and (header_hierarchy or base_metadata.get("category")):
                header_prefix = _format_header_prefix(
                    header_hierarchy,
                    base_metadata.get("category", "")
                )

            content = split.page_content.strip()
            
            if not content or len(content) < 10:
                continue
            
            full_content = header_prefix + content if header_prefix else content
            
            if len(full_content) > chunk_size:
                sub_chunks = text_splitter.split_text(content)
                for i, sub_chunk in enumerate(sub_chunks):
                    sub_content = header_prefix + sub_chunk
                    
                    chunk_metadata = _build_ordered_metadata(
                        base_metadata=base_metadata,
                        split_metadata=split.metadata,
                        header_hierarchy=header_hierarchy,
                        is_sub_chunk=True,
                        chunk_index=i,
                        total_sub_chunks=len(sub_chunks)
                    )
                    
                    chunk_doc = Document(
                        page_content=sub_content,
                        metadata=chunk_metadata
                    )
                    all_chunks.append(chunk_doc)
            else:
                full_content = header_prefix + content if header_prefix else content
                
                chunk_metadata = _build_ordered_metadata(
                    base_metadata=base_metadata,
                    split_metadata=split.metadata,
                    header_hierarchy=header_hierarchy,
                    is_sub_chunk=False
                )
                
                chunk_doc = Document(
                    page_content=full_content,
                    metadata=chunk_metadata
                )
                all_chunks.append(chunk_doc)
    
    # Sanitize metadata for vector store compatibility
    for chunk in all_chunks:
        chunk.metadata = _sanitize_metadata(chunk.metadata)

    # Filter small chunks
    all_chunks = [c for c in all_chunks if len(c.page_content) >= min_chunk_size]

    log(f"Successfully split into {len(all_chunks)} chunks from {len(documents)} documents.", level="info")

    return all_chunks


def _extract_base_metadata(doc: Document) -> Dict:
    """
    Extracts base metadata from document.
    """
    original_metadata = dict(doc.metadata) if doc.metadata else {}
    content = doc.page_content
    
    source = original_metadata.get("source", "")
    
    # Handle both Windows and Unix path separators
    filename = source.replace("\\", "/").split("/")[-1] if source else ""
    
    category = _detect_category(content, source)
    
    # Extract document title (first H1)
    title_match = re.search(r'^#\s+(.+?)$', content, re.MULTILINE)
    document_title = title_match.group(1).strip() if title_match else ""
    
    # Get page if present
    page = original_metadata.get("page")
    
    return {
        "source": source,
        "filename": filename,
        "category": category,
        "document_title": document_title,
        "page": page
    }


def _build_ordered_metadata(
    base_metadata: Dict,
    split_metadata: Dict,
    header_hierarchy: List[Tuple[str, str]],
    is_sub_chunk: bool,
    chunk_index: int = None,
    total_sub_chunks: int = None
) -> Dict:
    """
    Builds metadata in consistent order:
    source -> filename -> category -> document_title -> h1,h2,h3 -> header_hierarchy -> is_sub_chunk -> page
    """
    metadata = {}
    
    # 1. source
    metadata["source"] = base_metadata.get("source", "")
    
    # 2. filename
    metadata["filename"] = base_metadata.get("filename", "")
    
    # 3. category
    metadata["category"] = base_metadata.get("category", "Geral")
    
    # 4. document_title
    metadata["document_title"] = base_metadata.get("document_title", "")
    
    # 5. h1, h2, h3
    metadata["h1"] = split_metadata.get("h1", "")
    metadata["h2"] = split_metadata.get("h2", "")
    metadata["h3"] = split_metadata.get("h3", "")
    
    # 6. header_hierarchy
    metadata["header_hierarchy"] = _hierarchy_to_string(header_hierarchy)
    
    # 7. is_sub_chunk (and related fields)
    metadata["is_sub_chunk"] = is_sub_chunk
    if is_sub_chunk and chunk_index is not None:
        metadata["chunk_index"] = chunk_index
        metadata["total_sub_chunks"] = total_sub_chunks
    
    # 8. page
    page = base_metadata.get("page")
    if page is not None:
        metadata["page"] = page
    
    return metadata


def _detect_category(content: str, source: str = "") -> str:
    """
    Detects document category based on content and filename.
    """
    content_upper = content[:2000].upper()
    source_upper = source.upper()
    
    if "TABELA REMUNERATÓRIA" in content_upper or "REMUNERAÇÃO" in source_upper:
        return "Remunerações"
    
    if "REGIME DE CONTRATO ESPECIAL" in content_upper or "RCE" in content_upper[:500]:
        return "RCE"
    
    if "ÁREAS FUNCIONAIS OFICIAIS" in content_upper or "OFICIAIS" in source_upper:
        return "Oficiais"
    
    if "ÁREAS FUNCIONAIS SARGENTOS" in content_upper or "SARGENTOS" in source_upper:
        return "Sargentos"
    
    if "ESPECIALIDADES PRAÇAS" in content_upper or "PRAÇAS" in content_upper[:500] or "PRACAS" in source_upper:
        return "Praças"
    
    return "Geral"


def _build_header_hierarchy(metadata: Dict) -> List[Tuple[str, str]]:
    """Builds ordered list of headers from metadata."""
    hierarchy = []
    for level in ["h1", "h2", "h3"]:
        if level in metadata and metadata[level]:
            hierarchy.append((level, metadata[level]))
    return hierarchy


def _format_header_prefix(hierarchy: List[Tuple[str, str]], category: str = "") -> str:
    """Formats header hierarchy as context prefix, including category."""
    if not hierarchy and not category:
        return ""
    
    parts = []
    if category:
        parts.append(f"Categoria: {category}")
    if hierarchy:
        headers = [h[1] for h in hierarchy]
        breadcrumb = " > ".join(headers)
        parts.append(f"Contexto: {breadcrumb}")
    
    prefix = " | ".join(parts)
    return f"[{prefix}]\n\n"


def _hierarchy_to_string(hierarchy: List[Tuple[str, str]]) -> str:
    """Converts header hierarchy to string for vector store compatibility."""
    if not hierarchy:
        return ""
    return " > ".join([h[1] for h in hierarchy])


def _clean_content(content: str) -> str:
    """Cleans HTML tables and LaTeX from markdown content."""
    try:
        l2t = LatexNodes2Text()
    except:
        l2t = None
    
    h = html2text.HTML2Text()
    h.body_width = 0
    h.single_line_break = True
    
    cleaned = content
    
    cleaned = re.sub(
        r'<table.*?</table>',
        lambda m: f"\n{h.handle(m.group(0))}\n",
        cleaned,
        flags=re.DOTALL | re.IGNORECASE
    )
    
    if l2t:
        def clean_latex(match):
            try:
                plain = l2t.latex_to_text(match.group(1))
                return re.sub(r'\s+', ' ', plain).strip()
            except:
                return match.group(0)
        
        cleaned = re.sub(r'\$(.*?)\$', clean_latex, cleaned)
    
    return cleaned


def _sanitize_metadata(metadata: Dict) -> Dict:
    """
    Ensures all metadata values are vector store compatible.
    Converts lists/tuples to strings, removes None values.
    Enforces consistent key order.
    """
    # Define the desired order
    key_order = [
        "source", "filename", "category", "document_title",
        "h1", "h2", "h3", "header_hierarchy",
        "is_sub_chunk", "chunk_index", "total_sub_chunks", "page"
    ]
    
    def sanitize_value(value):
        if value is None:
            return None
        elif isinstance(value, (str, int, float, bool)):
            return value
        elif isinstance(value, (list, tuple)):
            if len(value) > 0 and isinstance(value[0], (list, tuple)):
                return " > ".join([str(v[1]) if len(v) > 1 else str(v[0]) for v in value])
            else:
                return ", ".join([str(v) for v in value])
        else:
            return str(value)
    
    # Build ordered dict
    sanitized = {}
    
    # First add keys in defined order
    for key in key_order:
        if key in metadata:
            value = sanitize_value(metadata[key])
            if value is not None:
                sanitized[key] = value
    
    # Then add any remaining keys not in the defined order
    for key, value in metadata.items():
        if key not in sanitized:
            value = sanitize_value(value)
            if value is not None:
                sanitized[key] = value
    
    return sanitized