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
    - "markdown_hierarchical": Hierarchical splitting with context preservation (recommended for MinerU)
    """
    if settings.LOADER_TYPE == "pdfloader":
        log("Using basic recursive splitter (PDF mode)", level="info")
        return _split_recursive(documents)
    
    if settings.SPLITTING_TYPE == "markdown_hierarchical_with_metadata":
        log("Using hierarchical markdown splitter with user metadata from landing zone", level="info")
        return split_documents_markdown_hierarchical_with_metadata(documents)
    elif settings.SPLITTING_TYPE == "markdown_hierarchical":
        log("Using hierarchical markdown splitter with context preservation", level="info")
        return split_documents_markdown_hierarchical(documents)
    elif settings.SPLITTING_TYPE == "markdown_recursive":
        log("Using markdown-aware splitter", level="info")
        return _split_markdown(documents)
    else:
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


def split_documents_markdown_hierarchical(
    documents: List[Document],
    chunk_size: int = None,
    chunk_overlap: int = None,
    include_header_prefix: bool = True,
    min_chunk_size: int = 50
) -> List[Document]:
    """
    Splits markdown documents hierarchically, preserving header context.
    Recommended for MinerU-extracted documents with rich structure.

    Args:
        documents: List of Document objects with markdown content
        chunk_size: Maximum chunk size (default: settings.CHUNK_SIZE)
        chunk_overlap: Overlap between chunks (default: settings.CHUNK_OVERLAP)
        include_header_prefix: Whether to prepend context metadata to chunks
        min_chunk_size: Minimum chunk size (smaller chunks filtered out)
    
    Returns:
        List of Document chunks with hierarchical context preserved
    """
    chunk_size = chunk_size or CHUNK_SIZE
    chunk_overlap = chunk_overlap or CHUNK_OVERLAP
    
    log("Splitting documents with hierarchical context preservation...", level="info")
    
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
        
        try:
            header_splits = md_header_splitter.split_text(cleaned_content)
        except Exception as e:
            log(f"Header splitting failed, falling back to recursive: {e}", level="warning")
            chunks = text_splitter.split_documents([doc])
            all_chunks.extend(chunks)
            continue
        
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

    log(f"Successfully split into {len(all_chunks)} chunks with hierarchical context from {len(documents)} documents.", level="info")

    return all_chunks


def _extract_base_metadata(doc: Document) -> Dict:
    """
    Extracts base metadata from document.
    Attempts to recover original filename from source path.
    """
    original_metadata = dict(doc.metadata) if doc.metadata else {}
    content = doc.page_content
    
    source = original_metadata.get("source", "")
    
    # Handle both Windows and Unix path separators
    filename = source.replace("\\", "/").split("/")[-1] if source else ""
    
    # Extract document category from content and filename
    category = _detect_category(content, source)
    
    # Extract document title (first H1)
    title_match = re.search(r'^#\s+(.+?)$', content, re.MULTILINE)
    document_title = title_match.group(1).strip() if title_match else ""
    
    # Get page if present
    page = original_metadata.get("page")
    
    # Get user-provided metadata if available
    user_title = original_metadata.get("user_title", "")
    user_category = original_metadata.get("user_category", "")
    
    return {
        "source": source,
        "filename": filename,
        "category": user_category or category,
        "document_title": user_title or document_title,
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
    Builds metadata in consistent order for vector store compatibility.
    """
    metadata = {}
    
    # Consistent key order
    metadata["source"] = base_metadata.get("source", "")
    metadata["filename"] = base_metadata.get("filename", "")
    metadata["category"] = base_metadata.get("category", "Geral")
    metadata["document_title"] = base_metadata.get("document_title", "")
    metadata["h1"] = split_metadata.get("h1", "")
    metadata["h2"] = split_metadata.get("h2", "")
    metadata["h3"] = split_metadata.get("h3", "")
    metadata["h4"] = split_metadata.get("h4", "")
    metadata["h5"] = split_metadata.get("h5", "")
    metadata["header_hierarchy"] = _hierarchy_to_string(header_hierarchy)
    metadata["is_sub_chunk"] = is_sub_chunk
    
    # Only add chunk_index and total_sub_chunks when this is actually a sub-chunk
    if is_sub_chunk:
        metadata["chunk_index"] = chunk_index if chunk_index is not None else 0
        metadata["total_sub_chunks"] = total_sub_chunks if total_sub_chunks is not None else 0
    
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
    for level in ["h1", "h2", "h3", "h4", "h5"]:
        if level in metadata and metadata[level]:
            hierarchy.append((level, metadata[level]))
    return hierarchy


def _format_header_prefix(hierarchy: List[Tuple[str, str]], category: str = "") -> str:
    """Formats header hierarchy as context prefix."""
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
    
    # Convert HTML tables to markdown
    cleaned = re.sub(
        r'<table.*?</table>',
        lambda m: f"\n{h.handle(m.group(0))}\n",
        cleaned,
        flags=re.DOTALL | re.IGNORECASE
    )
    
    # Clean LaTeX
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
    Converts lists/tuples to strings, removes None and empty string values.
    Converts booleans to strings to preserve them in vector store.
    """
    key_order = [
        "source", "filename", "category", "document_title",
        "h1", "h2", "h3", "h4", "h5", "header_hierarchy",
        "is_sub_chunk", "chunk_index", "total_sub_chunks", "page",
        "title", "keywords", "description", "original_filename"
    ]
    
    def sanitize_value(value):
        if value is None:
            return None
        elif isinstance(value, bool):  # Convert bool to string to preserve in vector store
            return "true" if value else "false"
        elif isinstance(value, int):  # Handle integers (including 0)
            return value
        elif isinstance(value, (str, float)):
            # Only return non-empty strings
            if isinstance(value, str) and value == "":
                return None
            return value
        elif isinstance(value, (list, tuple)):
            if len(value) > 0 and isinstance(value[0], (list, tuple)):
                return " > ".join([str(v[1]) if len(v) > 1 else str(v[0]) for v in value])
            else:
                return ", ".join([str(v) for v in value])
        else:
            return str(value)
    
    sanitized = {}
    
    # Add keys in defined order
    for key in key_order:
        if key in metadata:
            value = sanitize_value(metadata[key])
            if value is not None:  # Include all non-None values
                sanitized[key] = value
    
    # Add any remaining keys not in key_order
    for key, value in metadata.items():
        if key not in sanitized:
            value = sanitize_value(value)
            if value is not None:
                sanitized[key] = value
    
    return sanitized


def _split_recursive_simple(documents: List[Document]) -> List[Document]:
    """
    Dedicated simple recursive text splitter for user metadata workflow.
    
    Uses RecursiveCharacterTextSplitter without any hierarchical header logic.
    Works reliably with any markdown structure.
    
    Args:
        documents: List of Document objects with markdown content
    
    Returns:
        List of Document chunks
    """
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=CHUNK_SIZE,
        chunk_overlap=CHUNK_OVERLAP,
        length_function=len,
        separators=["\n\n", "\n", ". ", " ", ""]
    )
    
    return text_splitter.split_documents(documents)


def _load_user_metadata_for_chunk(source: str) -> dict:
    """
    Dedicated loader for user metadata from landing zone JSON files.
    
    Reads JSON file matching chunk source and returns metadata dictionary.
    Returns empty dict if file not found.
    
    Args:
        source: Source file path from chunk metadata
    
    Returns:
        Dictionary with title, keywords, description, category, original_filename
    """
    if not source:
        return {}
    
    try:
        # Extract file hash from source path
        source_file = Path(source)
        file_hash = source_file.stem
        
        # Construct path to metadata JSON
        metadata_file = Path("data_lakehouse/00_landing_zone") / f"{file_hash}.json"
        
        if metadata_file.exists():
            with open(metadata_file, 'r', encoding='utf-8') as f:
                return json.load(f)
    
    except Exception as e:
        log(f"Warning loading metadata for {source}: {e}", level="warning")
    
    return {}


def _inject_user_metadata_to_chunk(chunk: Document, user_metadata: dict) -> None:
    """
    Dedicated injector for user metadata into chunk metadata.
    
    Modifies chunk in-place, adding user-provided fields only if they have values.
    Does nothing if user_metadata is empty.
    
    Args:
        chunk: Document chunk to enrich (modified in-place)
        user_metadata: Dictionary with user metadata fields
    
    Returns:
        None (modifies chunk in-place)
    """
    if not user_metadata:
        return
    
    # Only add non-empty metadata fields
    if user_metadata.get("title"):
        chunk.metadata["title"] = user_metadata["title"]
    if user_metadata.get("keywords"):
        chunk.metadata["keywords"] = user_metadata["keywords"]
    if user_metadata.get("description"):
        chunk.metadata["description"] = user_metadata["description"]
    if user_metadata.get("original_filename"):
        chunk.metadata["original_filename"] = user_metadata["original_filename"]


def _sanitize_user_metadata_chunk(metadata: dict) -> dict:
    """
    Dedicated sanitizer for user metadata chunks.
    
    Custom logic specific to user metadata workflow:
    - Preserves all metadata fields
    - Removes empty strings and None values
    - Maintains key ordering for consistency
    
    Args:
        metadata: Metadata dictionary from chunk
    
    Returns:
        Cleaned metadata dictionary
    """
    if not metadata:
        return {}
    
    # Define order for consistent output
    key_order = [
        "source", "filename", "page",
        "category", "title", "keywords", "description", "original_filename"
    ]
    
    cleaned = {}
    
    # Add keys in defined order
    for key in key_order:
        if key in metadata:
            value = metadata[key]
            # Only include non-empty values
            if value is not None and value != "":
                cleaned[key] = value
    
    # Add any remaining keys not in key_order
    for key, value in metadata.items():
        if key not in cleaned and value is not None and value != "":
            cleaned[key] = value
    
    return cleaned


def split_documents_markdown_hierarchical_with_metadata(documents: List[Document]) -> List[Document]:
    """
    Split markdown documents hierarchically with user metadata from landing zone.
    
    Uses hierarchical splitting (respects header structure) and injects user metadata
    without cleaning the content.
    
    Workflow:
    1. Load user metadata ONCE per document from landing zone
    2. Use hierarchical markdown splitting (respects header structure)
    3. Inject metadata ONLY if it contains useful data
    4. Sanitize metadata for vector store compatibility
    
    Args:
        documents: List of Document objects with markdown content
    
    Returns:
        List of Document chunks enriched with user metadata and hierarchical context
    """
    
    log("Splitting markdown hierarchically with user metadata from landing zone", level="info")
    
    chunk_size = CHUNK_SIZE
    chunk_overlap = CHUNK_OVERLAP
    include_header_prefix = True
    min_chunk_size = 50
    
    headers_to_split_on = [
        ("#", "h1"),
        ("##", "h2"),
        ("###", "h3"),
        ("####", "h4"),
        ("#####", "h5"),
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
        # Load user metadata ONCE per document
        source = base_metadata.get("source", "")
        user_metadata = _load_user_metadata_for_chunk(source)
        
        # NO CLEANING - work with content as-is
        content = doc.page_content
        # Temporarily apply cleaning to see the effect
        #content = _clean_content(doc.page_content)
        
        try:
            header_splits = md_header_splitter.split_text(content)
        except Exception as e:
            log(f"Header splitting failed, falling back to recursive: {e}", level="warning")
            chunks = text_splitter.split_documents([doc])
            all_chunks.extend(chunks)
            continue
        
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
                    
                    # Inject user metadata if it has useful data
                    if user_metadata:
                        _inject_user_metadata_to_chunk(chunk_doc, user_metadata)
                    
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
                
                # Inject user metadata if it has useful data
                if user_metadata:
                    _inject_user_metadata_to_chunk(chunk_doc, user_metadata)
                
                all_chunks.append(chunk_doc)
    
    # Sanitize metadata for vector store compatibility
    for chunk in all_chunks:
        chunk.metadata = _sanitize_metadata(chunk.metadata)

    # Filter small chunks
    all_chunks = [c for c in all_chunks if len(c.page_content) >= min_chunk_size]

    log(f"Successfully split into {len(all_chunks)} chunks with hierarchical context and user metadata from {len(documents)} documents.", level="info")

    return all_chunks
