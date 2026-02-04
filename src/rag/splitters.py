from langchain_text_splitters import RecursiveCharacterTextSplitter, MarkdownHeaderTextSplitter
from langchain_core.documents import Document
from typing import List, Dict, Tuple, Optional
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
    if settings.SPLITTING_TYPE == "hierarchical":
        log("Using hierarchical splitter", level="info")
        return _split_hierarchichal(documents)
    elif settings.SPLITTING_TYPE == "markdown_recursive":
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
    Markdown splitting otimizado para RAG + enriquecimento de metadados via _catalog.json.

    - Descobre automaticamente o(s) _catalog.json com base no caminho em doc.metadata["source"]
      (suporta indexing via 03_gold/*.md e via 00_landing_zone/*.pdf).
    - Split estrutural por headers e reconstrução de hierarquia por heurística (numeração tipo 2.6.2).
    - Chunking recursivo dentro de cada secção com separadores mais semânticos.
    - Mantém/normaliza 'source' (para funcionar bem com Chroma delete/get where={"source": ...}).
    - SANITIZA metadata: garante que todos os valores são str/int/float/bool/None (sem listas/dicts),
      evitando erro no upsert do vector store.
    """

    # -----------------------------
    # Helpers: metadata sanitization
    # -----------------------------
    def _sanitize_metadata(meta: Dict) -> Dict:
        """
        Garante que todos os valores de metadata são: str/int/float/bool/None.
        Converte list/tuple/set/dict para string (join ou json).
        """
        clean = {}
        for k, v in (meta or {}).items():
            if v is None or isinstance(v, (str, int, float, bool)):
                clean[k] = v
            elif isinstance(v, (list, tuple, set)):
                # lista de strings -> join; caso contrário -> json
                if all(isinstance(x, str) for x in v):
                    clean[k] = " | ".join(x.strip() for x in v if x and x.strip())
                else:
                    clean[k] = json.dumps(list(v), ensure_ascii=False, default=str)
            elif isinstance(v, dict):
                clean[k] = json.dumps(v, ensure_ascii=False, default=str)
            else:
                clean[k] = str(v)
        return clean

    # -----------------------------
    # Helpers: paths & catálogo
    # -----------------------------
    cwd = Path.cwd()
    module_root = Path(__file__).resolve().parents[2]  # .../src/rag/splitters.py -> root do projeto

    def _normalize_source(meta: Dict) -> None:
        src = meta.get("source")
        if isinstance(src, str) and "\\" in src:
            meta["source"] = src.replace("\\", "/")

    def _infer_doc_id(doc: Document) -> str:
        md = doc.metadata or {}
        for k in ("doc_id", "id", "file_id", "sha", "sha256", "hash"):
            v = md.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()

        src = md.get("source")
        if isinstance(src, str) and src.strip():
            p = Path(src)
            # se for relativo, tenta resolver pelo cwd
            if not p.is_absolute():
                p = (cwd / p).resolve()
            stem = p.stem
            return stem if stem else ""
        return ""

    def _find_lakehouse_dirs(docs: List[Document]) -> List[Path]:
        found = []
        seen = set()

        for d in docs:
            src = (d.metadata or {}).get("source")
            if not isinstance(src, str) or not src.strip():
                continue

            p = Path(src)
            if not p.is_absolute():
                p = (cwd / p)

            # percorre parents à procura de ".../data_lakehouse"
            for parent in [p] + list(p.parents):
                if parent.name == "data_lakehouse":
                    rp = parent.resolve()
                    if str(rp) not in seen:
                        seen.add(str(rp))
                        found.append(rp)
                    break

        return found

    def _load_catalog_file(path: Path) -> Dict:
        try:
            if path.exists() and path.is_file():
                return json.loads(path.read_text(encoding="utf-8"))
        except Exception as e:
            log(f"Failed to load catalog at {path}: {e}", level="warning")
        return {}

    def _load_catalog(docs: List[Document]) -> Dict[str, Dict]:
        # cache simples entre chamadas (mesmo processo)
        cache_key = getattr(_split_markdown, "_catalog_cache_key", None)
        cache_val = getattr(_split_markdown, "_catalog_cache", None)

        lakehouse_dirs = _find_lakehouse_dirs(docs)

        # fallbacks (se não conseguir inferir a partir do source)
        for candidate in (cwd / "data_lakehouse", module_root / "data_lakehouse"):
            try:
                if candidate.exists() and candidate.is_dir():
                    rp = candidate.resolve()
                    if rp not in lakehouse_dirs:
                        lakehouse_dirs.append(rp)
            except Exception:
                pass

        # construir lista de catálogos candidatos
        candidates = []
        for lh in lakehouse_dirs:
            candidates.append(lh / "03_gold" / "_catalog.json")
            candidates.append(lh / "00_landing_zone" / "_catalog.json")

        # key determinística p/ cache
        key = tuple(sorted([str(p) for p in candidates]))

        if cache_key == key and isinstance(cache_val, dict):
            return cache_val

        # merge: landing_zone (base) + gold (override)
        merged: Dict[str, Dict] = {}

        # primeiro landing_zone
        for p in candidates:
            if p.as_posix().endswith("00_landing_zone/_catalog.json"):
                merged.update(_load_catalog_file(p))

        # depois 03_gold por cima
        for p in candidates:
            if p.as_posix().endswith("03_gold/_catalog.json"):
                merged.update(_load_catalog_file(p))

        _split_markdown._catalog_cache_key = key
        _split_markdown._catalog_cache = merged

        if merged:
            log("Catalog metadata loaded/merged successfully.", level="info")
        else:
            log("No _catalog.json found; proceeding without external metadata enrichment.", level="warning")

        return merged

    catalog = _load_catalog(documents)

    # -----------------------------
    # Helpers: headers -> secções
    # -----------------------------
    def _infer_depth_and_number(title: str) -> Tuple[int, str]:
        t = title.strip()

        # numeração tipo 2.6.2 ou 2.6.2.
        m = re.match(r"^(?P<num>\d+(?:\.\d+)*)(?:\.)?\s*(?P<rest>.*)$", t)
        if m:
            num = m.group("num")
            depth = num.count(".") + 1
            return depth, num

        up = t.upper()
        if up.startswith("PARTE"):
            return 1, ""
        if up.startswith("TÍTULO") or up.startswith("TITULO"):
            return 2, ""
        if up.startswith("CAPÍTULO") or up.startswith("CAPITULO"):
            return 3, ""
        if up.startswith("SECÇÃO") or up.startswith("SECCAO") or up.startswith("SEÇÃO") or up.startswith("SECAO"):
            return 4, ""
        if up.startswith("SUBSECÇÃO") or up.startswith("SUBSECCAO") or up.startswith("SUBSEÇÃO") or up.startswith("SUBSECAO"):
            return 5, ""
        if up.startswith("ANEXO"):
            return 2, ""

        return 1, ""

    def _split_into_sections(full_text: str, base_meta: Dict) -> List[Document]:
        header_re = re.compile(r"(?m)^(#{1,6})\s+(?P<title>.+?)\s*$")
        matches = list(header_re.finditer(full_text))

        if not matches:
            return [Document(page_content=full_text, metadata=base_meta)]

        sections: List[Document] = []
        stack: List[str] = []

        # preamble (texto antes do 1º header)
        if matches[0].start() > 0:
            pre = full_text[:matches[0].start()].strip()
            if pre:
                meta = dict(base_meta)
                meta.update({
                    "section_title": "Preamble",
                    "section_number": "",
                    "section_depth": 1,
                    "section_path": "Preamble",
                    "section_index": -1,
                    "is_toc": False,
                })
                sections.append(Document(page_content=pre, metadata=_sanitize_metadata(meta)))

        for i, m in enumerate(matches):
            start = m.start()
            end = matches[i + 1].start() if i + 1 < len(matches) else len(full_text)

            hashes = m.group(1)
            title = m.group("title").strip()

            # depth: max(nível do header, heurística por numeração)
            inferred_depth, secnum = _infer_depth_and_number(title)
            depth = max(len(hashes), inferred_depth)

            # atualizar breadcrumb stack
            while len(stack) >= depth:
                stack.pop()
            stack.append(title)
            section_path = " > ".join(stack)

            section_text = full_text[start:end].strip()

            meta = dict(base_meta)
            meta.update({
                "section_title": title,
                "section_number": secnum,
                "section_depth": depth,
                "section_path": section_path,
                "section_index": i,
                "is_toc": bool(re.search(r"\b(índice|indice|index)\b", title, flags=re.I)),
            })

            sections.append(Document(page_content=section_text, metadata=_sanitize_metadata(meta)))

        return sections

    # -----------------------------
    # Chunking interno por secção
    # -----------------------------
    inner_splitter = RecursiveCharacterTextSplitter(
        chunk_size=CHUNK_SIZE,
        chunk_overlap=CHUNK_OVERLAP,
        length_function=len,
        is_separator_regex=False,
        separators=[
            "\n\n",
            "\n- ", "\n* ", "\n• ", "\n ", "\n1. ",
            "\n```", "```",
            "\n<table", "</table>", "</tr>", "<tr",
            "\n",
            " ",
            ""
        ],
    )

    def _short_path(section_path: str, keep_last: int = 3) -> str:
        parts = [p.strip() for p in section_path.split(">")]
        parts = [p for p in parts if p]
        if len(parts) <= keep_last:
            return " > ".join(parts)
        return " > ".join(parts[-keep_last:])

    all_chunks: List[Document] = []

    for doc in documents:
        base_meta = dict(doc.metadata or {})
        _normalize_source(base_meta)

        # SANITIZA metadata que já vinha do loader
        base_meta = _sanitize_metadata(base_meta)

        # doc_id
        doc_id = _infer_doc_id(doc)
        if doc_id:
            base_meta.setdefault("doc_id", doc_id)

        # enriquecer metadados via catálogo
        if doc_id and doc_id in catalog:
            entry = catalog.get(doc_id, {})

            # merge sem rebentar o que já existe
            for k, v in entry.items():
                if k not in base_meta or base_meta.get(k) in (None, "", [], {}):
                    base_meta[k] = v

            # category em lista -> string (antes da sanitização final)
            if isinstance(base_meta.get("category"), list):
                joined = " | ".join(
                    x.strip() for x in base_meta["category"]
                    if isinstance(x, str) and x.strip()
                )
                base_meta["category"] = joined
                base_meta.setdefault("category_str", joined)

            # sanitiza após merge (converte quaisquer listas/dicts restantes)
            base_meta = _sanitize_metadata(base_meta)

        # split em secções
        section_docs = _split_into_sections(doc.page_content, base_meta)

        # split recursivo dentro de cada secção
        doc_chunks: List[Document] = []
        for sdoc in section_docs:
            s_chunks = inner_splitter.split_documents([sdoc])
            doc_chunks.extend(s_chunks)

        # pós-process: adicionar contexto por chunk (sem rebentar tamanho)
        total = len(doc_chunks)
        for idx, ch in enumerate(doc_chunks):
            ch.metadata = dict(ch.metadata or {})
            _normalize_source(ch.metadata)

            ch.metadata["chunk_index"] = idx
            ch.metadata["chunk_total"] = total

            # chunk_id estável (bom para debug)
            did = ch.metadata.get("doc_id") or doc_id or ""
            sec_i = ch.metadata.get("section_index", 0)
            ch.metadata["chunk_id"] = f"{did}:{sec_i}:{idx}" if did else f"{sec_i}:{idx}"

            # prefixo de contexto
            sec_path = ch.metadata.get("section_path")
            doc_title = ch.metadata.get("title") or ch.metadata.get("original_filename")

            prefix_lines = []
            if doc_title:
                prefix_lines.append(f"Documento: {doc_title}")
            if isinstance(sec_path, str) and sec_path.strip():
                prefix_lines.append(f"Secção: {_short_path(sec_path)}")

            prefix = "\n".join(prefix_lines).strip()
            if prefix and not ch.page_content.lstrip().startswith("#"):
                ch.page_content = prefix + "\n\n" + ch.page_content

            # SANITIZA metadata final do chunk (evita listas/dicts no upsert)
            ch.metadata = _sanitize_metadata(ch.metadata)

        all_chunks.extend(doc_chunks)

    # redundante mas seguro: sanitiza tudo no fim
    for ch in all_chunks:
        ch.metadata = _sanitize_metadata(ch.metadata)

    log(f"Successfully split documents into {len(all_chunks)} chunks (markdown-optimized + metadata).", level="info")
    return all_chunks

def _split_hierarchichal(documents: List[Document], chunk_size: Optional[int] = None, chunk_overlap: Optional[int] = None, include_header_prefix: bool = True, min_chunk_size: int = 50, ) -> List[Document]:
    """
    Hierarchical markdown splitting (headers -> subchunks) + metadata enrichment via indexed _catalog.json.

    - Loads/merges _catalog.json automatically (00_landing_zone base + 03_gold override) based on doc.metadata["source"]
      and caches it across calls in the same process.
    - Infers doc_id from metadata (doc_id/id/hash/sha...) or from filename stem of source.
    - Merges catalog entry into metadata without overwriting existing non-empty values.
    - Adds header breadcrumb context prefix (your "[Contexto: ...]") to each chunk.
    - Sanitizes metadata for vector stores (no lists/dicts; only str/int/float/bool).
    """

    chunk_size = chunk_size or CHUNK_SIZE
    chunk_overlap = chunk_overlap or CHUNK_OVERLAP

    # -----------------------------
    # Helpers: small utilities
    # -----------------------------
    def _normalize_source(meta: Dict) -> None:
        src = meta.get("source")
        if isinstance(src, str) and "\\" in src:
            meta["source"] = src.replace("\\", "/")

    def _is_empty(v) -> bool:
        return v is None or v == "" or v == [] or v == {}  # noqa: E711

    def _sanitize_value(value):
        if value is None:
            return None
        if isinstance(value, (str, int, float, bool)):
            return value
        if isinstance(value, (list, tuple, set)):
            # list of strings -> join; else json
            if all(isinstance(x, str) for x in value):
                return " | ".join(x.strip() for x in value if x and x.strip())
            return json.dumps(list(value), ensure_ascii=False, default=str)
        if isinstance(value, dict):
            return json.dumps(value, ensure_ascii=False, default=str)
        return str(value)

    def _sanitize_metadata(meta: Dict) -> Dict:
        """
        Vector store compatible: only str/int/float/bool; removes None.
        Preserves a consistent key order first, then appends remaining keys.
        """
        key_order = [
            # identity / provenance
            "source", "filename", "doc_id",
            # titles
            "document_title", "title", "original_filename",
            # catalog-ish common fields
            "category", "category_str",
            # hierarchy
            "h1", "h2", "h3", "header_hierarchy",
            # chunk bookkeeping
            "is_sub_chunk", "chunk_index", "total_sub_chunks", "chunk_id",
            # page (if coming from PDF loader)
            "page",
        ]

        clean = {}
        meta = meta or {}

        # first: ordered keys
        for k in key_order:
            if k in meta:
                v = _sanitize_value(meta.get(k))
                if v is not None:
                    clean[k] = v

        # then: any other keys not already present
        for k, v in meta.items():
            if k in clean:
                continue
            sv = _sanitize_value(v)
            if sv is not None:
                clean[k] = sv

        return clean

    def _infer_doc_id(doc: Document) -> str:
        md = doc.metadata or {}
        for k in ("doc_id", "id", "file_id", "sha", "sha256", "hash"):
            v = md.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()

        src = md.get("source")
        if isinstance(src, str) and src.strip():
            p = Path(src)
            if not p.is_absolute():
                p = (Path.cwd() / p).resolve()
            return p.stem or ""
        return ""

    def _extract_base_metadata(doc: Document) -> Dict:
        """
        Your original base metadata + keeps whatever loader had.
        """
        original_metadata = dict(doc.metadata) if doc.metadata else {}
        _normalize_source(original_metadata)

        source = original_metadata.get("source", "")
        filename = source.replace("\\", "/").split("/")[-1] if source else ""

        title_match = re.search(r"^#\s+(.+?)$", doc.page_content or "", re.MULTILINE)
        document_title = title_match.group(1).strip() if title_match else ""

        base = dict(original_metadata)
        base.setdefault("source", source)
        base.setdefault("filename", filename)
        base.setdefault("document_title", document_title)

        # keep page if provided
        if "page" in original_metadata:
            base["page"] = original_metadata.get("page")

        return base

    def _build_header_hierarchy(md: Dict) -> List[Tuple[str, str]]:
        hierarchy = []
        for level in ("h1", "h2", "h3"):
            v = md.get(level)
            if isinstance(v, str) and v.strip():
                hierarchy.append((level, v))
        return hierarchy

    def _hierarchy_to_string(hierarchy: List[Tuple[str, str]]) -> str:
        if not hierarchy:
            return ""
        return " > ".join([h[1] for h in hierarchy])

    def _format_header_prefix(hierarchy: List[Tuple[str, str]]) -> str:
        if not hierarchy:
            return ""
        breadcrumb = " > ".join([h[1] for h in hierarchy])
        return f"[Contexto: {breadcrumb}]\n\n"

    # -----------------------------
    # Helpers: content cleaning (your original)
    # -----------------------------
    def _clean_content(content: str) -> str:
        try:
            l2t = LatexNodes2Text()
        except Exception:
            l2t = None

        h = html2text.HTML2Text()
        h.body_width = 0
        h.single_line_break = True

        cleaned = content or ""

        # HTML tables -> markdown-ish text
        cleaned = re.sub(
            r"<table.*?</table>",
            lambda m: f"\n{h.handle(m.group(0))}\n",
            cleaned,
            flags=re.DOTALL | re.IGNORECASE,
        )

        # inline latex $...$ -> plain text
        if l2t:
            def clean_latex(match):
                try:
                    plain = l2t.latex_to_text(match.group(1))
                    return re.sub(r"\s+", " ", plain).strip()
                except Exception:
                    return match.group(0)

            cleaned = re.sub(r"\$(.*?)\$", clean_latex, cleaned)

        return cleaned

    # -----------------------------
    # Helpers: load/merge _catalog.json (indexed)
    # -----------------------------
    cwd = Path.cwd()
    module_root = Path(__file__).resolve().parents[2]  # .../src/... -> project root

    def _find_lakehouse_dirs(docs: List[Document]) -> List[Path]:
        found, seen = [], set()

        for d in docs:
            src = (d.metadata or {}).get("source")
            if not isinstance(src, str) or not src.strip():
                continue

            p = Path(src)
            if not p.is_absolute():
                p = (cwd / p)

            for parent in [p] + list(p.parents):
                if parent.name == "data_lakehouse":
                    rp = parent.resolve()
                    if str(rp) not in seen:
                        seen.add(str(rp))
                        found.append(rp)
                    break

        return found

    def _load_catalog_file(path: Path) -> Dict:
        try:
            if path.exists() and path.is_file():
                return json.loads(path.read_text(encoding="utf-8"))
        except Exception as e:
            log(f"Failed to load catalog at {path}: {e}", level="warning")
        return {}

    def _load_catalog(docs: List[Document]) -> Dict[str, Dict]:
        # simple cache on function object
        cache_key = getattr(_split_hierarchichal, "_catalog_cache_key", None)
        cache_val = getattr(_split_hierarchichal, "_catalog_cache", None)

        lakehouse_dirs = _find_lakehouse_dirs(docs)

        # fallbacks if source paths don't reveal lakehouse
        for candidate in (cwd / "data_lakehouse", module_root / "data_lakehouse"):
            try:
                if candidate.exists() and candidate.is_dir():
                    rp = candidate.resolve()
                    if rp not in lakehouse_dirs:
                        lakehouse_dirs.append(rp)
            except Exception:
                pass

        candidates = []
        for lh in lakehouse_dirs:
            candidates.append(lh / "03_gold" / "_catalog.json")
            candidates.append(lh / "00_landing_zone" / "_catalog.json")

        key = tuple(sorted([str(p) for p in candidates]))
        if cache_key == key and isinstance(cache_val, dict):
            return cache_val

        merged: Dict[str, Dict] = {}

        # landing_zone first
        for p in candidates:
            if p.as_posix().endswith("00_landing_zone/_catalog.json"):
                merged.update(_load_catalog_file(p))

        # gold overrides
        for p in candidates:
            if p.as_posix().endswith("03_gold/_catalog.json"):
                merged.update(_load_catalog_file(p))

        _split_hierarchichal._catalog_cache_key = key
        _split_hierarchichal._catalog_cache = merged

        if merged:
            log("Catalog metadata loaded/merged successfully.", level="info")
        else:
            log("No _catalog.json found; proceeding without external metadata enrichment.", level="warning")

        return merged

    catalog = _load_catalog(documents)

    # -----------------------------
    # Splitters (headers + recursive)
    # -----------------------------
    headers_to_split_on = [
        ("##", "h1"),
        ("###", "h2"),
        ("####", "h3"),
    ]

    md_header_splitter = MarkdownHeaderTextSplitter(
        headers_to_split_on=headers_to_split_on,
        strip_headers=False,
    )

    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
        length_function=len,
        separators=["\n\n", "\n", ". ", " ", ""],
    )

    # -----------------------------
    # Main loop
    # -----------------------------
    log("Splitting documents hierarchically + enriching with catalog metadata...", level="info")
    all_chunks: List[Document] = []

    for doc in documents:
        base_meta = _extract_base_metadata(doc)

        # doc_id
        doc_id = _infer_doc_id(doc)
        if doc_id:
            base_meta.setdefault("doc_id", doc_id)

        # enrich from catalog (without overwriting meaningful existing values)
        if doc_id and doc_id in catalog:
            entry = catalog.get(doc_id, {}) or {}
            for k, v in entry.items():
                if k not in base_meta or _is_empty(base_meta.get(k)):
                    base_meta[k] = v

            # category list -> string + category_str
            if isinstance(base_meta.get("category"), list):
                joined = " | ".join(
                    x.strip() for x in base_meta["category"]
                    if isinstance(x, str) and x.strip()
                )
                base_meta["category"] = joined
                base_meta.setdefault("category_str", joined)

        # sanitize once after merge
        base_meta = _sanitize_metadata(base_meta)

        cleaned_content = _clean_content(doc.page_content or "")
        header_splits = md_header_splitter.split_text(cleaned_content)

        for split in header_splits:
            header_hierarchy = _build_header_hierarchy(split.metadata or {})
            header_prefix = _format_header_prefix(header_hierarchy) if include_header_prefix else ""

            content = (split.page_content or "").strip()
            if not content or len(content) < 10:
                continue

            full_content = (header_prefix + content) if header_prefix else content

            def _make_chunk_doc(
                chunk_text: str,
                is_sub: bool,
                sub_index: Optional[int] = None,
                sub_total: Optional[int] = None,
            ) -> Document:
                meta = dict(base_meta)

                # include header fields
                sm = split.metadata or {}
                meta["h1"] = sm.get("h1", "") or ""
                meta["h2"] = sm.get("h2", "") or ""
                meta["h3"] = sm.get("h3", "") or ""
                meta["header_hierarchy"] = _hierarchy_to_string(header_hierarchy)

                meta["is_sub_chunk"] = bool(is_sub)
                if is_sub and sub_index is not None:
                    meta["chunk_index"] = int(sub_index)
                    meta["total_sub_chunks"] = int(sub_total or 0)

                # stable chunk_id
                did = meta.get("doc_id") or ""
                hh = meta.get("header_hierarchy") or ""
                if did:
                    # keep it compact-ish
                    meta["chunk_id"] = f"{did}:{hash(hh) & 0xffff}:{meta.get('chunk_index', 0)}"
                else:
                    meta["chunk_id"] = f"{hash(meta.get('source','')) & 0xffff}:{hash(hh) & 0xffff}:{meta.get('chunk_index', 0)}"

                meta = _sanitize_metadata(meta)
                return Document(page_content=chunk_text, metadata=meta)

            if len(full_content) > chunk_size:
                sub_chunks = text_splitter.split_text(content)
                for i, sub_chunk in enumerate(sub_chunks):
                    sub_text = (header_prefix + sub_chunk) if header_prefix else sub_chunk
                    all_chunks.append(_make_chunk_doc(sub_text, is_sub=True, sub_index=i, sub_total=len(sub_chunks)))
            else:
                all_chunks.append(_make_chunk_doc(full_content, is_sub=False))

    # final safety sanitize + filter tiny chunks
    final_chunks: List[Document] = []
    for ch in all_chunks:
        ch.metadata = _sanitize_metadata(dict(ch.metadata or {}))
        if len((ch.page_content or "")) >= min_chunk_size:
            final_chunks.append(ch)

    log(
        f"Successfully split into {len(final_chunks)} chunks from {len(documents)} documents "
        f"(hierarchical + catalog metadata).",
        level="info",
    )
    return final_chunks
