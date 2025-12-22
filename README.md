# LOSS-J

**Locate, Organize, Summarize, Suggest, and Justify**

## Prerequisites

- Python 3.8+
- OpenAI API key

## Quick Setup

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Edit `.env` and add your OpenAI API key:

```ini
OPENAI_API_KEY=sk-...
```

## Configuration

Edit [src/settings.py](src/settings.py) to customize:

- `MINERU_BACKEND` — "pipeline" or "vlm-http-client"
- `ENABLE_HTML_CLEANING`, `ENABLE_LATEX_CLEANING`, `ENABLE_HIERARCHY_REBUILDING` — cleaning options
- `SPLITTING_TYPE` — "recursive", "markdown_recursive"
- `CHUNK_SIZE`, `CHUNK_OVERLAP` — document splitting parameters
- `LLM_MODEL_NAME`, `EMBEDDING_MODEL_NAME` — model selection
- `RETRIEVER_K` — number of documents to retrieve

## Running

### ETL Pipeline

Converts PDFs from landing zone to RAG-ready markdown:

```
00_landing_zone/ → 03_gold/
```

```bash
python run_etl.py
```

### Indexing Pipeline

Indexes gold layer markdown into vector store:

```
03_gold/ → vector database
```

```bash
python run_indexing.py
```

## Reference

- [Data Lakehouse Standards](docs/data-lakehouse-standards.md)
- [Settings](src/settings.py)

---

[https://doi.org/10.54499/2024.07619.IACDC](https://doi.org/10.54499/2024.07619.IACDC)