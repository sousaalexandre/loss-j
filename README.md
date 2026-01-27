# LOSS-J

**Locate, Organize, Summarize, Suggest, and Justify**

A Retrieval-Augmented Generation (RAG) system for document processing and intelligent querying, designed for Portuguese military and administrative documentation.

---

## Table of Contents

- [Architecture](#architecture)
- [Data Lakehouse](#data-lakehouse)
- [Pipeline Components](#pipeline-components)
- [Configuration](#configuration)
- [Installation](#installation)
- [Usage](#usage)
- [Current Limitations](#current-limitations)

---

## Architecture

### Why Two Loader Types?

The system supports two distinct document loading strategies:

| Loader Type | Input | Process | Use Case |
|-------------|-------|---------|----------|
| `pdfloader` | PDF | Direct text extraction → Indexing | Simple PDFs, faster processing |
| `mineru` / `docling` | PDF | PDF → Markdown → Cleaning → Indexing | Complex PDFs with tables, images, formulas |

**Why PDF-to-Markdown?** Converters like MinerU or Docling extract structured information from PDFs—tables, headers, images, formulas—preserving document structure. The pdfloader approach extracts plain text only, losing all formatting and hierarchy.

This is why the **Data Lakehouse architecture** is essential—it manages the intermediate artifacts (Bronze: raw extraction, Gold: cleaned output) separately from the raw input (Landing Zone).

---

## Data Lakehouse

The preprocessing stage implements a **Medallion Architecture** for managing document transformation.

The ETL pipeline implements intelligent caching, with the goal of optimizing development:

1. **Bronze Cache**: Reused if extraction backend (`pipeline` vs `vlm-http-client`) unchanged
2. **Gold Cache**: Reused if cleaning settings (`enable_html_cleaning`, `enable_hierarchy_rebuilding`, etc.) unchanged
3. **Force Clean**: Option to rebuild gold even if settings unchanged

See [Data Lakehouse Standards](docs/data-lakehouse-standards.md) for complete specification.

---

## Pipeline Components

### 1. ETL Pipeline
[`pipeline_etl.py`](src/pipelines/pipeline_etl.py)

Transforms PDFs into RAG-ready markdown:

```python
ETLPipeline(force_clean=False).run(pdf_files)
```

**Stages:**
1. **Organize**: Hash PDFs, copy to landing zone, register in catalog
2. **Extract**: Convert PDF → Markdown using the configured converter (Bronze layer)
3. **Clean**: Apply HTML/LaTeX cleaning, rebuild hierarchy (configurable)
4. **Finalize**: Save cleaned markdown + assets to Gold layer

**Cleaning Options** (configured in [`settings.py`](src/settings.py)):

These options were designed to address issues in MinerU's markdown output:
- `ENABLE_HTML_CLEANING`: Convert HTML tables to Markdown (MinerU outputs tables as HTML)
- `ENABLE_LATEX_CLEANING`: Convert LaTeX equations to text (MinerU preserves LaTeX syntax)
- `ENABLE_HIERARCHY_REBUILDING`: Fix document header levels (MinerU has limited hierarchy extraction)
- `HIERARCHY_REBUILDING_MODE`: "font" (PDF analysis) or "llm" (AI-based)

### 2. Indexing Pipeline
[`pipeline_indexing.py`](src/pipelines/pipeline_indexing.py)

Indexes documents into the vector store:

```python
RAGIndexingPipeline(use_etl=True).run()
```

**Stages:**
1. **Load**: Read markdown from Gold layer (or PDFs if `use_etl=False`)
2. **Split**: Chunk documents using markdown-aware splitter
3. **Embed**: Generate embeddings via OpenAI
4. **Store**: Upsert into ChromaDB vector store

**Splitting Configuration:**
- `SPLITTING_TYPE`: `"recursive"` or `"markdown_recursive"`
- `CHUNK_SIZE`: Characters per chunk (default: 1000)
- `CHUNK_OVERLAP`: Overlap between chunks (default: 200)

### 3. Ingestion Controller 
[`pipeline_ingestion_controller.py`](src/pipelines/pipeline_ingestion_controller.py)

Orchestrates the complete pipeline:

```python
run_ingestion(pdf_files, file_metadata, progress_callback)
```

**Modes:**
- **ETL Mode** (e.g. `LOADER_TYPE="mineru"`): Runs ETL → Indexing
- **Direct Mode** (`LOADER_TYPE="pdfloader"`): Skips ETL, indexes PDFs directly

**Features:**
- Duplicate detection (skips already-indexed files)
- Progress callbacks for UI integration
- Metadata preservation through catalogs

### 4. Inference Pipeline
[`pipeline_inference.py`](src/pipelines/pipeline_inference.py)

Handles user queries:

```python
query_handler(prompt) → {"response": str, "documents": list}
```

**Process:**
1. Retrieve top-K similar documents from vector store
2. Construct prompt with retrieved context
3. Generate response via LLM
4. Return response with source references

---

## Configuration

All settings are centralized in [`settings.py`](src/settings.py):

---

## Installation

### Prerequisites

- Python 3.8+
- OpenAI API key

### Setup

```bash
git clone <repository-url>
cd loss-j

python -m venv venv
source venv/bin/activate  
# On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
```

Edit `.env` and add your OpenAI API key:

```ini
OPENAI_API_KEY=sk-...
```

### Optional: MinerU

To use MinerU for PDF-to-Markdown conversion, see the [MinerU documentation](https://opendatalab.github.io/MinerU/).

MinerU supports two backends:
- **`pipeline`** (local)
- **`vlm-http-client`** (remote): Faster conversion via external server (falls back to `pipeline` if unavailable or something goes wrong)

Configure the backend and server URL in `settings.py`.

---

## Usage

### Workflow Overview

The typical development workflow consists of three steps:

```
1. Prepare Landing Zone    →    2. Run ETL    →    3. Run Indexing
   (hash PDFs)                   (PDF → MD)         (MD → VectorDB)
```

### Command-Line Scripts

#### 1. Prepare Landing Zone

The landing zone requires:
- PDFs named by their SHA-256 content hash
- A `_catalog.json` file with document metadata

Use the utility script to hash your PDFs:

```bash
# Single file
python run_hash_pdf.py document.pdf data_lakehouse/00_landing_zone/

# Bulk (entire directory)
python run_hash_pdf.py ./my_pdfs/ data_lakehouse/00_landing_zone/
```

> [!NOTE]  
> The `_catalog.json` must be created manually for now. See [Data Lakehouse Standards](docs/data-lakehouse-standards.md) for the catalog schema or see example [here](data_lakehouse/00_landing_zone/_catalog.json).

#### 2. Run ETL Pipeline

Once the landing zone is populated, run ETL to convert PDFs to markdown:

```bash
python run_etl.py
```

```
Input:  data_lakehouse/00_landing_zone/*.pdf
Output: data_lakehouse/03_gold/{hash}/{hash}.md
```

#### 3. Run Indexing Pipeline

Finally, index the gold layer markdown into the vector store:

```bash
python run_indexing.py
```

```
Input:  data_lakehouse/03_gold/*/*.md
Output: vectorstore_db/
```

### User Interface

> [!WARNING]  
> The web interface is functional but development has been paused to focus on core pipeline logic. Use command-line scripts for ETL and indexing workflows.


```bash
streamlit run main.py
```

**Features:**
- **Chat**: Query indexed documents and receive RAG-powered responses with source references
- **Manage Context**: Upload new PDFs with metadata (runs the complete ingestion pipeline automatically)
- **Evaluation Results**: View semantic accuracy metrics from test runs

### Evaluation

Run semantic accuracy tests on RAG responses:

```bash
python test-eval.py
```

Configure test queries in [query.json](query.json).

See results table [here](https://docs.google.com/spreadsheets/d/1PSo9qhHn55MSrnKrOmchA2SSj48x_8-9jNRj0pcXxmg/edit?usp=sharing).

---

## Current Limitations

1. **Interface Development Paused**
   - The Streamlit web interface is functional but not production-polished
   - Command-line scripts are the recommended approach for now
   - Focus has shifted to core pipeline reliability and ETL logic

2. **Image Processing**
   - Images are extracted (if converter supports) and stored in Gold bundles, but not used as context by RAG

3. **MinerU Hardware Requirements**
   - MinerU requires significant computational resources
   - Fortunately, alternatives like Docling have been identified as lighter and potentially better options

---

[https://doi.org/10.54499/2024.07619.IACDC](https://doi.org/10.54499/2024.07619.IACDC)
