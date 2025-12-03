# LOSS-J
Locate, Organize, Summarize, Suggest, and Justify

## Code Structure

```bash
src
├── chains/                         # RAG orchestrators
│   ├── chain_indexing.py
│   └── chain_retrieving_generating.py
├── data_processing/               # Document preprocessing
│   ├── loader.py
│   ├── mineru_processor.py
│   ├── cleaner.py
│   └── splitter.py
├── services/                      # External integrations
│   ├── embedding_model.py
│   └── llm_generator.py
├── vector_store/                  # ChromaDB operations
│   ├── indexer.py
│   └── retriever.py
├── logger.py                      # Logging configuration
├── settings.py                    # Configuration & constants
└── utils.py                       # General utilities
```

## Prerequisites

- Python >= 3.8
- OpenAI API key
- **MinerU** (for PDF-to-Markdown conversion):
  - **Pipeline backend:** Runs locally (included with dependencies)
  - **VLM HTTP backend:** Requires a running MinerU VLM HTTP server (see [MinerU documentation](https://opendatalab.github.io/MinerU/usage/quick_usage/))


## Setup

1. Clone this repository

2. Create a virtual environment:
    ```bash
    python -m venv venv
    ```

3. Activate virtual environment:
    ```bash
    source venv/bin/activate
    ```

4. Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```

5. Configure environment variables:
    ```bash
    cp .env.example .env
    ```
    Edit [.env](.env) with your API keys and credentials.

## Configuration

To customize the system behavior, edit [`src/settings.py`](src/settings.py):

- **Loader type** (pdfloader or MinerU)
- **MinerU backend** (pipeline or vlm-http-client)
- **Document cleaning** (HTML, LaTeX, hierarchy rebuilding)
- **Text splitting** (chunk size, overlap)
- **Models** (LLM, embedding model)
- **Retriever K** (number of documents to retrieve)

## Running

### Visual Interface
```bash
streamlit run main.py
```
Access the app at [http://localhost:8501](http://localhost:8501).

### Evaluation & Testing

1. **Prepare test documents:**
   - Place the PDF files you want to test in the `docs/test_documents/` directory

2. **Edit test queries:**
   - Edit [query.json](query.json) with your test queries and expected responses

3. **Run evaluation with vectorstore reset:**
    ```bash
    python test-eval.py --reset
    ```
   
   This command will:
   - Reset the vectorstore (removes existing database and indexes)
   - Re-index all PDFs from `docs/test_documents/`
   - Run the evaluation tests
   - Save results to `outputs/results/test_results_TIMESTAMP.csv`

4. **View results:**
   - Check the results in the web interface under the **Results Comparison** page

**Note:** Run without `--reset` to skip vectorstore recreation and use the existing database:
```bash
python test-eval.py
```

## PDF Conversion Caching

The system caches PDF-to-Markdown conversions to avoid re-processing:

- **Cached files location:** `outputs/mineru/` (individual Markdown files)
- **How it works:** 
  - When a PDF is processed using **MinerU loader**, the system first checks if a cached Markdown file exists in `outputs/mineru/`
  - If found: Uses the cached Markdown file (no conversion needed)
  - If not found: Converts the PDF to Markdown and stores it in `outputs/mineru/` for future use
  - ⚠️ **Important:** When a cache hit occurs, the cached Markdown is reused regardless of which MinerU backend is configured (pipeline or vlm-http). This allows you to use pre-converted Markdowns without re-processing.

### Pre-computed Markdown Sets

Pre-computed Markdown conversions are available as separate zip files. Extract and use them to skip PDF conversion entirely:

1. **Extract the zip file into `outputs/mineru/`:**
    ```bash
    # pick one
    unzip outputs/mineru/backend-pipeline-md.zip -d outputs/mineru/
    # or
    unzip outputs/mineru/backend-vlm-http-md.zip -d outputs/mineru/
    # or
    unzip outputs/mineru/backend-vlm-and-pipeline-md.zip -d outputs/mineru/
    ```

2. **Available Markdown sets:**
   - **`backend-pipeline-md.zip`** — 36 test PDFs converted using MinerU pipeline backend
   - **`backend-vlm-http-md.zip`** — 27 test PDFs converted using MinerU VLM HTTP backend
   - **`backend-vlm-and-pipeline-md.zip`** — 36 test PDFs (27 from VLM HTTP + 9 from pipeline)

- **To force re-conversion of a specific PDF:** Delete its corresponding Markdown file from `outputs/mineru/` and re-run the indexing.

---
[https://doi.org/10.54499/2024.07619.IACDC](https://doi.org/10.54499/2024.07619.IACDC)