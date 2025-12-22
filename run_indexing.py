#!/usr/bin/env python3
"""Run the RAG indexing pipeline on documents from gold layer (ETL) or landing zone (PDFs)."""

import sys
from pathlib import Path
from src.pipelines.pipeline_indexing import RAGIndexingPipeline
from src.logger import log
from src import settings


def main():
    # Determine mode based on LOADER_TYPE setting
    # If pdfloader: use landing zone (PDFs)
    # If anything else (mineru, etc): use gold layer (markdown from ETL)
    use_etl = settings.LOADER_TYPE != "pdfloader"
    
    if use_etl:
        # ETL mode: load from gold layer
        gold_dir = Path("data_lakehouse/03_gold")
        gold_md_files = list(gold_dir.glob("*/*.md")) if gold_dir.exists() else []
        
        if not gold_md_files:
            log(f"ERROR: No markdown files found in gold layer", level="error")
            sys.exit(1)
        
        log(f"Found {len(gold_md_files)} markdown file(s) in gold layer - using ETL mode", level="info")
        
        try:
            indexing_pipeline = RAGIndexingPipeline(use_etl=True)
            results = indexing_pipeline.run()
            
            log(f"\n{'='*60}", level="info")
            log(f"RAG Indexing Pipeline Summary (ETL Mode):", level="info")
            log(f"  Documents indexed: {results.get('total_indexed', 0)}", level="info")
            log(f"  Documents replaced: {results.get('replaced', 0)}", level="info")
            log(f"  Total chunks created: {results.get('total_chunks', 0)}", level="info")
            log(f"  Failed: {results.get('failed', 0)}", level="info")
            log(f"  Status: {results.get('status', 'completed')}", level="info")
            log(f"{'='*60}\n", level="info")
            
            sys.exit(0)
            
        except Exception as e:
            log(f"RAG indexing pipeline failed: {e}", level="error")
            sys.exit(1)
    else:
        # PDF mode: load from landing zone
        landing_zone = Path("data_lakehouse/00_landing_zone")
        
        if not landing_zone.exists():
            log(f"ERROR: Landing zone not found at {landing_zone}", level="error")
            sys.exit(1)
        
        pdf_files = list(landing_zone.glob("*.pdf"))
        
        if not pdf_files:
            log(f"No PDF files found in landing zone", level="warning")
            sys.exit(0)
        
        log(f"Found {len(pdf_files)} PDF file(s) in landing zone - using PDF mode", level="info")
        
        try:
            indexing_pipeline = RAGIndexingPipeline(use_etl=False)
            results = indexing_pipeline.run()
            
            log(f"\n{'='*60}", level="info")
            log(f"RAG Indexing Pipeline Summary (PDF Mode):", level="info")
            log(f"  Documents indexed: {results.get('total_indexed', 0)}", level="info")
            log(f"  Documents replaced: {results.get('replaced', 0)}", level="info")
            log(f"  Total chunks created: {results.get('total_chunks', 0)}", level="info")
            log(f"  Failed: {results.get('failed', 0)}", level="info")
            log(f"  Status: {results.get('status', 'completed')}", level="info")
            log(f"{'='*60}\n", level="info")
            
            sys.exit(0)
            
        except Exception as e:
            log(f"RAG indexing pipeline failed: {e}", level="error")
            sys.exit(1)


if __name__ == "__main__":
    main()
