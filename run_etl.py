#!/usr/bin/env python3
"""Run the ETL pipeline on all PDFs in the landing zone."""

import sys
from pathlib import Path
from src.pipelines.pipeline_etl import ETLPipeline
from src.logger import log


def main():
    """Execute the ETL pipeline on all PDFs in the landing zone.
    
    This function discovers all PDF files in the landing zone directory,
    initializes the ETL pipeline, processes the documents, and logs a summary
    of the extraction metrics including total documents, successful extractions,
    and failed extractions.
    
    Raises:
        SystemExit: If the landing zone directory does not exist, no PDFs are found,
                    or the ETL pipeline encounters an error.
    """
    landing_zone = Path("data_lakehouse/01_bronze")
    
    if not landing_zone.exists():
        log(f"ERROR: Landing zone not found at {landing_zone}", level="error")
        sys.exit(1)
    
    pdf_files = list(landing_zone.glob("*.pdf"))
    
    if not pdf_files:
        log(f"No PDF files found in {landing_zone}", level="warning")
        sys.exit(0)
    
    log(f"Found {len(pdf_files)} PDF(s) in landing zone", level="info")
    
    try:
        etl_pipeline = ETLPipeline()
        etl_pipeline.run([str(pdf) for pdf in pdf_files])
        
        metrics = etl_pipeline.config.get("extraction_metrics", {})
        log(f"{'='*60}", level="info")
        log(f"ETL Pipeline Summary:", level="info")
        log(f"  Total documents: {metrics.get('total_documents', 0)}", level="info")
        log(f"  Successfully extracted: {metrics.get('successfully_extracted', 0)}", level="info")
        log(f"  Failed extractions: {metrics.get('failed_extractions', 0)}", level="info")
        log(f"{'='*60}\n", level="info")
        
    except Exception as e:
        log(f"ETL pipeline failed: {e}", level="error")
        sys.exit(1)


if __name__ == "__main__":
    main()
