from pathlib import Path
from src.pipelines.pipeline_etl import ETLPipeline
from src.pipelines.pipeline_indexing import RAGIndexingPipeline
from src import settings
from src.logger import log
from src.utils import generate_file_hash
from src.services.vector_db import is_file_already_indexed
import json
import shutil
import datetime


def run_ingestion(pdf_files: list, file_metadata: dict = None, progress_callback=None) -> dict:
    """
    Run ingestion pipeline automatically based on LOADER_TYPE and ENVIRONMENT settings.
    
    If LOADER_TYPE == "mineru": Runs ETL pipeline (PDF→MD→Cleaning→Indexing)
    If LOADER_TYPE == "pdfloader": Runs direct pipeline (PDF→Indexing)
    
    Args:
        pdf_files: List of PDF file paths to ingest
        file_metadata: Dict mapping filenames to metadata dicts with keys: title, keywords, description
        progress_callback: Optional callback function for progress updates (phase, progress_pct, message)
        
    Returns:
        dict: Ingestion results with statistics
    """
    duplicate_files = []
    for pdf_path in pdf_files:
        file_hash = generate_file_hash(str(pdf_path))
        if is_file_already_indexed(file_hash):
            duplicate_files.append(pdf_path)
    
    if duplicate_files:
        log(f"DUPLICATE DETECTION: Found {len(duplicate_files)} file(s) already indexed", level="warning")
        for f in duplicate_files:
            log(f"   - {Path(f).name}", level="warning")
        return {
            "status": "skipped",
            "message": f"Skipped {len(duplicate_files)} duplicate file(s) already in database",
            "duplicate_count": len(duplicate_files),
            "total_files": len(pdf_files),
            "etl": {},
            "indexing": {}
        }
    
    # Organize files only if metadata provided (new uploads) or if files not in landing zone
    landing_zone = Path("data_lakehouse/01_bronze")
    all_in_landing_zone = all(Path(f).parent == landing_zone for f in pdf_files)
    
    if file_metadata or not all_in_landing_zone:
        # New files or mixed sources: organize them
        organized_files = _organize_files_in_landing_zone(pdf_files, file_metadata or {})
    else:
        # Re-indexing existing landing zone files: skip organization, preserve metadata
        organized_files = pdf_files
        log(f"Re-indexing {len(pdf_files)} files from landing zone (preserving metadata)", level="info")
    
    if settings.LOADER_TYPE == "mineru":
        # Use ETL pipeline (no versioning needed with simplified structure)
        return run_ingestion_with_etl(organized_files, progress_callback=progress_callback)
    else:
        # Use direct PDF indexing (no ETL)
        return run_ingestion_direct(organized_files, progress_callback=progress_callback)


def _organize_files_in_landing_zone(pdf_files: list, file_metadata: dict) -> list:
    """
    Organize PDFs and metadata in landing zone (hash-named).
    Stores all metadata in a single _catalog.json file instead of individual JSON files.
    If PDFs are already in landing zone, skip copying. Preserve existing metadata.
    
    Args:
        pdf_files: List of PDF file paths (temp or landing zone locations)
        file_metadata: Dict mapping filenames to metadata dicts with keys: title, description, category
        
    Returns:
        List of PDF paths in landing zone
    """
    landing_zone = Path("data_lakehouse/01_bronze")
    landing_zone.mkdir(parents=True, exist_ok=True)
    
    # Load or initialize catalog
    catalog_path = landing_zone / "_catalog.json"
    if catalog_path.exists():
        with open(catalog_path, 'r', encoding='utf-8') as f:
            catalog = json.load(f)
    else:
        catalog = {}
    
    organized_pdfs = []
    
    for pdf_path in pdf_files:
        pdf_path = Path(pdf_path)
        
        # Generate hash for consistent naming
        file_hash = generate_file_hash(str(pdf_path))
        
        hash_pdf_path = landing_zone / f"{file_hash}.pdf"
        
        try:
            # Only copy if source is NOT already in landing zone
            if pdf_path.parent != landing_zone:
                shutil.copy2(pdf_path, hash_pdf_path)
                log(f"Moved PDF: {pdf_path.name} → {file_hash}.pdf", level="info")
                
                # Add metadata to catalog for new files
                original_filename = pdf_path.name
                metadata = file_metadata.get(original_filename, {})
                
                # Parse category from comma-separated string to array
                category_input = metadata.get("category", "Geral")
                if isinstance(category_input, str):
                    if category_input.strip():
                        categories = [cat.strip() for cat in category_input.split(",")]
                    else:
                        categories = ["Geral"]
                else:
                    categories = category_input if isinstance(category_input, list) else ["Geral"]
                
                catalog[file_hash] = {
                    "original_filename": original_filename,
                    "title": metadata.get("title", original_filename.replace(".pdf", "")),
                    "description": metadata.get("description", ""),
                    "category": categories,
                    "uploaded_by": "system",
                    "uploaded_at": datetime.datetime.now().isoformat(),
                    "active": True,
                    "processed_at": datetime.datetime.now().isoformat()
                }
                log(f"Added metadata to catalog: {file_hash}", level="info")
            else:
                log(f"PDF already in landing zone: {file_hash}.pdf", level="info")
            
            organized_pdfs.append(str(hash_pdf_path))
            
        except Exception as e:
            log(f"Error organizing {pdf_path.name}: {e}", level="error")
    
    # Save updated catalog
    try:
        with open(catalog_path, 'w', encoding='utf-8') as f:
            json.dump(catalog, f, indent=4, ensure_ascii=False)
        log(f"Updated catalog: {catalog_path}", level="info")
    except Exception as e:
        log(f"Error saving catalog: {e}", level="error")
    
    return organized_pdfs


def run_ingestion_with_etl(pdf_files: list, version: str = None, progress_callback=None) -> dict:
    """
    Run complete ingestion: ETL (PDF→MD) + RAG Indexing.
    
    Performs in batch:
    1. Complete ETL pipeline (check config, skip if already computed)
    2. RAG indexing on cleaned markdowns
    
    Args:
        pdf_files: List of PDF file paths
        progress_callback: Optional callback for progress updates
        
    Returns:
        dict: Ingestion results with statistics
    """
    log(f"Starting complete ingestion pipeline with ETL", level="info")
    
    results = {"etl": {}, "indexing": {}, "total_files": len(pdf_files)}
    
    # Step 1: Run ETL pipeline
    if progress_callback:
        progress_callback(phase="ETL", progress_pct=0, message="Iniciando pipeline ETL...")
    
    log(f"{'='*60}", level="info")
    log(f"STEP 1: ETL PIPELINE", level="info")
    log(f"{'='*60}", level="info")
    
    etl_pipeline = ETLPipeline(force_clean=True)
    etl_pipeline.run(pdf_files)
    results["etl"] = etl_pipeline.config.get("extraction_metrics", {})
    
    if progress_callback:
        progress_callback(phase="ETL", progress_pct=50, message="Pipeline ETL concluído. Iniciando indexação...")
    
    # Step 2: Run RAG indexing on finalized gold directory
    log(f"{'='*60}", level="info")
    log(f"STEP 2: RAG INDEXING PIPELINE", level="info")
    log(f"{'='*60}", level="info")
    
    indexing_pipeline = RAGIndexingPipeline(use_etl=True)
    indexing_results = indexing_pipeline.run()
    results["indexing"] = indexing_results or {}
    
    if progress_callback:
        progress_callback(phase="Indexing", progress_pct=100, message="Ingestion concluída com sucesso!")
    
    log(f"{'='*60}", level="info")
    log(f"Complete ingestion pipeline finished", level="info")
    log(f"{'='*60}\n", level="info")
    return results


def run_ingestion_direct(pdf_files: list, progress_callback=None) -> dict:
    """
    Run ingestion directly on PDFs (no ETL).
    
    Args:
        pdf_files: List of PDF file paths
        progress_callback: Optional callback for progress updates
        
    Returns:
        dict: Ingestion results with statistics
    """
    log(f"\n{'='*60}", level="info")
    log(f"DIRECT INDEXING PIPELINE (PDF→RAG)", level="info")
    log(f"{'='*60}", level="info")
    
    if progress_callback:
        progress_callback(phase="Direct", progress_pct=0, message="Iniciando indexação direta...")
    
    # Skip ETL, go directly to indexing
    indexing_pipeline = RAGIndexingPipeline(use_etl=False)
    indexing_results = indexing_pipeline.run(source_files=pdf_files)
    
    if progress_callback:
        progress_callback(phase="Direct", progress_pct=100, message="Indexação concluída com sucesso!")
    
    log(f"\n{'='*60}", level="info")
    log(f"Direct ingestion pipeline finished", level="info")
    log(f"{'='*60}\n", level="info")
    return {"indexing": indexing_results or {}, "total_files": len(pdf_files)}




