#!/usr/bin/env python3
"""Run ETL pipeline from silver layer - cleaning only (no extraction)."""

import sys
import json
import shutil
from pathlib import Path
from datetime import datetime
from src import settings
from src.preprocessing.cleaners import clean_html, clean_latex, rebuild_hierarchy
from src.logger import log


class SilverToGoldPipeline:
    """Pipeline that processes markdown from silver layer and produces cleaned output in gold layer."""
    
    def __init__(self, force_clean: bool = False):
        """
        Initialize the Silver→Gold cleaning pipeline.
        
        Args:
            force_clean: If True, re-process all files even if already in gold directory
        """
        self.silver_dir = Path("data_lakehouse/02_silver")
        self.gold_dir = Path("data_lakehouse/03_gold")
        self.config_path = Path("data_lakehouse/config.json")
        self.force_clean = force_clean
        
        # Create directories if they don't exist
        self.gold_dir.mkdir(parents=True, exist_ok=True)
        
        # Load or create config
        self.config = self._load_or_create_config()
    
    def _load_or_create_config(self) -> dict:
        """Load existing config or create new one with cleaning settings."""
        if self.config_path.exists():
            with open(self.config_path, 'r') as f:
                config = json.load(f)
        else:
            config = {
                "last_etl_run": None,
                "etl_settings": {},
                "extraction_metrics": {
                    "total_documents": 0,
                    "successfully_extracted": 0,
                    "failed_extractions": 0,
                    "last_run": None
                }
            }
        
        # Update cleaning settings from current settings
        config["etl_settings"].update({
            "enable_html_cleaning": settings.ENABLE_HTML_CLEANING,
            "enable_latex_cleaning": settings.ENABLE_LATEX_CLEANING,
            "enable_hierarchy_rebuilding": settings.ENABLE_HIERARCHY_REBUILDING,
            "hierarchy_rebuilding_mode": settings.HIERARCHY_REBUILDING_MODE,
        })
        
        return config
    
    def _save_config(self) -> None:
        """Save config to data_lakehouse directory."""
        self.config_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.config_path, 'w') as f:
            json.dump(self.config, f, indent=2, ensure_ascii=False)
        log(f"Config saved to {self.config_path}", level="info")
    
    def _load_gold_catalog(self) -> dict:
        """Load existing gold catalog if it exists."""
        catalog_path = self.gold_dir / "_catalog.json"
        if catalog_path.exists():
            with open(catalog_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}
    
    def _save_gold_catalog(self, catalog: dict) -> None:
        """Save gold catalog to _catalog.json."""
        catalog_path = self.gold_dir / "_catalog.json"
        self.gold_dir.mkdir(parents=True, exist_ok=True)
        with open(catalog_path, 'w', encoding='utf-8') as f:
            json.dump(catalog, f, indent=2, ensure_ascii=False)
    
    def _load_landing_catalog(self) -> dict:
        """Load landing zone catalog for metadata."""
        landing_zone = Path("data_lakehouse/01_bronze")
        catalog_path = landing_zone / "_catalog.json"
        if catalog_path.exists():
            with open(catalog_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}
    
    def _get_cleaned_markdown(self, markdown_content: str, pdf_path: str = None) -> str:
        """Apply cleaning steps based on config."""
        config = self.config["etl_settings"]
        content = markdown_content
        
        if config.get("enable_html_cleaning", False):
            log("Applying HTML cleaning...", level="info")
            content = clean_html(content)
        
        if config.get("enable_latex_cleaning", False):
            log("Applying LaTeX cleaning...", level="info")
            content = clean_latex(content)
        
        if config.get("enable_hierarchy_rebuilding", False):
            mode = config.get("hierarchy_rebuilding_mode", "font")
            log(f"Rebuilding hierarchy using '{mode}' mode...", level="info")
            content = rebuild_hierarchy(content, pdf_path, mode)
        
        return content
    
    def _get_silver_documents(self) -> list:
        """Get all markdown files from silver layer.
        
        Silver structure: 02_silver/{hash}/{hash}.md or other .md files
        
        Returns:
            List of (file_hash, md_file_path) tuples
        """
        documents = []
        
        # Look for markdown files in hash-named directories
        for hash_dir in self.silver_dir.glob("*"):
            if hash_dir.is_dir():
                # Look for .md files in the hash directory
                md_files = list(hash_dir.glob("*.md"))
                for md_file in md_files:
                    file_hash = hash_dir.name
                    documents.append((file_hash, md_file))
        
        return documents
    
    def run(self) -> dict:
        """
        Run the cleaning pipeline from silver to gold.
        
        Returns:
            dict: Processing results
        """
        silver_documents = self._get_silver_documents()
        
        if not silver_documents:
            log("No markdown files found in silver layer", level="warning")
            return {
                "processed": 0,
                "successful": 0,
                "failed": 0,
                "skipped": 0,
                "status": "completed"
            }
        
        log(f"Starting Silver→Gold Cleaning Pipeline", level="info")
        log(f"Silver directory: {self.silver_dir}", level="info")
        log(f"Gold directory: {self.gold_dir}", level="info")
        log(f"Force clean: {self.force_clean}", level="info")
        log(f"Found {len(silver_documents)} document(s) in silver layer", level="info")
        
        # Load catalogs
        landing_catalog = self._load_landing_catalog()
        gold_catalog = self._load_gold_catalog()
        
        processed_count = 0
        successful_count = 0
        failed_count = 0
        skipped_count = 0
        
        for i, (file_hash, md_file_path) in enumerate(silver_documents, 1):
            try:
                gold_doc_dir = self.gold_dir / file_hash
                gold_md_file = gold_doc_dir / f"{file_hash}.md"
                
                # Check if already processed in gold
                if gold_doc_dir.exists() and gold_md_file.exists() and not self.force_clean:
                    log(f"[{i}/{len(silver_documents)}] Skipping {file_hash} (already in gold directory)", level="info")
                    skipped_count += 1
                    continue
                
                log(f"[{i}/{len(silver_documents)}] Processing {file_hash}...", level="info")
                
                # Read markdown from silver
                with open(md_file_path, 'r', encoding='utf-8') as f:
                    markdown_content = f.read()
                
                # Apply cleaning
                cleaned_content = self._get_cleaned_markdown(markdown_content)
                
                # Create gold directory structure
                gold_doc_dir.mkdir(parents=True, exist_ok=True)
                
                # Save cleaned markdown to gold
                with open(gold_md_file, 'w', encoding='utf-8') as f:
                    f.write(cleaned_content)
                
                # Copy images if they exist
                silver_images_dir = md_file_path.parent / "images"
                if silver_images_dir.exists():
                    gold_images_dir = gold_doc_dir / "images"
                    if gold_images_dir.exists():
                        shutil.rmtree(gold_images_dir)
                    shutil.copytree(silver_images_dir, gold_images_dir)
                
                # Update gold catalog
                if file_hash in landing_catalog:
                    gold_entry = landing_catalog[file_hash].copy()
                else:
                    gold_entry = {
                        "file_hash": file_hash,
                        "original_filename": f"{file_hash}.pdf"
                    }
                
                gold_entry["processed_at"] = datetime.now().isoformat()
                gold_entry["cleaned"] = True
                
                # Count images in the gold bundle
                gold_images_dir = gold_doc_dir / "images"
                image_count = 0
                if gold_images_dir.exists():
                    image_count = len(list(gold_images_dir.glob("*")))
                gold_entry["images"] = image_count
                
                gold_catalog[file_hash] = gold_entry
                processed_count += 1
                successful_count += 1
                log(f"     ✓ Successfully processed", level="info")
                
            except Exception as e:
                log(f"     ✗ Error processing {file_hash}: {e}", level="error")
                failed_count += 1
        
        # Update and save config
        self.config["last_etl_run"] = datetime.now().isoformat()
        self.config["extraction_metrics"]["total_documents"] = processed_count
        self.config["extraction_metrics"]["successfully_extracted"] = successful_count
        self.config["extraction_metrics"]["failed_extractions"] = failed_count
        self.config["extraction_metrics"]["last_run"] = datetime.now().isoformat()
        self._save_config()
        
        # Save gold catalog
        self._save_gold_catalog(gold_catalog)
        
        log(f"Silver→Gold Cleaning Pipeline completed", level="info")
        log(f"{'='*60}", level="info")
        log(f"Processing Summary:", level="info")
        log(f"  Total processed: {processed_count}", level="info")
        log(f"  Successful: {successful_count}", level="info")
        log(f"  Failed: {failed_count}", level="info")
        log(f"  Skipped: {skipped_count}", level="info")
        log(f"{'='*60}", level="info")
        
        return {
            "processed": processed_count,
            "successful": successful_count,
            "failed": failed_count,
            "skipped": skipped_count,
            "status": "completed"
        }


def main():
    """Execute the Silver→Gold cleaning pipeline."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Run ETL cleaning pipeline from silver to gold layer"
    )
    parser.add_argument(
        "--force-clean",
        action="store_true",
        help="Re-process all files even if already in gold directory"
    )
    
    args = parser.parse_args()
    
    if not Path("data_lakehouse/02_silver").exists():
        log(f"ERROR: Silver directory not found at data_lakehouse/02_silver", level="error")
        sys.exit(1)
    
    try:
        pipeline = SilverToGoldPipeline(force_clean=args.force_clean)
        result = pipeline.run()
        sys.exit(0 if result["failed"] == 0 else 1)
    except Exception as e:
        log(f"Pipeline failed: {e}", level="error")
        sys.exit(1)


if __name__ == "__main__":
    main()
