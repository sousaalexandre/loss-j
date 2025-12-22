import os
import json
import shutil
from pathlib import Path
from src import settings
from src.preprocessing.converters import get_converter
from src.preprocessing.cleaners import clean_html, clean_latex, rebuild_hierarchy
from src.logger import log
from src.utils import generate_file_hash
from datetime import datetime


class ETLPipeline:
    def __init__(self, config: dict = None, force_clean: bool = False):
        """
        Initialize ETL pipeline with simplified structure.
        
        Single data warehouse structure:
        - 00_landing_zone/ - Original PDFs + metadata JSON
        - 01_bronze/ - Extracted markdown (cache)
        - 03_gold/ - Finalized markdown for RAG
        
        Args:
            config: Optional config dict. If None, uses current settings
            force_clean: If True, re-apply cleaning even if settings unchanged
        """
        # Setup simple directory structure (no versioning)
        self.landing_zone = Path("data_warehouse/00_landing_zone")
        self.bronze_dir = Path("data_warehouse/01_bronze")
        self.gold_dir = Path("data_warehouse/03_gold")
        self.config_path = Path("data_warehouse/config.json")
        self.force_clean = force_clean
        
        # Use provided config or create from settings
        self.config = config or self._create_config_from_settings()
        
    def _create_config_from_settings(self) -> dict:
        """Create config dict from current settings with last_etl_run timestamp."""
        return {
            "last_etl_run": None,
            "etl_settings": {
                "mineru_backend": settings.MINERU_BACKEND,
                "enable_html_cleaning": settings.ENABLE_HTML_CLEANING,
                "enable_latex_cleaning": settings.ENABLE_LATEX_CLEANING,
                "enable_hierarchy_rebuilding": settings.ENABLE_HIERARCHY_REBUILDING,
                "hierarchy_rebuilding_mode": settings.HIERARCHY_REBUILDING_MODE,
            },
            "extraction_metrics": {
                "total_documents": 0,
                "successfully_extracted": 0,
                "failed_extractions": 0,
                "last_run": None
            }
        }
    
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
    
    def _load_existing_config(self) -> dict:
        """Load existing config.json if it exists."""
        if self.config_path.exists():
            with open(self.config_path, 'r') as f:
                return json.load(f)
        return None
    
    def _save_config(self) -> None:
        """Save config.json to data_warehouse directory."""
        self.config_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.config_path, 'w') as f:
            json.dump(self.config, f, indent=2, ensure_ascii=False)
        log(f"Config saved to {self.config_path}", level="info")
    
    def _config_matches_settings(self) -> bool:
        """Check if existing config matches current settings."""
        existing = self._load_existing_config()
        if not existing:
            return False
        return existing.get("etl_settings") == self.config.get("etl_settings")
    
    def _extraction_backend_unchanged(self) -> bool:
        """Check if extraction backend (mineru_backend) hasn't changed."""
        existing = self._load_existing_config()
        if not existing:
            return False
        existing_backend = existing.get("etl_settings", {}).get("mineru_backend")
        current_backend = self.config.get("etl_settings", {}).get("mineru_backend")
        return existing_backend == current_backend
    
    def _cleaning_settings_unchanged(self) -> bool:
        """Check if cleaning/finalization settings haven't changed."""
        existing = self._load_existing_config()
        if not existing:
            return False
        existing_settings = existing.get("etl_settings", {})
        current_settings = self.config.get("etl_settings", {})
        
        cleaning_keys = [
            "enable_html_cleaning",
            "enable_latex_cleaning",
            "enable_hierarchy_rebuilding",
            "hierarchy_rebuilding_mode"
        ]
        
        for key in cleaning_keys:
            if existing_settings.get(key) != current_settings.get(key):
                return False
        return True
    
    def _get_cleaned_markdown(self, markdown_content: str, pdf_path: str = None) -> str:
        """Apply cleaning steps based on config."""
        config = self.config["etl_settings"]
        content = markdown_content
        
        if config["enable_html_cleaning"]:
            log("Applying HTML cleaning...", level="info")
            content = clean_html(content)
        
        if config["enable_latex_cleaning"]:
            log("Applying LaTeX cleaning...", level="info")
            content = clean_latex(content)
        
        if config["enable_hierarchy_rebuilding"]:
            log(f"Rebuilding hierarchy using '{config['hierarchy_rebuilding_mode']}' mode...", level="info")
            content = rebuild_hierarchy(content, pdf_path, config["hierarchy_rebuilding_mode"])
        
        return content
    
    def _all_files_processed(self, pdf_files: list) -> bool:
        """Check if all provided PDF files are already processed in gold directory."""
        from src.utils import generate_file_hash
        
        for pdf_path in pdf_files:
            file_hash = generate_file_hash(str(pdf_path))
            gold_file = self.gold_dir / f"{file_hash}.md"
            if not gold_file.exists():
                return False
        return True
    
    def run(self, pdf_files: list) -> None:
        """
        Run ETL pipeline with caching logic:
        
        1. If bronze files exist AND settings match config.json -> use cache, finalize to gold
        2. If gold files exist -> skip everything (already done)
        3. Otherwise -> extract PDFs to bronze, finalize to gold
        
        Args:
            pdf_files: List of PDF file paths to process
        """
        self.bronze_dir.mkdir(parents=True, exist_ok=True)
        self.gold_dir.mkdir(parents=True, exist_ok=True)
        self.landing_zone.mkdir(parents=True, exist_ok=True)
        
        # Load existing config if available
        existing_config = self._load_existing_config()
        
        # Check what changed since last run
        extraction_backend_same = self._extraction_backend_unchanged() if existing_config else False
        cleaning_settings_same = self._cleaning_settings_unchanged() if existing_config else False
        
        log(f"Running ETL Pipeline", level="info")
        log(f"Landing zone: {self.landing_zone}", level="info")
        log(f"Bronze directory: {self.bronze_dir}", level="info")
        log(f"Gold directory: {self.gold_dir}", level="info")
        if existing_config:
            log(f"Extraction backend unchanged: {extraction_backend_same}, Cleaning settings unchanged: {cleaning_settings_same}", level="info")
        else:
            log(f"No previous config found (fresh start)", level="info")
        
        # Initialize metrics
        self.config["extraction_metrics"]["total_documents"] = len(pdf_files)
        self.config["extraction_metrics"]["successfully_extracted"] = 0
        self.config["extraction_metrics"]["failed_extractions"] = 0
        
        # Load landing zone catalog to populate gold catalog
        landing_catalog_path = self.landing_zone / "_catalog.json"
        landing_catalog = {}
        if landing_catalog_path.exists():
            with open(landing_catalog_path, 'r', encoding='utf-8') as f:
                landing_catalog = json.load(f)
        
        # Load existing gold catalog
        gold_catalog = self._load_gold_catalog()
        
        for i, pdf_path in enumerate(pdf_files, 1):
            pdf_path = Path(pdf_path)
            
            try:
                # Generate hash for unique filename
                file_hash = generate_file_hash(str(pdf_path))
                bronze_dir_hash = self.bronze_dir / file_hash
                bronze_md_file = bronze_dir_hash / f"{file_hash}.md"
                gold_doc_dir = self.gold_dir / file_hash
                
                # Save raw PDF to landing zone with hash name
                landing_pdf = self.landing_zone / f"{file_hash}.pdf"
                if not landing_pdf.exists():
                    log(f"[{i}/{len(pdf_files)}] Saving raw PDF to landing zone: {file_hash}.pdf", level="info")
                    shutil.copy2(str(pdf_path), str(landing_pdf))
                
                # CACHE CHECK: Bronze (extraction)
                # Reuse bronze if: directory exists AND extraction backend hasn't changed
                bronze_exists = bronze_dir_hash.exists() and (bronze_md_file.exists() or (bronze_dir_hash).glob("*.md"))
                can_use_bronze_cache = bronze_exists and extraction_backend_same
                
                if can_use_bronze_cache:
                    log(f"[{i}/{len(pdf_files)}] Using cached extraction from bronze: {file_hash}/", level="info")
                    # Find the markdown file in the bronze directory
                    md_files = list(bronze_dir_hash.glob("*.md"))
                    if md_files:
                        with open(md_files[0], 'r', encoding='utf-8') as f:
                            cleaned_content = f.read()
                    else:
                        cleaned_content = ""
                else:
                    if not bronze_exists:
                        log(f"[{i}/{len(pdf_files)}] Bronze not found, extracting: {file_hash}/", level="info")
                    elif not extraction_backend_same:
                        log(f"[{i}/{len(pdf_files)}] Extraction backend changed, re-extracting: {file_hash}/", level="info")
                    
                    # EXTRACT: Need to extract (either bronze missing or backend changed)
                    log(f"[{i}/{len(pdf_files)}] Converting {pdf_path.name} (hash: {file_hash})...", level="info")
                    
                    # Create temp directory for mineru output
                    import tempfile
                    temp_output = tempfile.mkdtemp(prefix=f"mineru_{file_hash}_")
                    
                    try:
                        # Get converter (always from settings, not config)
                        converter = get_converter(
                            backend=settings.MINERU_BACKEND,
                            server_url=settings.MINERU_VLM_HTTP_URL
                        )
                        
                        # Convert PDF to Markdown + all assets (images, metadata, etc.)
                        markdown_content, mineru_output_dir, backend_used = converter.convert(str(pdf_path), output_dir=temp_output)
                        
                        # Create bronze hash directory
                        bronze_dir_hash.mkdir(parents=True, exist_ok=True)
                        
                        # Flatten mineru output: copy from auto/ or vlm/ subdirectories to bronze root
                        mineru_path = Path(mineru_output_dir)
                        for item in mineru_path.iterdir():
                            if item.is_dir() and item.name in ["auto", "vlm"]:
                                # Found method subdirectory, flatten its contents
                                for sub_item in item.iterdir():
                                    if sub_item.is_file():
                                        dest = bronze_dir_hash / sub_item.name
                                        shutil.copy2(str(sub_item), str(dest))
                                    elif sub_item.is_dir():
                                        dest = bronze_dir_hash / sub_item.name
                                        if dest.exists():
                                            shutil.rmtree(dest)
                                        shutil.copytree(str(sub_item), str(dest))
                            elif item.is_file():
                                # Copy loose files directly
                                dest = bronze_dir_hash / item.name
                                shutil.copy2(str(item), str(dest))
                        
                        # Apply cleaning to markdown content
                        cleaned_content = self._get_cleaned_markdown(markdown_content, str(pdf_path))
                        
                        # Save cleaned markdown to bronze
                        pdf_name = Path(pdf_path).stem
                        bronze_md_file = bronze_dir_hash / f"{pdf_name}.md"
                        with open(bronze_md_file, 'w', encoding='utf-8') as f:
                            f.write(cleaned_content)
                        
                        log(f"     ✓ Extracted to bronze: {file_hash}/", level="info")
                        
                    finally:
                        # Clean up temp directory
                        if Path(temp_output).exists():
                            shutil.rmtree(temp_output)
                
                # FINALIZE: Check if gold needs update based on cleaning settings
                # Re-finalize if: directory doesn't exist OR cleaning settings changed OR force_clean is True
                gold_doc_dir = self.gold_dir / file_hash
                gold_md_file = gold_doc_dir / f"{file_hash}.md"
                
                if not gold_doc_dir.exists() or not cleaning_settings_same or self.force_clean:
                    if not gold_doc_dir.exists():
                        log(f"     → Finalizing to gold: {file_hash}/", level="info")
                    elif self.force_clean:
                        log(f"     → Force re-cleaning enabled, re-finalizing to gold: {file_hash}/", level="info")
                    else:
                        log(f"     → Cleaning settings changed, re-finalizing to gold: {file_hash}/", level="info")
                    
                    # Re-apply cleaning if settings changed or force_clean
                    if not cleaning_settings_same or self.force_clean:
                        # Get raw markdown from bronze
                        md_files = list(bronze_dir_hash.glob("*.md"))
                        if md_files:
                            with open(md_files[0], 'r', encoding='utf-8') as f:
                                raw_markdown = f.read()
                            cleaned_content = self._get_cleaned_markdown(raw_markdown, str(pdf_path))
                        else:
                            cleaned_content = ""
                    
                    # Create gold document directory structure
                    gold_doc_dir.mkdir(parents=True, exist_ok=True)
                    
                    # 1. Save cleaned markdown to gold
                    with open(gold_md_file, 'w', encoding='utf-8') as f:
                        f.write(cleaned_content)
                    
                    # 2. Copy PDF from landing zone to gold
                    landing_pdf = self.landing_zone / f"{file_hash}.pdf"
                    gold_pdf = gold_doc_dir / f"{file_hash}.pdf"
                    if landing_pdf.exists():
                        shutil.copy2(str(landing_pdf), str(gold_pdf))
                    
                    # 3. Copy images from bronze to gold/images/
                    bronze_images_dir = bronze_dir_hash / "images"
                    if bronze_images_dir.exists():
                        gold_images_dir = gold_doc_dir / "images"
                        if gold_images_dir.exists():
                            shutil.rmtree(gold_images_dir)
                        shutil.copytree(str(bronze_images_dir), str(gold_images_dir))
                    
                    # 4. Create metadata.json with image enrichment data (empty for now)
                    gold_metadata = gold_doc_dir / "metadata.json"
                    metadata_content = {"images": {}}
                    
                    # If images directory exists, populate metadata with empty descriptions
                    gold_images_dir = gold_doc_dir / "images"
                    if gold_images_dir.exists():
                        for img_file in gold_images_dir.glob("*.jpg"):
                            metadata_content["images"][img_file.name] = {
                                "description": "",
                                "tags": []
                            }
                    
                    with open(gold_metadata, 'w', encoding='utf-8') as f:
                        json.dump(metadata_content, f, indent=2)
                else:
                    log(f"     → Already finalized in gold (cleaning unchanged): {file_hash}/", level="info")
                
                # UPDATE GOLD CATALOG
                # Copy entry from landing catalog or create new one
                if file_hash in landing_catalog:
                    gold_entry = landing_catalog[file_hash].copy()
                else:
                    gold_entry = {
                        "original_filename": pdf_path.name,
                        "title": pdf_path.stem,
                        "description": "",
                        "category": ["Geral"],
                        "uploaded_by": "system",
                        "uploaded_at": datetime.now().isoformat()
                    }
                
                # Add processing metadata
                gold_entry["processed_at"] = datetime.now().isoformat()
                
                # Count images in the gold bundle
                gold_images_dir = gold_doc_dir / "images"
                image_count = 0
                if gold_images_dir.exists():
                    image_count = len(list(gold_images_dir.glob("*.jpg")))
                gold_entry["images"] = image_count
                
                # Update gold catalog
                gold_catalog[file_hash] = gold_entry
                
                self.config["extraction_metrics"]["successfully_extracted"] += 1
                
            except Exception as e:
                log(f"     ✗ Error processing {pdf_path.name}: {e}", level="error")
                self.config["extraction_metrics"]["failed_extractions"] += 1
        
        # Update timestamp and save config
        self.config["last_etl_run"] = datetime.now().isoformat()
        self.config["extraction_metrics"]["last_run"] = datetime.now().isoformat()
        self._save_config()
        
        # Save gold catalog
        self._save_gold_catalog(gold_catalog)
        
        log(f"ETL Pipeline completed", level="info")