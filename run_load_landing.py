"""
Simple script to load PDFs from a directory into the bronze layer with hashed names
and create/update the _catalog.json file.

Usage:
    python load_bronze_layer.py <path_to_pdf_directory>

Example:
    python load_bronze_layer.py ./my_pdfs
"""

import sys
import json
import shutil
from pathlib import Path
from datetime import datetime
from src.utils import generate_file_hash


def extract_title_from_filename(filename: str) -> str:
    """Extract a clean title from PDF filename.
    
    Removes .pdf extension and replaces underscores and hyphens with spaces.
    """
    title = filename.replace(".pdf", "").replace("_", " ").replace("-", " ")
    return " ".join(title.split())


def load_pdfs_to_bronze(pdf_dir: str) -> None:
    """Load PDFs from directory to bronze layer with hashed names.
    
    Args:
        pdf_dir: Path to directory containing PDF files
    """
    pdf_dir = Path(pdf_dir)
    if not pdf_dir.exists():
        print(f"❌ Directory not found: {pdf_dir}")
        sys.exit(1)
    
    # Create bronze layer directory
    bronze_layer = Path("data_lakehouse/01_bronze")
    bronze_layer.mkdir(parents=True, exist_ok=True)
    
    # Load existing catalog or create new one
    catalog_path = bronze_layer / "_catalog.json"
    if catalog_path.exists():
        with open(catalog_path, 'r', encoding='utf-8') as f:
            catalog = json.load(f)
        print(f"📂 Loading existing catalog ({len(catalog)} entries)")
    else:
        catalog = {}
        print("📂 Creating new catalog")
    
    # Find all PDFs in directory
    pdf_files = list(pdf_dir.glob("*.pdf"))
    if not pdf_files:
        print(f"❌ No PDF files found in {pdf_dir}")
        sys.exit(1)
    
    print(f"📄 Found {len(pdf_files)} PDF files")
    print()
    
    # Process each PDF
    new_count = 0
    existing_count = 0
    
    for pdf_path in sorted(pdf_files):
        try:
            # Generate hash for consistent naming
            file_hash = generate_file_hash(str(pdf_path))
            
            # Check if already in catalog
            if file_hash in catalog:
                print(f"⏭️  {pdf_path.name} → {file_hash}.pdf (already in catalog)")
                existing_count += 1
                continue
            
            # Copy PDF to bronze layer with hash name
            hash_pdf_path = bronze_layer / f"{file_hash}.pdf"
            shutil.copy2(str(pdf_path), str(hash_pdf_path))
            
            # Create catalog entry
            title = extract_title_from_filename(pdf_path.name)
            catalog[file_hash] = {
                "original_filename": pdf_path.name,
                "title": title,
                "description": "",
                "category": [],
                "uploaded_by": "system",
                "uploaded_at": datetime.now().isoformat(),
                "active": True,
                "processed_at": None
            }
            
            print(f"✓ {pdf_path.name} → {file_hash}.pdf")
            new_count += 1
        
        except Exception as e:
            print(f"❌ Error processing {pdf_path.name}: {e}")
    
    # Save updated catalog
    with open(catalog_path, 'w', encoding='utf-8') as f:
        json.dump(catalog, f, indent=4, ensure_ascii=False)
    
    print()
    print("="*60)
    print(f"✓ Bronze layer loading complete!")
    print(f"  New files added: {new_count}")
    print(f"  Already existing: {existing_count}")
    print(f"  Total in catalog: {len(catalog)}")
    print(f"  Catalog saved to: {catalog_path}")
    print("="*60)
    print()
    print("📝 Note: Categories are empty. You can fill them manually in the Streamlit UI")
    print("   or by editing the _catalog.json file directly.")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python run_load_landing.py <path_to_pdf_directory>")
        print()
        print("Example: python run_load_landing.py ./my_pdfs")
        sys.exit(1)
    
    pdf_dir = sys.argv[1]
    load_pdfs_to_bronze(pdf_dir)
