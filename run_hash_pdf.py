#!/usr/bin/env python3
"""
Utility script to copy PDF(s) with content hash as filename.

Usage:
    # Single file
    python run_hash_pdf.py <input_pdf> [output_directory]
    
    # Bulk (directory)
    python run_hash_pdf.py <input_directory> <output_directory>

Examples:
    python run_hash_pdf.py document.pdf ./output/
    # Creates: ./output/a1b2c3d4...xyz.pdf
    
    python run_hash_pdf.py ./pdfs/ ./hashed/
    # Hashes all PDFs in ./pdfs/ to ./hashed/
"""

import hashlib
import shutil
import sys
from pathlib import Path


def generate_file_hash(file_path: str) -> str:
    """
    Generates a SHA-256 hash for the content of a file.
    """
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


def hash_pdf(input_path: str, output_dir: str = None) -> Path:
    """
    Copy a PDF file with its SHA-256 hash as the filename.
    
    Args:
        input_path: Path to the input PDF
        output_dir: Output directory (defaults to current directory)
    
    Returns:
        Path to the hashed PDF file
    """
    input_pdf = Path(input_path)
    
    if not input_pdf.exists():
        raise FileNotFoundError(f"File not found: {input_pdf}")
    
    if not input_pdf.suffix.lower() == ".pdf":
        raise ValueError(f"File is not a PDF: {input_pdf}")
    
    # Generate hash
    file_hash = generate_file_hash(str(input_pdf))
    
    # Determine output path
    if output_dir:
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
    else:
        output_path = Path.cwd()
    
    hashed_pdf = output_path / f"{file_hash}.pdf"
    
    # Copy file
    shutil.copy2(input_pdf, hashed_pdf)
    
    print(f"{input_pdf.name} → {file_hash}.pdf")
    
    return hashed_pdf


def hash_pdfs_bulk(input_dir: str, output_dir: str) -> list[Path]:
    """
    Copy all PDFs in a directory with their SHA-256 hash as filename.
    
    Args:
        input_dir: Path to directory containing PDFs
        output_dir: Output directory for hashed PDFs
    
    Returns:
        List of paths to hashed PDF files
    """
    input_path = Path(input_dir)
    
    if not input_path.exists():
        raise FileNotFoundError(f"Directory not found: {input_path}")
    
    if not input_path.is_dir():
        raise ValueError(f"Not a directory: {input_path}")
    
    pdf_files = list(input_path.glob("*.pdf")) + list(input_path.glob("*.PDF"))
    
    if not pdf_files:
        print(f"No PDF files found in: {input_path}")
        return []
    
    print(f"Found {len(pdf_files)} PDF(s) in {input_path}")
    print(f"Output directory: {output_dir}")
    print("-" * 60)
    
    results = []
    for pdf in sorted(pdf_files):
        try:
            hashed = hash_pdf(str(pdf), output_dir)
            results.append(hashed)
        except Exception as e:
            print(f"Error processing {pdf.name}: {e}")
    
    print("-" * 60)
    print(f"Processed {len(results)}/{len(pdf_files)} PDF(s)")
    
    return results


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    
    input_path = Path(sys.argv[1])
    output_dir = sys.argv[2] if len(sys.argv) > 2 else None
    
    try:
        if input_path.is_dir():
            if not output_dir:
                print("Error: Output directory required for bulk processing")
                sys.exit(1)
            hash_pdfs_bulk(str(input_path), output_dir)
        else:
            hash_pdf(str(input_path), output_dir)
    except (FileNotFoundError, ValueError) as e:
        print(f"Error: {e}")
        sys.exit(1)
