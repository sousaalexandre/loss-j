import os
from pathlib import Path
from loguru import logger
from docling.document_converter import DocumentConverter
from hierarchical.postprocessor import ResultPostprocessor

def parse_pdf_docling(pdf_path: str, output_dir: str) -> dict:
    try:
        converter = DocumentConverter()
        result = converter.convert(pdf_path)
        ResultPostprocessor(result).process()
        markdown_content = result.document.export_to_markdown()
        base_name = Path(pdf_path).stem
        output_filename = f"{base_name}.md"
        output_path = Path(output_dir) / output_filename
        output_path.write_text(markdown_content, encoding='utf-8')
        print(f"-> SUCCESS: Saved as '{output_filename}' in '{output_dir}'\n")
    except Exception as e:
        logger.exception(f"Error parsing single PDF: {e}")
        raise