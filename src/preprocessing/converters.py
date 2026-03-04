from abc import ABC, abstractmethod
from pathlib import Path
from src.logger import log
from src.preprocessing.mineru import parse_single_pdf_default
from src.preprocessing.docling import parse_pdf_docling
from src.preprocessing.docling_images import parse_pdf_docling_images
import tempfile
import shutil


class PDFConverter(ABC):
    """Abstract base class for PDF to Markdown converters."""
    
    @abstractmethod
    def convert(self, pdf_path: str, output_dir: str = None) -> tuple:
        """
        Convert a PDF file to Markdown and extract all assets.
        
        Args:
            pdf_path (str): Path to the PDF file
            output_dir (str): Directory to store output files. If None, uses temp directory.
            
        Returns:
            tuple: (markdown_content: str, output_directory: str, backend_used: str)
        """
        pass

class DoclingConverter(PDFConverter):
    """PDF to Markdown converter using Docling backend."""
    
    def __init__(self):
        """Initialize the Docling converter."""
        self.backend_used = "docling"
        
    def convert(self, pdf_path: str, output_dir: str = None) -> tuple:
        """
        Convert PDF using Docling.
        Preserves all extracted files (images, etc.) in output directory.
        
        Args:
            pdf_path (str): Path to the PDF file
            output_dir (str): Directory to store all extracted files
            
        Returns:
            tuple: (markdown_content: str, output_directory: str, backend_used: str)
                   output_directory contains all extracted files
        """
        log(f"Converting PDF using Docling: {pdf_path}", level="info")
        
        if output_dir is None:
            output_dir = tempfile.mkdtemp(prefix="docling_")
        
        docling_output = parse_pdf_docling(
            pdf_path=pdf_path,
            output_dir=output_dir,
        )
        
        log(f"Successfully converted {pdf_path} with Docling", level="info")
        
        return None
    
class DoclingImagesConverter(PDFConverter):
    """PDF to Markdown converter using Docling backend."""
    
    def __init__(self):
        """Initialize the Docling with image descriptions converter."""
        self.backend_used = "docling-images"
        
    def convert(self, pdf_path: str, output_dir: str = None) -> tuple:
        """
        Convert PDF using Docling with image descriptions.

        Preserves all extracted files (images, etc.) in output directory.
        
        Args:
            pdf_path (str): Path to the PDF file
            output_dir (str): Directory to store all extracted files
            
        Returns:
            None: Docling with images does not return markdown content directly, it is stored in output_dir
        """
        log(f"Converting PDF using Docling with image descriptions: {pdf_path}", level="info")
        
        if output_dir is None:
            output_dir = tempfile.mkdtemp(prefix="docling_")
        
        docling_output = parse_pdf_docling_images(
            pdf_path=pdf_path,
            output_dir=output_dir,
        )
        
        log(f"Successfully converted {pdf_path} with Docling", level="info")
        
        return None


class MinerUHTTPConverter(PDFConverter):
    """PDF to Markdown converter using MinerU VLM HTTP Client backend with fallback to pipeline."""
    
    def __init__(self, server_url: str):
        """
        Initialize the MinerU HTTP converter.
        
        Args:
            server_url (str): URL of the VLM HTTP server
        """
        self.server_url = server_url
        self.backend_used = None
        
    def convert(self, pdf_path: str, output_dir: str = None) -> tuple:
        """
        Convert PDF using MinerU VLM HTTP Client with fallback to pipeline.
        Preserves all extracted files (images, metadata, etc.) in output directory.
        
        Args:
            pdf_path (str): Path to the PDF file
            output_dir (str): Directory to store all extracted files (parent of auto/vlm/)
            
        Returns:
            tuple: (markdown_content: str, output_directory: str, backend_used: str)
                   output_directory contains auto/ or vlm/ subdirectories with all files
        """
        log(f"Converting PDF using MinerU HTTP Client: {pdf_path}", level="info")
        
        if output_dir is None:
            output_dir = tempfile.mkdtemp(prefix="mineru_")
        
        try:
            # Use parse_single_pdf_default to get full output including images/metadata
            mineru_output = parse_single_pdf_default(
                pdf_path=pdf_path,
                output_dir=output_dir,
                backend="vlm-http-client",
                server_url=self.server_url,
                draw_layout_bbox=False,
                draw_span_bbox=False,
                dump_orig_pdf=True,
                dump_md=True,
                dump_content_list=True,
                dump_middle_json=True,
                dump_model_output=True
            )
            self.backend_used = "vlm-http-client"
            log(f"Successfully converted with VLM HTTP Client", level="info")
            
        except Exception as e:
            log(f"VLM HTTP Client failed: {e}. Falling back to pipeline backend...", level="warning")
            mineru_output = parse_single_pdf_default(
                pdf_path=pdf_path,
                output_dir=output_dir,
                backend="pipeline",
                draw_layout_bbox=False,
                draw_span_bbox=False,
                dump_orig_pdf=True,
                dump_md=True,
                dump_content_list=True,
                dump_middle_json=True,
                dump_model_output=True
            )
            self.backend_used = "vlm-http-client-with-fallback"
            log(f"Successfully converted with pipeline (fallback)", level="info")
        
        # Extract markdown content (mineru_output is the actual output directory)
        pdf_name = Path(pdf_path).stem
        md_file = Path(mineru_output) / f"{pdf_name}.md"
        
        markdown_content = ""
        if md_file.exists():
            with open(md_file, 'r', encoding='utf-8') as f:
                markdown_content = f.read()
        
        # Return the parent directory (contains auto/ or vlm/) for proper flattening in ETL
        parent_output = Path(mineru_output).parent
        return markdown_content, str(parent_output), self.backend_used


class MinerUPipelineConverter(PDFConverter):
    """PDF to Markdown converter using MinerU local pipeline backend."""
    
    def __init__(self, lang: str = "pt"):
        """
        Initialize the MinerU Pipeline converter.
        
        Args:
            lang (str): Language for OCR (default: "pt" for Portuguese)
        """
        self.lang = lang
        self.backend_used = "pipeline"
        
    def convert(self, pdf_path: str, output_dir: str = None) -> tuple:
        """
        Convert PDF using MinerU local pipeline.
        Preserves all extracted files (images, metadata, etc.) in output directory.
        
        Args:
            pdf_path (str): Path to the PDF file
            output_dir (str): Directory to store all extracted files (parent of auto/vlm/)
            
        Returns:
            tuple: (markdown_content: str, output_directory: str, backend_used: str)
                   output_directory contains auto/ or vlm/ subdirectories with all files
        """
        log(f"Converting PDF using MinerU Pipeline: {pdf_path}", level="info")
        
        if output_dir is None:
            output_dir = tempfile.mkdtemp(prefix="mineru_")
        
        mineru_output = parse_single_pdf_default(
            pdf_path=pdf_path,
            output_dir=output_dir,
            backend="pipeline",
            lang=self.lang,
            draw_layout_bbox=False,
            draw_span_bbox=False,
            dump_orig_pdf=True,
            dump_md=True,
            dump_content_list=True,
            dump_middle_json=True,
            dump_model_output=True
        )
        
        log(f"Successfully converted {pdf_path} with MinerU Pipeline", level="info")
        
        # Extract markdown content (mineru_output is the actual output directory)
        pdf_name = Path(pdf_path).stem
        md_file = Path(mineru_output) / f"{pdf_name}.md"
        
        markdown_content = ""
        if md_file.exists():
            with open(md_file, 'r', encoding='utf-8') as f:
                markdown_content = f.read()
        
        # Return the parent directory (contains auto/ or vlm/) for proper flattening in ETL
        parent_output = Path(mineru_output).parent
        return markdown_content, str(parent_output), self.backend_used


def get_converter(loader: str, backend: str = None, server_url: str = None) -> PDFConverter:
    """
    Factory function to get the appropriate PDF converter.
    
    Args:
        loader (str): The loader type to use ("mineru" or "docling)
        backend (str): The backend to use ("vlm-http-client" or "pipeline")
        server_url (str): Server URL for VLM HTTP backend (required if backend="vlm-http-client")
        
    Returns:
        PDFConverter: An instance of the appropriate converter
        
    Raises:
        ValueError: If backend is invalid or required parameters are missing
    """
    if loader == "docling":
        log("Using Docling converter", level="info")
        return DoclingConverter()
    
    elif loader == "docling-images":
        log("Using Docling with Images converter", level="info")
        return DoclingImagesConverter()
    
    elif loader == "mineru" and backend == "vlm-http-client":
        if not server_url:
            raise ValueError("server_url is required for vlm-http-client backend")
        log(f"Using MinerU HTTP Client converter (server: {server_url})", level="info")
        return MinerUHTTPConverter(server_url=server_url)
    
    elif loader == "mineru" and backend == "pipeline":
        log("Using MinerU Pipeline converter", level="info")
        return MinerUPipelineConverter()
    
    else:
        raise ValueError(
            f"Unknown backend: {backend}. "
            f"Supported backends: 'vlm-http-client', 'pipeline'"
        )
