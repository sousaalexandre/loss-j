from langchain_community.document_loaders import PyPDFLoader
from langchain_core.documents import Document
from typing import List
from src.logger import log


def load_document(file_path: str) -> List[Document]:
    """
    Loads a document from the specified file path.
    Currently supports PDF files.

    Args:
        file_path (str): The path to the document file.

    Returns:
        List[Document]: A list of Document objects, where each object
                        represents a page in the PDF.
    """
    log(f"Loading document from: {file_path}", level="info")

    loader = PyPDFLoader(file_path)
    documents = loader.load()
    log(f"Successfully loaded {len(documents)} pages.", level="info")
    return documents

