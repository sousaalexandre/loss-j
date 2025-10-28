from langchain_community.document_loaders import PyPDFLoader
from langchain_core.documents import Document
from typing import List


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
    print(f"Loading document from: {file_path}")
    # For this example, we'll use a PDF loader.
    # You can easily add more loaders for other file types (e.g., TXT, DOCX).
    loader = PyPDFLoader(file_path)
    documents = loader.load()
    print(f"Successfully loaded {len(documents)} pages.")
    return documents

