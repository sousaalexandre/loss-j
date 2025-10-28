from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_core.documents import Document
from typing import List
from src import settings

def split_documents(documents: List[Document]) -> List[Document]:
    """
    Splits a list of documents into smaller chunks based on configured settings.

    Args:
        documents (List[Document]): The list of documents to split.

    Returns:
        List[Document]: A list of smaller document chunks.
    """
    print("Splitting documents into smaller chunks...")
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=settings.CHUNK_SIZE,
        chunk_overlap=settings.CHUNK_OVERLAP,
        length_function=len,
        is_separator_regex=False,
    )
    chunks = text_splitter.split_documents(documents)
    print(f"Successfully split documents into {len(chunks)} chunks.")
    return chunks