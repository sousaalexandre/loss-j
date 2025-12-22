import os
from pathlib import Path
from src import settings
from src.rag import loaders as loader
from src.rag import splitters as splitter
from src.services import embedder
from src.services.vector_db import check_file_exists_vector_store, store, get_vector_store
from src.logger import log


class RAGIndexingPipeline:
    def __init__(self, use_etl: bool = False):
        """
        Initialize RAG indexing pipeline.
        
        Args:
            use_etl: If True, loads Markdown from gold directory. If False, loads PDFs from landing zone
        """
        self.use_etl = use_etl
        
        if use_etl:
            # Load from gold directory (ETL output)
            self.source_dir = Path("data_warehouse/03_gold")
            self.env_name = "production"
            
            if not self.source_dir.exists():
                log(f"Gold directory not found (will be created on ETL run): {self.source_dir}", level="info")
                self.source_dir.mkdir(parents=True, exist_ok=True)
        else:
            # Load from landing zone (direct PDF indexing)
            self.source_dir = Path("data_warehouse/00_landing_zone")
            self.env_name = "raw"
            
            if not self.source_dir.exists():
                log(f"Landing zone directory not found: {self.source_dir}", level="error")
                raise FileNotFoundError(f"Landing zone not found at {self.source_dir}")
    
    def _get_documents_to_index(self, source_files: list = None) -> list:
        """
        Get documents to index.
        
        Args:
            source_files: If provided, specific files to index. Otherwise, all files in source directory.
        
        Returns:
            List of file paths to index
        """
        if self.use_etl:
            # Get markdown files from gold directory bundles (03_gold/{hash}/{hash}.md)
            md_files = list(self.source_dir.glob("*/*.md"))
            return [str(f) for f in md_files]
        else:
            # Get PDF files from landing zone if not provided
            if source_files:
                return source_files
            pdf_files = list(self.source_dir.glob("*.pdf"))
            return [str(f) for f in pdf_files]
    
    def run(self, source_files: list = None) -> dict:
        """
        Run RAG indexing pipeline.
        
        Args:
            source_files: List of source files (PDFs if not using ETL, ignored if using ETL)
            
        Returns:
            dict: Indexing results
        """
        documents_to_index = self._get_documents_to_index(source_files)
        
        if not documents_to_index:
            log("No documents to index (ETL may have failed or no markdown files generated)", level="warning")
            return {"indexed_documents": [], "total_indexed": 0, "skipped": 0}
        
        if self.use_etl:
            source_type = f"Markdown (from ETL - {self.env_name})"
        else:
            source_type = "PDF"
        
        log(f"Starting RAG Indexing Pipeline ({source_type})", level="info")
        
        indexed_count = 0
        failed_count = 0
        total_chunks = 0
        
        for i, file_path in enumerate(documents_to_index, 1):
            file_name = os.path.basename(file_path)
            
            # Check if already indexed
            if check_file_exists_vector_store(file_path):
                log(f"[{i}/{len(documents_to_index)}] '{file_name}' already indexed. Replacing chunks...", level="info")
                # Delete existing chunks for this file
                vector_store = get_vector_store()
                normalized_path = file_path.replace("\\", "/")
                vector_store.delete(where={"source": normalized_path})
                log(f"     Deleted existing chunks for {file_name}", level="info")
            
            try:
                log(f"[{i}/{len(documents_to_index)}] Indexing {file_name}...", level="info")
                
                # Load documents
                documents = loader.load_document(file_path, is_markdown=self.use_etl)
                
                # Split documents
                chunks = splitter.split_documents(documents)
                log(f"     Split into {len(chunks)} chunks", level="info")
                total_chunks += len(chunks)
                
                # Get embeddings and store
                embeddings = embedder.get_embedding_model()
                store(chunks, embeddings)
                
                log(f"     ✓ Successfully indexed", level="info")
                indexed_count += 1
                
            except Exception as e:
                log(f"     ✗ Error indexing {file_name}: {e}", level="error")
                failed_count += 1
        
        log(f"RAG Indexing Pipeline completed ({source_type})", level="info")
        return {
            "indexed_documents": documents_to_index,
            "total_indexed": indexed_count,
            "total_chunks": total_chunks,
            "failed": failed_count,
            "skipped": 0,
            "status": "completed"
        }

