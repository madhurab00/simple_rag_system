from pathlib import Path

from src.ingestion.ingest import extract_text_from_pdf
from src.ingestion.chunk import chunk_text
from src.ingestion.store import store_chunks_in_db

def ingest_document(file_path: str, method: str = "fixed"):
    """
    Full ingestion pipeline:
    1. Extract text from PDF
    2. Chunk text (fixed or overlap)
    3. Store chunks in PostgreSQL
    """
    print(f"Extracting text from {file_path}...")
    documents = extract_text_from_pdf(file_path)
    print(f"Chunking text ...")
    chunks = chunk_text(documents,"overlap")
    store_chunks_in_db(chunks)

if __name__ == '__main__':
    pdf_dir = Path("data/pdfs")
    pdf_files = list(pdf_dir.glob("*.pdf"))
    for pdf_path in pdf_files:
        ingest_document(pdf_path)