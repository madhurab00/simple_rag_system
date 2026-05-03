from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
from src.ingestion.ingest import extract_text_from_pdf
from src.ingestion.chunk import chunk_text
from src.ingestion.store import store_chunks_in_db

from src.utils import load_config

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
    print(f"Storing Chunks in Postgresql database...")
    store_chunks_in_db(chunks)
    print(f"Finished ingestion for {file_path}")

if __name__ == '__main__':

    config = load_config("config/config.yaml")
    pdf_dir = Path(config["data_root"]) / "pdfs"
    pdf_files = list(pdf_dir.glob("*.pdf"))
    for pdf_path in pdf_files:
        ingest_document(pdf_path)