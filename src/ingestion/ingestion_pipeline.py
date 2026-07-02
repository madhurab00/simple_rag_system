from pathlib import Path
import sys
import logging

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from src.ingestion.ingest import extract_text_from_pdf
from src.ingestion.chunk import chunk_text
from src.ingestion.store import store_chunks_in_db
from src.ingestion.document_store import generate_document_hash,store_document_metadata,document_exists
from src.utils import load_config

# Logger setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def ingest_document(file_path: str, method: str = "overlap"):
    """Run full document ingestion pipeline.

    This pipeline performs three main steps:
    1. Extract text from a PDF document.
    2. Chunk the extracted text using the selected method.
    3. Store processed chunks into PostgreSQL.

    Args:
        file_path (str): Path to the PDF file to ingest.
        method (str, optional): Chunking strategy ("fixed" or "overlap").
            Defaults to "fixed".

    Returns:
        None
    """
    logger.info(f"Starting ingestion pipeline for: {file_path}")

    # Step 1: Extraction
    logger.info("Step 1: Extracting text from PDF")
    documents = extract_text_from_pdf(file_path)

    # Step 2: Chunking
    logger.info(f"Step 2: Chunking text using method: {method}")
    chunks = chunk_text(documents, method)

    # Step 3.a: Document Storage
    logger.info("Step 3.a: Storing documents in PostgreSQL")
    full_text = " ".join(
                            doc["content"]
                            for doc in documents
                        )
    hashed_document = generate_document_hash(full_text)
    if not document_exists(hashed_document):
        store_document_metadata(pdf_path,hashed_document)
    else:
        logger.info("Document already exists in db")

    logger.info("Step 3.b: Storing chunks in PostgreSQL")
    store_chunks_in_db(chunks)

    logger.info(f"Ingestion completed successfully: {file_path}")


if __name__ == '__main__':
    config = load_config("config/config.yaml")

    pdf_dir = Path(config["data_root"]) / "pdfs"
    pdf_files = list(pdf_dir.glob("*.pdf"))

    logger.info(f"Found {len(pdf_files)} PDF files for ingestion")

    for pdf_path in pdf_files:
        try:
            ingest_document(str(pdf_path))
        except Exception as e:
            logger.exception(f"Failed ingestion for {pdf_path}")