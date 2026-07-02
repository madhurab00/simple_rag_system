import sys
import hashlib
import logging
import uuid
from typing import Optional
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

logger = logging.getLogger(__name__)


def generate_document_hash(text: str) -> str:
    """Generate a SHA256 hash from normalized document text.

    Args:
        text (str): Full normalized document text.

    Returns:
        str: SHA256 hexadecimal hash.
    """
    normalized_text = " ".join(text.lower().split())

    return hashlib.sha256(
        normalized_text.encode("utf-8")
    ).hexdigest()


def document_exists(file_hash: str) -> bool:
    """Check whether a document hash already exists in the database.

    Args:
        file_hash (str): SHA256 document hash.

    Returns:
        bool: True if document already exists, else False.
    """
    logger.info("Checking if document already exists.")

    query = """
        SELECT EXISTS(
            SELECT 1
            FROM documents
            WHERE file_hash = %s
        );
    """

    with psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        port=os.getenv("POSTGRES_PORT", "5432"),
    ) as conn:

        with conn.cursor() as cur:
            cur.execute(query, (file_hash,))
            exists = cur.fetchone()[0]

    logger.info("Document exists: %s", exists)

    return exists


def store_document_metadata(
    file_path: str,
    file_hash: str,
) -> str:
    """Store document metadata in the database.

    Args:
        file_name (str): Original file name.
        file_hash (str): SHA256 document hash.

    Returns:
        str: Generated document UUID.
    """
    logger.info("Storing document metadata.")
    file_name = Path(file_path).name
    document_id = str(uuid.uuid4())

    query = """
        INSERT INTO documents (
            document_id,
            file_name,
            file_hash
        )
        VALUES (%s, %s, %s)
    """

    with psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        port=os.getenv("POSTGRES_PORT", "5432"),
    ) as conn:

        with conn.cursor() as cur:
            cur.execute(
                query,
                (
                    document_id,
                    file_name,
                    file_hash,
                ),
            )

        conn.commit()

    logger.info(
        "Stored document metadata successfully. document_id=%s",
        document_id,
    )

    return document_id

if __name__ == '__main__':
    from src.ingestion.ingest import extract_text_from_pdf
    pdf_path = "data/pdfs/TaxTimeToolkit_IT professional.pdf"

    documents = extract_text_from_pdf(pdf_path)
    full_text = " ".join(
                            doc["content"]
                            for doc in documents
                        )
    hashed_document = generate_document_hash(full_text)
    if not document_exists(hashed_document):
        store_document_metadata(pdf_path,hashed_document)
    else:
        logger.info("Document already exists in db")