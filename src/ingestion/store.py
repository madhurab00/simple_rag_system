import os
import logging
import psycopg2
from psycopg2.extras import execute_values

from dotenv import load_dotenv

load_dotenv()

# Logger setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def store_chunks_in_db(chunks):
    """Insert chunks into PostgreSQL with deduplication on hash.

    This stores pre-processed document chunks into a PostgreSQL
    table. Duplicate chunks are skipped based on the
    unique `hash` field.

    Args:
        chunks (List[Dict]): List of chunk dictionaries. Each dictionary must contain:
            - chunk_id (str)
            - title (str)
            - page_number (int)
            - text (str)
            - strategy (str)
            - chunk_index (int)
            - token_count (int)
            - hash (str)

    Raises:
        psycopg2.DatabaseError: If database operation fails.
    """
    logger.info(f"Connecting to PostgreSQL... | Total chunks: {len(chunks)}")

    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        port=os.getenv("POSTGRES_PORT", "5432"),
    )

    cur = conn.cursor()

    insert_query = """
    INSERT INTO document_chunks (
        chunk_id, title, page_number, text, strategy, chunk_index, token_count, hash
    ) VALUES %s
    ON CONFLICT (hash) DO NOTHING;
    """

    values = [
        (
            c["chunk_id"],
            c.get("title"),
            c.get("page_number"),
            c["text"],
            c["strategy"],
            c.get("chunk_index"),
            c.get("token_count"),
            c["hash"]
        )
        for c in chunks
    ]

    logger.info("Executing bulk insert into document_chunks table...")

    execute_values(cur, insert_query, values)
    conn.commit()

    cur.close()
    conn.close()

    logger.info(f"Database insert completed | Attempted: {len(values)} chunks")