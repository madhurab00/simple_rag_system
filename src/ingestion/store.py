import os
import psycopg2
from psycopg2.extras import execute_values

from dotenv import load_dotenv
load_dotenv()

def store_chunks_in_db(chunks):
    """
    Insert chunks into PostgreSQL with deduplication on hash.
    Args:
        chunks: List of chunk dicts
    """
    # Read DB credentials from environment variables
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
        chunk_id, doc_id, page_number, title, text, strategy, chunk_index, token_count, hash
    ) VALUES %s
    ON CONFLICT (hash) DO NOTHING;
    """

    values = [
        (
            c["chunk_id"],
            c.get("doc_id", "unknown_doc"),
            c.get("page_number"),
            c.get("title"),
            c["text"],
            c["strategy"],
            c.get("chunk_index"),
            c.get("token_count"),
            c["hash"]
        )
        for c in chunks
    ]

    execute_values(cur, insert_query, values)
    conn.commit()
    cur.close()
    conn.close()
    print(f"Stored {len(values)} chunks in PostgreSQL (duplicates skipped).")
