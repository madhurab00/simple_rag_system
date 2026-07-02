import os
import logging

import psycopg2
from dotenv import load_dotenv

load_dotenv()

# Logger setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST", "localhost"),
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    port=os.getenv("POSTGRES_PORT", "5432"),
)

cur = conn.cursor()

create_query = """
CREATE TABLE IF NOT EXISTS documents (
    document_id UUID PRIMARY KEY,
    file_name TEXT NOT NULL,
    file_hash TEXT UNIQUE NOT NULL
);
"""

cur.execute(create_query)

conn.commit()

cur.close()
conn.close()

logger.info("Database table creation completed.")