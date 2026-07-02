import os
import sys
import logging
from pathlib import Path
from typing import Dict, Any, List

import numpy as np
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# project path setup
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from src.utils import load_config

load_dotenv()

# -----------------------
# Logging Configuration
# -----------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

def get_db_connection():
    """
    Create and return a PostgreSQL database connection.

    Returns:
        psycopg2.connection: Active database connection.
    """
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        port=os.getenv("POSTGRES_PORT", "5432"),
    )

def fetch_chunks(limit: int = None) -> List[Dict[str, Any]]:
    """
    Fetch document chunks from the database.

    Args:
        limit (int, optional): Maximum number of chunks to fetch.

    Returns:
        List[Dict[str, Any]]: List of document chunks.
    """
    logger.info("Fetching chunks from database (limit=%s)", limit)

    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        query = "SELECT * FROM document_chunks"
        if limit:
            query += f" LIMIT {limit}"

        cur.execute(query)
        rows = cur.fetchall()

        logger.info("Fetched %d chunks", len(rows))
        return rows

    except Exception as e:
        logger.exception("Failed to fetch chunks: %s", e)
        raise

    finally:
        cur.close()
        conn.close()


def _get_embedding_model(config: Dict[str, Any]):
    """
    Load embedding model based on configuration.

    Args:
        config (Dict[str, Any]): Configuration dictionary containing:
            - emb_provider (str): "openai" or "sbert"
            - emb_model (str): model name
            - normalize_embeddings (bool): optional flag

    Returns:
        Embeddings model instance

    Raises:
        ValueError: If embedding provider is unsupported.
    """
    provider = config["emb_provider"]
    model_name = config["emb_model"]

    logger.info("Loading embedding model: provider=%s, model=%s", provider, model_name)

    if provider == "openai":
        from langchain_openai import OpenAIEmbeddings
        return OpenAIEmbeddings(model=model_name)

    elif provider == "sbert":
        from langchain_huggingface import HuggingFaceEmbeddings

        model_kwargs = {"device": "cpu"}
        encode_kwargs = (
            {"normalize_embeddings": True}
            if config.get("normalize_embeddings", True)
            else {}
        )

        return HuggingFaceEmbeddings(
            model_name=model_name,
            model_kwargs=model_kwargs,
            encode_kwargs=encode_kwargs,
        )

    else:
        raise ValueError(f"Unknown emb_provider: {provider}")

def embed_chunks(chunks: List[str], config: Dict[str, Any]) -> np.ndarray:
    """
    Generate embeddings for a list of text chunks.

    Args:
        chunks (List[str]): Input text chunks.
        config (Dict[str, Any]): Embedding configuration.

    Returns:
        np.ndarray: Generated embeddings.
    """
    logger.info("Generating embeddings for %d chunks", len(chunks))

    model = _get_embedding_model(config)
    embeddings = model.embed_documents(chunks)

    logger.info("Embedding generation completed")
    return embeddings

def store_embeddings(chunk_ids: List[int], embeddings: List[List[float]]) -> None:
    """
    Store embeddings into the database.

    Args:
        chunk_ids (List[int]): List of chunk IDs.
        embeddings (List[List[float]]): Corresponding embeddings.

    Returns:
        None
    """
    logger.info("Storing embeddings into database...")

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        for cid, emb in zip(chunk_ids, embeddings):
            cur.execute(
                "UPDATE document_chunks SET embedding = %s WHERE chunk_id = %s",
                (emb, cid),
            )

        conn.commit()
        logger.info("Successfully stored embeddings for %d chunks", len(chunk_ids))

    except Exception as e:
        conn.rollback()
        logger.exception("Failed to store embeddings: %s", e)
        raise

    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    logger.info("Starting embedding pipeline...")

    config = load_config("config/config.yaml")

    chunks = fetch_chunks()
    chunk_ids = [chunk["chunk_id"] for chunk in chunks]
    chunk_texts = [chunk["text"] for chunk in chunks]

    embeddings = embed_chunks(chunk_texts, config)
    store_embeddings(chunk_ids, embeddings)

    logger.info("Embeddings stored successfully!")