import os
import sys
import time
import logging
from pathlib import Path
from typing import Dict, Any, List

import numpy as np
import psycopg2
from psycopg2.extras import RealDictCursor
from pgvector.psycopg2 import register_vector
from dotenv import load_dotenv

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from src.utils import load_config

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

class Retriever:
    """
    Vector-based document retriever using PostgreSQL + pgvector.

    Supports embedding-based semantic search over stored document chunks.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize retriever.

        Args:
            config (Dict[str, Any]): Configuration dictionary containing:
                - emb_provider (str)
                - emb_model (str)
                - normalize_embeddings (bool, optional)
        """
        self.config = config
        self.model = self._get_embedding_model()
        self.conn = self._create_connection()

        logger.info("Retriever initialized successfully")

    def _create_connection(self):
        """
        Create PostgreSQL connection and register pgvector.

        Returns:
            psycopg2.connection: Active DB connection.
        """
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            dbname=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            port=os.getenv("POSTGRES_PORT", "5432"),
        )
        register_vector(conn)
        return conn

    def _get_embedding_model(self):
        """
        Load embedding model based on configuration.

        Returns:
            Embedding model instance.

        Raises:
            ValueError: If provider is not supported.
        """
        provider = self.config["emb_provider"]
        model_name = self.config["emb_model"]

        logger.info("Loading embedding model: %s (%s)", provider, model_name)

        if provider == "openai":
            from langchain_openai import OpenAIEmbeddings
            return OpenAIEmbeddings(model=model_name)

        elif provider == "sbert":
            from langchain_huggingface import HuggingFaceEmbeddings

            model_kwargs = {"device": "cpu"}

            encode_kwargs = (
                {"normalize_embeddings": True}
                if self.config.get("normalize_embeddings", True)
                else {}
            )

            return HuggingFaceEmbeddings(
                model_name=model_name,
                model_kwargs=model_kwargs,
                encode_kwargs=encode_kwargs,
            )

        else:
            raise ValueError(f"Unknown emb_provider: {provider}")

    def _embed_query(self, query: str) -> np.ndarray:
        """
        Convert query text into embedding vector.

        Args:
            query (str): Input query string.

        Returns:
            np.ndarray: Query embedding vector.
        """
        return self.model.embed_query(query)

    def retrieve(self, query: str, top_k: int = 5) -> List[Dict[str, Any]]:
        """
        Retrieve top-k similar document chunks for a query.

        Args:
            query (str): User query.
            top_k (int): Number of results to return.

        Returns:
            List[Dict[str, Any]]: Ranked retrieval results filtered by similarity threshold.
        """
        logger.info("Retrieving top-%d results for query", top_k)

        query_embedding = self._embed_query(query)

        query_embedding = "[" + ",".join(map(str, query_embedding)) + "]"

        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT 
                    chunk_id,
                    title,
                    page_number,
                    chunk_index,
                    text,
                    embedding <=> %s AS distance
                FROM document_chunks
                ORDER BY distance ASC
                LIMIT %s;
                """,
                (query_embedding, top_k),
            )

            rows = cur.fetchall()

        results = [
            {
                "chunk_id": str(row["chunk_id"]),
                "title": row["title"],
                "page_number": row["page_number"],
                "chunk_index": row["chunk_index"],
                "content": row["text"],
                "score": 1 / (1 + row["distance"]),
            }
            for row in rows
        ]

        # similarity filtering
        filtered = [r for r in results if r["score"] > 0.5]

        logger.info("Retrieved %d relevant chunks", len(filtered))

        return filtered

if __name__ == "__main__":
    config = load_config("config/config.yaml")
    retriever = Retriever(config)

    queries = [
        # "In what situation is a software programmer not allowed to claim a deduction for study expenses related to project management?",
        # "Can pilots claim meal and snack expenses during normal duty flights without overnight travel?",
        # "Is study to switch from food delivery driver to chef eligible for tax deductions?",
        # "What aviation medical costs can be claimed as work-related expenses?",
        # "What are the conditions for claiming protective clothing and footwear expenses for police officers?",
        # "Under what conditions can self-education expenses be claimed by police officers?",
        # "Can I claim the cost of buying or cleaning conventional clothing such as black pants or a white shirt for work in hospitality?",
        # "How long am I required to keep my tax records after lodging a return?",
        # "Is there a specific tool the ATO recommends for tracking receipts throughout the year?",
        # "What is the cents per kilometre rate for work-related car expenses",
        "How is the Medicare levy surcharge (MLS) rate determined?"
    ]

    results = []

    for i, query in enumerate(queries, 1):
        start_time = time.time()

        retrieved = retriever.retrieve(query)

        logger.info("%d - %s", i, query[:100])
        logger.info("Results: %s", retrieved)
        logger.info("Time taken: %.4f sec", time.time() - start_time)
