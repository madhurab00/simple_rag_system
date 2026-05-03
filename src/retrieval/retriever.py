import os
import sys
import time 
from pathlib import Path
import numpy as np
import psycopg2
from psycopg2.extras import RealDictCursor
from pgvector.psycopg2 import register_vector
from dotenv import load_dotenv
from typing import Dict, Any

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
from src.utils import load_config
load_dotenv()

class Retriever:
    def __init__(self,config:Dict[str, Any]):
        self.config = config
        self.model = self._get_embedding_model()
        self.conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        port=os.getenv("POSTGRES_PORT", "5432"),
    )
        register_vector(self.conn)
        
    def _get_embedding_model(self):
        """
        Return LangChain embeddings based on config.
        
        Args:
            config: Configuration dictionary with text_emb_provider, text_emb_model, etc.
            
        Returns:
            LangChain embeddings instance
            
        Raises:
            ValueError: If provider is unknown
        """
        provider = self.config["emb_provider"]
        model_name = self.config["emb_model"]
        
        if provider == "openai":
            from langchain_openai import OpenAIEmbeddings
            return OpenAIEmbeddings(model=model_name)
        
        elif provider == "sbert":
            from langchain_huggingface import HuggingFaceEmbeddings
            model_kwargs = {"device": "cpu"} #cuda or mps
            
            if self.config.get("normalize_embeddings", True):
                encode_kwargs = {"normalize_embeddings": True}
            else:
                encode_kwargs = {}
            
            return HuggingFaceEmbeddings(
                model_name=model_name,
                model_kwargs=model_kwargs,
                encode_kwargs=encode_kwargs,
            )
        
        else:
            raise ValueError(f"Unknown text_emb_provider: {provider}")
        
    def _embed_query(self,query)->np.ndarray:

        return self.model.embed_query(query)
    
    def retrieve(self,query:str,top_k:int = 5)-> list[Dict[str,Any]]:

        query_embedding = self._embed_query(query)
        query_embedding = "[" + ",".join(map(str, query_embedding)) + "]"
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
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
            """, (query_embedding, top_k))

            rows = cur.fetchall()

        results = []
        for row in rows:
            results.append({
                "chunk_id": str(row["chunk_id"]),
                "title": row["title"],
                "page_number": row["page_number"],
                "chunk_index": row["chunk_index"],
                "content": row["text"],
                "score": 1 / (1 + row["distance"])
            })

        return [result for result in results if result["score"] > 0.65]

if __name__ == "__main__":
    config = load_config("config/config.yaml")
    retriever = Retriever(config)

    queries = [
        "In what situation is a software programmer not allowed to claim a deduction for study expenses related to project management?",
        "Can pilots claim meal and snack expenses during normal duty flights without overnight travel?",
        "Is study to switch from food delivery driver to chef eligible for tax deductions?",
        "What aviation medical costs can be claimed as work-related expenses?",
        "What are the conditions for claiming protective clothing and footwear expenses for police officers?",
        "Under what conditions can self-education expenses be claimed by police officers?",
        "Can I claim the cost of buying or cleaning conventional clothing such as black pants or a white shirt for work in hospitality?",

    ]

    results = []

    for i,query in enumerate(queries,1):
        start_time =  time.time()
        retrieved = retriever.retrieve(query)
        print(f"{i}- {query[:100]}")
        print(f"{retrieved}")
        print("TOTAL:", time.time() - start_time)
        print("\n")