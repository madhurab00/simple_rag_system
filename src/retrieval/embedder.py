import os
import sys
from pathlib import Path
import numpy as np
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from typing import Dict, Any

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
from src.utils import load_config

load_dotenv() 

def fetch_chunks(limit: int = None)->list[Dict[str,Any]]:
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        port=os.getenv("POSTGRES_PORT", "5432"),
    )
    cur = conn.cursor(cursor_factory=RealDictCursor)

    query = "SELECT * FROM document_chunks"
    if limit:
        query += f" LIMIT {limit}"

    cur.execute(query)
    rows = cur.fetchall()

    cur.close()
    conn.close()
    return rows

def _get_embedding_model(config: Dict[str, Any]):
    """
    Return LangChain embeddings based on config.
    
    Args:
        config: Configuration dictionary with text_emb_provider, text_emb_model, etc.
        
    Returns:
        LangChain embeddings instance
        
    Raises:
        ValueError: If provider is unknown
    """
    provider = config["emb_provider"]
    model_name = config["emb_model"]
    
    if provider == "openai":
        from langchain_openai import OpenAIEmbeddings
        return OpenAIEmbeddings(model=model_name)
    
    elif provider == "sbert":
        from langchain_huggingface import HuggingFaceEmbeddings
        model_kwargs = {"device": "cpu"} #cuda or mps
        
        if config.get("normalize_embeddings", True):
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
    
def embed_chunks(chunks:list[str],config: Dict[str,Any])->np.ndarray:
    model = _get_embedding_model(config)

    return model.embed_documents(chunks)

def store_embeddings(chunk_ids, embeddings):
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        port=os.getenv("POSTGRES_PORT", "5432"),
    )
    cur = conn.cursor()

    for cid, emb in zip(chunk_ids, embeddings):
        cur.execute(
            "UPDATE document_chunks SET embedding = %s WHERE chunk_id = %s",
            (emb, cid)
        )

    conn.commit()
    cur.close()
    conn.close()

if __name__=='__main__':
    config = load_config("config/config.yaml")
    chunks = fetch_chunks(100)
    chunk_ids = [chunk["chunk_id"] for chunk in chunks]
    chunk_texts = [chunk["text"] for chunk in chunks]

    embeddings = embed_chunks(chunk_texts,config)


    store_embeddings(chunk_ids, embeddings)

    print("Embeddings stored successfully!")
