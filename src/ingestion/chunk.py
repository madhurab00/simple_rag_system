import hashlib
from typing import List,Dict,Any
import uuid

from langchain_text_splitters import RecursiveCharacterTextSplitter

from src.utils import count_tokens

def fixed_chunk(documents: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Split documents into fixed-size chunks without overlapping.
    Args:
        documents: List of extracted texts
    Returns:
        List of chunk dicts with 'title', 'text', 'strategy', 'chunk_index'
    """
    chunks = []
    chunk_idx = 0
    
    # Character approximation: ~4 chars per token
    chunk_size_chars =  2000
    
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size_chars,
        length_function=len,
        separators=["\n\n", "\n", ". ", " ", ""]
    )
    
    for doc in documents:
        content = doc['content']
        page_number = doc['page']
        title = doc['title']
        
        # Split content
        doc_chunks = splitter.split_text(content)
        
        for text in doc_chunks:
            if text.strip():
                chunk_id = str(uuid.uuid4())
                hash_val = hashlib.sha256(text.strip().encode("utf-8")).hexdigest()
                token_count = count_tokens(text)
                chunks.append({
                    "chunk_id": chunk_id,
                    "title": title,
                    "page_number":page_number,
                    "text": text.strip(),
                    "strategy": "fixed",
                    "chunk_index": chunk_idx,
                    "token_count": token_count,
                    "hash": hash_val
                })
                chunk_idx += 1
    
    return chunks

def overlap_chunk(documents: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Split documents into fixed-size chunks with overlap.
    Args:
        documents: List of extracted texts
    Returns:
        List of chunk dicts with 'title', 'text', 'strategy', 'chunk_index'
    """
    chunks = []
    chunk_idx = 0
    
    # Character approximation: ~4 chars per token
    chunk_size_chars =  2000
    overlap_chars = 100
    
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size_chars,
        chunk_overlap=overlap_chars,
        length_function=len,
        separators=["\n\n", "\n", ". ", " ", ""]
    )
    
    for doc in documents:
        content = doc['content']
        page_number = doc['page']
        title = doc['title']
        
        # Split content
        doc_chunks = splitter.split_text(content)
        
        for text in doc_chunks:
            if text.strip():
                chunk_id = str(uuid.uuid4())
                hash_val = hashlib.sha256(text.strip().encode("utf-8")).hexdigest()
                token_count = count_tokens(text)
                chunks.append({
                    "chunk_id": chunk_id,
                    "title": title,
                    "page_number":page_number,
                    "text": text.strip(),
                    "strategy": "overlap",
                    "chunk_index": chunk_idx,
                    "token_count": token_count,
                    "hash": hash_val
                })
                chunk_idx += 1
    
    return chunks

def chunk_text(documents: List[Dict[str, Any]], method: str = "overlap") -> List[Dict[str, Any]]:
    
    if method == "fixed":
        return fixed_chunk(documents)
    elif method == "overlap":
        return overlap_chunk(documents)
    else:
        raise ValueError(f"Unknown chunking method: {method}")



