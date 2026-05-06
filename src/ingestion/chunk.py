import hashlib
import logging
from typing import List, Dict, Any
import uuid

from langchain_text_splitters import RecursiveCharacterTextSplitter
from src.utils import count_tokens

# Configure logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def fixed_chunk(documents: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Split documents into fixed-size chunks without overlap.

    Args:
        documents (List[Dict[str, Any]]): List of document dictionaries, each containing:
            - "content" (str): Text content
            - "page" (int): Page number
            - "title" (str): Document title

    Returns:
        List[Dict[str, Any]]: List of chunk dictionaries containing:
            - "chunk_id" (str): Unique identifier
            - "title" (str): Document title
            - "page_number" (int): Page number
            - "text" (str): Chunk text
            - "strategy" (str): Chunking strategy ("fixed")
            - "chunk_index" (int): Global chunk index
            - "token_count" (int): Token count of chunk
            - "hash" (str): SHA256 hash of chunk content
    """
    logger.info("Starting fixed chunking")

    chunks = []
    chunk_idx = 0
    
    chunk_size_chars = 2000
    
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size_chars,
        length_function=len,
        separators=["\n\n", "\n", ". ", " ", ""]
    )
    
    for doc in documents:
        content = doc['content']
        page_number = doc['page']
        title = doc['title']
        
        doc_chunks = splitter.split_text(content)
        
        for text in doc_chunks:
            if text.strip():
                chunk_id = str(uuid.uuid4())
                hash_val = hashlib.sha256(text.strip().encode("utf-8")).hexdigest()
                token_count = count_tokens(text)

                chunks.append({
                    "chunk_id": chunk_id,
                    "title": title,
                    "page_number": page_number,
                    "text": text.strip(),
                    "strategy": "fixed",
                    "chunk_index": chunk_idx,
                    "token_count": token_count,
                    "hash": hash_val
                })
                chunk_idx += 1

    logger.info(f"Fixed chunking completed | Total chunks: {len(chunks)}")
    return chunks


def overlap_chunk(documents: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Split documents into fixed-size chunks with overlap.

    Args:
        documents (List[Dict[str, Any]]): List of document dictionaries.

    Returns:
        List[Dict[str, Any]]: List of chunk dictionaries with overlap applied.
    """
    logger.info("Starting overlap chunking")

    chunks = []
    chunk_idx = 0
    
    chunk_size_chars = 2000
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
        
        doc_chunks = splitter.split_text(content)
        
        for text in doc_chunks:
            if text.strip():
                chunk_id = str(uuid.uuid4())
                hash_val = hashlib.sha256(text.strip().encode("utf-8")).hexdigest()
                token_count = count_tokens(text)

                chunks.append({
                    "chunk_id": chunk_id,
                    "title": title,
                    "page_number": page_number,
                    "text": text.strip(),
                    "strategy": "overlap",
                    "chunk_index": chunk_idx,
                    "token_count": token_count,
                    "hash": hash_val
                })
                chunk_idx += 1

    logger.info(f"Overlap chunking completed | Total chunks: {len(chunks)}")
    return chunks


def chunk_text(documents: List[Dict[str, Any]], method: str = "overlap") -> List[Dict[str, Any]]:
    """Select and apply a chunking strategy.

    Args:
        documents (List[Dict[str, Any]]): List of input documents.
        method (str, optional): Chunking method to use. Defaults to "overlap".
            Supported values:
            - "fixed"
            - "overlap"

    Returns:
        List[Dict[str, Any]]: List of chunked document segments.

    Raises:
        ValueError: If an unknown chunking method is provided.
    """
    logger.info(f"Chunking method selected: {method}")

    if method == "fixed":
        return fixed_chunk(documents)
    elif method == "overlap":
        return overlap_chunk(documents)
    else:
        logger.error(f"Unknown chunking method: {method}")
        raise ValueError(f"Unknown chunking method: {method}")