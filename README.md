# Simple RAG System

## 🎯 Project Goal

Build a modular pipeline that:

- Extracts text from PDFs

- Splits text into chunks (fixed or overlapping)

- Stores chunks + metadata in PostgreSQL

This sets the foundation for later steps (embeddings, retrieval, and semantic search).

# 🛠️ Milestone 1 

1. **PDF Extraction**: Using PyMuPDF to extract text page by page and return with metadata.

2. **Chunking**: Implemented fixed_chunk and overlap_chunk functions with RecursiveCharacterTextSplitter.

- *Metadata*: Each chunk includes chunk_id, title, page_number, text, strategy, chunk_index, token_count, and hash.

3. **Database Storage**: PostgreSQL table document_chunks created with schema to store chunks and enforce deduplication via hash.

- *Dockerized DB*: PostgreSQL runs via Docker Compose with credentials managed through .env.

Pipeline Script: ingestion_pipeline.py orchestrates extract → chunk → store.

## 📂 Project Structure
```
simple_rag_system/
│
├── docker-compose.yml        # PostgreSQL service
├── .env                      # DB credentials
├── src/
│   ├── utils.py              # helper functions
│   └── ingestion/
│       ├── __init__.py
│       ├── extract.py        # PDF text extraction
│       ├── chunking.py       # fixed + overlap chunkers
│       ├── storage.py        # DB storage functions
│       └── ingestion_pipeline.py  # main pipeline script
```
### 🧩 Database Schema
```
CREATE TABLE document_chunks (
    chunk_id UUID PRIMARY KEY,
    doc_id TEXT NOT NULL,
    page_number INT,
    title TEXT,
    text TEXT NOT NULL,
    strategy TEXT,
    chunk_index INT,
    token_count INT,
    hash TEXT UNIQUE
);
```
## 🖼️ High‑Level Architecture Diagram

          ┌───────────────┐
          │   PDF Files   │
          └───────┬───────┘
                  │
                  ▼
          ┌───────────────┐
          │  Extraction   │
          │ (PyMuPDF)     │
          └───────┬───────┘
                  │
                  ▼
          ┌───────────────┐
          │   Chunking    │
          │ (Fixed/Overlap│
          │  + Metadata)  │
          └───────┬───────┘
                  │
                  ▼
          ┌───────────────┐
          │   Storage     │
          │ (PostgreSQL   │
          │  via Docker)  │
          └───────────────┘
