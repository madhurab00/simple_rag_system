import pdfplumber
from pathlib import Path
from typing import Dict,List,Any
import re

def extract_text_from_pdf(file_path: str) -> List[Dict[str, Any]]:
    """Extract raw text from a PDF using pdfplumber."""
    path = Path(file_path)
    title = path.name
    if not path.exists():
        raise ValueError(f"Path not found: {file_path}")
    
    extracted = []
    with pdfplumber.open(file_path) as pdf:
        for page_number, page in enumerate(pdf.pages, start=1):
            text = page.extract_text()
            if text:
                text = _clean_text(text)
                extracted.append({
                    "title": title,
                    "page": page_number,
                    "content": text
                })
    return extracted


def _clean_text(text: str) -> str:
    """Basic cleaning for extracted PDF text."""
    if not text:
        return ""
    text = text.replace("\n", " ").replace("\r", " ").replace("\t", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


if __name__ =='__main__':
    from pprint import pprint

    pdf_path = "data/pdfs/TaxTimeToolkit_IT professional.pdf"

    try:
        results = extract_text_from_pdf(pdf_path)
        pprint(results[:2]) 
        print(f"Total pages extracted: {len(results)}")
    except Exception as e:
        print(f"Error: {e}")


