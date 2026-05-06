import re
import logging
from pathlib import Path
from typing import Dict, List, Any

import pdfplumber

# Configure logger 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def extract_text_from_pdf(file_path: str) -> List[Dict[str, Any]]:
    """Extract cleaned text content from a PDF file.

    This reads a PDF file using pdfplumber,cleans the extracted text, 
    and returns structured data including
    the document title, page number, and content.

    Args:
        file_path (str): Path to the PDF file.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries contains:
            - "title" (str): Name of the PDF file
            - "page" (int): Page number 
            - "content" (str): Cleaned text content of the page
    Raises:
        ValueError: If the provided file path does not exist.
        Exception: If PDF reading fails unexpectedly.
    """
    path = Path(file_path)
    title = path.name

    if not path.exists():
        logger.error(f"File not found: {file_path}")
        raise ValueError(f"Path not found: {file_path}")

    logger.info(f"Starting PDF extraction: {title}")

    extracted: List[Dict[str, Any]] = []

    try:
        with pdfplumber.open(path) as pdf:
            logger.info(f"Total pages detected: {len(pdf.pages)}")

            for page_number, page in enumerate(pdf.pages, start=1):
                text = page.extract_text()

                if text:
                    cleaned_text = _clean_text(text)

                    extracted.append(
                        {
                            "title": title,
                            "page": page_number,
                            "content": cleaned_text,
                        }
                    )
                else:
                    logger.debug(f"No text found on page {page_number}")

    except Exception as e:
        logger.exception(f"Failed to process PDF: {title}")
        raise
    logger.info(f"Extraction completed: {title} | Pages extracted: {len(extracted)}")
    return extracted


def _clean_text(text: str) -> str:
    """Clean raw text extracted from a PDF.

    This normalizes whitespace and removes unnecessary
    newline, tab, and carriage return characters.

    Args:
        text (str): Raw extracted text.

    Returns:
        str: Cleaned and normalized text.
    """
    if not text:
        return ""

    text = text.replace("\n", " ").replace("\r", " ").replace("\t", " ")
    text = re.sub(r"\s+", " ", text)

    return text.strip()


if __name__ == "__main__":
    from pprint import pprint

    pdf_path = "data/pdfs/TaxTimeToolkit_IT professional.pdf"

    try:
        results = extract_text_from_pdf(pdf_path)

        pprint(results[:2])
        print(f"Total pages extracted: {len(results)}")

    except Exception as e:
        print(f"Error: {e}")