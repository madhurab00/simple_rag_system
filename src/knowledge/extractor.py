import os
import sys
from pathlib import Path
import time
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Dict, Any
from dataclasses import dataclass
import json
import logging
from dotenv import load_dotenv

# Setup basic logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
from src.utils import load_config

load_dotenv()


@dataclass
class ExtractedOutput:
    """Structured output extracted from a text chunk.

    Attributes:
        topic (str): Short topic label.
        rule (str): Main extracted rule or fact.
        conditions (list[str]): Conditions where the rule applies.
        exceptions (list[str]): Cases where the rule does not apply.
    """
    topic: str
    rule: str
    conditions: list[str]
    exceptions: list[str]


class Extractor:
    """Extractor class for converting unstructured text into structured rules.

    This class uses an LLM (OpenAI or Gemini) to extract structured
    information from text chunks and store them in a PostgreSQL database.
    """

    def __init__(self, config: Dict[str, Any]):
        """Initialize the Extractor.

        Args:
            config (Dict[str, Any]): Configuration dictionary containing
                model settings and provider details.
        """
        self.config = config
        self.model = self._get_extractor_model(config)

        self.SYSTEM_PROMPT = """
        You are an information extraction assistant.
        A rule is defined as a conditional statement containing a requirement (e.g., 'must', 'can', 'entitled to') and a specific outcome.
        Given a text chunk from a tax guide, extract structured rules if possible only, No need to extract from each chunk and to extract,there must be a structured rule that can be seen.
        Do not create your own rules, If there is no rule skip
        Then summarize and return ONLY valid JSON in this exact format:
        {
        "topic": "short topic label",
        "rule": "the main rule or fact stated",
        "conditions": ["condition 1", "condition 2"],
        "exceptions": ["exception 1"]
        }
        Rules:
        - Keep topic short
        - rule must summarize main rule
        - conditions = when rule applies
        - exceptions = when rule does NOT apply
        - If a field has no relevant content, use an empty list [] or empty string "".
        Do NOT add any explanation. Return ONLY the JSON object.
        """

    def _get_extractor_model(self, config: Dict[str, Any]):
        """Initialize the LLM based on provider.

        Args:
            config (Dict[str, Any]): Configuration dictionary.

        Returns:
            Dict[str, Any]: Model configuration dictionary.

        Raises:
            ValueError: If unsupported provider is specified.
        """
        provider = config["llm_provider"]

        logging.info(f"Initializing extractor model with provider: {provider}")

        if provider == "openai":
            from openai import OpenAI
            client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

            return {
                "provider": "openai",
                "client": client,
                "model": config["extractor_model"]
            }

        elif provider == "gemini":
            import google.generativeai as genai

            genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))

            model = genai.GenerativeModel(config["extractor_model"])

            return {
                "provider": "gemini",
                "model": model
            }

        else:
            raise ValueError(f"Unknown llm_provider: {provider}")

    def _extract(self, text: str) -> ExtractedOutput | None:
        """Extract structured data from text using LLM.

        Args:
            text (str): Input text chunk.

        Returns:
            ExtractedOutput | None: Parsed structured output if successful,
            otherwise None.
        """
        prompt = self.SYSTEM_PROMPT + f"\n\nTEXT:\n{text}"

        try:
            if self.model["provider"] == "openai":
                response = self.model["client"].chat.completions.create(
                    model=self.model["model"],
                    messages=[
                        {"role": "system", "content": self.SYSTEM_PROMPT},
                        {"role": "user", "content": text}
                    ],
                    temperature=0,
                    max_tokens=self.config["max_tokens"]
                )

                content = response.choices[0].message.content.strip()

            elif self.model["provider"] == "gemini":
                response = self.model["model"].generate_content(
                    prompt,
                    generation_config={
                        "temperature": 0,
                        "max_output_tokens": self.config["max_tokens"]
                    }
                )

                content = response.text.strip()

            # Handle ```json blocks
            if content.startswith("```"):
                lines = content.splitlines()
                content = "\n".join(lines[1:-1]).strip()

            data = json.loads(content)

            logging.info("Extraction successful")
            return ExtractedOutput(**data)

        except Exception as e:
            logging.warning(f"Extraction failed: {e}")
            return None

    def store(self, chunk_id: str, data: ExtractedOutput):
        """Store extracted structured data into PostgreSQL.

        Args:
            chunk_id (str): Unique identifier of the chunk.
            data (ExtractedOutput): Extracted structured data.
        """
        try:
            with psycopg2.connect(
                host=os.getenv("POSTGRES_HOST", "localhost"),
                dbname=os.getenv("POSTGRES_DB"),
                user=os.getenv("POSTGRES_USER"),
                password=os.getenv("POSTGRES_PASSWORD"),
                port=os.getenv("POSTGRES_PORT", "5432"),
            ) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO structured_rules
                        (chunk_id, topic, rule, conditions, exceptions)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (
                        chunk_id,
                        data.topic,
                        data.rule,
                        json.dumps(data.conditions),
                        json.dumps(data.exceptions)
                    ))
                conn.commit()

            logging.info(f"Stored extracted rule for chunk_id: {chunk_id}")

        except Exception as e:
            logging.error(f"DB insert failed for chunk {chunk_id}: {e}")

    def process_chunk(self, chunk: Dict[str, Any]) -> bool:
        """Process a single chunk and store extracted data.

        Args:
            chunk (Dict[str, Any]): Chunk dictionary containing text and metadata.

        Returns:
            bool: True if extraction and storage succeeded, else False.
        """
        extracted = self._extract(chunk["text"])

        if extracted:
            self.store(chunk["chunk_id"], extracted)
            return True

        return False

    def process_chunks(self, chunks: list[Dict[str, Any]]):
        """Process multiple chunks sequentially.

        Args:
            chunks (list[Dict[str, Any]]): List of chunk dictionaries.

        Returns:
            int: Number of successfully processed chunks.
        """
        count = 0

        logging.info(f"Starting extraction for {len(chunks)} chunks")

        for chunk in chunks:
            success = self.process_chunk(chunk)
            time.sleep(1)  # rate limiting

            if success:
                count += 1

        logging.info(f"Completed extraction: {count}/{len(chunks)} successful")
        return count


if __name__ == "__main__":
    from src.retrieval.embedder import fetch_chunks

    config = load_config("config/config.yaml")
    extractor = Extractor(config)

    chunks = fetch_chunks(100)

    logging.info("Fetched chunks from database")

    extractor.process_chunks(chunks)

    logging.info("Finished extraction pipeline")