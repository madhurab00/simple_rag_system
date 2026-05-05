import os
import sys
from pathlib import Path
import time
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Dict,Any
from dataclasses import dataclass
import json
from dotenv import load_dotenv

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
from src.utils import load_config

load_dotenv()

@dataclass
class ExtractedOutput:
    topic: str
    rule : str
    conditions: list[str]
    exceptions: list[str]

class Extractor:

    def __init__(self,config:Dict[str, Any]):
        self.config = config
        self.model = self._get_extractor_model(config)
        self.SYSTEM_PROMPT = """
                                You are an information extraction assistant. Given a text chunk from a tax guide,
                                extract structured information then summarize and return ONLY valid JSON in this exact format:
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
        provider = config["llm_provider"]

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
            return ExtractedOutput(**data)

        except Exception as e:
            print(f"[WARN] Extraction failed: {e}")
            return None
        
    def store(self, chunk_id: str, data: ExtractedOutput):
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

    def process_chunk(self, chunk: Dict[str, Any]) -> bool:
        extracted = self._extract(chunk["text"])

        if extracted :
            self.store(chunk["chunk_id"], extracted)
            return True

        return False
    
    def process_chunks(self, chunks: list[Dict[str, Any]]):
        count = 0

        for chunk in chunks:
            success = self.process_chunk(chunk)
            time.sleep(1)
            if success:
                count += 1
        print(f"Completed. Successful extractions: {count}/{len(chunks)}")
        return count
        
if __name__ == "__main__":
    from src.retrieval.embedder import fetch_chunks

    config = load_config("config/config.yaml")
    extractor = Extractor(config)

    chunks = fetch_chunks(100)
    extractor.process_chunks(chunks)
    print("finished")