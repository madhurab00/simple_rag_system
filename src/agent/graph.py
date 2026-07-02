import os
import sys
import logging
from pathlib import Path
from typing import TypedDict, Dict, List, Any

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

from langgraph.graph import StateGraph, START, END

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from src.utils import load_config
from src.retrieval.retriever import Retriever

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)

config = load_config("config/config.yaml")


def initialize_llm():
    """
    Initialize LLM based on configuration.

    Returns:
        LLM instance.
    """
    provider = config["llm_provider"]

    logger.info("Initializing LLM provider: %s", provider)

    if provider == "openai":
        from langchain_openai import ChatOpenAI

        return ChatOpenAI(
            model=config["llm_model"],
            temperature=config["temperature"],
            max_tokens=config["max_tokens"],
            streaming=True,
        )

    elif provider == "gemini":
        from langchain_google_genai import ChatGoogleGenerativeAI

        return ChatGoogleGenerativeAI(
            model=config["llm_model"],
            temperature=config["temperature"],
            max_output_tokens=config["max_tokens"],
        )

    else:
        raise ValueError(f"Unsupported LLM provider: {provider}")


llm = initialize_llm()

class AgentState(TypedDict):
    """
    State schema for LangGraph pipeline.
    """

    query: str
    route: str
    context: List[Dict[str, Any]]
    rules: List[str]
    answer: str

def get_db_connection():
    """
    Create PostgreSQL connection.

    Returns:
        psycopg2.connection: Active DB connection.
    """
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        port=os.getenv("POSTGRES_PORT", "5432"),
    )


def get_available_topics() -> List[str]:
    """
    Fetch unique topics from structured_rules table.

    Returns:
        List[str]: Available rule topics.
    """
    logger.info("Fetching available topics")

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT topic FROM structured_rules")
            topics = [row[0] for row in cur.fetchall()]

    logger.info("Fetched %d topics", len(topics))

    return topics


# Router Node
def router_node(state: AgentState) -> AgentState:
    """
    Decide whether query should use retrieval or rules.

    Args:
        state (AgentState): Current graph state.

    Returns:
        AgentState: Updated state with selected route.
    """
    logger.info("Starting routing decision")

    query = state["query"].lower()
    topics = get_available_topics()

    topics_text = "\n".join(f"- {topic}" for topic in topics)

    prompt = f"""
You are a routing assistant.

Your task is to decide whether the query should use:

- retrieval
- rules

QUERY:
{query}

AVAILABLE TOPICS:
{topics_text}

Return ONLY one word:
retrieval OR rules
"""

    response = llm.invoke(prompt)

    decision = response.content.strip().lower()

    if decision not in ["retrieval", "rules"]:
        logger.warning("Invalid route returned. Defaulting to retrieval.")
        decision = "retrieval"

    logger.info("Selected route: %s", decision)

    state["route"] = decision

    return state


# Route Decision
def route_decision(state: AgentState) -> str:
    """
    Return route name for conditional edges.

    Args:
        state (AgentState): Current state.

    Returns:
        str: Route value.
    """
    return state["route"]


# Structured Rules Node
def structured_rules_node(state: AgentState) -> AgentState:
    """
    Retrieve structured rules from database.

    Args:
        state (AgentState): Current state.

    Returns:
        AgentState: Updated state with rules.
    """
    logger.info("Fetching structured rules")

    query = state["query"]

    stop_words = {
        "what", "are", "the", "is", "a", "an",
        "for", "in", "of", "do", "i", "can",
        "how", "does", "to", "my"
    }

    keywords = [
        word for word in query.lower().split()
        if word not in stop_words and len(word) > 2
    ]

    if not keywords:
        logger.warning("No valid keywords extracted")
        state["rules"] = []
        return state

    conditions = " OR ".join(
        ["topic ILIKE %s OR rule ILIKE %s" for _ in keywords]
    )

    params = [
        value
        for keyword in keywords
        for value in (f"%{keyword}%", f"%{keyword}%")
    ]

    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT
                    topic,
                    rule,
                    conditions,
                    exceptions
                FROM structured_rules
                WHERE {conditions}
                LIMIT 10
                """,
                params,
            )

            rows = cur.fetchall()

    formatted_rules = [
        (
            f"Topic: {row['topic']}\n"
            f"Rule: {row['rule']}\n"
            f"Conditions: {row['conditions']}\n"
            f"Exceptions: {row['exceptions']}"
        )
        for row in rows
    ]

    state["rules"] = formatted_rules

    logger.info("Retrieved %d structured rules", len(formatted_rules))

    return state


# Retrieval Node
def retrieve_node(state: AgentState) -> AgentState:
    """
    Retrieve relevant chunks using vector search.

    Args:
        state (AgentState): Current state.

    Returns:
        AgentState: Updated state with retrieved context.
    """
    logger.info("Running vector retrieval")

    retriever = Retriever(config)

    query = state["query"]

    chunks = retriever.retrieve(query)

    state["context"] = chunks

    logger.info("Retrieved %d chunks", len(chunks))

    return state


# Answer Generation Node
def answer_node(state: AgentState):
    """
    Generate streamed answer using LLM.

    Args:
        state (AgentState): Current state.

    Yields:
        Dict[str, str]: Streamed tokens and final answer.
    """
    logger.info("Starting answer generation")

    query = state["query"]

    context = (
        state["context"]
        if state["route"] == "retrieval"
        else state["rules"]
    )

    prompt = f"""
                ROLE
                You are an expert Australian Tax Assistant. Your goal is to provide accurate, concise, and helpful answers based ONLY on the provided tax documentation.

                GUIDELINES
                1. **Source Grounding:** Answer the question using ONLY the provided CONTEXT. If the answer is not in the context, state that you do not have enough information.
                2. **Precision:** Tax rules are specific. Mention specific dollar amounts, thresholds, and dates exactly as they appear (e.g., "$18,200" or "88c per km").
                3. **No Legal Advice Disclaimer:** for related queries,include a brief note at the end suggesting the user consult with a registered tax professional or visit ato.gov.au.
                4. **Irrelevant contexts:** Do not answer not related questions
                CONTEXT:
                {context}

                QUESTION:
                {query}

                ANSWER
                """

    full_answer = ""

    for chunk in llm.stream(prompt):
        token = chunk.content or ""

        full_answer += token

        yield {"answer": token}

    logger.info("Answer streaming completed")

    yield {"answer": full_answer}


# Graph Builder
def build_graph():
    """
    Build and compile LangGraph workflow.

    Returns:
        Compiled LangGraph instance.
    """
    logger.info("Building LangGraph pipeline")

    builder = StateGraph(AgentState)

    # nodes
    builder.add_node("router", router_node)
    builder.add_node("structured_rules", structured_rules_node)
    builder.add_node("retrieve_chunks", retrieve_node)
    builder.add_node("answer", answer_node)

    # edges
    builder.add_edge(START, "router")

    builder.add_conditional_edges(
        "router",
        route_decision,
        {
            "retrieval": "retrieve_chunks",
            "rules": "structured_rules",
        },
    )

    builder.add_edge("structured_rules", "answer")
    builder.add_edge("retrieve_chunks", "answer")

    builder.add_edge("answer", END)

    logger.info("Graph compilation completed")

    return builder.compile()


if __name__ == "__main__":
    logger.info("Starting graph execution")

    graph = build_graph()

    initial_state = {
        "query": "What is the cents per kilometre rate for work-related car expenses",
        "route": "",
        "context": [],
        "rules": [],
        "answer": "",
    }

    for event in graph.stream(initial_state):
        if "answer" in event:
            print(event["answer"], end="", flush=True)

    print("\nDone.")