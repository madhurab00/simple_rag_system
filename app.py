import os
import streamlit as st
import sys
from pathlib import Path

# ---------------------------------------------------
# PROJECT PATH
# ---------------------------------------------------
sys.path.append(str(Path(__file__).resolve().parent))

from src.ingestion.document_store import document_exists, generate_document_hash, store_document_metadata
from src.ingestion.ingest import extract_text_from_pdf
from src.utils import load_config
from src.agent.graph import build_graph
from src.ingestion.chunk import chunk_text
from src.ingestion.store import store_chunks_in_db
from src.retrieval.embedder import embed_chunks, fetch_chunks, store_embeddings

# ---------------------------------------------------
# CONFIG
# ---------------------------------------------------
st.set_page_config(
    page_title="Tax Guide Assistant",
    page_icon="🧾",
    layout="wide",
    initial_sidebar_state="expanded",
)

config = load_config("config/config.yaml")

# ---------------------------------------------------
# CUSTOM CSS
# ---------------------------------------------------
st.markdown("""
<style>

@import url('https://fonts.googleapis.com/css2?family=Sora:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

html, body, [class*="css"] {
    font-family: 'Sora', sans-serif;
}

.stApp {
    background-color: #F8F6F1;
}

.block-container {
    padding-top: 100 !important;
    padding-bottom: 5rem;
    max-width: 820px !important;
}

/* ── Sidebar ── */
[data-testid="stSidebar"] {
    background-color: #FFFFFF !important;
    border-right: 1px solid #E8E3DA !important;
}

[data-testid="stSidebar"] > div:first-child {
    padding-top: 2rem;
    padding-left: 1.5rem;
    padding-right: 1.5rem;
}

.sidebar-logo {
    display: flex;
    align-items: center;
    gap: 10px;
    margin-bottom: 2rem;
}

.sidebar-logo-icon {
    width: 38px;
    height: 38px;
    background: linear-gradient(135deg, #B07D2E, #D4A843);
    border-radius: 10px;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 18px;
    flex-shrink: 0;
    box-shadow: 0 2px 8px rgba(176,125,46,0.25);
}

.sidebar-logo-text {
    font-size: 0.95rem;
    font-weight: 700;
    color: #1A1A1A;
    letter-spacing: -0.01em;
}

.sidebar-logo-sub {
    font-size: 0.68rem;
    color: #ADADAD;
    margin-top: 1px;
}

.sidebar-divider {
    border: none;
    border-top: 1px solid #EDE9E1;
    margin: 1.4rem 0;
}

.sidebar-section-label {
    font-size: 0.63rem;
    font-weight: 700;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    color: #C0BAB0;
    margin-bottom: 0.9rem;
}

.upload-description {
    font-size: 0.79rem;
    color: #999999;
    line-height: 1.65;
    margin-bottom: 1rem;
}

.kb-status {
    display: inline-flex;
    align-items: center;
    gap: 7px;
    background: #F0FAF4;
    border: 1px solid #B8E0CA;
    border-radius: 20px;
    padding: 5px 12px;
    font-size: 0.71rem;
    color: #267A4A;
    font-weight: 600;
    margin-bottom: 1.5rem;
}

.kb-dot {
    width: 6px;
    height: 6px;
    border-radius: 50%;
    background: #267A4A;
    animation: pulse 2s infinite;
}

@keyframes pulse {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.3; }
}

[data-testid="stSidebar"] [data-testid="stFileUploader"] {
    background: #FAFAF8 !important;
    border: 1.5px dashed #D6CFC3 !important;
    border-radius: 10px !important;
}

/* ── Header ── */
.chat-header {
    position: sticky;
    top: 0;
    z-index: 100;
    background: linear-gradient(180deg, #F8F6F1 72%, transparent);
    padding: 1.5rem 0 1rem;
    margin-bottom: 0.5rem;
}

.chat-header-inner {
    display: flex;
    align-items: center;
    gap: 14px;
}

.header-icon {
    width: 46px;
    height: 46px;
    background: linear-gradient(135deg, #B07D2E 0%, #D4A843 100%);
    border-radius: 13px;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 22px;
    flex-shrink: 0;
    box-shadow: 0 4px 16px rgba(176,125,46,0.22);
}

.header-title {
    font-size: 1.25rem;
    font-weight: 700;
    color: #1A1A1A;
    letter-spacing: -0.02em;
}

.header-subtitle {
    font-size: 0.76rem;
    color: #BABABA;
    margin-top: 2px;
}

/* ── Welcome ── */
.welcome-wrap {
    padding: 2.5rem 1rem 1.5rem;
    text-align: center;
}

.welcome-hero-icon {
    width: 70px;
    height: 70px;
    background: linear-gradient(135deg, #B07D2E 0%, #D4A843 100%);
    border-radius: 20px;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 34px;
    margin: 0 auto 1.4rem;
    box-shadow: 0 6px 28px rgba(176,125,46,0.2);
}

.welcome-title {
    font-size: 1.85rem;
    font-weight: 700;
    color: #1A1A1A;
    letter-spacing: -0.03em;
    margin-bottom: 0.5rem;
}

.welcome-body {
    font-size: 0.875rem;
    color: #999999;
    max-width: 400px;
    margin: 0 auto 2.2rem;
    line-height: 1.7;
}

/* ── Messages ── */
.msg-row {
    display: flex;
    margin-bottom: 1.1rem;
    gap: 11px;
    align-items: flex-start;
}

.msg-row.user { flex-direction: row-reverse; }

.msg-avatar {
    width: 32px;
    height: 32px;
    border-radius: 10px;
    flex-shrink: 0;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 14px;
    margin-top: 2px;
}

.msg-avatar.ai {
    background: linear-gradient(135deg, #B07D2E, #D4A843);
    box-shadow: 0 2px 8px rgba(176,125,46,0.2);
}

.msg-avatar.human {
    background: #EDEAE4;
    color: #777;
}

.msg-bubble {
    max-width: 80%;
    padding: 0.85rem 1.1rem;
    border-radius: 16px;
    font-size: 0.875rem;
    line-height: 1.65;
}

.msg-bubble.user {
    background: linear-gradient(135deg, #B07D2E, #C89235);
    color: #FFFFFF;
    border-radius: 16px 4px 16px 16px;
    font-weight: 500;
    box-shadow: 0 2px 12px rgba(176,125,46,0.2);
}

.msg-bubble.bot {
    background: #FFFFFF;
    color: #2A2A2A;
    border: 1px solid #E8E3DA;
    border-radius: 4px 16px 16px 16px;
    box-shadow: 0 1px 6px rgba(0,0,0,0.05);
}

/* Thinking dots */
.thinking-row {
    display: flex;
    align-items: center;
    gap: 11px;
    margin-bottom: 1.1rem;
}

.thinking-avatar {
    width: 32px;
    height: 32px;
    border-radius: 10px;
    background: linear-gradient(135deg, #B07D2E, #D4A843);
    flex-shrink: 0;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 14px;
    box-shadow: 0 2px 8px rgba(176,125,46,0.2);
}

.thinking-dots {
    display: flex;
    gap: 5px;
    align-items: center;
    padding: 0.72rem 1rem;
    background: #FFFFFF;
    border: 1px solid #E8E3DA;
    border-radius: 4px 16px 16px 16px;
    box-shadow: 0 1px 6px rgba(0,0,0,0.05);
}

.thinking-dots span {
    width: 7px;
    height: 7px;
    border-radius: 50%;
    background: #C89235;
    animation: bounce 1.3s infinite;
}

.thinking-dots span:nth-child(2) { animation-delay: 0.2s; }
.thinking-dots span:nth-child(3) { animation-delay: 0.4s; }

@keyframes bounce {
    0%, 80%, 100% { transform: translateY(0); opacity: 0.35; }
    40% { transform: translateY(-5px); opacity: 1; }
}

/* ── References expander ── */
[data-testid="stExpander"] {
    background: #FFFFFF !important;
    border: 1px solid #E8E3DA !important;
    border-radius: 12px !important;
    margin-top: -0.4rem;
    margin-bottom: 1.1rem;
    margin-left: 43px;
    box-shadow: 0 1px 4px rgba(0,0,0,0.04) !important;
}

[data-testid="stExpander"] summary {
    font-size: 0.77rem !important;
    color: #BABABA !important;
    font-weight: 600 !important;
}

.source-card {
    background: #FAFAF8;
    border-radius: 10px;
    padding: 0.8rem 1rem;
    margin-bottom: 0.6rem;
    border-left: 3px solid #C89235;
}

.source-title {
    font-size: 0.78rem;
    font-weight: 700;
    color: #B07D2E;
    margin-bottom: 0.25rem;
}

.source-meta {
    font-size: 0.68rem;
    color: #C0BAB0;
    margin-bottom: 0.4rem;
    font-family: 'JetBrains Mono', monospace;
}

.source-content {
    font-size: 0.75rem;
    color: #888888;
    line-height: 1.55;
}

/* ── Chat input ── */
[data-testid="stChatInput"] {
    background: #FFFFFF !important;
    border: 1.5px solid #E0DAD0 !important;
    border-radius: 14px !important;
    color: #1A1A1A !important;
    box-shadow: 0 2px 12px rgba(0,0,0,0.06) !important;
}

[data-testid="stChatInput"]:focus-within {
    border-color: #B07D2E !important;
    box-shadow: 0 0 0 3px rgba(176,125,46,0.1), 0 2px 12px rgba(0,0,0,0.06) !important;
}

/* ── Suggestion buttons ── */
.stButton > button {
    background: #FFFFFF !important;
    border: 1.5px solid #E8E3DA !important;
    color: #444444 !important;
    border-radius: 12px !important;
    font-family: 'Sora', sans-serif !important;
    font-size: 0.8rem !important;
    font-weight: 500 !important;
    padding: 0.65rem 0.9rem !important;
    text-align: left !important;
    transition: all 0.18s ease !important;
    box-shadow: 0 1px 3px rgba(0,0,0,0.05) !important;
    height: auto !important;
    min-height: 56px !important;
}

.stButton > button:hover {
    border-color: #B07D2E !important;
    color: #B07D2E !important;
    background: #FEF9F2 !important;
    box-shadow: 0 2px 10px rgba(176,125,46,0.1) !important;
}

/* Sidebar clear button override */
[data-testid="stSidebar"] .stButton > button {
    background: #FAFAF8 !important;
    border: 1.5px solid #E8E3DA !important;
    color: #ADADAD !important;
    font-size: 0.78rem !important;
    min-height: 40px !important;
    text-align: center !important;
}

[data-testid="stSidebar"] .stButton > button:hover {
    border-color: #E06060 !important;
    color: #CC4444 !important;
    background: #FFF5F5 !important;
}

hr { border-color: #EDE9E1 !important; }

::-webkit-scrollbar { width: 4px; }
::-webkit-scrollbar-track { background: transparent; }
::-webkit-scrollbar-thumb { background: #DEDAD4; border-radius: 10px; }

</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------
# SESSION STATE
# ---------------------------------------------------
if "messages" not in st.session_state:
    st.session_state.messages = []

if "send_query" not in st.session_state:
    st.session_state.send_query = None

# ---------------------------------------------------
# LOAD GRAPH
# ---------------------------------------------------
@st.cache_resource(show_spinner=False)
def load_graph():
    return build_graph()

graph = load_graph()

# ---------------------------------------------------
# UPLOAD DIRECTORY
# ---------------------------------------------------
UPLOAD_DIR = Path("./data/uploads")
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

# ===================================================
# SIDEBAR
# ===================================================
with st.sidebar:

    st.markdown("""
    <div class="sidebar-logo">
        <div class="sidebar-logo-icon">🧾</div>
        <div>
            <div class="sidebar-logo-text">Tax Assistant</div>
            <div class="sidebar-logo-sub">Powered by Deep AI Lab</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("""
    <div class="kb-status">
        <div class="kb-dot"></div>
        Internal knowledge base active
    </div>
    """, unsafe_allow_html=True)

    st.markdown('<div class="sidebar-section-label">About</div>', unsafe_allow_html=True)
    st.markdown("""
    <div class="upload-description">
        Ask anything about <strong style="color:#B07D2E">ATO tax rules</strong>,
        deductions, work-related expenses, and compliance —
        all powered by our internal knowledge base.
    </div>
    """, unsafe_allow_html=True)

    st.markdown('<hr class="sidebar-divider">', unsafe_allow_html=True)

    st.markdown('<div class="sidebar-section-label">Optional · Upload Your Document</div>', unsafe_allow_html=True)
    st.markdown("""
    <div class="upload-description">
        Have a specific tax document or ruling? Upload it to ask
        questions directly against its content.
    </div>
    """, unsafe_allow_html=True)

    uploaded_file = st.file_uploader(
        "Upload PDF",
        type=["pdf"],
        label_visibility="collapsed",
    )

    if uploaded_file is not None:
        try:
            with st.status("Processing PDF…", expanded=True) as status:

                status.write("💾 Saving file…")
                file_path = UPLOAD_DIR / uploaded_file.name
                with open(file_path, "wb") as f:
                    f.write(uploaded_file.getbuffer())

                status.write("📖 Extracting text…")
                documents = extract_text_from_pdf(str(file_path))
                full_text = " ".join(doc["content"] for doc in documents)

                status.write("🔑 Checking for duplicates…")
                hashed_document = generate_document_hash(full_text)

                if not document_exists(hashed_document):
                    status.write("🗂 Storing metadata…")
                    store_document_metadata(str(file_path), hashed_document)

                    status.write("✂️ Chunking document…")
                    chunks = chunk_text(documents)

                    status.write("💽 Storing chunks…")
                    store_chunks_in_db(chunks)

                    status.write("🧠 Generating embeddings…")
                    cfg = load_config("config/config.yaml")
                    chunks      = fetch_chunks()
                    chunk_ids   = [c["chunk_id"] for c in chunks]
                    chunk_texts = [c["text"]     for c in chunks]
                    embeddings  = embed_chunks(chunk_texts, cfg)

                    status.write("📦 Storing embeddings…")
                    store_embeddings(chunk_ids, embeddings)

                    status.update(label="✅ Document indexed", state="complete")
                    st.success(f"**{uploaded_file.name}** indexed successfully.")
                else:
                    status.update(label="ℹ️ Already indexed", state="complete")
                    st.info("This document is already in the knowledge base.")

        except Exception as e:
            st.error(f"Upload error: {str(e)}")

    st.markdown('<hr class="sidebar-divider">', unsafe_allow_html=True)

    if st.button("🗑  Clear conversation", use_container_width=True):
        st.session_state.messages = []
        st.rerun()

# ===================================================
# MAIN CONTENT
# ===================================================

st.markdown("""
<div class="chat-header">
    <div class="chat-header-inner">
        <div class="header-icon">🧾</div>
        <div>
            <div class="header-title">Tax Guide Assistant</div>
            <div class="header-subtitle">ATO rules · Deductions · Compliance · Work expenses</div>
        </div>
    </div>
</div>
""", unsafe_allow_html=True)

# ---------------------------------------------------
# WELCOME / EMPTY STATE
# ---------------------------------------------------
SUGGESTIONS = [
    ("💼", "What work-from-home expenses can I claim?"),
    ("🚗", "Can I deduct car travel for work?"),
    ("📚", "Are self-education costs tax deductible?"),
    ("🧾", "What records do I need to keep for the ATO?"),
]

if len(st.session_state.messages) == 0:

    st.markdown("""
    <div class="welcome-wrap">
        <div class="welcome-hero-icon">🧾</div>
        <div class="welcome-title">How can I help you today?</div>
        <div class="welcome-body">
            Ask me about deductions, ATO guidelines, work expenses, or
            upload your own tax document for specific questions.
        </div>
    </div>
    """, unsafe_allow_html=True)

    cols = st.columns(2)
    for idx, (emoji, text) in enumerate(SUGGESTIONS):
        with cols[idx % 2]:
            if st.button(f"{emoji}  {text}", key=f"chip_{idx}", use_container_width=True):
                st.session_state.send_query = text
                st.rerun()

    st.markdown("---")

# ---------------------------------------------------
# RENDER CHAT HISTORY
# ---------------------------------------------------
for message in st.session_state.messages:

    if message["role"] == "user":
        st.markdown(f"""
        <div class="msg-row user">
            <div class="msg-avatar human">👤</div>
            <div class="msg-bubble user">{message["content"]}</div>
        </div>
        """, unsafe_allow_html=True)

    else:
        st.markdown(f"""
        <div class="msg-row">
            <div class="msg-avatar ai">🧾</div>
            <div class="msg-bubble bot">{message["content"]}</div>
        </div>
        """, unsafe_allow_html=True)

        if message.get("sources"):
            with st.expander(f"📚 {len(message['sources'])} source(s) referenced"):
                for i, source in enumerate(message["sources"], 1):
                    if isinstance(source, dict):
                        st.markdown(f"""
                        <div class="source-card">
                            <div class="source-title">{source.get("title", "Unknown Document")}</div>
                            <div class="source-meta">Page {source.get("page_number", "—")}</div>
                            <div class="source-content">{source.get("content", "")[:450]}…</div>
                        </div>
                        """, unsafe_allow_html=True)
                    else:
                        st.write(source)

# ---------------------------------------------------
# CHAT INPUT
# ---------------------------------------------------
user_query = st.chat_input("Ask a tax question…")

if st.session_state.send_query:
    user_query = st.session_state.send_query
    st.session_state.send_query = None

# ---------------------------------------------------
# HANDLE QUERY
# ---------------------------------------------------
if user_query:

    st.session_state.messages.append({"role": "user", "content": user_query})

    st.markdown(f"""
    <div class="msg-row user">
        <div class="msg-avatar human">👤</div>
        <div class="msg-bubble user">{user_query}</div>
    </div>
    """, unsafe_allow_html=True)

    thinking_slot = st.empty()
    answer_slot   = st.empty()

    thinking_slot.markdown("""
    <div class="thinking-row">
        <div class="thinking-avatar">🧾</div>
        <div class="thinking-dots">
            <span></span><span></span><span></span>
        </div>
    </div>
    """, unsafe_allow_html=True)

    full_answer = ""
    final_state = {"context": [], "rules": [], "route": ""}

    try:
        for step in graph.stream({
            "query": user_query,
            "route": "",
            "context": [],
            "rules": [],
            "answer": "",
        }):
            for node_name, node_output in step.items():

                if isinstance(node_output, dict):

                    if "context" in node_output:
                        final_state["context"] = node_output["context"]

                    if "rules" in node_output:
                        final_state["rules"] = node_output["rules"]

                    if "route" in node_output:
                        final_state["route"] = node_output["route"]

            if "answer" in step:
                token = step["answer"].get("answer", "")
                if token:
                    full_answer += token
                    thinking_slot.empty()
                    answer_slot.markdown(f"""
                    <div class="msg-row">
                        <div class="msg-avatar ai">🧾</div>
                        <div class="msg-bubble bot">{full_answer}</div>
                    </div>
                    """, unsafe_allow_html=True)

        route   = final_state.get("route", "retrieval")
        sources = (
            final_state.get("context", [])
            if route == "retrieval"
            else final_state.get("rules", [])
        )
        print('1'*20)
        print(sources)
        if sources:
            print(sources)
            with st.expander(f"📚 {len(sources)} source(s) referenced"):
                for i, source in enumerate(sources, 1):
                    if isinstance(source, dict):
                        st.markdown(f"""
                        <div class="source-card">
                            <div class="source-title">{source.get("title", "Unknown Document")}</div>
                            <div class="source-meta">Page {source.get("page_number", "—")}</div>
                            <div class="source-content">{source.get("content", "")[:450]}…</div>
                        </div>
                        """, unsafe_allow_html=True)
                    else:
                        st.write(source)

        st.session_state.messages.append({
            "role": "assistant",
            "content": full_answer,
            "sources": sources,
        })

    except Exception as e:
        thinking_slot.empty()
        st.error(f"Something went wrong: {str(e)}")