import streamlit as st
import requests
import pandas as pd

API_URL = "http://localhost:8000/ask"

st.set_page_config(page_title="Flight Delay Data Agent", page_icon="✈️", layout="centered")

# ---------- Styling ----------
st.markdown("""
<style>
    .stApp {
        background-color: #0e1117;
    }
    .main-title {
        text-align: center;
        font-size: 2.2rem;
        font-weight: 700;
        margin-bottom: 0.2rem;
    }
    .main-subtitle {
        text-align: center;
        color: #9aa0a6;
        margin-bottom: 2rem;
        font-size: 1rem;
    }
    .example-chip {
        display: inline-block;
        background-color: #1c1f26;
        border: 1px solid #2d313a;
        border-radius: 20px;
        padding: 8px 16px;
        margin: 4px;
        font-size: 0.85rem;
        color: #c9ccd1;
    }
    div[data-testid="stChatMessage"] {
        padding: 0.75rem 1rem;
        border-radius: 12px;
        margin-bottom: 0.5rem;
    }
</style>
""", unsafe_allow_html=True)

# ---------- Session state ----------
if "messages" not in st.session_state:
    st.session_state.messages = []  # list of {role, content, sql?, df?}


def call_agent(question: str):
    try:
        response = requests.post(API_URL, json={"question": question}, timeout=60)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        return {"error": f"Couldn't reach the agent service: {e}"}


def render_assistant_message(msg: dict):
    if "error" in msg:
        st.error(msg["error"])
        if msg.get("sql"):
            with st.expander("Generated SQL"):
                st.code(msg["sql"], language="sql")
        return

    st.markdown(msg["content"])

    if msg.get("sql"):
        with st.expander("View generated SQL"):
            st.code(msg["sql"], language="sql")

    if msg.get("rows") and msg.get("columns"):
        df = pd.DataFrame(msg["rows"], columns=msg["columns"])
        st.dataframe(df, use_container_width=True)


# ---------- Empty state: centered welcome ----------
if not st.session_state.messages:
    st.markdown('<div class="main-title">✈️ Flight Delay Data Agent</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="main-subtitle">Ask anything about 2025 US flight delay data — in plain English.</div>',
        unsafe_allow_html=True
    )
    st.markdown(
        """
        <div style="text-align:center;">
            <span class="example-chip">Which carrier had the most delays in March?</span>
            <span class="example-chip">Top 5 most delayed routes</span>
            <span class="example-chip">Average delay for Delta in 2025</span>
        </div>
        """,
        unsafe_allow_html=True
    )
else:
    # ---------- Chat history ----------
    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            if msg["role"] == "assistant":
                render_assistant_message(msg)
            else:
                st.markdown(msg["content"])

# ---------- Input (always pinned to bottom by Streamlit) ----------
question = st.chat_input("Ask a question about flight delays...")

if question:
    st.session_state.messages.append({"role": "user", "content": question})

    with st.spinner("Generating and running query..."):
        result = call_agent(question)

    if "error" in result:
        st.session_state.messages.append({
            "role": "assistant",
            "error": result["error"],
            "sql": result.get("sql")
        })
    else:
        st.session_state.messages.append({
            "role": "assistant",
            "content": result.get("summary", "Here's what I found:"),
            "sql": result.get("sql"),
            "columns": result.get("columns"),
            "rows": result.get("rows"),
        })

    st.rerun()