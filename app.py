import os
import io
import json
import streamlit as st
import pandas as pd
import snowflake.connector
from PyPDF2 import PdfReader

# ----------------------------------------------------------------------
# Page config
# ----------------------------------------------------------------------
st.set_page_config(
    page_title="AI Resume Analytics",
    page_icon="📄",
    layout="wide",
)

# ----------------------------------------------------------------------
# Snowflake connection
# ----------------------------------------------------------------------
@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database="RESUME_ANALYTICS",
        schema="PUBLIC",
        client_session_keep_alive=True,
    )

def run_query(sql, params=None):
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(sql, params or [])
        cols = [c[0] for c in cur.description] if cur.description else []
        rows = cur.fetchall()
        return pd.DataFrame(rows, columns=cols)
    finally:
        cur.close()

def call_analyze(resume_text, file_name):
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "CALL RESUME_ANALYTICS.PUBLIC.ANALYZE_RESUME(%s, %s)",
            (resume_text, file_name),
        )
        row = cur.fetchone()
        raw = row[0] if row else None
        return json.loads(raw) if isinstance(raw, str) else raw
    finally:
        cur.close()

# 🔥 FIXED PDF FUNCTION
def extract_pdf_text(file_bytes: bytes) -> str:
    try:
        reader = PdfReader(io.BytesIO(file_bytes))
        text = ""

        for page in reader.pages:
            page_text = page.extract_text()
            if page_text is not None:
                text += str(page_text) + "\n"

        return text if text else ""
    except Exception as e:
        return ""

# ----------------------------------------------------------------------
# UI
# ----------------------------------------------------------------------
st.title("📄 AI Resume Analytics & Interview Insights")
st.caption("Powered by Snowflake Cortex AI")

tab_analyze, tab_past, tab_dash = st.tabs(
    ["🔍 Analyze Resume", "📋 Past Analyses", "📊 Dashboard"]
)

# ---------------- TAB 1: Analyze ----------------
with tab_analyze:
    st.subheader("Analyze a new resume")
    mode = st.radio("Input method", ["Paste text", "Upload PDF"], horizontal=True)

    resume_text = ""
    file_name = ""

    if mode == "Paste text":
        file_name = st.text_input("Candidate / file name", value="resume.txt")
        resume_text = st.text_area("Paste resume text here", height=300)
    else:
        uploaded = st.file_uploader("Upload PDF resume", type=["pdf"])
        if uploaded is not None:
            file_name = uploaded.name
            try:
                resume_text = extract_pdf_text(uploaded.getvalue())

                if resume_text:
                    st.success(f"Extracted {len(resume_text)} characters from {uploaded.name}")
                    with st.expander("Preview extracted text"):
                        st.text(resume_text[:2000] + ("..." if len(resume_text) > 2000 else ""))
                else:
                    st.warning("No readable text found in PDF")

            except Exception as e:
                st.error(f"PDF parse error: {e}")

    if st.button("🚀 Analyze Resume", type="primary"):
        if not resume_text or not str(resume_text).strip():
        st.warning("Please provide resume text")
    else:
        with st.spinner("AI is analyzing..."):
            try:
                result = call_analyze(resume_text, file_name)

                if result is None:
                    result = {}

                st.session_state["last_analysis"] = result
                st.success("Analysis complete!")

            except Exception as e:
                st.error(f"Error: {e}")
    # Render results
    if "last_analysis" in st.session_state:
    a = st.session_state["last_analysis"]

    if not a or not isinstance(a, dict):
        st.error("No valid analysis data received")
    else:
        if isinstance(a, str):
            a = json.loads(a)

        st.divider()
        c1, c2, c3 = st.columns(3)
        c1.metric("Candidate", a.get("name", "N/A"))
        c2.metric("Experience", a.get("experience_level", "N/A"))
        c3.metric("Skills found", len(a.get("skills", [])))

        st.subheader("🎯 Recommended Roles")
        for r in a.get("recommended_roles", []):
            score_str = str(r.get("match_score", "0")).replace("%", "").strip()
            try:
                score = int(float(score_str))
            except:
                score = 0
            st.progress(min(max(score, 0), 100) / 100,
                        text=f"**{r.get('role')}** — {r.get('match_score')}")

        col_a, col_b = st.columns(2)
        with col_a:
            st.subheader("💪 Strengths")
            for s in a.get("strengths", []):
                st.write(f"- {s}")
            st.subheader("🛠️ Skills")
            st.write(", ".join(a.get("skills", [])))
            st.subheader("🏅 Certifications")
            for c in a.get("certifications", []):
                st.write(f"- {c}")
        with col_b:
            st.subheader("⚠️ Weaknesses / Improvements")
            for w in a.get("weaknesses", []):
                st.write(f"- {w}")
            st.subheader("📂 Key Projects")
            for p in a.get("key_projects", []):
                st.write(f"- {p}")

        st.subheader("❓ Interview Questions")
        q1, q2 = st.columns(2)
        with q1:
            st.markdown("**Technical**")
            for q in a.get("interview_questions", {}).get("technical", []):
                st.write(f"- {q}")
        with q2:
            st.markdown("**HR**")
            for q in a.get("interview_questions", {}).get("hr", []):
                st.write(f"- {q}")

        st.subheader("📝 Final Evaluation Summary")
        st.info(a.get("summary", ""))

        with st.expander("🔎 Raw JSON"):
            st.json(a)

# ---------------- TAB 2 ----------------
with tab_past:
    st.subheader("Past resume analyses")
    try:
        df = run_query("""
            SELECT analyzed_at, candidate_name, experience_level, file_name, summary
            FROM RESUME_ANALYTICS.PUBLIC.RESUME_ANALYSIS
            ORDER BY analyzed_at DESC
            LIMIT 100
        """)
        st.dataframe(df, use_container_width=True)
    except Exception as e:
        st.error(f"Error loading past analyses: {e}")

# ---------------- TAB 3 ----------------
with tab_dash:
    st.subheader("Candidate analytics")
    try:
        total_df = run_query("SELECT COUNT(*) AS C FROM RESUME_ANALYTICS.PUBLIC.RESUME_ANALYSIS")
        total = int(total_df.iloc[0, 0])
        st.metric("Total resumes analyzed", total)

        if total > 0:
            exp_df = run_query("""
                SELECT experience_level, COUNT(*) AS cnt
                FROM RESUME_ANALYTICS.PUBLIC.RESUME_ANALYSIS
                GROUP BY experience_level
            """)
            st.bar_chart(exp_df.set_index("EXPERIENCE_LEVEL"))
    except Exception as e:
        st.error(f"Dashboard error: {e}")
