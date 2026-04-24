import os
import io
import json
import streamlit as st
import pandas as pd
import snowflake.connector
import pdfplumber

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
        # Use dollar-quoting to safely pass large text to Snowflake
        safe_text = str(resume_text).replace("$$", "")
        safe_name = str(file_name).replace("$$", "")
        cur.execute(
            f"CALL RESUME_ANALYTICS.PUBLIC.ANALYZE_RESUME($${safe_text}$$, $${safe_name}$$)"
        )
        row = cur.fetchone()

        if not row or row[0] is None:
            return {}

        raw = row[0]

        if isinstance(raw, dict):
            return raw

        if isinstance(raw, str):
            raw = raw.strip()
            if not raw:
                return {}
            # Strip markdown code fences if Cortex wraps response in backticks
            if raw.startswith("```"):
                parts = raw.split("```")
                raw = parts[1] if len(parts) > 1 else raw
                if raw.lower().startswith("json"):
                    raw = raw[4:].strip()
            return json.loads(raw)

        return {}

    except json.JSONDecodeError as e:
        st.error(f"Failed to parse AI response as JSON: {e}")
        return {}
    except Exception as e:
        st.error(f"Snowflake error: {e}")
        return {}
    finally:
        cur.close()


# ----------------------------------------------------------------------
# PDF extraction — pdfplumber (no NoneType / .find() errors)
# ----------------------------------------------------------------------
def extract_pdf_text(file_bytes: bytes) -> str:
    pages_text = []
    try:
        with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
            for page in pdf.pages:
                try:
                    text = page.extract_text()
                    if text and text.strip():
                        pages_text.append(text.strip())
                except Exception:
                    continue  # skip individual broken pages
    except Exception as e:
        st.error(f"PDF read error: {e}")
        return ""
    return "\n\n".join(pages_text)


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------
def safe_list(val):
    return val if isinstance(val, list) else []

def safe_dict(val):
    return val if isinstance(val, dict) else {}


# ----------------------------------------------------------------------
# UI
# ----------------------------------------------------------------------
st.title("📄 AI Resume Analytics & Interview Insights")
st.caption("Powered by Snowflake Cortex AI")

tab_analyze, tab_past, tab_dash = st.tabs(
    ["🔍 Analyze Resume", "📋 Past Analyses", "📊 Dashboard"]
)

# ── TAB 1: Analyze ────────────────────────────────────────────────────
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
            with st.spinner("Reading PDF..."):
                resume_text = extract_pdf_text(uploaded.getvalue())

            if resume_text:
                st.success(f"Extracted {len(resume_text):,} characters from {uploaded.name}")
                with st.expander("Preview extracted text"):
                    st.text(resume_text[:2000] + ("..." if len(resume_text) > 2000 else ""))
            else:
                st.warning("No readable text found. The PDF may be image-based (scanned).")

    # ── Analyze button ──
    if st.button("🚀 Analyze Resume", type="primary"):
        if not resume_text or not resume_text.strip():
            st.warning("Please provide resume text before analyzing.")
        else:
            with st.spinner("AI is analyzing the resume..."):
                try:
                    result = call_analyze(resume_text, file_name)
                    if not isinstance(result, dict):
                        result = {}
                    st.session_state["last_analysis"] = result
                    st.success("Analysis complete!")
                except Exception as e:
                    st.error(f"Analysis error: {e}")

    # ── Render results ──
    if "last_analysis" in st.session_state:
        a = st.session_state["last_analysis"]

        if not a or not isinstance(a, dict):
            st.error("No valid analysis data received.")
        else:
            if isinstance(a, str):
                try:
                    a = json.loads(a)
                except json.JSONDecodeError:
                    st.error("Could not parse analysis response.")
                    st.stop()

            st.divider()

            # Top metrics
            c1, c2, c3 = st.columns(3)
            c1.metric("Candidate",    a.get("name", "N/A"))
            c2.metric("Experience",   a.get("experience_level", "N/A"))
            c3.metric("Skills found", len(safe_list(a.get("skills"))))

            # Recommended roles
            st.subheader("🎯 Recommended Roles")
            for r in safe_list(a.get("recommended_roles")):
                if not isinstance(r, dict):
                    continue
                score_raw = str(r.get("match_score", "0")).replace("%", "").strip()
                try:
                    score = min(max(int(float(score_raw)), 0), 100)
                except (ValueError, TypeError):
                    score = 0
                st.progress(
                    score / 100,
                    text=f"**{r.get('role', 'Unknown')}** — {r.get('match_score', 'N/A')}",
                )

            # Two-column detail
            col_a, col_b = st.columns(2)

            with col_a:
                st.subheader("💪 Strengths")
                for s in safe_list(a.get("strengths")):
                    st.write(f"- {s}")

                st.subheader("🛠️ Skills")
                skills = safe_list(a.get("skills"))
                st.write(", ".join(skills) if skills else "N/A")

                st.subheader("🏅 Certifications")
                certs = safe_list(a.get("certifications"))
                if certs:
                    for cert in certs:
                        st.write(f"- {cert}")
                else:
                    st.write("None listed")

            with col_b:
                st.subheader("⚠️ Weaknesses / Improvements")
                for w in safe_list(a.get("weaknesses")):
                    st.write(f"- {w}")

                st.subheader("📂 Key Projects")
                projects = safe_list(a.get("key_projects"))
                if projects:
                    for p in projects:
                        st.write(f"- {p}")
                else:
                    st.write("None listed")

            # Interview questions
            st.subheader("❓ Interview Questions")
            q1, q2 = st.columns(2)
            iq = safe_dict(a.get("interview_questions"))

            with q1:
                st.markdown("**Technical**")
                for q in safe_list(iq.get("technical")):
                    st.write(f"- {q}")

            with q2:
                st.markdown("**HR**")
                for q in safe_list(iq.get("hr")):
                    st.write(f"- {q}")

            # Summary
            st.subheader("📝 Final Evaluation Summary")
            st.info(a.get("summary", "No summary available."))

            with st.expander("🔎 Raw JSON"):
                st.json(a)

# ── TAB 2: Past analyses ───────────────────────────────────────────────
with tab_past:
    st.subheader("Past resume analyses")
    try:
        df = run_query("""
            SELECT analyzed_at, candidate_name, experience_level, file_name, summary
            FROM RESUME_ANALYTICS.PUBLIC.RESUME_ANALYSIS
            ORDER BY analyzed_at DESC
            LIMIT 100
        """)
        if df.empty:
            st.info("No past analyses found.")
        else:
            st.dataframe(df, use_container_width=True)
    except Exception as e:
        st.error(f"Error loading past analyses: {e}")

# ── TAB 3: Dashboard ───────────────────────────────────────────────────
with tab_dash:
    st.subheader("Candidate analytics")
    try:
        total_df = run_query(
            "SELECT COUNT(*) AS C FROM RESUME_ANALYTICS.PUBLIC.RESUME_ANALYSIS"
        )
        total = int(total_df.iloc[0, 0])
        st.metric("Total resumes analyzed", total)

        if total > 0:
            exp_df = run_query("""
                SELECT experience_level, COUNT(*) AS cnt
                FROM RESUME_ANALYTICS.PUBLIC.RESUME_ANALYSIS
                GROUP BY experience_level
                ORDER BY cnt DESC
            """)
            st.bar_chart(exp_df.set_index("EXPERIENCE_LEVEL"))
        else:
            st.info("No data yet. Analyze some resumes first!")
    except Exception as e:
        st.error(f"Dashboard error: {e}")
