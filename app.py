import os
import io
import json
import traceback
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


# ----------------------------------------------------------------------
# Call RESUME_PARSE stored procedure
# ----------------------------------------------------------------------
def call_resume_parse(resume_text: str) -> dict:
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "CALL RESUME_ANALYTICS.PUBLIC.RESUME_PARSE(%s)",
            (resume_text,)
        )
        row = cur.fetchone()

        if not row or row[0] is None:
            st.error("Snowflake returned no data. Check if the procedure ran successfully.")
            return {}

        raw = row[0]

        # Snowflake VARIANT columns come back as a dict or a JSON string
        if isinstance(raw, dict):
            return raw
        if isinstance(raw, str):
            raw = raw.strip()
            if not raw:
                st.error("Snowflake returned an empty string.")
                return {}
            return json.loads(raw)

        st.error(f"Unexpected response type from Snowflake: {type(raw)}")
        return {}

    except json.JSONDecodeError as e:
        st.error(f"Could not parse JSON from Snowflake: {e}")
        return {}
    except Exception as e:
        st.error(f"Snowflake error: {e}")
        st.code(traceback.format_exc())
        return {}
    finally:
        cur.close()


# ----------------------------------------------------------------------
# PDF extraction
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
                    continue
    except Exception as e:
        st.error(f"PDF read error: {e}")
        return ""
    return "\n\n".join(pages_text)


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------
def safe_list(val):
    return val if isinstance(val, list) else []

def safe_str(val):
    return str(val) if val is not None else "N/A"


# ----------------------------------------------------------------------
# UI
# ----------------------------------------------------------------------
st.title("📄 AI Resume Parser")
st.caption("Powered by Snowflake Cortex — mistral-large2")

tab_parse, tab_past = st.tabs(["🔍 Parse Resume", "📋 Past Parses"])

# ── TAB 1: Parse ──────────────────────────────────────────────────────
with tab_parse:
    st.subheader("Upload or paste a resume to extract structured data")
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
                st.success(f"✅ Extracted {len(resume_text):,} characters from **{uploaded.name}**")
                with st.expander("Preview extracted text"):
                    st.text(resume_text[:2000] + ("..." if len(resume_text) > 2000 else ""))
            else:
                st.warning("⚠️ No readable text found. PDF may be scanned/image-based.")

    if st.button("🚀 Parse Resume", type="primary"):
        if not resume_text or not resume_text.strip():
            st.warning("Please provide resume text before parsing.")
        else:
            with st.spinner("Cortex AI is parsing the resume..."):
                result = call_resume_parse(resume_text)
                if result:
                    st.session_state["last_parse"] = result
                    st.session_state["last_file"] = file_name
                    st.success("✅ Parsing complete!")
                else:
                    st.error("Parsing returned empty result. Check the errors above.")

    # ── Render results ──
    if "last_parse" in st.session_state:
        a = st.session_state["last_parse"]

        if not isinstance(a, dict):
            st.error("Invalid data format received.")
        else:
            st.divider()

            # ── Top info ──
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Name",     safe_str(a.get("full_name")))
            c2.metric("Email",    safe_str(a.get("email")))
            c3.metric("Phone",    safe_str(a.get("phone")))
            c4.metric("Location", safe_str(a.get("location")))

            # ── Summary ──
            if a.get("summary"):
                st.subheader("📝 Professional Summary")
                st.info(a["summary"])

            # ── Skills ──
            skills = safe_list(a.get("skills"))
            if skills:
                st.subheader("🛠️ Skills")
                st.write(", ".join(str(s) for s in skills))

            # ── Certifications ──
            certs = safe_list(a.get("certifications"))
            if certs:
                st.subheader("🏅 Certifications")
                for c in certs:
                    st.write(f"- {c}")

            # ── Experience ──
            experience = safe_list(a.get("experience"))
            if experience:
                st.subheader("💼 Work Experience")
                for exp in experience:
                    if not isinstance(exp, dict):
                        continue
                    title   = safe_str(exp.get("title"))
                    company = safe_str(exp.get("company"))
                    start   = safe_str(exp.get("start_date"))
                    end     = safe_str(exp.get("end_date"))
                    desc    = safe_str(exp.get("description"))
                    with st.expander(f"{title} @ {company}  ({start} – {end})"):
                        st.write(desc)

            # ── Education ──
            education = safe_list(a.get("education"))
            if education:
                st.subheader("🎓 Education")
                for edu in education:
                    if not isinstance(edu, dict):
                        continue
                    degree  = safe_str(edu.get("degree"))
                    field   = safe_str(edu.get("field"))
                    inst    = safe_str(edu.get("institution"))
                    year    = safe_str(edu.get("graduation_year"))
                    st.write(f"- **{degree} in {field}** — {inst} ({year})")

            # ── Raw JSON ──
            with st.expander("🔎 Raw JSON"):
                st.json(a)

# ── TAB 2: Past parses ─────────────────────────────────────────────────
with tab_past:
    st.subheader("Past resume parses")
    try:
        df = run_query("""
            SELECT analyzed_at, candidate_name, experience_level, file_name, summary
            FROM RESUME_ANALYTICS.PUBLIC.RESUME_ANALYSIS
            ORDER BY analyzed_at DESC
            LIMIT 100
        """)
        if df.empty:
            st.info("No past parses found.")
        else:
            st.dataframe(df, use_container_width=True)
    except Exception as e:
        st.error(f"Error loading past parses: {e}")
