import streamlit as st
import pandas as pd
import json
import os
import snowflake.connector
import pdfplumber

st.set_page_config(page_title="AI Resume Analytics", page_icon="📄", layout="wide")

def get_connection():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
    )

def run_query(sql):
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql)
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        return pd.DataFrame(rows, columns=columns)
    finally:
        conn.close()

def run_query_raw(sql):
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql)
        return cur.fetchall()
    finally:
        conn.close()

# ✅ NEW: PDF → TEXT FUNCTION
def extract_text_from_pdf(uploaded_file):
    text = ""
    with pdfplumber.open(uploaded_file) as pdf:
        for page in pdf.pages:
            text += page.extract_text() or ""
    return text

st.title("📄 AI Resume Analytics & Interview Insights")
st.caption("Powered by Snowflake Cortex AI")

tab_upload, tab_results, tab_dashboard = st.tabs(["🔍 Analyze Resume", "📋 Past Analyses", "📊 Dashboard"])

with tab_upload:
    st.subheader("Analyze a new resume")
    input_mode = st.radio("Input method", ["Paste text", "Upload PDF"], horizontal=True)

    resume_text = ""
    file_name = ""

    if input_mode == "Paste text":
        file_name = st.text_input("Candidate / file name", value="resume.txt")
        resume_text = st.text_area("Paste resume text here", height=300)

    else:
        uploaded = st.file_uploader("Upload a PDF resume", type=["pdf"])
        if uploaded:
            file_name = uploaded.name

            try:
                # ✅ LOCAL PDF TEXT EXTRACTION (NO SNOWFLAKE PARSE)
                resume_text = extract_text_from_pdf(uploaded)
                st.success("PDF text extracted successfully")
                st.text_area("Extracted text", value=resume_text, height=200)

            except Exception as e:
                st.error(f"Failed to read PDF: {e}")

    if st.button("🚀 Analyze Resume", type="primary", disabled=not resume_text.strip()):
        with st.spinner("AI is analyzing the resume..."):
            try:
                safe_text = resume_text.replace("'", "''")
                safe_name = file_name.replace("'", "''")

                result = run_query_raw(
                    f"CALL RESUME_ANALYTICS.PUBLIC.ANALYZE_RESUME('{safe_text}', '{safe_name}')"
                )

                analysis = json.loads(result[0][0]) if isinstance(result[0][0], str) else result[0][0]
                a = analysis

                st.success("Analysis complete!")

                c1, c2, c3 = st.columns(3)
                c1.metric("Name", a.get("name", "N/A"))
                c2.metric("Experience", a.get("experience_level", "N/A"))
                c3.metric("Skills found", len(a.get("skills", [])))

                st.subheader("🎯 Recommended Roles")
                for r in a.get("recommended_roles", []):
                    st.progress(
                        int(str(r.get("match_score", "0")).replace("%", "")) / 100,
                        text=f"{r.get('role')} — {r.get('match_score')}"
                    )

                col_a, col_b = st.columns(2)

                with col_a:
                    st.subheader("💪 Strengths")
                    for s in a.get("strengths", []):
                        st.write(f"- {s}")

                    st.subheader("🛠️ Skills")
                    st.write(", ".join(a.get("skills", [])))

                with col_b:
                    st.subheader("⚠️ Weaknesses")
                    for w in a.get("weaknesses", []):
                        st.write(f"- {w}")

                st.subheader("📝 Summary")
                st.write(a.get("summary", ""))

            except Exception as e:
                st.error(f"Analysis failed: {e}")
