import streamlit as st
import pandas as pd
import json
import os
import snowflake.connector

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
            import base64
            raw = base64.b64encode(uploaded.read()).decode("utf-8")
            parse_sql = f"""
                SELECT SNOWFLAKE.CORTEX.PARSE_DOCUMENT(
                    TO_BINARY('{raw}', 'BASE64'), 'pdf'
                ):content::STRING AS CONTENT
            """
            try:
                result = run_query_raw(parse_sql)
                content = json.loads(result[0][0]) if isinstance(result[0][0], str) else result[0][0]
                resume_text = content.get("content", "") if isinstance(content, dict) else str(content)
                st.text_area("Extracted text", value=resume_text, height=200)
            except Exception as e:
                st.error(f"Failed to parse PDF: {e}")

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
                        text=f"**{r.get('role')}** — {r.get('match_score')}"
                    )

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
                    st.subheader("⚠️ Weaknesses")
                    for w in a.get("weaknesses", []):
                        st.write(f"- {w}")
                    st.subheader("📂 Key Projects")
                    for p in a.get("key_projects", []):
                        st.write(f"- {p}")

                st.subheader("❓ Interview Questions")
                qcol1, qcol2 = st.columns(2)
                with qcol1:
                    st.markdown("**Technical**")
                    for q in a.get("interview_questions", {}).get("technical", []):
                        st.write(f"- {q}")
                with qcol2:
                    st.markdown("**HR**")
                    for q in a.get("interview_questions", {}).get("hr", []):
                        st.write(f"- {q}")

                st.subheader("📝 Summary")
                st.write(a.get("summary", ""))
            except Exception as e:
                st.error(f"Analysis failed: {e}")

with tab_results:
    st.subheader("Past Analyses")
    try:
        df = run_query("""
            SELECT ANALYSIS_ID, FILE_NAME, CANDIDATE_NAME, EXPERIENCE_LEVEL, ANALYZED_AT
            FROM RESUME_ANALYTICS.PUBLIC.RESUME_ANALYSIS
            ORDER BY ANALYZED_AT DESC
        """)
        if len(df) > 0:
            st.dataframe(df, use_container_width=True)
        else:
            st.info("No analyses yet.")
    except Exception as e:
        st.error(f"Error: {e}")

with tab_dashboard:
    st.subheader("Dashboard")
    try:
        count_df = run_query("SELECT COUNT(*) AS CNT FROM RESUME_ANALYTICS.PUBLIC.RESUME_ANALYSIS")
        total = count_df["CNT"].iloc[0] if len(count_df) > 0 else 0
        st.metric("Total resumes analyzed", total)

        if total > 0:
            st.markdown("**Experience level distribution**")
            exp_df = run_query("""
                SELECT EXPERIENCE_LEVEL, COUNT(*) AS CNT
                FROM RESUME_ANALYTICS.PUBLIC.RESUME_ANALYSIS
                GROUP BY EXPERIENCE_LEVEL
            """)
            st.bar_chart(exp_df.set_index("EXPERIENCE_LEVEL"))

            st.markdown("**Top skills across all resumes**")
            skills_df = run_query("""
                SELECT s.value::STRING AS SKILL, COUNT(*) AS CNT
                FROM RESUME_ANALYTICS.PUBLIC.RESUME_ANALYSIS,
                     LATERAL FLATTEN(input => skills) s
                GROUP BY SKILL ORDER BY CNT DESC LIMIT 20
            """)
            st.bar_chart(skills_df.set_index("SKILL"))

            st.markdown("**Top recommended roles**")
            roles_df = run_query("""
                SELECT r.value:role::STRING AS ROLE, COUNT(*) AS CNT
                FROM RESUME_ANALYTICS.PUBLIC.RESUME_ANALYSIS,
                     LATERAL FLATTEN(input => recommended_roles) r
                GROUP BY ROLE ORDER BY CNT DESC LIMIT 10
            """)
            st.bar_chart(roles_df.set_index("ROLE"))
    except Exception as e:
        st.error(f"Error: {e}")
