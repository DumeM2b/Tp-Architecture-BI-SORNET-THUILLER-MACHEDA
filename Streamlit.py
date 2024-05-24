import streamlit as st
from snowflake.snowpark.context import get_active_session

#Grand titre
st.title("DataViz - Linkedin Jobs")

#Description
st.write(
    """
Affichage des résultats des requêytes, sous forme de graphiques, en utlisant streamlit.
    """
)

#Activation de la session
session = get_active_session()

# Graph 1 : TABLEAU
sql = f"SELECT title, COUNT(*) AS job_count FROM jobs_posting GROUP BY title ORDER BY job_count DESC LIMIT 10;"
data = session.sql(sql).collect()
st.subheader("Top 10 des jobs titles les plus postés")
st.dataframe(data, use_container_width=True)

#Graph 2 : TABLEAU
sql = f"SELECT title, COALESCE(MAX(max_salary),0) AS highest_salary FROM jobs_posting WHERE currency = 'USD' GROUP BY title ORDER BY highest_salary DESC LIMIT 1;"
data = session.sql(sql).collect()
st.subheader("Job title le mieux rémunéré")
st.dataframe(data, use_container_width=True)

#Graph 3 : BAR CHART
sql = f"SELECT companies.company_size, COUNT(*) AS job_count FROM jobs_posting JOIN companies ON jobs_posting.company_id = companies.company_id GROUP BY companies.company_size;"
data = session.sql(sql).collect()
st.subheader("Répartition des offres d'emploi par taille d'entreprise")
st.bar_chart(data=data, x="COMPANY_SIZE", y="JOB_COUNT")

#Graph 4 : BAR CHART
sql = f"SELECT industries.industry_name, COUNT(*) AS job_count FROM jobs_posting JOIN job_industries ON jobs_posting.job_id = job_industries.job_id JOIN industries ON job_industries.industry_id = industries.industry_id GROUP BY industries.industry_name;"
data = session.sql(sql).collect()
st.subheader("Répartition des offres d'emploi par types d'industrie")
st.bar_chart(data=data, x="INDUSTRY_NAME", y="JOB_COUNT")

#Graph 5 : BAR CHART
sql = f"SELECT formatted_work_type, COUNT(*) AS job_count FROM jobs_posting GROUP BY formatted_work_type;"
data = session.sql(sql).collect()
st.subheader("Répartition des offres d'emploi par types d'emploi")
st.bar_chart(data=data, x="FORMATTED_WORK_TYPE", y="JOB_COUNT")