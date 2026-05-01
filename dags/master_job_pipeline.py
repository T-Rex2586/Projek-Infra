from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import json
import glob
import logging
import re
from psycopg2.extras import execute_values

DATA_DIR = '/opt/airflow/data'

SKILL_KEYWORDS = [
    'Python','SQL','PostgreSQL','Airflow','Docker','Kubernetes',
    'Machine Learning','FastAPI','Flask','Pandas','PySpark',
    'Tableau','AWS','GCP','Java','React','Excel','Hadoop',
    'Spark','Git','Power BI','MongoDB','Kafka','Golang',
    'TypeScript','JavaScript','Node.js','Django','TensorFlow'
]

SPIDERS = [
    'coursera', 'dealls', 'dicoding',
    'flip', 'jobstreet', 'kalibrr', 'karir',
    'koltiva', 'mekari', 'softwareone',
    'techinasia','vidio'
]

default_args = {
    'owner': 'theodosius',
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

@dag(
    dag_id='master_job_market_pipeline_enterprise',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['etl','skills-gap', 'enterprise']
)
def pipeline():

    def create_scrapy_task(spider_name):
        return BashOperator(
            task_id=f'scrape_{spider_name}',
            bash_command=(
                f'export PYTHONPATH=/opt/airflow:$PYTHONPATH && '
                f'cd /opt/airflow/freya && '
                f'scrapy crawl {spider_name} '
                f'-O {DATA_DIR}/{spider_name}_output.jl:jl || true'
            )
        )

    scrapy_tasks = [create_scrapy_task(spider) for spider in SPIDERS]

    @task()
    def process_and_load():
        all_data = []

        for file in glob.glob(f"{DATA_DIR}/*.jl"):
            with open(file, 'r') as f:
                for line in f:
                    try:
                        obj = json.loads(line)
                        obj['data_type'] = 'course' if 'course_title' in obj else 'job'
                        all_data.append(obj)
                    except:
                        continue

        if not all_data:
            logging.info("No data found to process.")
            return

        df = pd.DataFrame(all_data)
        if 'data_type' not in df.columns: return

        jobs_df = df[df['data_type'] == 'job'].copy()
        courses_df = df[df['data_type'] == 'course'].copy()

        def clean_text(x):
            if pd.isna(x) or not x: return 'N/A'
            x = re.sub(r'<[^>]+>', '', str(x))
            return re.sub(r'\s+', ' ', x).strip()

        def clean_nulls(x):
            if pd.isna(x) or str(x).strip().upper() == 'N/A' or x == '': return None
            return x

        def standardize_location(loc):
            if not loc: return None
            l = str(loc).lower()
            if 'jakarta' in l: return 'DKI Jakarta'
            if 'tangerang' in l: return 'Tangerang'
            if 'surabaya' in l: return 'Surabaya'
            if 'bandung' in l: return 'Bandung'
            return str(loc).title()

        def standardize_category(cat, title):
            if cat and str(cat).upper() != 'N/A': return cat
            t = str(title).lower()
            if 'data engineer' in t or 'database' in t or 'etl' in t: return 'Data Engineering'
            if 'data scientist' in t or 'machine learning' in t or 'ai ' in t: return 'Data Science & AI'
            if 'data analyst' in t or 'analytics' in t: return 'Data Analytics'
            if 'software' in t or 'developer' in t or 'backend' in t or 'frontend' in t: return 'Software Engineering'
            return 'Other'

        def standardize_work_setup(setup):
            if pd.isna(setup) or not setup or str(setup).strip().upper() == 'N/A':
                return 'Tidak Disebutkan'
            s = str(setup).lower()
            if 'partially remote' in s or 'hybrid' in s: return 'Hybrid'
            if 'on-site' in s or 'onsite' in s or 'wfo' in s: return 'Onsite'
            if 'remote' in s or 'wfh' in s: return 'Remote'
            return str(setup).title()

        if not jobs_df.empty:
            if 'job_url' in jobs_df.columns:
                jobs_df.drop_duplicates(subset=['job_url'], inplace=True)
            if 'job_title' in jobs_df.columns:
                jobs_df['job_title'] = jobs_df['job_title'].apply(clean_text)
            jobs_df['desc'] = jobs_df['desc'].apply(clean_text) if 'desc' in jobs_df.columns else ''

        if not courses_df.empty:
            if 'course_title' in courses_df.columns:
                courses_df.drop_duplicates(subset=['course_title'], inplace=True)
                courses_df['course_title'] = courses_df['course_title'].apply(clean_text)
            courses_df['desc'] = courses_df.get('desc', '').apply(clean_text)

        pg = PostgresHook(postgres_conn_id='dwh_conn')
        conn = pg.get_conn()

        try:
            with conn.cursor() as cur:

                job_records = []
                if not jobs_df.empty:
                    for j in jobs_df.to_dict('records'):
                        clean_cat = standardize_category(j.get('job_department'), j.get('job_title'))
                        clean_loc = standardize_location(j.get('job_location'))
                        clean_setup = standardize_work_setup(j.get('work_arrangement'))

                        job_records.append((
                            clean_nulls(j.get('job_title')),
                            clean_nulls(j.get('company')),
                            clean_nulls(clean_loc),
                            clean_nulls(j.get('job_board')),
                            j.get('job_url', ''),
                            float(j.get('base_salary', 0) or 0),
                            clean_nulls(clean_cat),
                            clean_nulls(j.get('job_level')),
                            clean_setup,
                            clean_nulls(j.get('job_type'))
                        ))

                if job_records:
                    execute_values(cur, """
                    INSERT INTO job_vacancies (
                        job_title, company_name, location, source, job_url,
                        salary_avg, job_category, experience_level, work_setup, job_type
                    )
                    VALUES %s
                    ON CONFLICT (job_url) DO UPDATE SET
                        last_seen = CURRENT_TIMESTAMP,
                        is_active = TRUE,
                        job_title = EXCLUDED.job_title,
                        salary_avg = EXCLUDED.salary_avg,
                        work_setup = EXCLUDED.work_setup
                    """, job_records)

                cur.execute("""
                    UPDATE job_vacancies
                    SET is_active = FALSE, closed_at = CURRENT_TIMESTAMP
                    WHERE last_seen < CURRENT_DATE - INTERVAL '3 days' AND is_active = TRUE
                """)

                cur.execute("SELECT job_id, job_url FROM job_vacancies")
                job_map = {r[1]: r[0] for r in cur.fetchall()}

                course_records = [(clean_nulls(j.get('course_title')), clean_nulls(j.get('platform')), j.get('url', '')) for j in courses_df.to_dict('records')]
                if course_records:
                    execute_values(cur, "INSERT INTO courses (course_title, platform, url) VALUES %s ON CONFLICT (url) DO NOTHING", course_records)

                cur.execute("SELECT course_id, course_title FROM courses")
                course_map = {r[1]: r[0] for r in cur.fetchall()}

                job_skills = []
                course_skills = []

                if not jobs_df.empty:
                    for j in jobs_df.to_dict('records'):
                        text = (str(j.get('job_title','')) + ' ' + str(j.get('desc',''))).lower()
                        jid = job_map.get(j.get('job_url'))
                        if jid is None: continue
                        for s in SKILL_KEYWORDS:
                            if re.search(r'\b' + re.escape(s.lower()) + r'\b', text):
                                job_skills.append((jid, s))

                if not courses_df.empty:
                    for c in courses_df.to_dict('records'):
                        text = (str(c.get('course_title','')) + ' ' + str(c.get('desc',''))).lower()
                        cid = course_map.get(c.get('course_title'))
                        if cid is None: continue
                        for s in SKILL_KEYWORDS:
                            if re.search(r'\b' + re.escape(s.lower()) + r'\b', text):
                                course_skills.append((cid, s))

                if job_skills: execute_values(cur, "INSERT INTO job_skills (job_id, skill_name) VALUES %s ON CONFLICT DO NOTHING", list(set(job_skills)))
                if course_skills: execute_values(cur, "INSERT INTO course_skills (course_id, skill_name) VALUES %s ON CONFLICT DO NOTHING", list(set(course_skills)))

                conn.commit()
                logging.info("SUCCESS PIPELINE ENTERPRISE: All data loaded.")

        except Exception as e:
            conn.rollback()
            logging.error(f"DATABASE ERROR: {e}")
            raise
        finally:
            conn.close()

    scrapy_tasks >> process_and_load()

pipeline = pipeline()
