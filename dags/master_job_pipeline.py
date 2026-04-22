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

# --- KONFIGURASI ---
DATA_DIR = '/opt/airflow/data'

SKILL_KEYWORDS = [
    'Python', 'SQL', 'PostgreSQL', 'Airflow', 'Docker', 'Kubernetes',
    'Machine Learning', 'Data Mining', 'FastAPI', 'Flask', 'Pandas',
    'PySpark', 'Tableau', 'AWS', 'GCP', 'SQL Server', 'NoSQL', 'Java',
    'React', 'Excel', 'Hadoop', 'Spark', 'Git', 'Power BI', 'MongoDB',
    'Redis', 'Kafka', 'Golang', 'TypeScript', 'JavaScript', 'Node.js',
    'C#', '.NET', 'PHP', 'Vue', 'Angular', 'Django', 'Spring Boot',
    'TensorFlow', 'PyTorch', 'Scikit-learn', 'R', 'MATLAB',
    'Azure', 'Terraform', 'Jenkins', 'CI/CD', 'GraphQL', 'REST API',
    'Linux', 'SAP', 'Odoo', 'Figma', 'Jira', 'Agile', 'Scrum',
    'Data Warehouse', 'ETL', 'Data Lake', 'dbt', 'Looker',
]

default_args = {
    'owner': 'theodosius',
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

@dag(
    dag_id='master_job_market_pipeline_final',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['production', 'scrapy', 'etl']
)
def master_job_pipeline():

    # 🔥 SCRAPY TASK (JSON/API based - tidak butuh Playwright)
    def create_scrapy_task(spider_name):
        return BashOperator(
            task_id=f'scrape_{spider_name}',
            bash_command=(
                f'export PYTHONPATH=/opt/airflow && '
                f'export PATH=$PATH:/home/airflow/.local/bin && '
                f'cd /opt/airflow/freya && '
                f'scrapy crawl {spider_name} '
                f'-O {DATA_DIR}/{spider_name}_output.jl:jl '
                f'--loglevel INFO '
                f'2>&1 || true'  # Jangan gagalkan seluruh pipeline jika 1 spider error
            ),
        )

    # 🔥 SCRAPY TASK with Playwright (butuh browser)
    def create_playwright_task(spider_name):
        return BashOperator(
            task_id=f'scrape_{spider_name}',
            bash_command=(
                f'export PYTHONPATH=/opt/airflow && '
                f'export PATH=$PATH:/home/airflow/.local/bin && '
                f'cd /opt/airflow/freya && '
                f'playwright install chromium 2>/dev/null; '  # Install browser jika belum
                f'scrapy crawl {spider_name} '
                f'-O {DATA_DIR}/{spider_name}_output.jl:jl '
                f'--loglevel INFO '
                f'2>&1 || true'
            ),
        )

    # ============================================================
    # 📋 SCRAPING TASKS - API Based (lebih reliable)
    # ============================================================
    task_dealls = create_scrapy_task('dealls')
    task_kalibrr = create_scrapy_task('kalibrr')
    task_karir = create_scrapy_task('karir')
    task_jobstreet = create_scrapy_task('jobstreet')
    task_techinasia = create_scrapy_task('techinasia')
    task_blibli = create_scrapy_task('blibli')
    task_evermos = create_scrapy_task('evermos')
    task_goto = create_scrapy_task('goto')
    task_tiket = create_scrapy_task('tiket')
    task_softwareone = create_scrapy_task('softwareone')
    task_koltiva = create_scrapy_task('koltiva')

    # ============================================================
    # 📋 SCRAPING TASKS - Playwright Based (butuh browser headless)
    # ============================================================
    task_flip = create_playwright_task('flip')
    task_kredivo = create_playwright_task('kredivo')
    task_mekari = create_playwright_task('mekari')
    task_vidio = create_playwright_task('vidio')
    task_glints = create_playwright_task('glints')

    # 🔥 ETL TASK
    @task()
    def process_and_load_data():
        all_jobs = []

        # ============================================================
        # 📥 PHASE 1: INGESTION
        # ============================================================
        file_patterns = glob.glob(f'{DATA_DIR}/*_output.jl')
        if not file_patterns:
            logging.warning("❌ DATA LAKE KOSONG - tidak ada file .jl")
            return

        for file_path in file_patterns:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    for line_num, line in enumerate(f, 1):
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            job = json.loads(line)
                            if job and isinstance(job, dict) and job.get('job_url'):
                                all_jobs.append(job)
                        except json.JSONDecodeError as e:
                            logging.warning(f"JSON error in {file_path} line {line_num}: {e}")
            except Exception as e:
                logging.error(f"Gagal baca {file_path}: {e}")

        if not all_jobs:
            logging.warning("❌ TIDAK ADA DATA VALID")
            return

        logging.info(f"📥 Total raw records: {len(all_jobs)}")

        # ============================================================
        # 🔄 PHASE 2: TRANSFORM
        # ============================================================
        df = pd.DataFrame(all_jobs)

        # --- 2a. Dedup by job_url ---
        before_dedup = len(df)
        df.drop_duplicates(subset=['job_url'], inplace=True)
        logging.info(f"🔄 Dedup: {len(df)} (removed {before_dedup - len(df)})")

        # --- 2b. Standardize Location ---
        INDONESIA_KEYWORDS = [
            'indonesia', 'jakarta', 'bandung', 'surabaya', 'yogyakarta',
            'semarang', 'medan', 'makassar', 'bali', 'denpasar', 'tangerang',
            'bekasi', 'depok', 'bogor', 'malang', 'solo', 'palembang',
            'balikpapan', 'pontianak', 'manado', 'batam', 'pekanbaru',
            'lampung', 'aceh', 'padang', 'jawa', 'kalimantan', 'sulawesi',
            'sumatera', 'sumatra', 'papua', 'nusa tenggara', 'banten',
            'dki', 'jabodetabek', 'cikarang', 'karawang', 'cibubur',
            'serpong', 'bsd', 'pik', 'kelapa gading', 'sunter',
            'central jakarta', 'south jakarta', 'north jakarta',
            'west jakarta', 'east jakarta', 'id', 'n/a', 'on-site',
        ]

        def is_indonesia_location(loc):
            if not loc or not isinstance(loc, str):
                return True  # Default: termasuk (unknown = assume Indonesia)
            loc_lower = loc.lower().strip()
            if loc_lower in ('n/a', '', 'on-site', 'remote', 'hybrid'):
                return True
            return any(kw in loc_lower for kw in INDONESIA_KEYWORDS)

        before_filter = len(df)
        df = df[df['job_location'].apply(is_indonesia_location)]
        logging.info(f"🌏 Filter Indonesia: {len(df)} (removed {before_filter - len(df)} non-ID)")

        def clean_location(loc):
            if not loc or not isinstance(loc, str) or loc.strip().lower() in ('n/a', '', 'on-site'):
                return 'Indonesia'
            loc = loc.strip()
            # Standardize common patterns
            loc = re.sub(r'\s*,\s*Indonesia\s*$', '', loc, flags=re.IGNORECASE)
            loc = re.sub(r'\s*-\s*Indonesia\s*$', '', loc, flags=re.IGNORECASE)
            loc = re.sub(r'\s*,\s*ID\s*$', '', loc, flags=re.IGNORECASE)
            # Capitalize properly
            loc = loc.strip().strip(',').strip('-').strip()
            if not loc:
                return 'Indonesia'
            return loc.title() if loc == loc.lower() else loc

        df['job_location'] = df['job_location'].apply(clean_location)

        # --- 2c. Standardize Job Type ---
        JOB_TYPE_MAP = {
            'full time': 'Full-time', 'full-time': 'Full-time', 'fulltime': 'Full-time',
            'full_time': 'Full-time', 'permanent': 'Full-time', 'reguler': 'Full-time',
            'part time': 'Part-time', 'part-time': 'Part-time', 'parttime': 'Part-time',
            'contract': 'Contract', 'kontrak': 'Contract', 'temporary': 'Contract',
            'intern': 'Internship', 'internship': 'Internship', 'magang': 'Internship',
            'freelance': 'Freelance', 'freelancer': 'Freelance',
        }

        def standardize_job_type(jt):
            if not jt or not isinstance(jt, str) or jt.strip().lower() in ('n/a', '', 'none'):
                return 'Full-time'
            jt_lower = jt.strip().lower()
            for key, val in JOB_TYPE_MAP.items():
                if key in jt_lower:
                    return val
            return jt.strip().title()

        df['job_type'] = df['job_type'].apply(standardize_job_type)

        # --- 2d. Standardize Work Arrangement ---
        def standardize_work_arrangement(wa):
            if not wa or not isinstance(wa, str):
                return 'On-site'
            wa_lower = wa.strip().lower()
            if 'remote' in wa_lower or 'wfh' in wa_lower or 'work from home' in wa_lower:
                return 'Remote'
            elif 'hybrid' in wa_lower:
                return 'Hybrid'
            return 'On-site'

        df['work_arrangement'] = df['work_arrangement'].apply(standardize_work_arrangement)

        # --- 2e. Clean Salary ---
        def clean_salary(val):
            try:
                if val is None or str(val).strip() in ('', 'N/A', 'None', '0', 'Rp'):
                    return 0.0
                s = str(val).strip()
                # Remove currency symbols
                s = re.sub(r'(IDR|Rp|Rp\.?|USD|\$|€)', '', s, flags=re.IGNORECASE).strip()
                # Handle "juta" / "jt"
                juta_match = re.search(r'([\d,.]+)\s*(juta|jt)', s, re.IGNORECASE)
                if juta_match:
                    num = float(juta_match.group(1).replace('.', '').replace(',', '.'))
                    return num * 1_000_000
                # Extract numbers
                s = s.replace('.', '').replace(',', '')
                nums = re.findall(r'\d+', s)
                return float(nums[0]) if nums else 0.0
            except:
                return 0.0

        df['salary_numeric'] = df['base_salary'].apply(clean_salary)

        # --- 2f. Clean Text Fields ---
        def clean_text(val, max_len=250):
            if not val or not isinstance(val, str):
                return 'N/A'
            val = re.sub(r'<[^>]+>', ' ', val)  # Strip HTML
            val = re.sub(r'\s+', ' ', val).strip()
            val = val.replace('\n', ' ').replace('\r', '')
            return val[:max_len] if val else 'N/A'

        df['job_title'] = df['job_title'].apply(lambda x: clean_text(x).title() if x else 'N/A')
        df['company'] = df['company'].apply(clean_text)
        df['job_department'] = df['job_department'].apply(lambda x: clean_text(x, 100))
        df['job_level'] = df['job_level'].apply(lambda x: clean_text(x, 50))

        # --- 2g. Filter empty titles ---
        df = df[df['job_title'].str.strip().str.lower() != 'n/a']
        df = df[df['job_title'].str.len() > 2]

        logging.info(f"✅ After transform: {len(df)} clean records")

        # Log per source
        if 'job_board' in df.columns:
            source_counts = df['job_board'].value_counts()
            for source, count in source_counts.items():
                logging.info(f"  📊 {source}: {count} jobs")

        if len(df) == 0:
            logging.warning("❌ No records after transform")
            return

        # ============================================================
        # 📤 PHASE 3: LOAD
        # ============================================================
        pg_hook = PostgresHook(postgres_conn_id='dwh_conn')
        conn = pg_hook.get_conn()

        insert_job_query = """
            INSERT INTO job_vacancies (
                job_title, company_name, location, source, job_url,
                salary_avg, job_category, experience_level, work_setup, job_type
            )
            VALUES %s
            ON CONFLICT (job_url) DO UPDATE SET
                scraped_at = CURRENT_TIMESTAMP,
                salary_avg = EXCLUDED.salary_avg,
                work_setup = EXCLUDED.work_setup,
                job_type = EXCLUDED.job_type
        """

        try:
            with conn.cursor() as cur:

                # 🔹 Insert jobs
                job_records = [(
                    str(j.get('job_title', ''))[:250],
                    str(j.get('company', ''))[:250],
                    str(j.get('job_location', ''))[:250],
                    str(j.get('job_board', ''))[:50],
                    str(j.get('job_url', '')),
                    j.get('salary_numeric', 0.0),
                    str(j.get('job_department', ''))[:100],
                    str(j.get('job_level', ''))[:50],
                    str(j.get('work_arrangement', ''))[:50],
                    str(j.get('job_type', ''))[:100],
                ) for j in df.to_dict('records')]

                execute_values(cur, insert_job_query, job_records)
                logging.info(f"✅ Inserted/Updated {len(job_records)} job records")

                # 🔹 Mapping job_id
                cur.execute("SELECT job_id, job_url FROM job_vacancies")
                url_to_id = {row[1]: row[0] for row in cur.fetchall()}

                # 🔹 Extract skills
                skill_records = []
                for j in df.to_dict('records'):
                    jid = url_to_id.get(j.get('job_url'))
                    if jid:
                        search_text = ' '.join([
                            str(j.get('job_title', '')),
                            str(j.get('job_department', '')),
                            str(j.get('desc', '')),
                        ]).lower()
                        search_text = re.sub(r'<[^>]+>', ' ', search_text)

                        for s in SKILL_KEYWORDS:
                            if re.search(rf'\b{re.escape(s.lower())}\b', search_text):
                                skill_records.append((jid, s))

                if skill_records:
                    skill_records = list(set(skill_records))
                    execute_values(
                        cur,
                        "INSERT INTO job_skills (job_id, skill_name) VALUES %s ON CONFLICT DO NOTHING",
                        skill_records
                    )
                    logging.info(f"✅ Inserted {len(skill_records)} skill records")
                else:
                    logging.warning("⚠️ No skills detected from any job")

                conn.commit()
                logging.info(f"🎉 SUCCESS: {len(job_records)} jobs + {len(skill_records)} skills loaded to DWH")

        except Exception as e:
            conn.rollback()
            logging.error(f"❌ DB ERROR: {e}")
            raise

        finally:
            conn.close()

    # 🔗 FLOW - Semua spider jalan parallel, lalu ETL
    all_scrapers = [
        # API-based spiders
        task_dealls,
        task_kalibrr,
        task_karir,
        task_jobstreet,
        task_techinasia,
        task_blibli,
        task_evermos,
        task_goto,
        task_tiket,
        task_softwareone,
        task_koltiva,
        # Playwright-based spiders
        task_flip,
        task_kredivo,
        task_mekari,
        task_vidio,
        task_glints,
    ]

    all_scrapers >> process_and_load_data()


pipeline = master_job_pipeline()