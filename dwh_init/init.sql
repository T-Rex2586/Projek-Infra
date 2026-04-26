-- ============================================
-- 🔥 Buat database DWH terpisah dari Airflow
-- ============================================
SELECT 'CREATE DATABASE dwh' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'dwh')\gexec

\c dwh;

-- ============================================
-- 📋 Tabel 1: Job Vacancies (Enhanced)
-- ============================================
CREATE TABLE IF NOT EXISTS job_vacancies (
    job_id SERIAL PRIMARY KEY,
    job_title VARCHAR(255) NOT NULL,
    company_name VARCHAR(255),
    location VARCHAR(255),
    source VARCHAR(50),
    job_url TEXT UNIQUE NOT NULL,
    salary_avg NUMERIC(15, 2) DEFAULT 0,
    job_category VARCHAR(100),
    experience_level VARCHAR(50),
    work_setup VARCHAR(50),
    job_type VARCHAR(100),
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- 📋 Tabel 2: Job Skills
-- ============================================
CREATE TABLE IF NOT EXISTS job_skills (
    id SERIAL PRIMARY KEY,
    job_id INTEGER REFERENCES job_vacancies(job_id) ON DELETE CASCADE,
    skill_name VARCHAR(100) NOT NULL,
    UNIQUE(job_id, skill_name)
);

-- ============================================
-- 📋 Tabel 3: Courses
-- ============================================
CREATE TABLE IF NOT EXISTS courses (
    course_id SERIAL PRIMARY KEY,
    course_title VARCHAR(255) NOT NULL,
    platform VARCHAR(255),
    url TEXT UNIQUE NOT NULL,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- 📋 Tabel 4: Course Skills
-- ============================================
CREATE TABLE IF NOT EXISTS course_skills (
    id SERIAL PRIMARY KEY,
    course_id INTEGER REFERENCES courses(course_id) ON DELETE CASCADE,
    skill_name VARCHAR(100) NOT NULL,
    UNIQUE(course_id, skill_name)
);

-- ============================================
-- 🔢 Indexes untuk query performa
-- ============================================
CREATE INDEX IF NOT EXISTS idx_jv_source       ON job_vacancies(source);
CREATE INDEX IF NOT EXISTS idx_jv_location     ON job_vacancies(location);
CREATE INDEX IF NOT EXISTS idx_jv_job_type     ON job_vacancies(job_type);
CREATE INDEX IF NOT EXISTS idx_jv_work_setup   ON job_vacancies(work_setup);
CREATE INDEX IF NOT EXISTS idx_jv_scraped_at   ON job_vacancies(scraped_at);
CREATE INDEX IF NOT EXISTS idx_jv_salary       ON job_vacancies(salary_avg);
CREATE INDEX IF NOT EXISTS idx_jv_exp_level    ON job_vacancies(experience_level);
CREATE INDEX IF NOT EXISTS idx_js_skill_name   ON job_skills(skill_name);


ALTER TABLE job_vacancies 
ADD COLUMN IF NOT EXISTS last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT TRUE,
ADD COLUMN IF NOT EXISTS closed_at TIMESTAMP;