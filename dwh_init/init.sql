-- ============================================
-- 🔥 Buat database DWH terpisah dari Airflow
-- ============================================
CREATE DATABASE dwh;

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
-- 📊 View: Insight - Top Skills
-- ============================================
CREATE OR REPLACE VIEW v_top_skills AS
SELECT
    skill_name,
    COUNT(*) as demand_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(DISTINCT job_id) FROM job_skills), 2) as demand_percentage
FROM job_skills
GROUP BY skill_name
ORDER BY demand_count DESC;

-- ============================================
-- 📊 View: Insight - Jobs By Source
-- ============================================
CREATE OR REPLACE VIEW v_jobs_by_source AS
SELECT
    source as job_board,
    COUNT(*) as total_jobs,
    ROUND(AVG(CASE WHEN salary_avg > 0 THEN salary_avg END), 0) as avg_salary,
    COUNT(CASE WHEN salary_avg > 0 THEN 1 END) as jobs_with_salary
FROM job_vacancies
GROUP BY source
ORDER BY total_jobs DESC;

-- ============================================
-- 📊 View: Insight - Jobs By Location
-- ============================================
CREATE OR REPLACE VIEW v_jobs_by_location AS
SELECT
    location,
    COUNT(*) as total_jobs,
    ROUND(AVG(CASE WHEN salary_avg > 0 THEN salary_avg END), 0) as avg_salary,
    COUNT(DISTINCT company_name) as unique_companies
FROM job_vacancies
GROUP BY location
ORDER BY total_jobs DESC
LIMIT 30;

-- ============================================
-- 📊 View: Insight - Jobs By Work Setup
-- ============================================
CREATE OR REPLACE VIEW v_jobs_by_work_setup AS
SELECT
    COALESCE(work_setup, 'Unknown') as work_setup,
    COUNT(*) as total_jobs,
    ROUND(AVG(CASE WHEN salary_avg > 0 THEN salary_avg END), 0) as avg_salary
FROM job_vacancies
GROUP BY work_setup
ORDER BY total_jobs DESC;

-- ============================================
-- 📊 View: Insight - Top Hiring Companies
-- ============================================
CREATE OR REPLACE VIEW v_top_companies AS
SELECT
    company_name,
    COUNT(*) as total_openings,
    ROUND(AVG(CASE WHEN salary_avg > 0 THEN salary_avg END), 0) as avg_salary,
    COUNT(DISTINCT source) as listed_on_boards
FROM job_vacancies
WHERE company_name IS NOT NULL AND company_name != 'Private Advertiser'
GROUP BY company_name
ORDER BY total_openings DESC
LIMIT 50;

-- ============================================
-- 📊 View: Insight - Salary Distribution
-- ============================================
CREATE OR REPLACE VIEW v_salary_distribution AS
SELECT
    CASE
        WHEN salary_avg = 0 THEN 'Tidak Disebutkan'
        WHEN salary_avg < 5000000 THEN '< 5 Juta'
        WHEN salary_avg < 10000000 THEN '5 - 10 Juta'
        WHEN salary_avg < 15000000 THEN '10 - 15 Juta'
        WHEN salary_avg < 25000000 THEN '15 - 25 Juta'
        WHEN salary_avg < 50000000 THEN '25 - 50 Juta'
        ELSE '> 50 Juta'
    END as salary_range,
    COUNT(*) as total_jobs
FROM job_vacancies
GROUP BY salary_range
ORDER BY MIN(salary_avg);

-- ============================================
-- 📊 View: Insight - Skills by Category
-- ============================================
CREATE OR REPLACE VIEW v_skills_by_category AS
SELECT
    jv.job_category,
    js.skill_name,
    COUNT(*) as frequency
FROM job_skills js
JOIN job_vacancies jv ON js.job_id = jv.job_id
WHERE jv.job_category IS NOT NULL
GROUP BY jv.job_category, js.skill_name
ORDER BY jv.job_category, frequency DESC;

-- ============================================
-- 📊 View: Insight - Daily Scraping Summary
-- ============================================
CREATE OR REPLACE VIEW v_daily_summary AS
SELECT
    DATE(scraped_at) as scrape_date,
    COUNT(*) as total_jobs,
    COUNT(DISTINCT source) as active_sources,
    COUNT(DISTINCT company_name) as unique_companies,
    ROUND(AVG(CASE WHEN salary_avg > 0 THEN salary_avg END), 0) as avg_salary
FROM job_vacancies
GROUP BY DATE(scraped_at)
ORDER BY scrape_date DESC;