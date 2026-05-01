# Projek-Infra: End-to-End Job & EdTech Data Pipeline

## Overview

**Projek-Infra** is an automated, end-to-end Data Engineering pipeline built to scrape, process, and visualize job vacancies and IT course data across various platforms in Indonesia. 

The pipeline runs on a scheduled basis, extracts data using highly concurrent web scrapers, loads it into a persistent Data Warehouse, and provides a Business Intelligence layer to visualize insights such as top hiring companies, in-demand skills, and salary distributions.

## Architecture & Tech Stack

This project implements a modern data stack orchestrated completely within Docker:

- **Orchestration**: Apache Airflow
- **Data Extraction**: Scrapy & Playwright (Python)
- **Data Warehouse**: PostgreSQL
- **Business Intelligence**: Metabase
- **Containerization**: Docker & Docker Compose

## Project Structure

```
Projek-Infra/
│
├── dags/                   # Airflow DAGs (e.g., master_job_pipeline.py)
├── dwh_init/               # SQL scripts for initializing the Data Warehouse (init.sql)
├── freya/                  # Scrapy project containing all spiders
│   └── spiders/            # Scraper scripts for 20+ platforms
├── data/                   # Output directory for raw scraped data (e.g., .jl or .csv)
├── logs/                   # Airflow execution logs
├── metabase-data/          # Persistent volume for Metabase dashboards
├── docker-compose.yaml     # Infrastructure definitions for all services
└── README.md               # Project documentation
```

## Data Sources (Spiders)

The Scrapy project (`freya`) is equipped with spiders for multiple platforms, including but not limited to:
- **Job Boards**: JobStreet, Glints, TechInAsia, Dealls, Kalibrr, Karir, KitaLulus, LokerID, TopKarir, etc.
- **Tech Companies**: Goto, Blibli, Tiket, Kredivo, Mekari, Vidio, etc.
- **EdTech / Courses**: Dicoding, BuildWithAngga, Skilvul, MySkill.

*Note: The pipeline uses Playwright to render JavaScript-heavy websites.*

## Data Warehouse Schema

The PostgreSQL Data Warehouse (`dwh`) is initialized with the following core tables:
1. `job_vacancies` - Stores job details (title, company, location, salary, setup, etc.)
2. `job_skills` - Stores the required skills mapped to specific jobs.
3. `it_courses` - Stores information about IT courses, providers, and prices.

Several **Views** are also created automatically for Metabase dashboards, such as `v_top_skills`, `v_jobs_by_source`, `v_top_companies`, and `v_salary_distribution`.

## Getting Started

### Prerequisites
- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)

### Running the Infrastructure

1. Clone the repository and navigate to the project directory:
   ```bash
   cd Projek-Infra
   ```

2. Build and start all services using Docker Compose:
   ```bash
   docker-compose up -d
   ```
   *This will start PostgreSQL, Airflow Init, Airflow Webserver, Airflow Scheduler, and Metabase.*

3. Wait for the `airflow-init` container to complete its setup. You can check the logs via:
   ```bash
   docker logs projek-infra-airflow-init-1
   ```

### Accessing the Services

Once everything is up and running, you can access the following interfaces:

- **Apache Airflow UI**: [http://localhost:8089](http://localhost:8089)
  - **Username:** `admin`
  - **Password:** `admin`
  - *Use this to trigger and monitor the scraping DAGs.*

- **Metabase UI**: [http://localhost:3010](http://localhost:3010)
  - *Connect to the PostgreSQL DWH to build or view dashboards.*

- **PostgreSQL Database**:
  - **Host:** `localhost`
  - **Port:** `5431`
  - **Database:** `dwh` (for Data Warehouse) or `airflow` (for Airflow Metadata)
  - **User:** `airflow`
  - **Password:** `airflow`

## Troubleshooting

- **Missing DAGs in Airflow:** Ensure the scheduler is running and the `dags` folder is properly mapped in `docker-compose.yaml`.
- **Playwright/Scraper Errors:** The pipeline requires Playwright browsers to be installed. This is handled during the `airflow-init` phase. If scraping fails, verify that the Playwright cache is successfully mounted.
- **Out of Memory (OOM):** Running 20+ spiders concurrently might consume high RAM. If containers crash, consider adjusting Airflow parallelism (`AIRFLOW__CORE__PARALLELISM`) or maximum active tasks in the `docker-compose.yaml` file.
