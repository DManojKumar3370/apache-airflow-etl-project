# Apache Airflow ETL Project

## Production-ready Data Workflows with Python, PostgreSQL, and (Optional) Docker

---

## 📋 Project Overview

This repository implements a complete **data engineering pipeline** orchestrated using **Apache Airflow-style DAGs**. The project is carefully designed so that:

- DAG code can be developed and tested locally  
- Unit tests run **without requiring Airflow or a real PostgreSQL instance**  
- The same code remains compatible with a full Airflow + Docker deployment if available  

### What this project demonstrates:

- Designing **multi-DAG data workflows**
- Building **idempotent ETL pipelines**
- Writing **testable DAG code** using pytest
- Implementing **business rules** in Python
- Exporting analytics-ready data to **Parquet format**

---

## ✨ Key Features

The project showcases **5 DAGs** covering common orchestration patterns:

- CSV → PostgreSQL ingestion  
- Data transformation and enrichment  
- PostgreSQL → Parquet export  
- Conditional branching workflows  
- Notification and callback pipelines  

### Technical Highlights

- Test-friendly DAG design
- Clear Python-based transformations
- Parquet exports with Snappy compression
- Examples using:
    - PythonOperator  
    - BranchPythonOperator  
    - EmptyOperator  
- Proper logging and idempotency

---

## 📁 Project Structure

```
apache-airflow-etl-project/
├── docker-compose.yml
├── requirements.txt
├── README.md
├── .gitignore
├── dags/
│   ├── dag1_csv_to_postgres.py
│   ├── dag2_data_transformation.py
│   ├── dag3_postgres_to_parquet.py
│   ├── dag4_conditional_workflow.py
│   └── dag5_notification_workflow.py
├── tests/
│   ├── __init__.py
│   ├── test_dag1.py
│   ├── test_dag2.py
│   └── test_utils.py
├── data/
│   └── input.csv
├── output/
│   └── *.parquet
├── logs/
└── plugins/
```

## 🔧 Prerequisites

### For Local Testing (Required)
- Python 3.10+
- pip
- Git

### For Full Stack Deployment (Optional)
- Docker Desktop with Docker Compose

### System Requirements

| Resource | Minimum | Recommended |
|----------|---------|-------------|
| RAM      | 8 GB    | 16 GB       |
| CPU      | 2 cores | 4+ cores    |
| Disk     | 10 GB   | 20 GB       |

## 🚀 Setup & Quick Start

### 1️⃣ Clone the Repository

```bash
git clone <repository-url>
cd apache-airflow-etl-project
```

## 🧪 Running Unit Tests

```bash
python -m pytest tests/ -v
```

**test_dag1.py** — Module imports, DAG object, callable functions

**test_dag2.py** — Module imports, DAG object, callable functions

**test_utils.py** — DAG files exist, valid dag_ids, task validation

## 📊 DAG Catalog

### DAG 1 — CSV → PostgreSQL Ingestion

- **DAG ID:** csv_to_postgres_ingestion
- **Schedule:** @daily

| Column    | Type         |
|-----------|--------------|
| id        | INTEGER (PK) |
| name      | VARCHAR      |
| age       | INTEGER      |
| city      | VARCHAR      |
| salary    | FLOAT        |
| join_date | DATE         |

### DAG 2 — Data Transformation Pipeline

- **DAG ID:** data_transformation_pipeline
- **Schedule:** @daily

**Transformations:** full_info, age_group, salary_category, year_joined

### DAG 3 — PostgreSQL → Parquet Export

- **DAG ID:** postgres_to_parquet_export
- **Schedule:** @weekly
- **Output:** /opt/airflow/output/employee_data_YYYY-MM-DD.parquet

### DAG 4 — Conditional Workflow

- **DAG ID:** conditional_workflow_pipeline
- **Schedule:** @daily

| Days        | Branch                   |
|-------------|--------------------------|
| Mon–Wed     | weekday_processing       |
| Thu–Fri     | end_of_week_processing   |
| Sat–Sun     | weekend_processing       |

### DAG 5 — Notifications & Callbacks

- **DAG ID:** notification_workflow
- **Schedule:** @daily

## 🐳 Optional: Run with Docker + Airflow

```bash
docker-compose up -d
```

**Airflow UI:** http://localhost:8080 (airflow / airflow)

## ✅ Verification Checklist

- [ ] All 5 DAG files exist under `dags/`
- [ ] `data/input.csv` exists with ≥100 rows
- [ ] `python -m pytest tests/ -v` passes
- [ ] README explains setup, testing, and DAG behavior
