# Apache Airflow Data Processing Workflows with Docker
## Production‑ready ETL pipelines using Apache Airflow, Docker, and PostgreSQL
## 📋 Project Overview

This repository implements a complete **data engineering pipeline** orchestrated with **Apache Airflow** and fully containerized using **Docker Compose**. The solution demonstrates best practices for ETL/ELT workflows including ingestion, transformation, export, branching logic, notifications, testing, and observability.

**What you’ll learn:**

* Designing **multi‑DAG Airflow architectures**
* Managing **stateful services** with Docker
* Building **idempotent ETL pipelines**
* Writing **testable DAGs** with pytest
* Exporting analytics‑ready data (Parquet)

---

## ✨ Key Features

* **5 production‑ready DAGs** covering common orchestration patterns
* **Dockerized stack** for reproducible local development
* **PostgreSQL** for metadata and warehouse storage
* **Python‑based transformations** with clear business rules
* **Parquet exports** with compression (Snappy)
* **Conditional branching** using runtime context
* **Robust error handling** with success/failure callbacks
* **Unit tests** for DAG structure and utilities
* **LocalExecutor** for parallel task execution

---

## 📁 Project Structure

```text
airflow-project/
├── docker-compose.yml        # Docker services configuration
├── requirements.txt          # Python dependencies
├── README.md                 # Project documentation
├── .gitignore                # Git ignore rules
│
├── dags/                     # Airflow DAG definitions
│   ├── dag1_csv_to_postgres.py
│   ├── dag2_data_transformation.py
│   ├── dag3_postgres_to_parquet.py
│   ├── dag4_conditional_workflow.py
│   └── dag5_notification_workflow.py
│
├── tests/                    # Unit tests (pytest)
│   ├── __init__.py
│   ├── test_dag1.py
│   ├── test_dag2.py
│   └── test_utils.py
│
├── data/                     # Input datasets
│   └── input.csv             # 100+ employee records
│
├── output/                   # Generated artifacts
│   └── *.parquet
│
├── logs/                     # Airflow logs
│   └── airflow/
│
└── plugins/                  # Optional custom plugins
```

---

## 🔧 Prerequisites

* **Docker Desktop** (latest)
* **Docker Compose** (bundled with Docker Desktop)
* **Git**
* **Python 3.9+** (for local testing)

### Recommended System Requirements

| Resource | Minimum | Recommended |
| -------- | ------- | ----------- |
| RAM      | 8 GB    | 16 GB       |
| CPU      | 2 cores | 4+ cores    |
| Disk     | 10 GB   | 20 GB       |

---

## 🚀 Quick Start

### 1️⃣ Clone the Repository

```bash
git clone https://github.com/yourusername/airflow-data-pipeline.git
cd airflow-data-pipeline
```

### 2️⃣ Start the Stack

```bash
# Build and start all services
docker-compose up -d

# Follow logs (optional)
docker-compose logs -f
```

> ⏳ **First startup may take 2–3 minutes** while images initialize.

### 3️⃣ Verify Containers

```bash
docker-compose ps
```

All services should be **running** and **healthy**.

### 4️⃣ Access Airflow UI

* **URL:** [http://localhost:8080](http://localhost:8080)
* **Username:** `airflow`
* **Password:** `airflow`

---

## 🧪 Running Unit Tests

### Option A: Local (Recommended)

```bash
pip install -r requirements.txt
pytest tests/ -v --cov=dags
```

### Option B: Inside Docker

```bash
docker-compose exec airflow-webserver pytest /opt/airflow/tests/ -v
```

---

## 📊 DAG Catalog

### DAG 1 — CSV → PostgreSQL Ingestion

* **ID:** `csv_to_postgres_ingestion`
* **Schedule:** `@daily`
* **Purpose:** Load raw employee data
* **Key Traits:** Idempotent, schema‑safe

**Tasks**

1. Create table if not exists
2. Truncate existing data
3. Load CSV and return row count

---

### DAG 2 — Data Transformation Pipeline

* **ID:** `data_transformation_pipeline`
* **Schedule:** `@daily`
* **Purpose:** Apply business logic

**Transformations**

* `full_info` → `Name - City`
* `age_group` → Young / Mid / Senior
* `salary_category` → Low / Medium / High
* `year_joined` → Extracted from join date

---

### DAG 3 — PostgreSQL → Parquet Export

* **ID:** `postgres_to_parquet_export`
* **Schedule:** `@weekly`
* **Output:** `/opt/airflow/output/employee_data_YYYY-MM-DD.parquet`

**Features**

* Data existence checks
* Snappy compression
* File integrity validation

---

### DAG 4 — Conditional Workflow

* **ID:** `conditional_workflow_pipeline`
* **Schedule:** `@daily`

**Branching Logic**

* Mon–Wed → Weekday branch
* Thu–Fri → End‑of‑week branch
* Sat–Sun → Weekend branch

---

### DAG 5 — Notifications & Callbacks

* **ID:** `notification_workflow`
* **Schedule:** `@daily`

**Behavior**

* Executes a risky task
* Sends success or failure notifications
* Always runs cleanup

---

## 🗄️ PostgreSQL Access

```bash
docker-compose exec postgres psql -U airflow_user -d airflow_db
```

Useful commands:

```sql
\dt;
SELECT COUNT(*) FROM raw_employee_data;
\q
```

**Credentials**

* DB: `airflow_db`
* User: `airflow_user`
* Password: `airflow_pass`
* Host: `postgres`
* Port: `5432`

---

## 🐳 Common Docker Commands

```bash
docker-compose up -d        # Start services
docker-compose down         # Stop services
docker-compose down -v      # Reset volumes
docker-compose ps           # Service status
docker-compose logs -f      # View logs
docker-compose exec airflow-webserver bash
```

---

## 🔧 Troubleshooting

### Airflow UI not loading

```bash
docker-compose restart airflow-webserver
docker-compose logs airflow-webserver
```

### Database connection issues

```bash
docker-compose restart postgres airflow-webserver
```

### DAGs not visible

```bash
docker-compose restart airflow-scheduler
```

### Test failures

```bash
pytest tests/ -v -s
```

---

## 📚 References

* Apache Airflow Documentation
* Docker Documentation
* PostgreSQL Documentation
* PyArrow & Pandas Documentation

---

## ✅ Verification Checklist

* [ ] All 5 DAGs visible in Airflow UI
* [ ] Docker stack starts without errors
* [ ] Unit tests pass
* [ ] PostgreSQL tables created
* [ ] Parquet files generated
* [ ] README accurate and complete

---

## 📜 License

Created for **educational purposes** as part of the **Partnr Network – Global Placement Program**.

---

## 🎉 Summary

This project demonstrates **real‑world data engineering skills**:

* ETL/ELT design with Airflow
* Docker‑based deployment
* Relational data modeling
* Analytics‑ready exports
* Testing and observability

**Version:** 1.1
**Last Updated:** January 2026
**Status:** Production Ready ✅

---

🚀 *Ready to deploy, extend, and showcase in interviews!*
