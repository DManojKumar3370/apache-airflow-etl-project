from datetime import datetime
import logging
import pandas as pd

logger = logging.getLogger(__name__)

try:
    from airflow import DAG  # type: ignore
    from airflow.operators.python import PythonOperator
    from airflow.providers.postgres.hooks.postgres import PostgresHook
except ModuleNotFoundError:
    class DAG:
        def __init__(self, **kwargs):
            self.dag_id = kwargs.get("dag_id")
            self.schedule_interval = kwargs.get("schedule_interval")
            self.catchup = kwargs.get("catchup", False)
            self.description = kwargs.get("description")
            self.tasks = []

        def __rshift__(self, other):
            return other

    class PythonOperator:
        def __init__(self, python_callable=None, task_id=None, dag=None, **_):
            self.python_callable = python_callable
            self.task_id = task_id
            self.downstream = []
            if dag is not None and hasattr(dag, "tasks"):
                dag.tasks.append(self)

        def __rshift__(self, other):
            self.downstream.append(other)
            return other

    class PostgresHook:
        def __init__(self, *_, **__):
            pass

        def get_conn(self):
            raise RuntimeError("PostgresHook stub: no real DB connection in test mode")


dag = DAG(
    dag_id="csv_to_postgres_ingestion",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    description="Ingest employee data from CSV into PostgreSQL",
)


def create_employee_table():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()
    cur = conn.cursor()

    sql = """
    CREATE TABLE IF NOT EXISTS raw_employee_data (
        id INTEGER PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        age INTEGER NOT NULL,
        city VARCHAR(100) NOT NULL,
        salary FLOAT NOT NULL,
        join_date DATE NOT NULL
    );
    """
    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()
    logger.info("raw_employee_data table ensured")


def truncate_employee_table():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE raw_employee_data;")
    conn.commit()
    cur.close()
    conn.close()
    logger.info("raw_employee_data truncated")


def load_csv_data():
    csv_path = "/opt/airflow/data/input.csv"
    df = pd.read_csv(csv_path)

    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()
    cur = conn.cursor()

    rows_inserted = 0
    insert_sql = """
    INSERT INTO raw_employee_data (id, name, age, city, salary, join_date)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (id) DO UPDATE
    SET name = EXCLUDED.name,
        age = EXCLUDED.age,
        city = EXCLUDED.city,
        salary = EXCLUDED.salary,
        join_date = EXCLUDED.join_date;
    """

    for _, row in df.iterrows():
        cur.execute(
            insert_sql,
            (
                int(row["id"]),
                str(row["name"]),
                int(row["age"]),
                str(row["city"]),
                float(row["salary"]),
                str(row["join_date"]),
            ),
        )
        rows_inserted += 1

    conn.commit()
    cur.close()
    conn.close()
    logger.info("Inserted %s rows into raw_employee_data", rows_inserted)
    return rows_inserted


create_table_task = PythonOperator(
    task_id="create_table_if_not_exists",
    python_callable=create_employee_table,
    dag=dag,
)

truncate_table_task = PythonOperator(
    task_id="truncate_table",
    python_callable=truncate_employee_table,
    dag=dag,
)

load_csv_task = PythonOperator(
    task_id="load_csv_to_postgres",
    python_callable=load_csv_data,
    dag=dag,
)

create_table_task >> truncate_table_task >> load_csv_task
