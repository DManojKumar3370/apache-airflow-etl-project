from datetime import datetime
import logging
import os
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
    dag_id="postgres_to_parquet_export",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@weekly",
    catchup=False,
    description="Export transformed data to Parquet",
)


def check_table_exists(table_name: str):
    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()
    cur = conn.cursor()

    cur.execute(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s);",
        (table_name,),
    )
    exists = cur.fetchone()[0]

    if not exists:
        cur.close()
        conn.close()
        raise ValueError(f"Table {table_name} does not exist")

    cur.execute(f"SELECT COUNT(*) FROM {table_name};")
    count = cur.fetchone()[0]

    cur.close()
    conn.close()

    if count == 0:
        raise ValueError(f"Table {table_name} has no rows")

    logger.info("Table %s exists with %s rows", table_name, count)
    return True


def export_table_to_parquet(table_name: str, output_path: str):
    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()

    df = pd.read_sql(f"SELECT * FROM {table_name};", conn)
    conn.close()

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    df.to_parquet(output_path, engine="pyarrow", compression="snappy")

    row_count = int(len(df))
    file_size = int(os.path.getsize(output_path))

    logger.info(
        "Exported %s rows from %s to %s (size=%s bytes)",
        row_count,
        table_name,
        output_path,
        file_size,
    )
    return {"file_path": output_path, "row_count": row_count, "file_size_bytes": file_size}


def validate_parquet(file_path: str):
    if not os.path.exists(file_path):
        raise FileNotFoundError(file_path)

    df = pd.read_parquet(file_path, engine="pyarrow")
    if len(df) == 0:
        raise ValueError("Parquet file has no rows")

    expected_cols = [
        "id",
        "name",
        "age",
        "city",
        "salary",
        "join_date",
        "full_info",
        "age_group",
        "salary_category",
        "year_joined",
    ]
    for col in expected_cols:
        if col not in df.columns:
            raise ValueError(f"Missing expected column {col} in Parquet file")

    logger.info("Parquet file %s validated successfully", file_path)
    return True


check_table_task = PythonOperator(
    task_id="check_source_table_exists",
    python_callable=check_table_exists,
    op_kwargs={"table_name": "transformed_employee_data"},
    dag=dag,
)

export_task = PythonOperator(
    task_id="export_to_parquet",
    python_callable=export_table_to_parquet,
    op_kwargs={
        "table_name": "transformed_employee_data",
        "output_path": "/opt/airflow/output/employee_data_{{ ds }}.parquet",
    },
    dag=dag,
)

validate_task = PythonOperator(
    task_id="validate_parquet_file",
    python_callable=validate_parquet,
    op_kwargs={"file_path": "/opt/airflow/output/employee_data_{{ ds }}.parquet"},
    dag=dag,
)

check_table_task >> export_task >> validate_task
