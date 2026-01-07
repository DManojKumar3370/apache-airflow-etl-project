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
    dag_id="data_transformation_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    description="Transform raw employee data into enriched table",
)


def create_transformed_table():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()
    cur = conn.cursor()

    sql = """
    CREATE TABLE IF NOT EXISTS transformed_employee_data (
        id INTEGER PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        age INTEGER NOT NULL,
        city VARCHAR(100) NOT NULL,
        salary FLOAT NOT NULL,
        join_date DATE NOT NULL,
        full_info VARCHAR(500),
        age_group VARCHAR(20),
        salary_category VARCHAR(20),
        year_joined INTEGER
    );
    """
    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()
    logger.info("transformed_employee_data table ensured")


def transform_data():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()

    df = pd.read_sql("SELECT * FROM raw_employee_data;", conn)
    logger.info("Read %s rows from raw_employee_data", len(df))

    df["full_info"] = df["name"] + " - " + df["city"]

    def _age_group(age):
        if age < 30:
            return "Young"
        if age < 50:
            return "Mid"
        return "Senior"

    df["age_group"] = df["age"].apply(_age_group)

    def _salary_cat(s):
        if s < 50000:
            return "Low"
        if s < 80000:
            return "Medium"
        return "High"

    df["salary_category"] = df["salary"].apply(_salary_cat)

    df["join_date"] = pd.to_datetime(df["join_date"])
    df["year_joined"] = df["join_date"].dt.year

    cur = conn.cursor()
    rows_inserted = 0
    insert_sql = """
    INSERT INTO transformed_employee_data
    (id, name, age, city, salary, join_date,
     full_info, age_group, salary_category, year_joined)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (id) DO UPDATE SET
        name = EXCLUDED.name,
        age = EXCLUDED.age,
        city = EXCLUDED.city,
        salary = EXCLUDED.salary,
        join_date = EXCLUDED.join_date,
        full_info = EXCLUDED.full_info,
        age_group = EXCLUDED.age_group,
        salary_category = EXCLUDED.salary_category,
        year_joined = EXCLUDED.year_joined;
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
                row["join_date"].date(),
                str(row["full_info"]),
                str(row["age_group"]),
                str(row["salary_category"]),
                int(row["year_joined"]),
            ),
        )
        rows_inserted += 1

    conn.commit()
    cur.close()
    conn.close()
    logger.info("Transformed %s rows into transformed_employee_data", rows_inserted)
    return {"rows_processed": int(len(df)), "rows_inserted": int(rows_inserted)}


create_transformed_table_task = PythonOperator(
    task_id="create_transformed_table",
    python_callable=create_transformed_table,
    dag=dag,
)

transform_and_load_task = PythonOperator(
    task_id="transform_and_load",
    python_callable=transform_data,
    dag=dag,
)

create_transformed_table_task >> transform_and_load_task
