from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import logging

logger = logging.getLogger(__name__)

# Define your DAG with the required configuration
dag = DAG(
    dag_id='csv_to_postgres_ingestion',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    description='Ingest employee data from CSV into PostgreSQL'
)

def create_employee_table():
    """
    Creates the raw_employee_data table in PostgreSQL if it doesn't exist.
    Table schema:
    - id: INTEGER PRIMARY KEY
    - name: VARCHAR(255)
    - age: INTEGER
    - city: VARCHAR(100)
    - salary: FLOAT
    - join_date: DATE
    
    Use PostgresHook to connect and execute SQL.
    """
    try:
        hook = PostgresHook(postgres_conn_id='postgres_default')
        connection = hook.get_conn()
        cursor = connection.cursor()
        
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS raw_employee_data (
            id INTEGER PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            age INTEGER NOT NULL,
            city VARCHAR(100) NOT NULL,
            salary FLOAT NOT NULL,
            join_date DATE NOT NULL
        );
        """
        
        cursor.execute(create_table_sql)
        connection.commit()
        cursor.close()
        connection.close()
        logger.info("Table 'raw_employee_data' created successfully (or already exists)")
        
    except Exception as e:
        logger.error(f"Error creating table: {str(e)}")
        raise

def truncate_employee_table():
    """
    Truncates the raw_employee_data table to ensure idempotency.
    This allows the DAG to be run multiple times without duplicating data.
    Use TRUNCATE TABLE command.
    """
    try:
        hook = PostgresHook(postgres_conn_id='postgres_default')
        connection = hook.get_conn()
        cursor = connection.cursor()
        
        truncate_sql = "TRUNCATE TABLE raw_employee_data;"
        cursor.execute(truncate_sql)
        connection.commit()
        cursor.close()
        connection.close()
        logger.info("Table 'raw_employee_data' truncated successfully")
        
    except Exception as e:
        logger.error(f"Error truncating table: {str(e)}")
        raise

def load_csv_data():
    """
    Loads data from /opt/airflow/data/input.csv into raw_employee_data table.
    Steps:
    1. Read CSV file using pandas
    2. Use pandas to_sql or iterate and insert rows
    3. Return the number of rows inserted
    
    Returns:
    int: Number of rows inserted
    """
    try:
        # Read CSV file
        csv_file_path = '/opt/airflow/data/input.csv'
        df = pd.read_csv(csv_file_path)
        
        logger.info(f"Read {len(df)} rows from CSV file")
        
        # Connect to PostgreSQL
        hook = PostgresHook(postgres_conn_id='postgres_default')
        connection = hook.get_conn()
        cursor = connection.cursor()
        
        # Insert data into raw_employee_data
        rows_inserted = 0
        for index, row in df.iterrows():
            insert_sql = """
            INSERT INTO raw_employee_data (id, name, age, city, salary, join_date)
            VALUES (%s, %s, %s, %s, %s, %s);
            """
            try:
                cursor.execute(insert_sql, (
                    int(row['id']),
                    str(row['name']),
                    int(row['age']),
                    str(row['city']),
                    float(row['salary']),
                    str(row['join_date'])
                ))
                rows_inserted += 1
            except Exception as e:
                logger.warning(f"Error inserting row {index}: {str(e)}")
        
        connection.commit()
        cursor.close()
        connection.close()
        
        logger.info(f"Successfully inserted {rows_inserted} rows into 'raw_employee_data'")
        return rows_inserted
        
    except Exception as e:
        logger.error(f"Error loading CSV data: {str(e)}")
        raise

# Define tasks using PythonOperator
create_table_task = PythonOperator(
    task_id='create_table_if_not_exists',
    python_callable=create_employee_table,
    dag=dag
)

truncate_table_task = PythonOperator(
    task_id='truncate_table',
    python_callable=truncate_employee_table,
    dag=dag
)

load_csv_task = PythonOperator(
    task_id='load_csv_to_postgres',
    python_callable=load_csv_data,
    dag=dag
)

# Set task dependencies
create_table_task >> truncate_table_task >> load_csv_task
