from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import os
import logging

logger = logging.getLogger(__name__)

dag = DAG(
    dag_id='postgres_to_parquet_export',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@weekly',
    catchup=False,
    description='Export transformed data to Parquet format'
)

def check_table_exists(table_name: str):
    """
    Checks if the specified table exists and contains data.
    Args:
    table_name: Name of the table to check
    
    Returns:
    bool: True if table exists and has data, raises exception otherwise
    """
    try:
        hook = PostgresHook(postgres_conn_id='postgres_default')
        connection = hook.get_conn()
        cursor = connection.cursor()
        
        # Check if table exists
        check_table_sql = """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_name = %s
        );
        """
        cursor.execute(check_table_sql, (table_name,))
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            raise Exception(f"Table '{table_name}' does not exist")
        
        # Check if table has at least one row
        count_sql = f"SELECT COUNT(*) FROM {table_name};"
        cursor.execute(count_sql)
        row_count = cursor.fetchone()[0]
        
        if row_count == 0:
            raise Exception(f"Table '{table_name}' is empty")
        
        cursor.close()
        connection.close()
        
        logger.info(f"Table '{table_name}' exists with {row_count} rows")
        return True
        
    except Exception as e:
        logger.error(f"Error checking table: {str(e)}")
        raise

def export_table_to_parquet(table_name: str, output_path: str):
    """
    Exports a PostgreSQL table to Parquet format.
    Args:
    table_name: Name of the source table
    output_path: Full path where Parquet file should be saved
    
    Returns:
    dict: {
        'file_path': str,
        'row_count': int,
        'file_size_bytes': int
    }
    """
    try:
        # Render output_path with Airflow variables
        output_path = output_path.replace('{{ ds }}', datetime.now().strftime('%Y-%m-%d'))
        
        # Create output directory if it doesn't exist
        output_dir = os.path.dirname(output_path)
        os.makedirs(output_dir, exist_ok=True)
        
        # Read table data using pandas
        hook = PostgresHook(postgres_conn_id='postgres_default')
        connection = hook.get_conn()
        read_sql = f"SELECT * FROM {table_name};"
        df = pd.read_sql(read_sql, connection)
        connection.close()
        
        logger.info(f"Read {len(df)} rows from '{table_name}'")
        
        # Write to Parquet using pyarrow engine with snappy compression
        df.to_parquet(output_path, engine='pyarrow', compression='snappy', index=False)
        
        # Get file statistics
        file_size_bytes = os.path.getsize(output_path)
        row_count = len(df)
        
        logger.info(f"Successfully exported to {output_path} ({file_size_bytes} bytes)")
        
        # Return metadata dictionary
        return {
            'file_path': output_path,
            'row_count': row_count,
            'file_size_bytes': file_size_bytes
        }
        
    except Exception as e:
        logger.error(f"Error exporting to parquet: {str(e)}")
        raise

def validate_parquet(file_path: str):
    """
    Validates that the Parquet file is readable and has correct schema.
    Args:
    file_path: Path to the Parquet file
    
    Returns:
    bool: True if validation passes, raises exception otherwise
    """
    try:
        # Render file_path with Airflow variables
        file_path = file_path.replace('{{ ds }}', datetime.now().strftime('%Y-%m-%d'))
        
        # Read Parquet file
        df = pd.read_parquet(file_path)
        
        # Verify expected columns exist
        expected_columns = ['id', 'name', 'age', 'city', 'salary', 'join_date', 
                          'full_info', 'age_group', 'salary_category', 'year_joined']
        
        for col in expected_columns:
            if col not in df.columns:
                raise Exception(f"Expected column '{col}' not found in Parquet file")
        
        # Verify row count > 0
        if len(df) == 0:
            raise Exception("Parquet file contains no rows")
        
        logger.info(f"Parquet file validation passed. File contains {len(df)} rows with correct schema")
        return True
        
    except Exception as e:
        logger.error(f"Error validating parquet: {str(e)}")
        raise

# Define tasks with parameters
check_table_task = PythonOperator(
    task_id='check_source_table_exists',
    python_callable=check_table_exists,
    op_kwargs={'table_name': 'transformed_employee_data'},
    dag=dag
)

export_task = PythonOperator(
    task_id='export_to_parquet',
    python_callable=export_table_to_parquet,
    op_kwargs={
        'table_name': 'transformed_employee_data',
        'output_path': '/opt/airflow/output/employee_data_{{ ds }}.parquet'
    },
    dag=dag
)

validate_task = PythonOperator(
    task_id='validate_parquet_file',
    python_callable=validate_parquet,
    op_kwargs={'file_path': '/opt/airflow/output/employee_data_{{ ds }}.parquet'},
    dag=dag
)

# Set dependencies
check_table_task >> export_task >> validate_task
