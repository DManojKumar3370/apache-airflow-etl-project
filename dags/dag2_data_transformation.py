from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import logging

logger = logging.getLogger(__name__)

dag = DAG(
    dag_id='data_transformation_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    description='Transform raw employee data and load into new table'
)

def create_transformed_table():
    """
    Creates the transformed_employee_data table with additional columns.
    Table schema:
    - All columns from raw_employee_data
    - full_info: VARCHAR(500) - concatenation of name and city
    - age_group: VARCHAR(20) - categorized age
    - salary_category: VARCHAR(20) - categorized salary
    - year_joined: INTEGER - extracted year from join_date
    """
    try:
        hook = PostgresHook(postgres_conn_id='postgres_default')
        connection = hook.get_conn()
        cursor = connection.cursor()
        
        create_table_sql = """
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
        
        cursor.execute(create_table_sql)
        connection.commit()
        cursor.close()
        connection.close()
        logger.info("Table 'transformed_employee_data' created successfully (or already exists)")
        
    except Exception as e:
        logger.error(f"Error creating transformed table: {str(e)}")
        raise

def transform_data():
    """
    Reads from raw_employee_data, applies transformations, loads to transformed_employee_data.
    Transformations:
    1. full_info = name + " - " + city
    2. age_group = "Young" if age < 30, "Mid" if 30 <= age < 50, else "Senior"
    3. salary_category = "Low" if salary < 50000, "Medium" if 50000 <= salary < 80000, else "High"
    4. year_joined = extract year from join_date
    
    Returns:
    dict: {
        'rows_processed': int,
        'rows_inserted': int
    }
    """
    try:
        hook = PostgresHook(postgres_conn_id='postgres_default')
        connection = hook.get_conn()
        cursor = connection.cursor()
        
        # Read data from raw_employee_data
        read_sql = "SELECT * FROM raw_employee_data;"
        df = pd.read_sql(read_sql, connection)
        
        logger.info(f"Read {len(df)} rows from 'raw_employee_data'")
        
        # Apply transformation logic
        df['full_info'] = df['name'] + ' - ' + df['city']
        
        # Age group categorization
        df['age_group'] = df['age'].apply(lambda x: 
            'Young' if x < 30 else 'Mid' if x < 50 else 'Senior'
        )
        
        # Salary category categorization
        df['salary_category'] = df['salary'].apply(lambda x: 
            'Low' if x < 50000 else 'Medium' if x < 80000 else 'High'
        )
        
        # Extract year from join_date
        df['join_date'] = pd.to_datetime(df['join_date'])
        df['year_joined'] = df['join_date'].dt.year
        
        # Insert into transformed_employee_data
        rows_inserted = 0
        for index, row in df.iterrows():
            insert_sql = """
            INSERT INTO transformed_employee_data 
            (id, name, age, city, salary, join_date, full_info, age_group, salary_category, year_joined)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
            full_info = EXCLUDED.full_info,
            age_group = EXCLUDED.age_group,
            salary_category = EXCLUDED.salary_category,
            year_joined = EXCLUDED.year_joined;
            """
            try:
                cursor.execute(insert_sql, (
                    int(row['id']),
                    str(row['name']),
                    int(row['age']),
                    str(row['city']),
                    float(row['salary']),
                    row['join_date'].date(),
                    str(row['full_info']),
                    str(row['age_group']),
                    str(row['salary_category']),
                    int(row['year_joined'])
                ))
                rows_inserted += 1
            except Exception as e:
                logger.warning(f"Error inserting transformed row {index}: {str(e)}")
        
        connection.commit()
        cursor.close()
        connection.close()
        
        logger.info(f"Successfully transformed and inserted {rows_inserted} rows")
        return {
            'rows_processed': len(df),
            'rows_inserted': rows_inserted
        }
        
    except Exception as e:
        logger.error(f"Error transforming data: {str(e)}")
        raise

# Define tasks
create_transformed_table_task = PythonOperator(
    task_id='create_transformed_table',
    python_callable=create_transformed_table,
    dag=dag
)

transform_and_load_task = PythonOperator(
    task_id='transform_and_load',
    python_callable=transform_data,
    dag=dag
)

# Set dependencies
create_transformed_table_task >> transform_and_load_task
