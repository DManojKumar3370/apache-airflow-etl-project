from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

dag = DAG(
    dag_id='conditional_workflow_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    description='Conditional workflow based on day of week'
)

def determine_branch(**context):
    """
    Determines which branch to execute based on day of week.
    Logic:
    - Monday (0) to Wednesday (2): return 'weekday_processing'
    - Thursday (3) to Friday (4): return 'end_of_week_processing'
    - Saturday (5) to Sunday (6): return 'weekend_processing'
    
    Args:
    context: Airflow context dictionary containing execution_date
    
    Returns:
    str: task_id of the next task to execute
    """
    try:
        execution_date = context['execution_date']
        day_of_week = execution_date.weekday()  # 0=Monday, 6=Sunday
        
        logger.info(f"Execution date: {execution_date}, Day of week: {day_of_week}")
        
        if day_of_week < 3:  # Monday-Wednesday
            logger.info("Branching to weekday processing")
            return 'weekday_processing'
        elif day_of_week < 5:  # Thursday-Friday
            logger.info("Branching to end-of-week processing")
            return 'end_of_week_processing'
        else:  # Saturday-Sunday
            logger.info("Branching to weekend processing")
            return 'weekend_processing'
            
    except Exception as e:
        logger.error(f"Error in determine_branch: {str(e)}")
        raise

def weekday_process():
    """
    Processes weekday-specific logic.
    Returns:
    dict: {
        'day_name': str,
        'task_type': 'weekday',
        'record_count': int
    }
    """
    try:
        logger.info("Executing weekday processing")
        result = {
            'day_name': 'Weekday (Mon-Wed)',
            'task_type': 'weekday',
            'record_count': 100,
            'status': 'completed'
        }
        logger.info(f"Weekday processing result: {result}")
        return result
    except Exception as e:
        logger.error(f"Error in weekday_process: {str(e)}")
        raise

def end_of_week_process():
    """
    Processes end-of-week specific logic.
    Returns:
    dict: {
        'day_name': str,
        'task_type': 'end_of_week',
        'weekly_summary': str
    }
    """
    try:
        logger.info("Executing end-of-week processing")
        result = {
            'day_name': 'End of Week (Thu-Fri)',
            'task_type': 'end_of_week',
            'weekly_summary': 'Week summary generated',
            'status': 'completed'
        }
        logger.info(f"End-of-week processing result: {result}")
        return result
    except Exception as e:
        logger.error(f"Error in end_of_week_process: {str(e)}")
        raise

def weekend_process():
    """
    Processes weekend-specific logic.
    Returns:
    dict: {
        'day_name': str,
        'task_type': 'weekend',
        'cleanup_status': str
    }
    """
    try:
        logger.info("Executing weekend processing")
        result = {
            'day_name': 'Weekend (Sat-Sun)',
            'task_type': 'weekend',
            'cleanup_status': 'cleanup completed',
            'status': 'completed'
        }
        logger.info(f"Weekend processing result: {result}")
        return result
    except Exception as e:
        logger.error(f"Error in weekend_process: {str(e)}")
        raise

# Start task
start_task = EmptyOperator(
    task_id='start',
    dag=dag
)

# Branching task
branch_task = BranchPythonOperator(
    task_id='branch_by_day',
    python_callable=determine_branch,
    dag=dag
)

# Weekday branch tasks
weekday_task = PythonOperator(
    task_id='weekday_processing',
    python_callable=weekday_process,
    dag=dag
)

weekday_summary_task = EmptyOperator(
    task_id='weekday_summary',
    dag=dag
)

# End of week branch tasks
end_of_week_task = PythonOperator(
    task_id='end_of_week_processing',
    python_callable=end_of_week_process,
    dag=dag
)

end_of_week_report_task = EmptyOperator(
    task_id='end_of_week_report',
    dag=dag
)

# Weekend branch tasks
weekend_task = PythonOperator(
    task_id='weekend_processing',
    python_callable=weekend_process,
    dag=dag
)

weekend_cleanup_task = EmptyOperator(
    task_id='weekend_cleanup',
    dag=dag
)

# End task with trigger rule to run after any branch
end_task = EmptyOperator(
    task_id='end',
    trigger_rule='none_failed_min_one_success',
    dag=dag
)

# Set up dependencies
start_task >> branch_task

# Weekday branch
branch_task >> weekday_task >> weekday_summary_task >> end_task

# End of week branch
branch_task >> end_of_week_task >> end_of_week_report_task >> end_task

# Weekend branch
branch_task >> weekend_task >> weekend_cleanup_task >> end_task
