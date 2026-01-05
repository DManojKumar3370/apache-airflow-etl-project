from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

dag = DAG(
    dag_id='notification_workflow',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    description='Workflow with success/failure notifications'
)

def send_success_notification(context):
    """
    Callback function executed on task success.
    Args:
    context: Airflow context containing task instance info
    
    Returns:
    dict: {
        'notification_type': 'success',
        'status': 'sent',
        'message': str,
        'timestamp': str
    }
    """
    try:
        task_instance = context['task_instance']
        execution_date = context['execution_date']
        
        notification = {
            'notification_type': 'success',
            'status': 'sent',
            'message': f"Task {task_instance.task_id} completed successfully",
            'timestamp': str(execution_date),
            'task_id': task_instance.task_id
        }
        
        logger.info(f"SUCCESS NOTIFICATION: {notification}")
        return notification
        
    except Exception as e:
        logger.error(f"Error sending success notification: {str(e)}")
        raise

def send_failure_notification(context):
    """
    Callback function executed on task failure.
    Args:
    context: Airflow context containing task instance and exception info
    
    Returns:
    dict: {
        'notification_type': 'failure',
        'status': 'sent',
        'message': str,
        'error': str,
        'timestamp': str
    }
    """
    try:
        task_instance = context['task_instance']
        execution_date = context['execution_date']
        
        notification = {
            'notification_type': 'failure',
            'status': 'sent',
            'message': f"Task {task_instance.task_id} failed",
            'error': str(context.get('exception', 'Unknown error')),
            'timestamp': str(execution_date),
            'task_id': task_instance.task_id
        }
        
        logger.error(f"FAILURE NOTIFICATION: {notification}")
        return notification
        
    except Exception as e:
        logger.error(f"Error sending failure notification: {str(e)}")
        raise

def risky_operation(**context):
    """
    Simulates an operation that may fail based on execution date.
    Failure logic: Fails if the day of month is divisible by 5.
    
    Args:
    context: Airflow context containing execution_date
    
    Returns:
    dict: {
        'status': 'success' or 'failed',
        'execution_date': str,
        'success': bool
    }
    
    Raises:
    Exception: If failure condition is met
    """
    try:
        execution_date = context['execution_date']
        day_of_month = execution_date.day
        
        logger.info(f"Risky operation executing on day {day_of_month}")
        
        # Fail if day is divisible by 5
        if day_of_month % 5 == 0:
            error_message = f"Risky operation failed on day {day_of_month} (divisible by 5)"
            logger.error(error_message)
            raise Exception(error_message)
        
        result = {
            'status': 'success',
            'execution_date': str(execution_date),
            'success': True,
            'day_of_month': day_of_month
        }
        
        logger.info(f"Risky operation completed successfully: {result}")
        return result
        
    except Exception as e:
        logger.error(f"Error in risky_operation: {str(e)}")
        raise

def cleanup_task():
    """
    Cleanup task that always executes regardless of success/failure.
    
    Returns:
    dict: {
        'cleanup_status': 'completed',
        'timestamp': str
    }
    """
    try:
        cleanup_result = {
            'cleanup_status': 'completed',
            'timestamp': str(datetime.now()),
            'message': 'Cleanup operations finished'
        }
        
        logger.info(f"Cleanup task executed: {cleanup_result}")
        return cleanup_result
        
    except Exception as e:
        logger.error(f"Error in cleanup_task: {str(e)}")
        raise

# Start task
start_task = EmptyOperator(
    task_id='start_task',
    dag=dag
)

# Risky operation with callbacks
risky_task = PythonOperator(
    task_id='risky_operation',
    python_callable=risky_operation,
    on_success_callback=send_success_notification,
    on_failure_callback=send_failure_notification,
    dag=dag
)

# Success notification task
success_notification_task = EmptyOperator(
    task_id='success_notification',
    trigger_rule='all_success',
    dag=dag
)

# Failure notification task
failure_notification_task = EmptyOperator(
    task_id='failure_notification',
    trigger_rule='all_failed',
    dag=dag
)

# Always execute cleanup
always_execute_task = PythonOperator(
    task_id='always_execute',
    python_callable=cleanup_task,
    trigger_rule='all_done',
    dag=dag
)

# Set dependencies
start_task >> risky_task >> [success_notification_task, failure_notification_task] >> always_execute_task
