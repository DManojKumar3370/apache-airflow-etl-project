from datetime import datetime
import logging

logger = logging.getLogger(__name__)

try:
    from airflow import DAG  # type: ignore
    from airflow.operators.python import PythonOperator
    from airflow.operators.empty import EmptyOperator
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

    class EmptyOperator:
        def __init__(self, task_id=None, dag=None, **_):
            self.task_id = task_id
            self.downstream = []
            if dag is not None and hasattr(dag, "tasks"):
                dag.tasks.append(self)

        def __rshift__(self, other):
            self.downstream.append(other)
            return other


dag = DAG(
    dag_id="notification_workflow",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    description="Success/failure notification workflow",
)


def send_success_notification(context):
    ti = context.get("task_instance")
    ts = context.get("ts")
    message = f"Task {ti.task_id if ti else 'unknown'} succeeded at {ts}"
    logger.info(message)
    return {
        "notification_type": "success",
        "status": "sent",
        "message": message,
        "timestamp": str(ts),
    }


def send_failure_notification(context):
    ti = context.get("task_instance")
    ts = context.get("ts")
    err = context.get("exception")
    message = f"Task {ti.task_id if ti else 'unknown'} failed at {ts}"
    logger.error(message)
    return {
        "notification_type": "failure",
        "status": "sent",
        "message": message,
        "error": str(err),
        "timestamp": str(ts),
    }


def risky_operation(**context):
    execution_date = context.get("execution_date")
    if execution_date is None:
        raise ValueError("execution_date missing from context")

    day = execution_date.day
    if day % 5 == 0:
        raise Exception(f"Simulated failure for day {day}")

    return {
        "status": "success",
        "execution_date": str(execution_date),
        "success": True,
    }


def cleanup_task():
    now = datetime.utcnow().isoformat()
    logger.info("Cleanup completed at %s", now)
    return {"cleanup_status": "completed", "timestamp": now}


start_task = EmptyOperator(task_id="start_task", dag=dag)

risky_task = PythonOperator(
    task_id="risky_operation",
    python_callable=risky_operation,
    on_success_callback=send_success_notification,
    on_failure_callback=send_failure_notification,
    dag=dag,
)

success_notification_task = EmptyOperator(
    task_id="success_notification",
    trigger_rule="all_success",
    dag=dag,
)

failure_notification_task = EmptyOperator(
    task_id="failure_notification",
    trigger_rule="all_failed",
    dag=dag,
)

always_execute_task = PythonOperator(
    task_id="always_execute",
    python_callable=cleanup_task,
    trigger_rule="all_done",
    dag=dag,
)

start_task >> risky_task >> [success_notification_task, failure_notification_task]
[success_notification_task, failure_notification_task] >> always_execute_task
