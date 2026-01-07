from datetime import datetime
import logging

logger = logging.getLogger(__name__)

try:
    from airflow import DAG  # type: ignore
    from airflow.operators.python import PythonOperator, BranchPythonOperator
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

    class BranchPythonOperator(PythonOperator):
        pass

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
    dag_id="conditional_workflow_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    description="Branching example based on day-of-week",
)


def determine_branch(**context):
    execution_date = context.get("execution_date")
    if execution_date is None:
        raise ValueError("execution_date missing from context")

    weekday = execution_date.weekday()  # 0=Mon, 6=Sun

    if 0 <= weekday <= 2:
        return "weekday_processing"
    if 3 <= weekday <= 4:
        return "end_of_week_processing"
    return "weekend_processing"


def weekday_process():
    return {"day_name": "weekday", "task_type": "weekday", "record_count": 0}


def end_of_week_process():
    return {
        "day_name": "end_of_week",
        "task_type": "end_of_week",
        "weekly_summary": "ok",
    }


def weekend_process():
    return {
        "day_name": "weekend",
        "task_type": "weekend",
        "cleanup_status": "pending",
    }


start_task = EmptyOperator(task_id="start", dag=dag)

branch_task = BranchPythonOperator(
    task_id="branch_by_day",
    python_callable=determine_branch,
    dag=dag,
)

weekday_task = PythonOperator(
    task_id="weekday_processing",
    python_callable=weekday_process,
    dag=dag,
)
weekday_summary_task = EmptyOperator(task_id="weekday_summary", dag=dag)

end_of_week_task = PythonOperator(
    task_id="end_of_week_processing",
    python_callable=end_of_week_process,
    dag=dag,
)
end_of_week_report_task = EmptyOperator(task_id="end_of_week_report", dag=dag)

weekend_task = PythonOperator(
    task_id="weekend_processing",
    python_callable=weekend_process,
    dag=dag,
)
weekend_cleanup_task = EmptyOperator(task_id="weekend_cleanup", dag=dag)

end_task = EmptyOperator(
    task_id="end",
    trigger_rule="none_failed_min_one_success",
    dag=dag,
)

start_task >> branch_task

branch_task >> weekday_task >> weekday_summary_task >> end_task
branch_task >> end_of_week_task >> end_of_week_report_task >> end_task
branch_task >> weekend_task >> weekend_cleanup_task >> end_task
