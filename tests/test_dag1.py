from airflow.models import DagBag
import pytest
import os

def test_dag1_loaded():
    """
    Verifies that DAG 1 can be loaded without errors.
    """
    dag_folder = os.path.join(os.path.dirname(__file__), '../dags')
    dagbag = DagBag(dag_folder=dag_folder, include_examples=False)
    
    assert 'csv_to_postgres_ingestion' in dagbag.dags
    assert len(dagbag.import_errors) == 0

def test_dag1_structure():
    """
    Verifies DAG 1 has the correct number of tasks.
    """
    dag_folder = os.path.join(os.path.dirname(__file__), '../dags')
    dagbag = DagBag(dag_folder=dag_folder, include_examples=False)
    dag = dagbag.dags['csv_to_postgres_ingestion']
    
    assert len(dag.tasks) == 3, f"Expected 3 tasks, got {len(dag.tasks)}"
    
    task_ids = [task.task_id for task in dag.tasks]
    expected_task_ids = ['create_table_if_not_exists', 'truncate_table', 'load_csv_to_postgres']
    
    for expected_id in expected_task_ids:
        assert expected_id in task_ids, f"Expected task '{expected_id}' not found"

def test_dag1_task_dependencies():
    """
    Verifies task dependencies are correctly configured.
    """
    dag_folder = os.path.join(os.path.dirname(__file__), '../dags')
    dagbag = DagBag(dag_folder=dag_folder, include_examples=False)
    dag = dagbag.dags['csv_to_postgres_ingestion']
    
    # Get tasks
    create_task = dag.get_task('create_table_if_not_exists')
    truncate_task = dag.get_task('truncate_table')
    load_task = dag.get_task('load_csv_to_postgres')
    
    # Verify dependencies
    assert truncate_task in create_task.downstream_list, \
        "truncate_table should be downstream of create_table_if_not_exists"
    assert load_task in truncate_task.downstream_list, \
        "load_csv_to_postgres should be downstream of truncate_table"

def test_dag1_no_cycles():
    """
    Verifies DAG has no cycles.
    """
    dag_folder = os.path.join(os.path.dirname(__file__), '../dags')
    dagbag = DagBag(dag_folder=dag_folder, include_examples=False)
    dag = dagbag.dags['csv_to_postgres_ingestion']
    
    # This will raise an exception if there's a cycle
    try:
        dag.test_cycle()
    except Exception as e:
        pytest.fail(f"DAG contains cycles: {str(e)}")

def test_dag1_schedule():
    """
    Verifies the schedule interval is correct.
    """
    dag_folder = os.path.join(os.path.dirname(__file__), '../dags')
    dagbag = DagBag(dag_folder=dag_folder, include_examples=False)
    dag = dagbag.dags['csv_to_postgres_ingestion']
    
    assert dag.schedule_interval == '@daily', \
        f"Expected schedule interval '@daily', got '{dag.schedule_interval}'"

def test_dag1_catchup():
    """
    Verifies catchup is set to False.
    """
    dag_folder = os.path.join(os.path.dirname(__file__), '../dags')
    dagbag = DagBag(dag_folder=dag_folder, include_examples=False)
    dag = dagbag.dags['csv_to_postgres_ingestion']
    
    assert dag.catchup == False, "Catchup should be False"

def test_dag1_start_date():
    """
    Verifies start_date is configured.
    """
    dag_folder = os.path.join(os.path.dirname(__file__), '../dags')
    dagbag = DagBag(dag_folder=dag_folder, include_examples=False)
    dag = dagbag.dags['csv_to_postgres_ingestion']
    
    assert dag.start_date is not None, "start_date should not be None"

def test_dag1_tasks_are_operators():
    """
    Verifies all tasks are PythonOperators.
    """
    dag_folder = os.path.join(os.path.dirname(__file__), '../dags')
    dagbag = DagBag(dag_folder=dag_folder, include_examples=False)
    dag = dagbag.dags['csv_to_postgres_ingestion']
    
    from airflow.operators.python import PythonOperator
    
    for task in dag.tasks:
        assert isinstance(task, PythonOperator), \
            f"Task {task.task_id} should be a PythonOperator"
