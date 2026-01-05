from airflow.models import DagBag
import pytest
import os

def test_dag2_loaded():
    """
    Verifies that DAG 2 can be loaded without errors.
    """
    dag_folder = os.path.join(os.path.dirname(__file__), '../dags')
    dagbag = DagBag(dag_folder=dag_folder, include_examples=False)
    
    assert 'data_transformation_pipeline' in dagbag.dags
    assert len(dagbag.import_errors) == 0

def test_dag2_structure():
    """
    Verifies DAG 2 has the correct number of tasks.
    """
    dag_folder = os.path.join(os.path.dirname(__file__), '../dags')
    dagbag = DagBag(dag_folder=dag_folder, include_examples=False)
    dag = dagbag.dags['data_transformation_pipeline']
    
    assert len(dag.tasks) == 2, f"Expected 2 tasks, got {len(dag.tasks)}"
    
    task_ids = [task.task_id for task in dag.tasks]
    expected_task_ids = ['create_transformed_table', 'transform_and_load']
    
    for expected_id in expected_task_ids:
        assert expected_id in task_ids, f"Expected task '{expected_id}' not found"

def test_dag2_task_dependencies():
    """
    Verifies task dependencies are correctly configured.
    """
    dag_folder = os.path.join(os.path.dirname(__file__), '../dags')
    dagbag = DagBag(dag_folder=dag_folder, include_examples=False)
    dag = dagbag.dags['data_transformation_pipeline']
    
    # Get tasks
    create_task = dag.get_task('create_transformed_table')
    transform_task = dag.get_task('transform_and_load')
    
    # Verify dependencies
    assert transform_task in create_task.downstream_list, \
        "transform_and_load should be downstream of create_transformed_table"

def test_dag2_no_cycles():
    """
    Verifies DAG has no cycles.
    """
    dag_folder = os.path.join(os.path.dirname(__file__), '../dags')
    dagbag = DagBag(dag_folder=dag_folder, include_examples=False)
    dag = dagbag.dags['data_transformation_pipeline']
    
    # This will raise an exception if there's a cycle
    try:
        dag.test_cycle()
    except Exception as e:
        pytest.fail(f"DAG contains cycles: {str(e)}")

def test_dag2_schedule():
    """
    Verifies the schedule interval is correct.
    """
    dag_folder = os.path.join(os.path.dirname(__file__), '../dags')
    dagbag = DagBag(dag_folder=dag_folder, include_examples=False)
    dag = dagbag.dags['data_transformation_pipeline']
    
    assert dag.schedule_interval == '@daily', \
        f"Expected schedule interval '@daily', got '{dag.schedule_interval}'"

def test_dag2_catchup():
    """
    Verifies catchup is set to False.
    """
    dag_folder = os.path.join(os.path.dirname(__file__), '../dags')
    dagbag = DagBag(dag_folder=dag_folder, include_examples=False)
    dag = dagbag.dags['data_transformation_pipeline']
    
    assert dag.catchup == False, "Catchup should be False"

def test_dag2_start_date():
    """
    Verifies start_date is configured.
    """
    dag_folder = os.path.join(os.path.dirname(__file__), '../dags')
    dagbag = DagBag(dag_folder=dag_folder, include_examples=False)
    dag = dagbag.dags['data_transformation_pipeline']
    
    assert dag.start_date is not None, "start_date should not be None"

def test_dag2_tasks_are_operators():
    """
    Verifies all tasks are PythonOperators.
    """
    dag_folder = os.path.join(os.path.dirname(__file__), '../dags')
    dagbag = DagBag(dag_folder=dag_folder, include_examples=False)
    dag = dagbag.dags['data_transformation_pipeline']
    
    from airflow.operators.python import PythonOperator
    
    for task in dag.tasks:
        assert isinstance(task, PythonOperator), \
            f"Task {task.task_id} should be a PythonOperator"
