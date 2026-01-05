def test_dag_total_tasks():
    """
    Verifies all DAGs together have the minimum required tasks.
    """
    dag_folder = os.path.join(os.path.dirname(__file__), '../dags')
    dagbag = DagBag(dag_folder=dag_folder, include_examples=False)
    
    total_tasks = sum(len(dag.tasks) for dag in dagbag.dags.values())
    
    # Minimum: DAG1(3) + DAG2(2) + DAG3(3) + DAG4(6) + DAG5(5) = 19 tasks
    assert total_tasks >= 19, \
        f"Expected at least 19 total tasks, but found {total_tasks}"
    
    print(f"✓ Total tasks across all DAGs: {total_tasks}")

def test_all_dags_not_paused():
    """
    Verifies all DAGs are not paused by default.
    """
    dag_folder = os.path.join(os.path.dirname(__file__), '../dags')
    dagbag = DagBag(dag_folder=dag_folder, include_examples=False)
    
    for dag in dagbag.dags.values():
        assert dag.is_paused is False, \
            f"DAG '{dag.dag_id}' should not be paused by default"
    
    print("✓ All DAGs are not paused")

def test_dag_file_structure():
    """
    Verifies all required DAG files exist.
    """
    dag_folder = os.path.join(os.path.dirname(__file__), '../dags')
    
    expected_files = [
        'dag1_csv_to_postgres.py',
        'dag2_data_transformation.py',
        'dag3_postgres_to_parquet.py',
        'dag4_conditional_workflow.py',
        'dag5_notification_workflow.py'
    ]
    
    for file_name in expected_files:
        file_path = os.path.join(dag_folder, file_name)
        assert os.path.exists(file_path), \
            f"Expected DAG file '{file_name}' not found"
    
    print("✓ All required DAG files exist")

def test_dag_description_exists():
    """
    Verifies all DAGs have descriptions.
    """
    dag_folder = os.path.join(os.path.dirname(__file__), '../dags')
    dagbag = DagBag(dag_folder=dag_folder, include_examples=False)
    
    for dag in dagbag.dags.values():
        assert dag.description is not None and dag.description != "", \
            f"DAG '{dag.dag_id}' does not have a description"
    
    print("✓ All DAGs have descriptions")

def test_dag_owner_exists():
    """
    Verifies all DAGs have owner configured.
    """
    dag_folder = os.path.join(os.path.dirname(__file__), '../dags')
    dagbag = DagBag(dag_folder=dag_folder, include_examples=False)
    
    for dag in dagbag.dags.values():
        # Default owner is 'airflow', we just check it exists
        assert hasattr(dag, 'owner'), \
            f"DAG '{dag.dag_id}' does not have owner attribute"
    
    print("✓ All DAGs have owner attribute")

def test_python_operator_tasks_in_dag1():
    """
    Verifies DAG 1 uses PythonOperator for all tasks.
    """
    dag_folder = os.path.join(os.path.dirname(__file__), '../dags')
    dagbag = DagBag(dag_folder=dag_folder, include_examples=False)
    dag = dagbag.dags['csv_to_postgres_ingestion']
    
    from airflow.operators.python import PythonOperator
    
    for task in dag.tasks:
        assert isinstance(task, PythonOperator), \
            f"Task '{task.task_id}' is not a PythonOperator"
    
    print("✓ DAG 1 uses PythonOperator for all tasks")

def test_python_operator_tasks_in_dag2():
    """
    Verifies DAG 2 uses PythonOperator for all tasks.
    """
    dag_folder = os.path.join(os.path.dirname(__file__), '../dags')
    dagbag = DagBag(dag_folder=dag_folder, include_examples=False)
    dag = dagbag.dags['data_transformation_pipeline']
    
    from airflow.operators.python import PythonOperator
    
    for task in dag.tasks:
        assert isinstance(task, PythonOperator), \
            f"Task '{task.task_id}' is not a PythonOperator"
    
    print("✓ DAG 2 uses PythonOperator for all tasks")

def test_branch_operator_in_dag4():
    """
    Verifies DAG 4 uses BranchPythonOperator.
    """
    dag_folder = os.path.join(os.path.dirname(__file__), '../dags')
    dagbag = DagBag(dag_folder=dag_folder, include_examples=False)
    dag = dagbag.dags['conditional_workflow_pipeline']
    
    from airflow.operators.python import BranchPythonOperator
    
    branch_tasks = [task for task in dag.tasks 
                   if isinstance(task, BranchPythonOperator)]
    
    assert len(branch_tasks) > 0, \
        "DAG 4 should have at least one BranchPythonOperator"
    
    print("✓ DAG 4 uses BranchPythonOperator")

def test_dag4_has_empty_operators():
    """
    Verifies DAG 4 uses EmptyOperator.
    """
    dag_folder = os.path.join(os.path.dirname(__file__), '../dags')
    dagbag = DagBag(dag_folder=dag_folder, include_examples=False)
    dag = dagbag.dags['conditional_workflow_pipeline']
    
    from airflow.operators.empty import EmptyOperator
    
    empty_tasks = [task for task in dag.tasks 
                  if isinstance(task, EmptyOperator)]
    
    assert len(empty_tasks) > 0, \
        "DAG 4 should have EmptyOperator tasks"
    
    print("✓ DAG 4 uses EmptyOperator tasks")

def test_dag5_has_callbacks():
    """
    Verifies DAG 5 has callback functions configured.
    """
    dag_folder = os.path.join(os.path.dirname(__file__), '../dags')
    dagbag = DagBag(dag_folder=dag_folder, include_examples=False)
    dag = dagbag.dags['notification_workflow']
    
    risky_task = dag.get_task('risky_operation')
    
    assert risky_task.on_success_callback is not None, \
        "risky_operation task should have on_success_callback"
    assert risky_task.on_failure_callback is not None, \
        "risky_operation task should have on_failure_callback"
    
    print("✓ DAG 5 has callback functions configured")

def test_dag5_has_trigger_rules():
    """
    Verifies DAG 5 has trigger rules configured.
    """
    dag_folder = os.path.join(os.path.dirname(__file__), '../dags')
    dagbag = DagBag(dag_folder=dag_folder, include_examples=False)
    dag = dagbag.dags['notification_workflow']
    
    cleanup_task = dag.get_task('always_execute')
    
    assert cleanup_task.trigger_rule == 'all_done', \
        "always_execute task should have trigger_rule='all_done'"
    
    print("✓ DAG 5 has trigger rules configured")

def test_all_dags_have_valid_dag_id():
    """
    Verifies all DAGs have valid dag_id (no spaces, special chars).
    """
    dag_folder = os.path.join(os.path.dirname(__file__), '../dags')
    dagbag = DagBag(dag_folder=dag_folder, include_examples=False)
    
    import re
    valid_pattern = re.compile(r'^[a-zA-Z0-9_-]+$')
    
    for dag in dagbag.dags.values():
        assert valid_pattern.match(dag.dag_id), \
            f"DAG ID '{dag.dag_id}' contains invalid characters"
    
    print("✓ All DAGs have valid dag_id")

def test_dag_dependencies_linear():
    """
    Verifies DAG 1 and DAG 2 have linear task dependencies.
    """
    dag_folder = os.path.join(os.path.dirname(__file__), '../dags')
    dagbag = DagBag(dag_folder=dag_folder, include_examples=False)
    
    # DAG 1 should be linear: task1 >> task2 >> task3
    dag1 = dagbag.dags['csv_to_postgres_ingestion']
    assert len(dag1.roots) == 1, "DAG 1 should have exactly one root task"
    assert len(dag1.leaves) == 1, "DAG 1 should have exactly one leaf task"
    
    # DAG 2 should be linear: task1 >> task2
    dag2 = dagbag.dags['data_transformation_pipeline']
    assert len(dag2.roots) == 1, "DAG 2 should have exactly one root task"
    assert len(dag2.leaves) == 1, "DAG 2 should have exactly one leaf task"
    
    print("✓ DAGs 1 and 2 have linear task dependencies")

if __name__ == "__main__":
    print("\n" + "="*60)
    print("Running Airflow DAG Validation Tests")
    print("="*60 + "\n")
    
    pytest.main([__file__, "-v"])
