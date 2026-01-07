# tests/test_dag1.py

import importlib
import types

MODULE_NAME = "dags.dag1_csv_to_postgres"


def test_dag1_module_imports():
    """Module should be importable without errors."""
    module = importlib.import_module(MODULE_NAME)
    assert isinstance(module, types.ModuleType)


def test_dag1_has_dag_object():
    """The DAG module should expose a `dag` object."""
    module = importlib.import_module(MODULE_NAME)
    assert hasattr(module, "dag"), "dag1_csv_to_postgres.py must define `dag`"


def test_dag1_required_functions_exist():
    """Required helper functions must exist."""
    module = importlib.import_module(MODULE_NAME)

    for fn in ["create_employee_table", "truncate_employee_table", "load_csv_data"]:
        assert hasattr(module, fn), f"`{fn}` must be defined in dag1_csv_to_postgres.py"


def test_dag1_load_csv_data_is_callable():
    """
    Ensure load_csv_data exists and is callable.

    Do NOT call it, because real execution requires CSV + Postgres.
    """
    module = importlib.import_module(MODULE_NAME)
    fn = getattr(module, "load_csv_data", None)
    assert callable(fn), "`load_csv_data` must be callable"
