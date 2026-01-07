# tests/test_dag2.py

import importlib
import types

MODULE_NAME = "dags.dag2_data_transformation"


def test_dag2_module_imports():
    """Module should be importable without errors."""
    module = importlib.import_module(MODULE_NAME)
    assert isinstance(module, types.ModuleType)


def test_dag2_has_dag_object():
    """The DAG module should expose a `dag` object."""
    module = importlib.import_module(MODULE_NAME)
    assert hasattr(module, "dag"), "dag2_data_transformation.py must define `dag`"


def test_dag2_required_functions_exist():
    """Required transformation functions must exist."""
    module = importlib.import_module(MODULE_NAME)

    for fn in ["create_transformed_table", "transform_data"]:
        assert hasattr(module, fn), f"`{fn}` must be defined in dag2_data_transformation.py"


def test_dag2_transform_data_is_callable():
    """
    Ensure transform_data exists and is callable.

    Do NOT call it, because real execution requires a database connection.
    """
    module = importlib.import_module(MODULE_NAME)
    fn = getattr(module, "transform_data", None)
    assert callable(fn), "`transform_data` must be callable"
