"""DAG import test. Runs only where Airflow is installed (e.g. inside the container)."""
from __future__ import annotations

from pathlib import Path

import pytest

airflow = pytest.importorskip("airflow")


def test_dag_loads_without_errors():
    from airflow.models import DagBag

    bag = DagBag(dag_folder=str(Path(__file__).resolve().parents[1] / "dags"), include_examples=False)
    assert not bag.import_errors, bag.import_errors
    assert "ecommerce_daily_batch" in bag.dags
    dag = bag.dags["ecommerce_daily_batch"]  # no metadata DB needed
    assert [t.task_id for t in dag.topological_sort()] == [
        "check_source_files",
        "ingest_bronze",
        "transform_silver",
        "build_gold",
        "validate_outputs",
    ]
    assert dag.max_active_runs == 1
