from __future__ import annotations

from airflow import DAG
from airflow.operators.bash import BashOperator
from pendulum import datetime

with DAG(
    dag_id="demo-dag-1",
    catchup=True,
    start_date=datetime(2024, 1, 1, tz="UTC"),
    schedule="@daily",
) as demo_dag_1:
    op_long_task = BashOperator(
        task_id="long_task",
        bash_command="sleep 60"
    )

    op_short_task = BashOperator(
        task_id="short_task",
        bash_command="sleep 5"
    )

    op_last_task = BashOperator(
        task_id="last_task",
        bash_command="sleep 5",
        max_active_tis_per_dag=1
    )

    [op_long_task, op_short_task] >> op_last_task



