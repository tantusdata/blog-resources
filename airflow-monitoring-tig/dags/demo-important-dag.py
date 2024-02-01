from __future__ import annotations

from airflow import DAG
from airflow.operators.bash import BashOperator
from pendulum import datetime

with DAG(
    dag_id="demo-important-dag",
    catchup=True,
    start_date=datetime(2024, 1, 1, tz="UTC"),
    schedule="@daily",
) as demo_dag_1:
    op_task_1 = BashOperator(
        task_id="task_1",
        bash_command="sleep $((RANDOM % 31))",
    )

    op_task_2 = BashOperator(
        task_id="task_2",
        bash_command="sleep $((10 + RANDOM % 11))",
    )

    op_last_task = BashOperator(
        task_id="last_task",
        bash_command="sleep $((RANDOM % 61))",
    )

    op_task_1 >> op_task_2 >> op_last_task



