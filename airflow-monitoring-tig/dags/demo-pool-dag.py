from __future__ import annotations

from airflow import DAG
from airflow.models import Pool
from airflow.operators.bash import BashOperator
from pendulum import datetime

dedicated_pool_1 = Pool.create_or_update_pool(
    name="dedicated_pool_1",
    slots=8,
    description="Dedicated pool #1.",
    include_deferred=False,
)

dedicated_pool_2 = Pool.create_or_update_pool(
    name="dedicated_pool_2",
    slots=16,
    description="Dedicated pool #2.",
    include_deferred=False,
)

with DAG(
    dag_id="demo-pool-dag",
    catchup=True,
    start_date=datetime(2024, 1, 1, tz="UTC"),
    schedule="@daily",
) as demo_dag_1:
    op_task_1 = BashOperator(
        task_id="task_1",
        bash_command="sleep 60",
        pool=dedicated_pool_1.pool,
    )

    op_task_2 = BashOperator(
        task_id="task_2",
        bash_command="sleep 60",
        pool=dedicated_pool_2.pool,
    )

    op_last_task = BashOperator(
        task_id="last_task",
        bash_command="sleep 30"
    )

    [op_task_1, op_task_2] >> op_last_task
