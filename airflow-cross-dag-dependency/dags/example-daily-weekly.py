from __future__ import annotations

from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.bash import BashSensor
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup


with DAG(
        dag_id="dag-daily",
        schedule_interval="@daily",
        start_date=datetime(2023, 7, 1),
        catchup=True,
) as dag_daily:
    BashSensor(task_id="some-input-wait", bash_command="true") >> \
        BashOperator(task_id="some-data-processing-1", bash_command="sleep 3") >> \
        BashOperator(task_id="some-data-processing-2", bash_command="sleep 3") >> \
        BashOperator(task_id="exporting-to-hdfs", bash_command="sleep 3")

with DAG(
        dag_id="dag-weekly",
        schedule_interval="@weekly",
        start_date=datetime(2023, 7, 1),
        catchup=True,
) as dag_weekly:
    # TaskGroup is purely optional, but makes DAG in the UI a bit clearer.
    with TaskGroup(group_id="dag-daily-sensors") as sensors:
        [
            ExternalTaskSensor(
                task_id=f"wait-{days_offset}d",
                external_dag_id="dag-daily",
                timeout=24 * 60 * 60,
                mode="reschedule",
                execution_delta=timedelta(days=days_offset)
            ) for days_offset in range(1, 8)
        ]

    sensors >> \
        BashOperator(task_id="some-data-processing", bash_command="sleep 3") >> \
        BashOperator(task_id="exporting-to-hdfs", bash_command="sleep 3")
