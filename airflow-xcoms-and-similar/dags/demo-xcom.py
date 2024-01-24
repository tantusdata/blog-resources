from __future__ import annotations

from datetime import datetime
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor


with DAG(
        dag_id="xcom-source",
        schedule_interval="@daily",
        start_date=datetime(2023, 7, 1),
        catchup=True,
) as xcom_source:
    op_update_hive_events_triggers = BashOperator(
        task_id="update-hive-table-events-triggers",
        bash_command="date '+%Y%m%d'",
        do_xcom_push=True
    )

with DAG(
        dag_id="xcom-sink",
        schedule_interval="@daily",
        start_date=datetime(2023, 7, 1),
        catchup=True,
) as xcom_sink:
    ExternalTaskSensor(
        task_id="wait-for-dependency",
        external_dag_id="xcom-source",
        external_task_id="update-hive-table-events-triggers"
    ) >> BashOperator(
        task_id="show-xcom",
        bash_command="echo {{ ti.xcom_pull(dag_id='xcom-source', task_ids='update-hive-table-events-triggers') }}"
    )
