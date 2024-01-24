from __future__ import annotations

from datetime import datetime
from airflow.models import DAG, Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

SYNC_VAR_NAME = "SYNC_VARIABLE"


def logical_date_to_variable_key(logical_date: datetime) -> str:
    return logical_date.strftime("%Y-%m-%d")


with DAG(
        dag_id="var-source",
        schedule_interval="@daily",
        start_date=datetime(2023, 7, 1),
        catchup=True,
) as var_source:
    def store_variable(*args, **kwargs):
        XCOM_DAG = "var-source"
        XCOM_TASK = "generate-xcom"

        xcom_value = str(kwargs["ti"].xcom_pull(dag_id=XCOM_DAG, task_ids=XCOM_TASK))

        variable = Variable.get(key=SYNC_VAR_NAME, deserialize_json=True, default_var=dict())
        variable[logical_date_to_variable_key(kwargs["logical_date"])] = xcom_value
        Variable.set(key=SYNC_VAR_NAME, serialize_json=True, value=variable)


    op_update_hive_events_triggers = BashOperator(
        task_id="generate-xcom",
        bash_command="date '+%Y%m%d'",
        do_xcom_push=True
    ) >> PythonOperator(
        task_id="store-variable",
        python_callable=store_variable,
        provide_context=True,
        max_active_tis_per_dag=1
    )

with DAG(
        dag_id="var-sink",
        schedule_interval="@daily",
        start_date=datetime(2023, 7, 1),
        catchup=True,
) as var_sink:
    ExternalTaskSensor(
        task_id="wait-for-dependency",
        external_dag_id="var-source",
        external_task_id="store-variable"
    ) >> BashOperator(
        task_id="show-xcom",
        bash_command="echo {{ var.value.get('" + SYNC_VAR_NAME + "', '') }}"
    )
