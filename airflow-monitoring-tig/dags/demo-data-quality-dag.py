from __future__ import annotations

import json
import random

from airflow import DAG, settings
from airflow.models import Connection
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from pendulum import datetime

from operators.influxdb import InfluxDBWriteOperator

INFLUXDB_CONNECTION_ID = "influxdb"

# Connection provisioning.
#  Should not happen in the DAG code in production use.
influxdb_connection = Connection(
    conn_id=INFLUXDB_CONNECTION_ID,
    conn_type="influxdb",
    schema="http",
    host="influxdb",
    port=8086,
    login="airflow-tig",
    extra=json.dumps({
      "token": "QWGM4w8fnahXdsS3",
      "organization": "af",
      "bucket": "airflow_tig"
    })
)

with settings.Session() as session:
    conn_id = INFLUXDB_CONNECTION_ID

    if str(session.query(Connection).filter(Connection.conn_id == conn_id).first()) != str(conn_id):
        session.add(influxdb_connection)
        session.commit()

with DAG(
    dag_id="demo-data-quality-dag",
    catchup=True,
    start_date=datetime(2024, 1, 1, tz="UTC"),
    schedule="@daily",
) as demo_dag_1:

    # Main data "pipeline" - dummy.

    op_load_data = BashOperator(
        task_id="load_data",
        bash_command="sleep $((RANDOM % 11))",
    )

    op_process_data = BashOperator(
        task_id="process_data",
        bash_command="sleep $((RANDOM % 31))",
    )

    op_export_data = BashOperator(
        task_id="export_data",
        bash_command="sleep $((RANDOM % 11))",
    )

    # Callbacks to compute Data Quality (DQ) metrics.
    # Here - dummy plugs. IRL would be replaced with Hive queries
    # or DQ framework callbacks.

    def fn_generate_dummy_dq(**kwargs):
        task_instance = kwargs["task_instance"]

        rand = random.Random()
        rows_count = rand.randint(100, 10_000)
        null_rows_count = rand.randint(0, rows_count // 10)
        unique_rows_count = rand.randint(rows_count // 2, (rows_count - null_rows_count))

        task_instance.xcom_push("rows_count", rows_count)
        task_instance.xcom_push("null_rows_count", null_rows_count)
        task_instance.xcom_push("unique_rows_count", unique_rows_count)

    # DQ metrics computation operators.

    op_load_data_dq = PythonOperator(
        task_id="load_data_dq",
        python_callable=fn_generate_dummy_dq,
        do_xcom_push=True,
    )

    op_process_data_dq = PythonOperator(
        task_id="process_data_dq",
        python_callable=fn_generate_dummy_dq,
        do_xcom_push=True,
    )

    op_export_data_dq = PythonOperator(
        task_id="export_data_dq",
        python_callable=fn_generate_dummy_dq,
        do_xcom_push=True,
    )

    # Callbacks to InfluxDB to store DQ metrics.

    op_load_data_dq_push = InfluxDBWriteOperator(
        task_id="load_data_dq_push",
        conn_id=INFLUXDB_CONNECTION_ID,
        measurement="data_quality_metrics",
        tags={
            "dag": "demo_data_quality_dag",
            "operator": "load_data",
        },
        fields={
            "rows_count": "{{ ti.xcom_pull(task_ids='load_data_dq', key='rows_count') }}",
            "null_rows_count": "{{ ti.xcom_pull(task_ids='load_data_dq', key='null_rows_count') }}",
            "unique_rows_count": "{{ ti.xcom_pull(task_ids='load_data_dq', key='unique_rows_count') }}",
        },
        force_fields_to="int",
        timestamp="{{ logical_date.int_timestamp }}"
    )

    op_process_data_dq_push = InfluxDBWriteOperator(
        task_id="process_data_dq_push",
        conn_id=INFLUXDB_CONNECTION_ID,
        measurement="data_quality_metrics",
        tags={
            "dag": "demo_data_quality_dag",
            "operator": "process_data",
        },
        fields={
            "rows_count": "{{ ti.xcom_pull(task_ids='process_data_dq', key='rows_count') }}",
            "null_rows_count": "{{ ti.xcom_pull(task_ids='process_data_dq', key='null_rows_count') }}",
            "unique_rows_count": "{{ ti.xcom_pull(task_ids='process_data_dq', key='unique_rows_count') }}",
        },
        force_fields_to="int",
        timestamp="{{ logical_date.int_timestamp }}"
    )

    op_export_data_dq_push = InfluxDBWriteOperator(
        task_id="export_data_dq_push",
        conn_id=INFLUXDB_CONNECTION_ID,
        measurement="data_quality_metrics",
        tags={
            "dag": "demo_data_quality_dag",
            "operator": "export_data",
        },
        fields={
            "rows_count": "{{ ti.xcom_pull(task_ids='export_data_dq', key='rows_count') }}",
            "null_rows_count": "{{ ti.xcom_pull(task_ids='export_data_dq', key='null_rows_count') }}",
            "unique_rows_count": "{{ ti.xcom_pull(task_ids='export_data_dq', key='unique_rows_count') }}",
        },
        force_fields_to="int",
        timestamp="{{ logical_date.int_timestamp }}"
    )

    op_load_data >> op_process_data >> op_export_data
    op_load_data >> op_load_data_dq >> op_load_data_dq_push
    op_process_data >> op_process_data_dq >> op_process_data_dq_push
    op_export_data >> op_export_data_dq >> op_export_data_dq_push
