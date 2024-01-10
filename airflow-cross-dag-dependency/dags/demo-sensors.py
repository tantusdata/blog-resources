from __future__ import annotations

from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.sftp.sensors.sftp import SFTPSensor
from airflow.sensors.external_task import ExternalTaskSensor


SFTP_CONNECTION_ID = "sftp-21"


with DAG(
        dag_id="sensor-events",
        schedule_interval="@daily",
        start_date=datetime(2023, 7, 1),
        catchup=True,
) as sensor_events:
    SFTPSensor(
        task_id="wait-for-csv",
        path="/inbox/events/{{ dag_run.logical_date | ds }}.csv",
        sftp_conn_id=SFTP_CONNECTION_ID
    ) >> SparkSubmitOperator(
        task_id="csv-to-parquet",
        application="/spark/applications/csv-to-parquet.py"
    ) >> SparkSubmitOperator(
        task_id="update-hive-table",
        application="/spark/applications/update-hive-table.py"
    )

with DAG(
        dag_id="sensor-users",
        schedule_interval="@daily",
        start_date=datetime(2023, 7, 1),
        catchup=True,
) as sensor_users:
    SFTPSensor(
        task_id="wait-for-csv",
        path="/inbox/users/{{ dag_run.logical_date | ds }}.csv",
        sftp_conn_id=SFTP_CONNECTION_ID
    ) >> SparkSubmitOperator(
        task_id="csv-to-parquet",
        application="/spark/applications/csv-to-parquet.py"
    ) >> SparkSubmitOperator(
        task_id="update-hive-table",
        application="/spark/applications/update-hive-table.py"
    )

with DAG(
        dag_id="sensor-events-with-users",
        schedule_interval="@daily",
        start_date=datetime(2023, 7, 1),
        catchup=True,
) as dag_daily:
    [
        ExternalTaskSensor(
            task_id="wait-for-events",
            external_dag_id="sensor-events",
            check_existence=True
        ),
        ExternalTaskSensor(
            task_id="wait-for-users",
            external_dag_id="sensor-users",
            check_existence=True
        )
    ] >> SparkSubmitOperator(
        task_id="compute-events-with-users",
        application="/spark/applications/compute-events-with-users.py"
    ) >> SparkSubmitOperator(
        task_id="update-hive-table",
        application="/spark/applications/update-hive-table.py"
    )

with DAG(
        dag_id="sensors-reports-1",
        schedule="@daily",
        start_date=datetime(2023, 7, 1),
) as dag_reports_1:
    ExternalTaskSensor(
        task_id="wait-for-events-with-users",
        external_dag_id="sensor-events-with-users",
        check_existence=True
    ) >> SparkSubmitOperator(
        task_id="compute-report",
        application="/spark/applications/compute-report-1.py"
    )

with DAG(
        dag_id="sensors-reports-2",
        schedule="@daily",
        start_date=datetime(2023, 7, 1),
) as dag_reports_2:
    ExternalTaskSensor(
        task_id="wait-for-events-with-users",
        external_dag_id="sensor-events-with-users",
        check_existence=True
    ) >> SparkSubmitOperator(
        task_id="compute-report",
        application="/spark/applications/compute-report-2.py"
    )
