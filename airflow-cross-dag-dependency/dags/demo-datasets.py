from __future__ import annotations

from airflow import DAG, Dataset
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.sftp.sensors.sftp import SFTPSensor
from pendulum import datetime


dataset_users = Dataset("af://datasets/users")
dataset_events = Dataset("af://datasets/events")
dataset_events_with_users = Dataset("af://datasets/events-with-users")
dataset_reports_1 = Dataset("af://datasets/reports-1")
dataset_reports_2 = Dataset("af://datasets/reports-2")

SFTP_CONNECTION_ID = "sftp-21"

with DAG(
    dag_id="dataset-events",
    catchup=True,
    start_date=datetime(2023, 7, 1, tz="UTC"),
    schedule="@daily",
    max_active_runs=1,
) as dag_producer_events:
    SFTPSensor(
        task_id="wait-for-csv",
        path="/inbox/events/{{ dag_run.logical_date | ds }}.csv",
        sftp_conn_id=SFTP_CONNECTION_ID
    ) >> SparkSubmitOperator(
        task_id="csv-to-parquet",
        application="/spark/applications/csv-to-parquet.py"
    ) >> SparkSubmitOperator(
        task_id="update-hive-table",
        application="/spark/applications/update-hive-table.py",
        outlets=[dataset_events]
    )

with DAG(
    dag_id="dataset-users",
    catchup=True,
    start_date=datetime(2023, 7, 1, tz="UTC"),
    schedule="@daily",
    max_active_runs=1,
) as dag_producer_users:
    SFTPSensor(
        task_id="wait-for-csv",
        path="/inbox/users/{{ dag_run.logical_date | ds }}.csv",
        sftp_conn_id=SFTP_CONNECTION_ID
    ) >> SparkSubmitOperator(
        task_id="csv-to-parquet",
        application="/spark/applications/csv-to-parquet.py"
    ) >> SparkSubmitOperator(
        task_id="update-hive-table",
        application="/spark/applications/update-hive-table.py",
        outlets=[dataset_users]
    )

with DAG(
    dag_id="dataset-events-with-users",
    catchup=True,
    start_date=datetime(2023, 7, 1, tz="UTC"),
    schedule=[dataset_events, dataset_users],
    max_active_runs=1,
) as dag_processor_events_with_users:
    SparkSubmitOperator(
        task_id="compute-events-with-users",
        application="/spark/applications/compute-events-with-users.py"
    ) >> SparkSubmitOperator(
        task_id="update-hive-table",
        application="/spark/applications/update-hive-table.py",
        outlets=[dataset_events_with_users]
    )

with DAG(
    dag_id="dataset-reports-1",
    catchup=True,
    start_date=datetime(2023, 7, 1, tz="UTC"),
    schedule=[dataset_events_with_users],
) as dag_processor_reports_1:
    SparkSubmitOperator(
        task_id="compute-report",
        application="/spark/applications/compute-report-1.py",
        outlets=[dataset_reports_1]
    )

with DAG(
    dag_id="dataset-reports-2",
    catchup=True,
    start_date=datetime(2023, 7, 1, tz="UTC"),
    schedule=[dataset_events_with_users],
) as dag_processor_reports_2:
    SparkSubmitOperator(
        task_id="compute-report",
        application="/spark/applications/compute-report-2.py",
        outlets=[dataset_reports_2]
    )
