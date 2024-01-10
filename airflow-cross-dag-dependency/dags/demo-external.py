from __future__ import annotations

from datetime import datetime

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.hdfs.sensors.web_hdfs import WebHdfsSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.sftp.sensors.sftp import SFTPSensor

HDFS_CONNECTION_ID = "hdfs-14"
SFTP_CONNECTION_ID = "sftp-21"

with DAG(
    dag_id="external-events",
    schedule="@daily",
    start_date=datetime(2023, 7, 1),
) as dag_events:
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
    ) >> BashOperator(
        task_id="create-hdfs-marker-file",
        bash_command="hdfs dfs -touch /marker/events/{{ dag_run.logical_date | ds }}.success"
    )

with DAG(
    dag_id="external-users",
    schedule="@daily",
    start_date=datetime(2023, 7, 1),
) as dag_users:
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
    ) >> BashOperator(
        task_id="create-hdfs-marker-file",
        bash_command="hdfs dfs -touch /marker/users/{{ dag_run.logical_date | ds }}.success"
    )

with DAG(
    dag_id="external-events-with-users",
    schedule="@daily",
    start_date=datetime(2023, 7, 1),
) as dag_events_with_users:
    [
        WebHdfsSensor(
            task_id="wait-for-marker-events",
            filepath="/marker/events/{{ dag_run.logical_date | ds }}.success",
            webhdfs_conn_id=HDFS_CONNECTION_ID
        ),
        WebHdfsSensor(
            task_id="wait-for-marker-users",
            filepath="/marker/users/{{ dag_run.logical_date | ds }}.success",
            webhdfs_conn_id=HDFS_CONNECTION_ID
        )
    ] >> SparkSubmitOperator(
        task_id="compute-events-with-users",
        application="/spark/applications/compute-events-with-users.py"
    ) >> SparkSubmitOperator(
        task_id="update-hive-table",
        application="/spark/applications/update-hive-table.py"
    ) >> BashOperator(
        task_id="create-hdfs-marker-file",
        bash_command="hdfs dfs -touch /marker/events-with-users/{{ dag_run.logical_date | ds }}.success"
    )

with DAG(
    dag_id="external-reports-1",
    schedule="@daily",
    start_date=datetime(2023, 7, 1),
) as dag_reports_1:
    WebHdfsSensor(
        task_id="wait-for-events-with-users",
        filepath="/marker/events-with-users/{{ dag_run.logical_date | ds }}.success",
        webhdfs_conn_id=HDFS_CONNECTION_ID
    ) >> SparkSubmitOperator(
        task_id="compute-report",
        application="/spark/applications/compute-report-1.py"
    )

with DAG(
    dag_id="external-reports-2",
    schedule="@daily",
    start_date=datetime(2023, 7, 1),
) as dag_reports_2:
    WebHdfsSensor(
        task_id="wait-for-events-with-users",
        filepath="/marker/events-with-users/{{ dag_run.logical_date | ds }}.success",
        webhdfs_conn_id=HDFS_CONNECTION_ID
    ) >> SparkSubmitOperator(
        task_id="compute-report",
        application="/spark/applications/compute-report-2.py"
    )
