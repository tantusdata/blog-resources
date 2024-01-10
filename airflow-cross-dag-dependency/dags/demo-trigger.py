from __future__ import annotations

from datetime import datetime
from airflow.models import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.sftp.sensors.sftp import SFTPSensor
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule


SFTP_CONNECTION_ID = "sftp-21"


with DAG(
    dag_id="trigger-events-with-users",
    schedule="@daily",
    start_date=datetime(2023, 7, 1),
) as dag_events_with_users:
    with TaskGroup(group_id="gather-events") as group_events:
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

    with TaskGroup(group_id="gather-users") as group_users:
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

    [group_events, group_users] >> SparkSubmitOperator(
        task_id="compute-events-with-users",
        application="/spark/applications/compute-events-with-users.py"
    ) >> SparkSubmitOperator(
        task_id="update-hive-table",
        application="/spark/applications/update-hive-table.py"
    ) >> [
        TriggerDagRunOperator(
            task_id="trigger-reports-1",
            trigger_dag_id="trigger-reports-1",
            trigger_rule=TriggerRule.ALL_SUCCESS,
        ),
        TriggerDagRunOperator(
            task_id="trigger-reports-2",
            trigger_dag_id="trigger-reports-2",
            trigger_rule=TriggerRule.ALL_SUCCESS,
        )
    ]

with DAG(
    dag_id="trigger-reports-1",
    catchup=True,
    start_date=datetime(2023, 7, 1),
) as dag_processor_reports_1:
    SparkSubmitOperator(
        task_id="compute-report",
        application="/spark/applications/compute-report-1.py",
    )

with DAG(
    dag_id="trigger-reports-2",
    catchup=True,
    start_date=datetime(2023, 7, 1),
) as dag_processor_reports_2:
    SparkSubmitOperator(
        task_id="compute-report",
        application="/spark/applications/compute-report-2.py",
    )
