from __future__ import annotations

from datetime import datetime

from airflow import Dataset
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.bash import BashSensor

dataset_clients = Dataset("af://dataset/clients")
dataset_events = Dataset("af://dataset/events")
dataset_transactions = Dataset("af://dataset/transactions")
dataset_users = Dataset("af://dataset/users")

with DAG(
    dag_id="complex-dag-clients",
    schedule_interval="@daily",
    start_date=datetime(2023, 7, 1),
) as complex_dag_clients:
    sensor_clients = BashSensor(task_id="wait-clients", bash_command="true")

    op_preprocess_step_1 = EmptyOperator(task_id="preprocess-step-1")
    op_preprocess_step_2 = EmptyOperator(task_id="preprocess-step-2")
    op_preprocess_step_3 = EmptyOperator(task_id="preprocess-step-3")
    op_preprocess_step_4 = EmptyOperator(task_id="preprocess-step-4", outlets=[dataset_clients])

    sensor_clients >> op_preprocess_step_1 >> \
        [op_preprocess_step_2 >> op_preprocess_step_3] >> \
        op_preprocess_step_4

with DAG(
    dag_id="complex-dag-events",
    schedule_interval="@daily",
    start_date=datetime(2023, 7, 1),
) as complex_dag_events:
    sensor_events = BashSensor(task_id="wait-events", bash_command="true")

    op_preprocess_step_1 = EmptyOperator(task_id="preprocess-step-1")
    op_preprocess_step_2 = EmptyOperator(task_id="preprocess-step-2")
    op_preprocess_step_3 = EmptyOperator(task_id="preprocess-step-3", outlets=[dataset_events])

    sensor_events >> op_preprocess_step_1 >> op_preprocess_step_2 >> op_preprocess_step_3

with DAG(
    dag_id="complex-dag-transactions",
    schedule_interval="@daily",
    start_date=datetime(2023, 7, 1),
) as complex_dag_transactions:
    sensor_gateway_01 = BashSensor(task_id="wait-gateway-01", bash_command="true")
    sensor_gateway_02 = BashSensor(task_id="wait-gateway-02", bash_command="true")
    sensor_gateway_03 = BashSensor(task_id="wait-gateway-03", bash_command="true")
    sensor_gateway_04 = BashSensor(task_id="wait-gateway-04", bash_command="true")
    sensor_gateway_05 = BashSensor(task_id="wait-gateway-05", bash_command="true")

    op_preprocess_step_1 = EmptyOperator(task_id="preprocess-step-1")
    op_preprocess_step_2 = EmptyOperator(task_id="preprocess-step-2")
    op_preprocess_step_3 = EmptyOperator(task_id="preprocess-step-3")
    op_preprocess_step_4 = EmptyOperator(task_id="preprocess-step-4")
    op_preprocess_step_5 = EmptyOperator(task_id="preprocess-step-5", outlets=[dataset_transactions])

    [
        sensor_gateway_01,
        sensor_gateway_02,
        sensor_gateway_03,
        sensor_gateway_04,
        sensor_gateway_05
    ] >> op_preprocess_step_1 >> op_preprocess_step_2 >> op_preprocess_step_3 >> \
        op_preprocess_step_4 >> op_preprocess_step_5

with DAG(
    dag_id="complex-dag-users",
    schedule_interval="@daily",
    start_date=datetime(2023, 7, 1),
) as complex_dag_users:
    sensor_users = BashSensor(task_id="wait-users", bash_command="true")

    op_preprocess_step_1 = EmptyOperator(task_id="preprocess-step-1")
    op_preprocess_step_2 = EmptyOperator(task_id="preprocess-step-2")
    op_preprocess_step_3 = EmptyOperator(task_id="preprocess-step-3")
    op_preprocess_step_4 = EmptyOperator(task_id="preprocess-step-4")
    op_preprocess_step_5 = EmptyOperator(task_id="preprocess-step-5")
    op_preprocess_step_6 = EmptyOperator(task_id="preprocess-step-6", outlets=[dataset_users])

    sensor_users >> [op_preprocess_step_1, op_preprocess_step_2] >> op_preprocess_step_3 >> \
        [op_preprocess_step_4, op_preprocess_step_5] >> op_preprocess_step_6

with DAG(
    dag_id="complex-dag-report-clients",
    schedule=[dataset_clients, dataset_events],
    start_date=datetime(2023, 7, 1),
) as complex_dag_report_clients:
    op_rich_clients = EmptyOperator(task_id="enrich-clients")
    op_report_clients = BashOperator(task_id="report-clients", bash_command="sleep 3")

    op_rich_clients >> op_report_clients

with DAG(
    dag_id="complex-dag-report-users",
    schedule=[dataset_events, dataset_users],
    start_date=datetime(2023, 7, 1),
) as complex_dag_report_users:
    op_rich_users = EmptyOperator(task_id="enrich-events")
    op_report_users = BashOperator(task_id="report-users", bash_command="sleep 3")

    op_rich_users >> op_report_users

with DAG(
    dag_id="complex-dag-report-full",
    schedule=[dataset_events, dataset_transactions, dataset_users],
    start_date=datetime(2023, 7, 1),
) as complex_dag_report_full:
    op_report_users = BashOperator(task_id="report-full", bash_command="sleep 3")
