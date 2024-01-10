from __future__ import annotations

from datetime import datetime
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.bash import BashSensor
from airflow.utils.task_group import TaskGroup

with DAG(
    dag_id="complex-dag-before",
    schedule_interval="@daily",
    start_date=datetime(2023, 7, 1),
) as complex_dag:
    sensor_clients = BashSensor(task_id="wait-clients", bash_command="true")
    sensor_events = BashSensor(task_id="wait-events", bash_command="true")
    sensor_users = BashSensor(task_id="wait-users", bash_command="true")

    sensor_gateway_01 = BashSensor(task_id="wait-gateway-01", bash_command="true")
    sensor_gateway_02 = BashSensor(task_id="wait-gateway-02", bash_command="true")
    sensor_gateway_03 = BashSensor(task_id="wait-gateway-03", bash_command="true")
    sensor_gateway_04 = BashSensor(task_id="wait-gateway-04", bash_command="true")
    sensor_gateway_05 = BashSensor(task_id="wait-gateway-05", bash_command="true")

    op_preprocess_clients = TaskGroup(group_id="preprocess-clients")
    op_preprocess_events = TaskGroup(group_id="preprocess-events")
    op_preprocess_transactions = TaskGroup(group_id="preprocess-transactions")
    op_preprocess_users = TaskGroup(group_id="preprocess-users")

    with op_preprocess_clients:
        EmptyOperator(task_id="step-1") >> \
            [EmptyOperator(task_id="step-2"), EmptyOperator(task_id="step-3")] >> \
            EmptyOperator(task_id="step-4")

    with op_preprocess_events:
        EmptyOperator(task_id="step-1") >> \
            EmptyOperator(task_id="step-2") >> \
            EmptyOperator(task_id="step-3")

    with op_preprocess_transactions:
        EmptyOperator(task_id="step-1") >> \
        EmptyOperator(task_id="step-2") >> \
        EmptyOperator(task_id="step-3") >> \
        EmptyOperator(task_id="step-4") >> \
        EmptyOperator(task_id="step-5")

    with op_preprocess_users:
        [EmptyOperator(task_id="step-1"), EmptyOperator(task_id="step-2")] >> \
            EmptyOperator(task_id="step-3") >> \
            [EmptyOperator(task_id="step-4"), EmptyOperator(task_id="step-5")] >> \
            EmptyOperator(task_id="step-6")

    op_rich_clients = EmptyOperator(task_id="enrich-clients")
    op_rich_users = EmptyOperator(task_id="enrich-users")

    op_report_clients = BashOperator(task_id="report-clients", bash_command="sleep 3")
    op_report_users = BashOperator(task_id="report-users", bash_command="sleep 3")
    op_report_full = BashOperator(task_id="report-full", bash_command="sleep 3")

    sensor_clients >> op_preprocess_clients
    sensor_events >> op_preprocess_events
    sensor_users >> op_preprocess_users

    [
        sensor_gateway_01,
        sensor_gateway_02,
        sensor_gateway_03,
        sensor_gateway_04,
        sensor_gateway_05
    ] >> op_preprocess_transactions

    [op_preprocess_events, op_preprocess_users] >> op_rich_clients >> op_report_users
    [op_preprocess_clients, op_preprocess_events] >> op_rich_users >> op_report_clients
    [op_preprocess_clients, op_preprocess_events, op_preprocess_transactions] >> op_report_full
