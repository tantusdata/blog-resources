from __future__ import annotations

import pendulum
import random
import time
import uuid

from airflow.decorators import dag, task

@dag(
    dag_id="example-max_active_tis_per_dag",
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False
)
def dag_example_max_active_tis_per_dag():
    @task()
    def extract():

        time.sleep(5)

        return [{
            "id": str(uuid.uuid4()),
            "kind": random.choice(["string", "integer", "float"]),
            "price": random.uniform(-9.9, 9.9)
        } for _ in range(1, 10)]

    @task()
    def transform(items):

        time.sleep(5)

        return [{
            "id": item["id"],
            "kind": item["kind"],
            "price": item["price"],
            "is_positive": (item["price"] > 0.0)
        } for item in items if item["price"] != 0.0]

    # The simplest way is to set the _max_active_tis_per_dag_ setting this task to 1: now Airflow will not
    #  allow to run multiple instances of this task concurrently. However, it gives no guarantee that AF will
    #  keep the runs within any specific order.
    # Whenever there are multiple instances of the task ready to be run, log files will have a following entry:
    #  INFO - Not executing <TaskInstance: ...> since the task concurrency for this task has been reached.
    # Airflow documentation link:
    #  https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html#placing-limits-on-mapped-tasks

    @task(
        max_active_tis_per_dag=1
    )
    def load_1(items):
        for item in items:
            time.sleep(2)
            print("Loaded item: {}".format(item))

        return True

    @task(
        max_active_tis_per_dag=1
    )
    def load_2(items):
        for item in items:
            time.sleep(3)
            print("Loaded item: {}".format(item))

        return True


    submitted_data = extract()
    curated_data = transform(submitted_data)
    load_result = [load_1(curated_data), load_2(curated_data)]


dag_example_max_active_tis_per_dag()
