from __future__ import annotations

import pendulum
import random
import time
import uuid

from airflow.decorators import dag, task

@dag(
    dag_id="example-depends_on_past",
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False
)
def dag_example_example_depends_on_past():
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

    # In this implementation AF will wait for the previous task instance (not whole DAG run) to be completed
    #  successfully before running the next one. Be sure to monitor the tasks, as waiting, failed or skipped instances
    #  will pause any future runs.
    # Airflow documentation link:
    #  https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html#concepts-depends-on-past

    @task(
        depends_on_past=True
    )
    def load_1(items):
        for item in items:
            time.sleep(2)
            print("Loaded item: {}".format(item))

        return True

    @task(
        depends_on_past=True
    )
    def load_2(items):
        for item in items:
            time.sleep(3)
            print("Loaded item: {}".format(item))

        return True

    submitted_data = extract()
    curated_data = transform(submitted_data)
    load_result = [load_1(curated_data), load_2(curated_data)]


dag_example_example_depends_on_past()
