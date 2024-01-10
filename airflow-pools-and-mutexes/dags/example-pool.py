from __future__ import annotations

import pendulum
import random
import time
import uuid

from airflow.decorators import dag, task
from airflow.models import Pool


# Programmatic configuration for the AF pool, details provided above 'load' operators.
load_pool = Pool.create_or_update_pool(
    name="load_pool",
    slots=1,
    description="Pool for data load tasks."
)

@dag(
    dag_id="example-pool",
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False
)
def dag_example_pool():
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

    # The most complex scenario here is a case when two (or more) operators share the same mutex and only one instance
    #  of them can be running at the same time. This can be done via pools and limiting its slots to fit only one task.
    # Airflow documentation link:
    #  https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/pools.html

    @task(
        pool=load_pool.pool
    )
    def load_1(items):
        for item in items:
            time.sleep(2)
            print("Loaded item: {}".format(item))

        return True

    @task(
        pool=load_pool.pool
    )
    def load_2(items):
        for item in items:
            time.sleep(3)
            print("Loaded item: {}".format(item))

        return True

    submitted_data = extract()
    curated_data = transform(submitted_data)
    load_result = [load_1(curated_data), load_2(curated_data)]


dag_example_pool()
