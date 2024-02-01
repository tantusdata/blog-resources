from __future__ import annotations

from typing import Dict

from airflow.hooks.base import BaseHook
from airflow.models import Connection
from influxdb_client.client.influxdb_client import InfluxDBClient
from influxdb_client.client.write.point import Point
from influxdb_client.client.write_api import SYNCHRONOUS


class InfluxDBHook(BaseHook):

    conn_type = "influxdb"
    hook_name = "InfluxDB"

    def __init__(self, conn_id: str, *args, **kwargs) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id

        self.client = None
        self.connection = kwargs.pop("connection", None)

        self.bucket: str = ""
        self.organization: str = ""

    def get_conn(self) -> InfluxDBClient:
        self.connection: Connection = self.get_connection(self.conn_id)
        extra_config = self.connection.extra_dejson.copy()

        self.bucket = extra_config["bucket"]
        self.organization = extra_config["organization"]

        self.client = InfluxDBClient(
            url=f"{self.connection.schema}://{self.connection.host}:{self.connection.port}",
            token=extra_config["token"],
            org=extra_config["organization"]
        )

        return self.client

    def write(self, measurement: str, tags: Dict[str, str], fields: Dict[str, any], timestamp: any) -> None:
        data_to_write = {
            "measurement": measurement,
            "tags": tags,
            "fields": fields,
            "time": timestamp
        }

        self.log.info(f"Trying to write: {measurement}")

        self.get_conn()
        write_api = self.client.write_api(write_options=SYNCHRONOUS)
        write_api.write(self.bucket, self.organization, Point.from_dict(data_to_write))

        self.log.info(f"Data point(s) saved for {measurement}")
