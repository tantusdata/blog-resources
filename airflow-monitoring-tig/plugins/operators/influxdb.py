from __future__ import annotations

from typing import Sequence, Dict

from airflow.models import BaseOperator
from airflow.utils.context import Context

from hooks.influxdb import InfluxDBHook


class InfluxDBWriteOperator(BaseOperator):
    """
    """

    template_fields: Sequence[str] = ("fields", "timestamp",)

    def __init__(
        self,
        measurement: str,
        tags: Dict[str, str],
        fields: Dict[str, any],
        timestamp: any,
        conn_id: str = "influxdb",
        force_fields_to: str = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id

        self.measurement = measurement
        self.tags = tags if tags is not None else {}
        self.timestamp = timestamp
        self.fields = fields
        self.force_fields_to = force_fields_to

    def execute(self, context: Context) -> None:
        self.log.info(f"Executing for InfluxDB write for {self.measurement}, "
                      f"tags: {self.tags}, fields: {self.fields}, timestamp: {self.timestamp}.")

        if self.force_fields_to is "int":
            fields = dict({(field, int(value)) for (field, value) in self.fields.items()})
        elif self.force_fields_to is "float":
            fields = dict({(field, float(value)) for (field, value) in self.fields.items()})
        else:
            fields = self.fields

        timestamp = int(self.timestamp) * 1_000_000_000

        self.hook = InfluxDBHook(self.conn_id)
        self.hook.write(self.measurement, self.tags, fields, timestamp)
