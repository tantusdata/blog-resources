FROM apache/airflow:2.8.0

USER airflow
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}"
RUN pip install --no-cache-dir "apache-airflow[statsd]"
RUN pip install --no-cache-dir "apache-airflow[influxdb]"

RUN echo "[metrics]"                >> /opt/airflow/airflow.cfg && \
    echo "statsd_on = True"         >> /opt/airflow/airflow.cfg && \
    echo "statsd_host = telegraf"   >> /opt/airflow/airflow.cfg && \
    echo "statsd_port = 8125"       >> /opt/airflow/airflow.cfg && \
    echo "statsd_prefix = airflow"  >> /opt/airflow/airflow.cfg
