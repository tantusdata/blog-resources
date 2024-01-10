FROM apache/airflow:2.8.0

USER root
RUN apt update && \
    apt-get install -y gcc libkrb5-dev && \
    apt-get clean

USER airflow
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}"     && \
    pip install --no-cache-dir "apache-airflow-providers-apache-hdfs"   && \
    pip install --no-cache-dir "apache-airflow-providers-apache-spark"  && \
    pip install --no-cache-dir "apache-airflow-providers-sftp"