# Airflow monitoring via TIG stack

This repository contains source code for demo for Airflow monitoring via
TIG (Telegraf - InfluxDB - Grafana) stack. It is preconfigured to show two 
dashboards: example Airflow metrics and dummy Data Quality data.

## Disclaimer
This repository is not a production-ready solution. The code, especially
the IaaC part is not on a production-ready level, including the security
best practices and should not be used as 1:1 to deploy anything on the 
live environment. 

## Usage
Run `docker compose up airflow-init` to initialize Airflow database (once), 
then `docker compose up` to start the environment and `docker compose down` 
to shut it down. Provisioning of all resources (including Grafana dashboards
or InfluxDB connections) is automated.

When up and running you can access the WebUIs via:
- Airflow: `http://localhost:8080/`, user `airflow`, password `airflow`,
- Grafana: `http://localhost:3000/`, user `admin`, password `admin`.

## References
- [Docker image for Airflow](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
- [TIG stack](https://www.influxdata.com/grafana/)
