FROM apache/airflow:2.8.1-python3.10

USER root
# Install basic git for dbt
RUN apt-get update && apt-get install -y git && apt-get clean

USER airflow
# Install the airflow, dbt, pyspark for your project
RUN pip install --no-cache-dir \
    apache-airflow-providers-google \
    dbt-bigquery \
    pyspark