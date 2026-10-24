FROM apache/airflow:2.8.2-python3.10

USER airflow

# dbt-core + adaptateur Trino, versions figées pour un build reproductible
RUN pip install --no-cache-dir dbt-core==1.12.4 dbt-trino==1.10.3
