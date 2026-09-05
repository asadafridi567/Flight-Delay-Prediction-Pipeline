FROM apache/airflow:2.9.3-python3.11

COPY requirements-airflow.txt /requirements-airflow.txt
COPY requirements-dbt.txt /requirements-dbt.txt

RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt" \
    -r /requirements-airflow.txt

RUN pip install --no-cache-dir -r /requirements-dbt.txt