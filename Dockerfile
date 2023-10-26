FROM apache/airflow:2.7.2

RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" "apache-airflow[amazon]"
