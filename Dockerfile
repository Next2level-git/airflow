FROM apache/airflow:2.10.1

USER root
RUN apt-get update && apt-get install -y \
    libcairo2-dev \
    libffi-dev \
    libgirepository1.0-dev \
    pkg-config \
    python3-cairo \
    && apt-get clean

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
