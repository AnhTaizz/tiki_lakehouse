FROM apache/airflow:2.8.1-python3.10

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends docker.io \
    && rm -rf /var/lib/apt/lists/*

COPY requirement.txt /requirements.txt
RUN PIP_USER=false python -m pip install --no-cache-dir --no-user -r /requirements.txt

USER airflow
