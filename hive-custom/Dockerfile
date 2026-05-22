FROM apache/hive:3.1.3

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl netcat-openbsd \
    && curl -fsSL -o /opt/hive/lib/postgresql-42.7.3.jar https://jdbc.postgresql.org/download/postgresql-42.7.3.jar \
    && rm -rf /var/lib/apt/lists/*

USER hive
