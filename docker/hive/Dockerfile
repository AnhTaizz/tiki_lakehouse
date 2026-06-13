FROM apache/hive:3.1.3

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl netcat-openbsd \
    && curl -fsSL -o /opt/hive/lib/postgresql-42.7.3.jar https://jdbc.postgresql.org/download/postgresql-42.7.3.jar \
    && ln -s /opt/hadoop/share/hadoop/tools/lib/hadoop-aws-3.1.0.jar /opt/hive/lib/hadoop-aws-3.1.0.jar \
    && ln -s /opt/hadoop/share/hadoop/tools/lib/aws-java-sdk-bundle-1.11.271.jar /opt/hive/lib/aws-java-sdk-bundle-1.11.271.jar \
    && rm -rf /var/lib/apt/lists/*

USER hive
