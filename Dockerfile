FROM apache/airflow:2.8.3-python3.11

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre-headless procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow
RUN pip install --no-cache-dir psycopg2-binary pyyaml pandas pyspark==3.5.3

