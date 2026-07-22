FROM python:3.12-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        default-jdk-headless \
        procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV HADOOP_HOME=/tmp
ENV SPARK_LOCAL_IP=127.0.0.1

WORKDIR /app

COPY streaming/requirements.txt streaming/requirements.txt
RUN pip install --no-cache-dir -r streaming/requirements.txt

COPY streaming/ streaming/

CMD ["python", "-m", "streaming.jobs.bronze_ingestion"]
