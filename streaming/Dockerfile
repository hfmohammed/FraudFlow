# python:3.12-slim is Debian-based (glibc), same as the producer image.
# PySpark requires a JVM — we install the headless JDK (no GUI, smaller footprint).
FROM python:3.12-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        default-jdk-headless \
        procps \
    && rm -rf /var/lib/apt/lists/*

# Debian's default-jdk creates /usr/lib/jvm/default-java regardless of architecture,
# so this works on both amd64 (CI/servers) and arm64 (Apple Silicon laptops).
ENV JAVA_HOME=/usr/lib/jvm/default-java

# Suppress PySpark's Hadoop native library warning — we're not using HDFS.
ENV HADOOP_HOME=/tmp
ENV SPARK_LOCAL_IP=127.0.0.1

WORKDIR /app

COPY requirements.txt .
# pip install caches wheels; the JARs are downloaded by Spark at runtime from Maven.
# To cache JARs across container restarts, mount a volume at /root/.ivy2.
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Default: start the bronze job. Override CMD in docker-compose for silver/gold.
CMD ["python", "jobs/bronze_ingestion.py"]
