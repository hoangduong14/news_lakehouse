FROM eclipse-temurin:17-jre-jammy

# Cài Python + pip + curl
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Đặt python3 thành python
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3 1

WORKDIR /app

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Tải JAR Iceberg + GCS connector
RUN mkdir -p /opt/jars && \
    curl -L -o /opt/jars/iceberg-spark-runtime-3.5_2.12-1.6.1.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.1/iceberg-spark-runtime-3.5_2.12-1.6.1.jar && \
    curl -L -o /opt/jars/gcs-connector-hadoop3-2.2.22.jar \
      https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.22/gcs-connector-hadoop3-2.2.22.jar

# Copy code
COPY spark_jobs/ ./spark_jobs/

ENV PYTHONUNBUFFERED=1
