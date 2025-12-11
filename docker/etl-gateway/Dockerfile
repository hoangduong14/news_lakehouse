FROM eclipse-temurin:11-jre-jammy

#############################
# 1. Cài Python + tool cơ bản
#############################
RUN apt-get update && \
    apt-get install -y python3 python3-pip curl bash && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

#############################
# 2. Cài Spark từ file .tgz đã tải sẵn
#############################
ENV SPARK_VERSION=3.5.7
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark

# File này đã nằm trong repo của bạn: docker/etl-gateway/spark-3.5.7-bin-hadoop3.tgz
COPY docker/etl-gateway/spark-3.5.7-bin-hadoop3.tgz /tmp/spark.tgz

RUN tar zx -C /opt -f /tmp/spark.tgz && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} && \
    rm /tmp/spark.tgz

#############################
# 3. Thêm GCS connector (shaded) + Iceberg runtime vào SPARK_HOME/jars
#############################
ENV GCS_CONNECTOR_VERSION=2.2.22
ENV ICEBERG_VERSION=1.6.1
# Tên artifact Iceberg tương ứng Spark 3.5 + Scala 2.12
ENV ICEBERG_SPARK_ARTIFACT=iceberg-spark-runtime-3.5_2.12

RUN curl -L \
      -o ${SPARK_HOME}/jars/gcs-connector-hadoop3-${GCS_CONNECTOR_VERSION}-shaded.jar \
      https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-${GCS_CONNECTOR_VERSION}/gcs-connector-hadoop3-${GCS_CONNECTOR_VERSION}-shaded.jar \
 && curl -L \
      -o ${SPARK_HOME}/jars/${ICEBERG_SPARK_ARTIFACT}-${ICEBERG_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/${ICEBERG_SPARK_ARTIFACT}/${ICEBERG_VERSION}/${ICEBERG_SPARK_ARTIFACT}-${ICEBERG_VERSION}.jar

# Đảm bảo các binary Spark có trong PATH
ENV PATH="${SPARK_HOME}/bin:${PATH}"

#############################
# 4. Cài libs Python cho Spark jobs
#############################
COPY docker/etl-gateway/requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

#############################
# 5. Copy Spark jobs vào container
#############################
WORKDIR /opt/spark_jobs
COPY spark_jobs/ /opt/spark_jobs/

CMD ["bash"]
