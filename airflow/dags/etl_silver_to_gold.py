# from datetime import datetime, timedelta

# from airflow import DAG
# from airflow.providers.docker.operators.docker import DockerOperator

# default_args = {
#     "owner": "duong",
#     "depends_on_past": False,
#     "retries": 1,
#     "retry_delay": timedelta(minutes=5),
# }

# with DAG(
#     dag_id="gcs_silver_to_gold_docker",
#     default_args=default_args,
#     schedule_interval="@daily",
#     start_date=datetime(2025, 1, 1),
#     catchup=False,
#     tags=["etl", "spark", "silver_to_gold"],
# ) as dag:

#     run_spark_job = DockerOperator(
#         task_id="run_spark_gcs_silver_to_gold",
#         image="etl-gateway:latest",
#         api_version="auto",
#         auto_remove=True,
#         command="""
#         $SPARK_HOME/bin/spark-submit \
#           --master 'local[1]' \
#           --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.22 \
#           --conf "spark.driver.extraJavaOptions=-XX:TieredStopAtLevel=1" \
#           --conf "spark.executor.extraJavaOptions=-XX:TieredStopAtLevel=1" \
#           --conf "spark.sql.execution.arrow.pyspark.enabled=false" \
#           --conf "spark.sql.parquet.enableVectorizedReader=false" \
#           --conf "spark.sql.orc.enableVectorizedReader=false" \
#           --conf "spark.sql.codegen.wholeStage=false" \
#           --driver-memory 4g \
#           --executor-memory 4g \
#           /opt/spark_jobs/gcs_silver_to_gold.py
#         """,
#         docker_url="unix://var/run/docker.sock",
#         network_mode="airflow_default",
#     )

#     run_spark_job



from datetime import datetime, timedelta
import socket

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import PythonOperator

from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

default_args = {
    "owner": "duong",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

PUSHGATEWAY_URL = "http://pushgateway:9091"  # service pushgateway trong stack monitoring


def push_etl_metrics(ti, dag, **_):
    """
    Task này chạy sau DockerOperator:
      - Lấy start_date, end_date, state của task run_spark_job
      - Tính duration (giây)
      - Đẩy metric lên Prometheus Pushgateway
    """
    monitored_task_id = "run_spark_gcs_silver_to_gold"

    dag_run = ti.get_dagrun()
    spark_ti = dag_run.get_task_instance(monitored_task_id)

    if not spark_ti or not spark_ti.start_date or not spark_ti.end_date:
        return

    duration = (spark_ti.end_date - spark_ti.start_date).total_seconds()
    status = "success" if spark_ti.state == "success" else "failure"
    host = socket.gethostname()
    dag_id = dag.dag_id

    registry = CollectorRegistry()

    duration_gauge = Gauge(
        "etl_job_duration_seconds",
        "Duration of ETL job in seconds",
        ["job_name", "dag_id", "task_id", "status", "host"],
        registry=registry,
    )

    duration_gauge.labels(
        job_name="gcs_silver_to_gold",
        dag_id=dag_id,
        task_id=monitored_task_id,
        status=status,
        host=host,
    ).set(duration)

    push_to_gateway(
        PUSHGATEWAY_URL,
        job="gcs_silver_to_gold",
        registry=registry,
    )


with DAG(
    dag_id="gcs_silver_to_gold_docker",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["etl", "spark", "silver_to_gold"],
) as dag:

    run_spark_job = DockerOperator(
        task_id="run_spark_gcs_silver_to_gold",
        image="etl-gateway:latest",
        api_version="auto",
        auto_remove=True,
        command="""
        spark-submit \
          --master 'local[1]' \
          --conf "spark.driver.extraJavaOptions=-XX:TieredStopAtLevel=1" \
          --conf "spark.executor.extraJavaOptions=-XX:TieredStopAtLevel=1" \
          --conf "spark.sql.execution.arrow.pyspark.enabled=false" \
          --conf "spark.sql.parquet.enableVectorizedReader=false" \
          --conf "spark.sql.orc.enableVectorizedReader=false" \
          --conf "spark.sql.codegen.wholeStage=false" \
          --conf "spark.sql.shuffle.partitions=50" \
          --driver-memory 4g \
          --executor-memory 4g \
          /opt/spark_jobs/gcs_silver_to_gold.py
        """,
        docker_url="unix://var/run/docker.sock",
        network_mode="airflow_default",
        mount_tmp_dir=False,
    )

    push_metrics = PythonOperator(
        task_id="push_metrics_to_pushgateway",
        python_callable=push_etl_metrics,
    )

    run_spark_job >> push_metrics
