import time
import socket
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

# Nếu Airflow và Monitoring cùng docker network "monitoring"
PUSHGATEWAY_URL = "http://pushgateway:9091"


def run_etl_with_metrics(
    job_name: str,
    dag_id: str,
    task_id: str,
    etl_callable,
    etl_kwargs: dict | None = None,
    records_extractor=None,
):
    """
    Wrapper cho ETL:
      - chạy hàm etl_callable
      - đo thời gian chạy
      - lấy số record xử lý (nếu có)
      - push metrics lên Pushgateway
    """
    if etl_kwargs is None:
        etl_kwargs = {}

    start_time = time.time()
    success = True
    records_processed = None

    try:
        result = etl_callable(**etl_kwargs)

        if records_extractor is not None:
            try:
                records_processed = records_extractor(result)
            except Exception:
                records_processed = None

    except Exception:
        success = False
        raise

    finally:
        duration = time.time() - start_time
        _push_metrics_to_pushgateway(
            job_name=job_name,
            dag_id=dag_id,
            task_id=task_id,
            duration=duration,
            success=success,
            records_processed=records_processed,
        )

    return result


def _push_metrics_to_pushgateway(
    job_name: str,
    dag_id: str,
    task_id: str,
    duration: float,
    success: bool,
    records_processed: int | None,
):
    registry = CollectorRegistry()

    duration_gauge = Gauge(
        "etl_job_duration_seconds",
        "Duration of ETL job in seconds",
        ["job_name", "dag_id", "task_id", "status", "host"],
        registry=registry,
    )

    records_gauge = Gauge(
        "etl_job_records_processed_total",
        "Total records processed by ETL job",
        ["job_name", "dag_id", "task_id", "status", "host"],
        registry=registry,
    )

    status = "success" if success else "failure"
    host = socket.gethostname()

    duration_gauge.labels(
        job_name=job_name,
        dag_id=dag_id,
        task_id=task_id,
        status=status,
        host=host,
    ).set(duration)

    if records_processed is not None:
        records_gauge.labels(
            job_name=job_name,
            dag_id=dag_id,
            task_id=task_id,
            status=status,
            host=host,
        ).set(records_processed)

    push_to_gateway(
        PUSHGATEWAY_URL,
        job=job_name,
        registry=registry,
    )
