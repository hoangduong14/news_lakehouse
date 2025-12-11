# -*- coding: utf-8 -*-
"""
Đẩy bảng GOLD (Iceberg) lên BigQuery để dùng Looker Studio.
- Dùng Spark BigQuery Connector (df.write.format("bigquery"))
- Full refresh: WRITE_TRUNCATE + mode("overwrite") (không TRUNCATE thủ công)
- Tự tạo dataset nếu thiếu
- Tự tạo partition theo ngày xuất bản cho fact (nếu có PublicationDate)
- Kiểm tra bảng Iceberg bằng SHOW TABLES IN gold.gold_db
"""
import os
import logging
from typing import Dict, Optional, Tuple
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from google.cloud import bigquery
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import to_timestamp

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("GoldToBigQuery")

# ENV / Config (có thể override qua env bên ngoài)
os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", "/Users/hoangtheduong/Downloads/mythical-bazaar-475215-i7-337ee07f878c.json")
os.environ.setdefault("GCP_PROJECT", "mythical-bazaar-475215-i7")
os.environ.setdefault("BQ_DATASET", "news_gold")
os.environ.setdefault("BIGQUERY_TEMP_BUCKET", "my-crawl-bucket-gold")
os.environ.setdefault("BQ_LOCATION", "US")

GCP_PROJECT = os.getenv("GCP_PROJECT", "").strip()
BQ_DATASET = os.getenv("BQ_DATASET", "news_gold").strip()
BQ_TEMP_BUCKET = os.getenv("BIGQUERY_TEMP_BUCKET", "").strip()
BQ_LOCATION = os.getenv("BQ_LOCATION", "US").strip()

if not GCP_PROJECT:
    raise RuntimeError("Thiếu GCP_PROJECT.")
if not BQ_TEMP_BUCKET:
    raise RuntimeError("Thiếu BIGQUERY_TEMP_BUCKET.")

# Danh sách bảng: Iceberg (gold) ➜ BigQuery FQ (project.dataset.table)
TABLE_MAP: Dict[str, str] = {
    "gold.gold_db.dim_author": f"{GCP_PROJECT}.{BQ_DATASET}.dim_author",
    "gold.gold_db.dim_topic": f"{GCP_PROJECT}.{BQ_DATASET}.dim_topic",
    "gold.gold_db.dim_sub_topic": f"{GCP_PROJECT}.{BQ_DATASET}.dim_sub_topic",
    "gold.gold_db.dim_keyword": f"{GCP_PROJECT}.{BQ_DATASET}.dim_keyword",
    "gold.gold_db.dim_reference_source": f"{GCP_PROJECT}.{BQ_DATASET}.dim_reference_source",
    "gold.gold_db.dim_date": f"{GCP_PROJECT}.{BQ_DATASET}.dim_date",
    "gold.gold_db.dim_interaction_type": f"{GCP_PROJECT}.{BQ_DATASET}.dim_interaction_type",  
    "gold.gold_db.fact_article_publication": f"{GCP_PROJECT}.{BQ_DATASET}.fact_article_publication",
    "gold.gold_db.fact_article_keyword": f"{GCP_PROJECT}.{BQ_DATASET}.fact_article_keyword",
    "gold.gold_db.fact_article_reference": f"{GCP_PROJECT}.{BQ_DATASET}.fact_article_reference",
    "gold.gold_db.fact_top_comment_activity": f"{GCP_PROJECT}.{BQ_DATASET}.fact_top_comment_activity",
    "gold.gold_db.fact_top_comment_interaction_detail": f"{GCP_PROJECT}.{BQ_DATASET}.fact_top_comment_interaction_detail",
}

# Timestamp chính (nếu muốn thêm cột PublicationDate để partition BQ)
FACT_TS_COL = {
    "gold.gold_db.fact_article_publication": "ArticlePublicationTimestamp",
    "gold.gold_db.fact_article_keyword": None,
    "gold.gold_db.fact_article_reference": None,
    "gold.gold_db.fact_top_comment_activity": None,
    "gold.gold_db.fact_top_comment_interaction_detail": None,
}

# Spark session
spark = (
    SparkSession.builder
    .appName("Gold_to_BigQuery_FullRefresh")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.gold", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.gold.type", "hadoop")
    .config("spark.sql.catalog.gold.warehouse", "gs://my-crawl-bucket-gold/warehouse/")
    .config("spark.sql.catalog.gold.cache-enabled", "false")
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Helpers
def ensure_bq_dataset(client: bigquery.Client, project: str, dataset: str, location: str):
    ds_ref = bigquery.Dataset(f"{project}.{dataset}")
    try:
        client.get_dataset(ds_ref)
        logger.info(f"BigQuery dataset tồn tại: {project}.{dataset}")
    except Exception:
        ds_ref.location = location
        client.create_dataset(ds_ref, exists_ok=True)
        logger.info(f"Đã tạo BigQuery dataset: {project}.{dataset} ({location})")

def parse_bq_fq_name(fq: str) -> Tuple[str, str, str]:
    """Parse 'project.dataset.table' -> (project, dataset, table)."""
    parts = fq.split(".")
    if len(parts) != 3:
        raise ValueError(f"Bảng BigQuery không đúng định dạng project.dataset.table: {fq}")
    return parts[0], parts[1], parts[2]

def check_iceberg_table(spark: SparkSession, iceberg_table: str) -> bool:
    """Kiểm tra tồn tại bảng Iceberg qua SHOW TABLES IN gold.gold_db."""
    try:
        rows = spark.sql("SHOW TABLES IN gold.gold_db").collect()
        names = {r[1] for r in rows}  # r[1] = tableName
        target = iceberg_table.split(".")[-1]
        if target not in names:
            logger.error(f"Bảng Iceberg `{iceberg_table}` không tồn tại trong gold.gold_db. Các bảng hiện có: {sorted(list(names))}")
            return False
        df = spark.table(iceberg_table)
        _ = df.limit(1).count()  # kích hoạt đọc để chắc chắn
        logger.info(f"Bảng `{iceberg_table}` tồn tại. Schema: {df.schema}")
        return True
    except Exception as e:
        logger.error(f"Lỗi khi kiểm tra bảng Iceberg `{iceberg_table}`: {e}")
        return False

# Ghi BigQuery với overwrite + optional partitionField
def write_df_to_bq_full_refresh(df, bq_table_fq: str, partition_field: Optional[str] = None):
    proj, ds, tbl = parse_bq_fq_name(bq_table_fq)

    writer = (
        df.write
        .format("bigquery")
        # chỉ định đích đúng cách cho spark-bigquery:
        .option("project", proj)                 # project đích của bảng
        .option("parentProject", GCP_PROJECT)    # project để bill / job (thường giống project)
        .option("dataset", ds)
        .option("table", tbl)
        # cấu hình load
        .option("writeMethod", "direct")
        .option("temporaryGcsBucket", BQ_TEMP_BUCKET)
        .option("createDisposition", "CREATE_IF_NEEDED")
        .option("writeDisposition", "WRITE_TRUNCATE")  # full refresh
    )
    if partition_field:
        writer = (writer
              .option("partitionField", partition_field)
              .option("partitionType", "DAY")
              # gợi ý clustering cho fact
              .option("clusteredFields", "TopicKey,AuthorKey"))
    # Với spark-bigquery: khi đã set project/dataset/table ở option thì save() KHÔNG truyền path
    writer.mode("append").save()

# Core: Full refresh một bảng
def full_refresh_one_table(iceberg_table: str, bq_table_fq: str):
    logger.info(f"=== BẮT ĐẦU đồng bộ: {iceberg_table} -> {bq_table_fq}")
    # Parse đích để ensure dataset đúng
    proj, ds, tbl = parse_bq_fq_name(bq_table_fq)
    client = bigquery.Client(project=proj)

    # 1) Kiểm tra bảng Iceberg
    if not check_iceberg_table(spark, iceberg_table):
        raise RuntimeError(f"Bảng Iceberg `{iceberg_table}` không tồn tại hoặc trống.")

    # 2) Đảm bảo dataset đích
    ensure_bq_dataset(client, proj, ds, BQ_LOCATION)

    # 3) Đọc từ Iceberg Gold
    df = spark.table(iceberg_table)

    # 4) Nếu là fact có timestamp -> tạo PublicationDate để partition ở BQ
    ts_col = FACT_TS_COL.get(iceberg_table)
    partition_field = None
    if ts_col and ts_col in df.columns:
        df = df.withColumn(ts_col, col(ts_col).cast(TimestampType()))
        if "PublicationDate" not in df.columns:
            df = df.withColumn("PublicationDate", to_date(col(ts_col)))
            logger.info(f"Đã thêm cột PublicationDate từ `{ts_col}` cho `{iceberg_table}`.")
        partition_field = "PublicationDate"

    # 5) Ghi dữ liệu mới (full refresh) vào BigQuery
    write_df_to_bq_full_refresh(df, bq_table_fq, partition_field=partition_field)
    logger.info(f"Đã ghi dữ liệu từ `{iceberg_table}` vào `{bq_table_fq}` (full refresh).")

    # 6) Log số dòng sau ghi (giúp xác nhận đích đúng)
    try:
        tbl_ref = client.get_table(f"{proj}.{ds}.{tbl}")
        logger.info(f"BigQuery {proj}.{ds}.{tbl}: {tbl_ref.num_rows} rows sau khi load.")
    except Exception as e:
        logger.warning(f"Không đọc được thông tin bảng vừa ghi: {e}")

    logger.info(f"=== HOÀN TẤT: {iceberg_table} -> {bq_table_fq}")

# Convenience view (phục vụ Looker Studio)
FLAT_VIEW_SQL = f"""
CREATE OR REPLACE VIEW `{GCP_PROJECT}.{BQ_DATASET}.vw_articles_flat` AS
SELECT
  f.ArticleID_NK,
  f.ArticlePublicationTimestamp,
  DATE(f.ArticlePublicationTimestamp) AS PublicationDate,
  f.ArticleTitle,
  f.ArticleDescription,
  f.OpinionCount,
  f.WordCountInMainContent,
  f.CharacterCountInMainContent,
  f.EstimatedReadTimeMinutes,
  f.TaggedKeywordCountInArticle,
  f.ReferenceSourceCountInArticle,
  a.AuthorName,
  t.TopicName,
  st.SubTopicName
FROM `{GCP_PROJECT}.{BQ_DATASET}.fact_article_publication` f
LEFT JOIN `{GCP_PROJECT}.{BQ_DATASET}.dim_author` a
  ON f.AuthorKey = a.AuthorKey
LEFT JOIN `{GCP_PROJECT}.{BQ_DATASET}.dim_topic` t
  ON f.TopicKey = t.TopicKey
LEFT JOIN `{GCP_PROJECT}.{BQ_DATASET}.dim_sub_topic` st
  ON f.SubTopicKey = st.SubTopicKey
"""

def create_flat_view():
    try:
        client = bigquery.Client(project=GCP_PROJECT)
        client.query(FLAT_VIEW_SQL).result()
        logger.info(f"Đã tạo/cập nhật view `{GCP_PROJECT}.{BQ_DATASET}.vw_articles_flat`.")
    except Exception as e:
        logger.warning(f"Tạo view flat thất bại: {e}")

# Main
if __name__ == "__main__":
    try:
        logger.info("Bắt đầu FULL REFRESH Gold -> BigQuery cho Looker Studio")
        for ice_tbl, bq_tbl in TABLE_MAP.items():
            full_refresh_one_table(ice_tbl, bq_tbl)
        create_flat_view()
        logger.info("Đồng bộ hoàn tất. Looker Studio → Add data → BigQuery → dataset news_gold.")
    except Exception as e:
        logger.exception("Lỗi khi đồng bộ Gold -> BigQuery")
        raise
    finally:
        spark.stop()
