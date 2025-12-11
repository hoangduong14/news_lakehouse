# -*- coding: utf-8 -*-
import os
import logging
import uuid
import re
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import MapType, StringType
from pyspark.sql.functions import (
    col, trim, sha2, concat_ws, to_timestamp, to_date, hour, explode_outer,
    map_entries, from_json, coalesce, input_file_name, regexp_extract, udf,
    transform_values, length, lit, when, from_unixtime
)
from tenacity import retry, stop_after_attempt, wait_fixed
from pyspark.storagelevel import StorageLevel

# ---- Import StructType schema cho JSON đầu vào
from schema import news_schema

# -----------------------
# Logging
# -----------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("GCS_Bronze_to_Silver")

# -----------------------
# ENV / Config
# -----------------------
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "my-crawl-bucket-bronze")
SILVER_BUCKET = os.getenv("SILVER_BUCKET", "my-crawl-bucket-silver")
ICEBERG_WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", f"gs://{SILVER_BUCKET}/warehouse/")
# Đổi prefix để buộc Spark đọc lại dữ liệu (tránh dùng checkpoint cũ)
CHECKPOINT_GCS_PREFIX = os.getenv("CHECKPOINT_GCS_PREFIX", f"gs://{SILVER_BUCKET}/checkpoints/silver_v1/")
GCS_TOPICS_ENV = os.getenv("GCS_TOPICS", "").strip()  # "" hoặc "ALL" -> auto-detect
MAX_FILES_PER_TRIGGER = int(os.getenv("MAX_FILES_PER_TRIGGER", "500"))
DATA_SOURCE = os.getenv("DATA_SOURCE", "vnexpress")
ICEBERG_NAMESPACE = os.getenv("ICEBERG_NAMESPACE", "silver.silver_db")

# Google Service Account Key (bắt buộc)
GCS_KEYFILE = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
if not GCS_KEYFILE:
    logger.error("GOOGLE_APPLICATION_CREDENTIALS not set. Stopping pipeline.")
    raise ValueError("Missing GCS credentials")

# -----------------------
# Spark session (Iceberg + GCS với retry)
# -----------------------
spark = (
    SparkSession.builder
    .appName("GCS_Bronze_to_Silver_Iceberg")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    # Iceberg catalog
    .config("spark.sql.catalog.silver", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.silver.type", "hadoop")
    .config("spark.sql.catalog.silver.warehouse", ICEBERG_WAREHOUSE)
    # GCS connector với retry
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", GCS_KEYFILE)
    .config("spark.hadoop.google.cloud.fs.gs.request.retries", "5")
    .config("spark.hadoop.google.cloud.fs.gs.request.retry.interval.ms", "1000")
    # Tối ưu hóa hiệu suất
    .config("spark.sql.shuffle.partitions", "200")
    .config("spark.executor.memory", "4g")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# -----------------------
# Ensure Iceberg namespace & tables
# -----------------------
def create_silver_namespace_and_tables():
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_NAMESPACE}")

    # Dimensions
    spark.sql(f"""
      CREATE TABLE IF NOT EXISTS {ICEBERG_NAMESPACE}.authors (
        AuthorID STRING,
        AuthorName STRING
      ) USING iceberg
    """)
    spark.sql(f"""
      CREATE TABLE IF NOT EXISTS {ICEBERG_NAMESPACE}.topics (
        TopicID STRING,
        TopicName STRING
      ) USING iceberg
    """)
    spark.sql(f"""
      CREATE TABLE IF NOT EXISTS {ICEBERG_NAMESPACE}.subtopics (
        SubTopicID STRING,
        SubTopicName STRING,
        TopicID STRING
      ) USING iceberg
    """)
    spark.sql(f"""
      CREATE TABLE IF NOT EXISTS {ICEBERG_NAMESPACE}.keywords (
        KeywordID STRING,
        KeywordText STRING
      ) USING iceberg
    """)
    spark.sql(f"""
      CREATE TABLE IF NOT EXISTS {ICEBERG_NAMESPACE}.references_table (
        ReferenceID STRING,
        ReferenceText STRING
      ) USING iceberg
    """)

    # Fact: articles (partition by date)
    spark.sql(f"""
      CREATE TABLE IF NOT EXISTS {ICEBERG_NAMESPACE}.articles (
        ArticleID STRING,
        Title STRING,
        URL STRING,
        Description STRING,
        PublicationDate TIMESTAMP,
        MainContent STRING,
        OpinionCount INT,
        AuthorID STRING,
        TopicID STRING,
        SubTopicID STRING,
        date DATE,
        hour INT
      ) USING iceberg
      PARTITIONED BY (date)
    """)

    # Link & comments
    spark.sql(f"""
      CREATE TABLE IF NOT EXISTS {ICEBERG_NAMESPACE}.article_keywords (
        ArticleID STRING,
        KeywordID STRING
      ) USING iceberg
    """)
    spark.sql(f"""
      CREATE TABLE IF NOT EXISTS {ICEBERG_NAMESPACE}.article_references (
        ArticleID STRING,
        ReferenceID STRING
      ) USING iceberg
    """)
    spark.sql(f"""
      CREATE TABLE IF NOT EXISTS {ICEBERG_NAMESPACE}.comments (
        CommentID STRING,
        ArticleID STRING,
        CommenterName STRING,
        CommentContent STRING,
        TotalLikes INT
      ) USING iceberg
    """)
    spark.sql(f"""
      CREATE TABLE IF NOT EXISTS {ICEBERG_NAMESPACE}.comment_interactions (
        CommentInteractionID STRING,
        CommentID STRING,
        InteractionType STRING,
        InteractionCount INT
      ) USING iceberg
    """)
    logger.info("Ensured silver namespace and Iceberg tables.")

# -----------------------
# VN datetime cleanup & normalize
# -----------------------
_WEEKDAY_RE = re.compile(
    r"^\s*(?:th(?:ứ|u)\s*(?:hai|ba|t(?:ư|u)|n(?:ă|a)m|s(?:á|a)u|b(?:ả|a)y)|ch(?:ủ|u)\s*nh(?:ậ|a)t)\s*,\s*",
    flags=re.IGNORECASE
)
_GMT_RE = re.compile(r"\(?\s*gmt\s*([+-])\s*(\d{1,2})(?::\s*(\d{2}))?\s*\)?", flags=re.IGNORECASE)

def _normalize_vn_datetime_str(s: str) -> str | None:
    """
    Chuẩn hoá chuỗi datetime VN -> 'dd/MM/yyyy HH:mm +HH:MM'
    Ví dụ: 'Thứ sáu, 10/10/2025, 14:27 (GMT+7)' -> '10/10/2025 14:27 +07:00'
    """
    if not s:
        return None
    txt = str(s).strip()
    # Bỏ thứ, ví dụ 'Thứ sáu,' 'Chủ nhật,'
    txt = _WEEKDAY_RE.sub("", txt)

    # Mặc định timezone
    tz = "+00:00"
    m = _GMT_RE.search(txt)
    if m:
        sign = m.group(1) or "+"
        hh = int(m.group(2))
        mm = m.group(3) or "00"
        tz = f"{sign}{hh:02d}:{mm}"
        # bỏ đoạn (GMT+7) ra khỏi chuỗi chính
        txt = _GMT_RE.sub("", txt)

    # Đổi dấu phẩy đầu thành khoảng trắng giữa ngày và giờ
    txt = re.sub(r",\s*", " ", txt, count=1)
    txt = txt.replace(",", " ").strip()

    # Bắt pattern ngày/tháng/năm + giờ:phút
    m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})\s+(\d{1,2}):(\d{2})", txt)
    if not m:
        return None

    d = int(m.group(1))
    mo = int(m.group(2))
    year = m.group(3)
    hh = int(m.group(4))
    mi = m.group(5)

    # Chuẩn hoá về dd/MM/yyyy HH:mm
    norm = f"{d:02d}/{mo:02d}/{year} {hh:02d}:{mi}"
    return f"{norm} {tz}".strip()


normalize_vn_datetime_udf = udf(_normalize_vn_datetime_str, StringType())

# -----------------------
# Helpers
# -----------------------
@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def autodetect_topics_from_gcs():
    """Liệt kê thư mục con trực tiếp dưới gs://{BRONZE_BUCKET}/{DATA_SOURCE}/ làm topic."""
    try:
        jvm = spark._jvm
        hconf = spark._jsc.hadoopConfiguration()
        base = f"gs://{BRONZE_BUCKET}/{DATA_SOURCE}/"
        base_path = jvm.org.apache.hadoop.fs.Path(base)
        fs = base_path.getFileSystem(hconf)

        if not fs.exists(base_path):
            logger.warning(f"Path not found: {base}")
            return []

        topics = []
        for st in fs.listStatus(base_path):
            if st.isDirectory():
                topics.append(st.getPath().getName())
        return sorted(set(t for t in topics if t))
    except Exception as e:
        logger.error(f"Auto-detect topics failed: {e}")
        raise

# -----------------------
# Transform helpers
# -----------------------
def make_authors(dfb: DataFrame) -> DataFrame:
    return (
        dfb.select(trim(col("author")).alias("AuthorName"))
           .filter(col("AuthorName").isNotNull() & (col("AuthorName") != ""))
           .dropDuplicates(["AuthorName"])
           .withColumn("AuthorID", sha2(col("AuthorName"), 256))
           .select("AuthorID", "AuthorName")
    )

def make_topics(dfb: DataFrame) -> DataFrame:
    return (
        dfb.select(trim(col("topic")).alias("TopicName"))
           .filter(col("TopicName").isNotNull() & (col("TopicName") != ""))
           .dropDuplicates(["TopicName"])
           .withColumn("TopicID", sha2(col("TopicName"), 256))
           .select("TopicID", "TopicName")
    )

def make_subtopics(dfb: DataFrame) -> DataFrame:
    return (
        dfb.select(
            when(length(trim(col("sub_topic"))) > 0, trim(col("sub_topic"))).otherwise(lit(None)).alias("SubTopicName"),
            when(length(trim(col("topic"))) > 0, trim(col("topic"))).otherwise(lit(None)).alias("topic_name")
        )
        .filter(col("SubTopicName").isNotNull())
        .dropDuplicates(["SubTopicName", "topic_name"])
        .withColumn("TopicID", sha2(col("topic_name"), 256))
        .withColumn("SubTopicID", sha2(concat_ws("||", col("SubTopicName"), col("TopicID")), 256))
        .select("SubTopicID", "SubTopicName", "TopicID")
    )

def make_keywords(dfb: DataFrame) -> DataFrame:
    return (
        dfb.select(explode_outer(col("keywords")).alias("KeywordText"))
           .filter(col("KeywordText").isNotNull() & (col("KeywordText") != ""))
           .withColumn("KeywordText", trim(col("KeywordText")))
           .dropDuplicates(["KeywordText"])
           .withColumn("KeywordID", sha2(col("KeywordText"), 256))
           .select("KeywordID", "KeywordText")
    )

def make_references(dfb: DataFrame) -> DataFrame:
    return (
        dfb.select(explode_outer(col("references")).alias("ReferenceText"))
           .filter(col("ReferenceText").isNotNull() & (col("ReferenceText") != ""))
           .withColumn("ReferenceText", trim(col("ReferenceText")))
           .dropDuplicates(["ReferenceText"])
           .withColumn("ReferenceID", sha2(col("ReferenceText"), 256))
           .select("ReferenceID", "ReferenceText")
    )

def make_articles(dfb: DataFrame) -> DataFrame:
    dfb = dfb.withColumn("trimmed_url", trim(col("url")))
    dfb = dfb.withColumn("ArticleID", sha2(col("trimmed_url"), 256))
    dfb = dfb.withColumn("trimmed_author", trim(col("author")))
    dfb = dfb.withColumn("AuthorID", sha2(col("trimmed_author"), 256))
    dfb = dfb.withColumn("trimmed_topic", trim(col("topic")))
    dfb = dfb.withColumn("TopicID", sha2(col("trimmed_topic"), 256))
    dfb = dfb.withColumn("trimmed_subtopic", when(length(trim(col("sub_topic"))) > 0, trim(col("sub_topic"))).otherwise(lit(None)))
    dfb = dfb.withColumn("SubTopicID", sha2(concat_ws("||", col("trimmed_subtopic"), col("TopicID")), 256))

    return (
        dfb.withColumn("Title", col("title"))
           .withColumn("URL", col("trimmed_url"))
           .withColumn("Description", col("description"))
           .withColumn("PublicationDate", col("publish_ts"))
           .withColumn("MainContent", col("main_content"))
           .withColumn("OpinionCount", col("comment_count").cast("int"))
           .withColumn("date", to_date(col("PublicationDate")))
           .withColumn("hour", hour(col("PublicationDate")))
           .select("ArticleID", "Title", "URL", "Description", "PublicationDate", "MainContent",
                   "OpinionCount", "AuthorID", "TopicID", "SubTopicID", "date", "hour")
    )

def make_article_keywords(dfb: DataFrame) -> DataFrame:
    return (
        dfb.withColumn("ArticleID", sha2(trim(col("url")), 256))
           .select("ArticleID", explode_outer(col("keywords")).alias("kw"))
           .filter(col("kw").isNotNull() & (col("kw") != ""))
           .withColumn("KeywordText", trim(col("kw")))
           .dropDuplicates(["ArticleID", "KeywordText"])
           .withColumn("KeywordID", sha2(col("KeywordText"), 256))
           .select("ArticleID", "KeywordID")
    )

def make_article_references(dfb: DataFrame) -> DataFrame:
    return (
        dfb.withColumn("ArticleID", sha2(trim(col("url")), 256))
           .select("ArticleID", explode_outer(col("references")).alias("ref"))
           .filter(col("ref").isNotNull() & (col("ref") != ""))
           .withColumn("ReferenceText", trim(col("ref")))
           .dropDuplicates(["ArticleID", "ReferenceText"])
           .withColumn("ReferenceID", sha2(col("ReferenceText"), 256))
           .select("ArticleID", "ReferenceID")
    )

def make_comments(dfb: DataFrame) -> DataFrame:
    return (
        dfb.withColumn("ArticleID", sha2(trim(col("url")), 256))
           .select("ArticleID", explode_outer(col("top_comments")).alias("c"))
           .select(
               sha2(concat_ws("||",
                   col("ArticleID"),
                   trim(col("c.commenter_name")),
                   trim(col("c.comment_content"))
               ), 256).alias("CommentID"),
               col("ArticleID"),
               trim(col("c.commenter_name")).alias("CommenterName"),
               trim(col("c.comment_content")).alias("CommentContent"),
               col("c.total_likes").cast("int").alias("TotalLikes")
           )
           .filter(col("CommentID").isNotNull())
    )

def make_comment_interactions(dfb: DataFrame) -> DataFrame:
    base = (
        dfb.withColumn("ArticleID", sha2(trim(col("url")), 256))
           .select("ArticleID", explode_outer(col("top_comments")).alias("c"))
    )

    with_ids = base.select(
        sha2(concat_ws("||",
            col("ArticleID"),
            trim(col("c.commenter_name")),
            trim(col("c.comment_content"))
        ), 256).alias("CommentID"),
        col("c.interaction_details").alias("interaction_raw")
    ).filter(col("interaction_raw").isNotNull() & (length(col("interaction_raw")) > 0))

    map_str_str = from_json(col("interaction_raw"), MapType(StringType(), StringType()))
    with_ids = with_ids.withColumn("interaction_map", map_str_str).filter(col("interaction_map").isNotNull())

    map_str_int = transform_values(col("interaction_map"), lambda k, v: coalesce(v.cast("int"), lit(0)))

    tmp = with_ids.withColumn("interaction_map", map_str_int)

    return (
        tmp.select("CommentID", explode_outer(map_entries(col("interaction_map"))).alias("kv"))
           .select(
               sha2(concat_ws("||", col("CommentID"), col("kv.key")), 256).alias("CommentInteractionID"),
               col("CommentID"),
               col("kv.key").alias("InteractionType"),
               col("kv.value").alias("InteractionCount")
           )
           .filter(col("InteractionCount").isNotNull())
    )

# -----------------------
# Upsert batch: MERGE dimensions & facts
# -----------------------
def upsert_batch(spark: SparkSession, df_batch: DataFrame, epoch_id: int, topic_name: str):
    logger.info(f"Upsert batch {epoch_id} for topic {topic_name}")
    if df_batch.rdd.isEmpty():
        logger.info("Empty batch, skip")
        return

    # Lọc rác: bắt buộc có URL và publish_ts
    base = (
        df_batch
        .filter(trim(col("url")).isNotNull() & (trim(col("url")) != ""))
        .filter(col("publish_ts").isNotNull())
    )

    if base.rdd.isEmpty():
        logger.info("Batch after basic filtering is empty, skip")
        return

    base.persist(StorageLevel.MEMORY_AND_DISK)
    suffix = f"_{uuid.uuid4().hex}"

    dfs = {
        "authors": make_authors(base),
        "topics": make_topics(base),
        "subtopics": make_subtopics(base),
        "keywords": make_keywords(base),
        "refs": make_references(base),
        "articles": make_articles(base),
        "article_keywords": make_article_keywords(base),
        "article_refs": make_article_references(base),
        "comments": make_comments(base),
        "comment_interactions": make_comment_interactions(base),
    }

    for name, df in dfs.items():
        # BƯỚC QUAN TRỌNG: deduplicate theo khóa chính để tránh MERGE_CARDINALITY_VIOLATION
        if name == "comments":
            df = df.dropna(subset=["CommentID"]).dropDuplicates(["CommentID"])
        elif name == "authors":
            df = df.dropna(subset=["AuthorID"]).dropDuplicates(["AuthorID"])
        elif name == "topics":
            df = df.dropna(subset=["TopicID"]).dropDuplicates(["TopicID"])
        elif name == "subtopics":
            df = df.dropna(subset=["SubTopicID"]).dropDuplicates(["SubTopicID"])
        elif name == "keywords":
            df = df.dropna(subset=["KeywordID"]).dropDuplicates(["KeywordID"])
        elif name == "refs":
            df = df.dropna(subset=["ReferenceID"]).dropDuplicates(["ReferenceID"])
        elif name == "articles":
            df = df.dropna(subset=["ArticleID"]).dropDuplicates(["ArticleID"])
        elif name == "comment_interactions":
            df = df.dropna(subset=["CommentInteractionID"]).dropDuplicates(["CommentInteractionID"])
        elif name == "article_keywords":
            df = df.dropna(subset=["ArticleID", "KeywordID"]).dropDuplicates(["ArticleID", "KeywordID"])
        elif name == "article_refs":
            df = df.dropna(subset=["ArticleID", "ReferenceID"]).dropDuplicates(["ArticleID", "ReferenceID"])

        count = df.count()
        logger.info(f"{name}: {count} records")
        if count == 0:
            continue

        view_name = f"tmp_{name}{suffix}"
        df.createOrReplaceGlobalTempView(view_name)

        try:
            if name in ["authors", "topics", "subtopics", "keywords", "refs", "articles", "comments", "comment_interactions"]:
                on_condition = {
                    "authors": "t.AuthorID = s.AuthorID",
                    "topics": "t.TopicID = s.TopicID",
                    "subtopics": "t.SubTopicID = s.SubTopicID",
                    "keywords": "t.KeywordID = s.KeywordID",
                    "refs": "t.ReferenceID = s.ReferenceID",
                    "articles": "t.ArticleID = s.ArticleID",
                    "comments": "t.CommentID = s.CommentID",
                    "comment_interactions": "t.CommentInteractionID = s.CommentInteractionID"
                }[name]
                table_name = name if name != 'refs' else 'references_table'
                spark.sql(f"""
                    MERGE INTO {ICEBERG_NAMESPACE}.{table_name} t
                    USING global_temp.{view_name} s
                    ON {on_condition}
                    WHEN MATCHED THEN UPDATE SET *
                    WHEN NOT MATCHED THEN INSERT *
                """)
            elif name in ["article_keywords", "article_refs"]:
                on_condition = {
                    "article_keywords": "t.ArticleID = s.ArticleID AND t.KeywordID = s.KeywordID",
                    "article_refs": "t.ArticleID = s.ArticleID AND t.ReferenceID = s.ReferenceID"
                }[name]
                table_name = {
                    "article_keywords": "article_keywords",
                    "article_refs": "article_references"
                }[name]
                spark.sql(f"""
                    MERGE INTO {ICEBERG_NAMESPACE}.{table_name} t
                    USING global_temp.{view_name} s
                    ON {on_condition}
                    WHEN NOT MATCHED THEN INSERT *
                """)
            logger.info(f"Merged {name} successfully.")
        finally:
            spark.catalog.dropGlobalTempView(view_name)

    base.unpersist()
    logger.info(f"Batch {epoch_id} processed successfully.")

# -----------------------
# Read from Bronze (GCS file stream) and run foreachBatch
# -----------------------
def process_all_topics_from_gcs(schema):
    """
    Xử lý tất cả topics trong một streaming query:
    - Bắt topic từ path, null-hoá nếu fail.
    - Chuẩn hoá publish_date -> publish_ts (epoch / ISO / VN).
    """
    input_path = f"gs://{BRONZE_BUCKET}/{DATA_SOURCE}/"
    checkpoint = f"{CHECKPOINT_GCS_PREFIX}all_topics/"
    logger.info(f"Watching {input_path} (checkpoint {checkpoint})")

    df = (
        spark.readStream
             .schema(schema)
             .option("recursiveFileLookup", "true")
             .option("maxFilesPerTrigger", MAX_FILES_PER_TRIGGER)
             .option("mode", "PERMISSIVE")
             .json(input_path)

             # bắt topic từ path & null-hoá khi fail
             .withColumn("topic", regexp_extract(input_file_name(), r"/" + DATA_SOURCE + r"/([^/]+)/", 1))
             .withColumn("topic", when(length(trim(col("topic"))) > 0, trim(col("topic"))).otherwise(lit(None)))
             .withColumn("sub_topic", when(length(trim(col("sub_topic"))) > 0, trim(col("sub_topic"))).otherwise(lit(None)))

             # chuẩn hoá publish_date
             .withColumn(
                 "publish_raw_norm",
                 coalesce(
                     when(col("publish_date").cast("long").isNotNull(), col("publish_date")),
                     normalize_vn_datetime_udf(col("publish_date")),
                     col("publish_date")
                 )
             )

             # parse thành timestamp:
             # - nếu là số: epoch giây/ms -> from_unixtime -> to_timestamp
             # - nếu là chuỗi: thử nhiều format (ISO, VN, fallback)
             .withColumn(
                 "publish_ts",
                 when(
                     col("publish_raw_norm").cast("long").isNotNull() & (length(trim(col("publish_raw_norm"))) <= 13),
                     when(
                         col("publish_raw_norm").cast("long") > lit(9999999999),
                         to_timestamp(from_unixtime((col("publish_raw_norm").cast("long")/1000).cast("long")))
                     ).otherwise(
                         to_timestamp(from_unixtime(col("publish_raw_norm").cast("long")))
                     )
                 ).otherwise(
                     coalesce(
                         to_timestamp(col("publish_raw_norm"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
                         to_timestamp(col("publish_raw_norm"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
                         to_timestamp(col("publish_raw_norm"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
                         to_timestamp(col("publish_raw_norm"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
                         to_timestamp(col("publish_raw_norm"), "yyyy-MM-dd'T'HH:mm:ss.SSS"),
                         to_timestamp(col("publish_raw_norm"), "yyyy-MM-dd'T'HH:mm:ss"),
                         to_timestamp(col("publish_raw_norm"), "dd/MM/yyyy HH:mm:ss XXX"),
                         to_timestamp(col("publish_raw_norm"), "dd/MM/yyyy HH:mm XXX"),
                         to_timestamp(col("publish_raw_norm"), "dd/MM/yyyy HH:mm:ss"),
                         to_timestamp(col("publish_raw_norm"), "dd/MM/yyyy HH:mm"),
                         to_timestamp(col("publish_raw_norm"))
                     )
                 )
             )
             .withColumn("date", to_date(col("publish_ts")))
             .withColumn("hour", hour(col("publish_ts")))
    )

    query = (
        df.writeStream
          .foreachBatch(lambda batch_df, epoch_id: upsert_batch(spark, batch_df, epoch_id, "all_topics"))
          .option("checkpointLocation", checkpoint)
          .start()
    )
    return query

# -----------------------
# Main
# -----------------------
if __name__ == "__main__":
    logger.info("Start Bronze -> Silver job (GCS -> Iceberg)")
    try:
        create_silver_namespace_and_tables()

        # Dò topics (để logging/giám sát), nhưng stream đọc toàn bộ
        if (GCS_TOPICS_ENV == "") or (GCS_TOPICS_ENV.lower() == "all"):
            topics = autodetect_topics_from_gcs()
            if not topics:
                raise RuntimeError(f"Không tìm thấy topic nào dưới gs://{BRONZE_BUCKET}/{DATA_SOURCE}/.")
            logger.info(f"Auto-detected topics: {topics}")
        else:
            topics = [t.strip() for t in GCS_TOPICS_ENV.split(",") if t.strip()]
            logger.info(f"Using topics from GCS_TOPICS: {topics}")

        # Xử lý tất cả topics trong một query
        query = process_all_topics_from_gcs(news_schema)
        query.awaitTermination()

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception:
        logger.exception("Fatal error")
        raise
    finally:
        logger.info("Stopping Spark")
        spark.stop()
