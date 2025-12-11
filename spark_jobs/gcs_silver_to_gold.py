# -*- coding: utf-8 -*-
"""
Silver ➜ Gold ETL on GCS + Iceberg (refactored, fixed)
- Upsert (MERGE) cho toàn bộ dim & fact
- check_table_exists chỉ còn 1 bản, dùng DESCRIBE TABLE
- Thêm check tồn tại silver.silver_db.articles cho facts
- Thống nhất InteractionTypeKey (lower + trim, BIGINT) giữa dim & fact
- Coalesce cho các chỉ số đếm
- dropDuplicates cho các key tự nhiên
- Join để lấy ParentTopicName cho dim_sub_topic
- Một số cấu hình GCS/Optimizer
"""

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, month, dayofmonth, dayofyear, quarter, date_format, lit, coalesce,
    xxhash64, split, size, length, regexp_replace, lower, trim
)
from pyspark.sql.utils import AnalysisException

# =======================
# Logging
# =======================
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("Gold_ETL")

# =======================
# ENV / CONFIG
# =======================
SILVER_BUCKET = os.getenv("SILVER_BUCKET", "my-crawl-bucket-silver")
GOLD_BUCKET = os.getenv("GOLD_BUCKET", "my-crawl-bucket-gold")
SILVER_WAREHOUSE = os.getenv("SILVER_WAREHOUSE", f"gs://{SILVER_BUCKET}/warehouse/")
GOLD_WAREHOUSE = os.getenv("GOLD_WAREHOUSE", f"gs://{GOLD_BUCKET}/warehouse/")
GCS_KEYFILE = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# =======================
# Spark Session
# =======================

def create_spark_session():
    spark_builder = (
        SparkSession.builder
        .appName("Silver_to_Gold_Iceberg")
        .master("local[1]")  # chạy 1 thread cho ổn định trên Mac
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        # Silver catalog
        .config("spark.sql.catalog.silver", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.silver.type", "hadoop")
        .config("spark.sql.catalog.silver.warehouse", SILVER_WAREHOUSE)
        # Gold catalog
        .config("spark.sql.catalog.gold", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.gold.type", "hadoop")
        .config("spark.sql.catalog.gold.warehouse", GOLD_WAREHOUSE)
        # GCS connector
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

        # SAFE MODE: tắt bớt thứ hay crash native
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .config("spark.sql.orc.enableVectorizedReader", "false")
        .config("spark.sql.codegen.wholeStage", "false")

        # Optimizations
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "200"))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.optimizer.dynamicPartitionPruning", "true")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
    )

    if GCS_KEYFILE:
        spark_builder = (
            spark_builder
            .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
            .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", GCS_KEYFILE)
            .config("spark.hadoop.fs.gs.auth.service.account.enable", "true")
            .config("spark.hadoop.fs.gs.auth.service.account.json.keyfile", GCS_KEYFILE)
        )
    else:
        logger.warning("GOOGLE_APPLICATION_CREDENTIALS not set. Using workload identity/default credentials.")

    spark = spark_builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# =======================
# Helpers
# =======================

def int_date_key_from_ts(ts_col):
    """Tạo khóa ngày YYYYMMDD dạng INT từ cột timestamp."""
    return (year(ts_col) * lit(10000) + month(ts_col) * lit(100) + dayofmonth(ts_col))


def check_table_exists(spark, full_table_name: str) -> bool:
    """
    Kiểm tra sự tồn tại của bảng dùng tên ĐẦY ĐỦ: 'silver.silver_db.articles' hoặc 'gold.gold_db.dim_author'.
    Dùng DESCRIBE TABLE để đảm bảo đi qua catalog Iceberg.
    """
    try:
        spark.sql(f"DESCRIBE TABLE {full_table_name}").limit(1).collect()
        return True
    except AnalysisException:
        return False


def debug_catalog(spark):
    """Tiện ích debug catalog khi cần (không gọi trong main)."""
    try:
        print("=== SHOW CATALOGS ===")
        spark.sql("SHOW CATALOGS").show(truncate=False)
    except Exception as e:
        print(f"SHOW CATALOGS error: {e}")

    try:
        print("=== SHOW NAMESPACES IN silver ===")
        spark.sql("SHOW NAMESPACES IN silver").show(truncate=False)
    except Exception as e:
        print(f"SHOW NAMESPACES IN silver error: {e}")

    try:
        print("=== SHOW TABLES IN silver.silver_db ===")
        spark.sql("SHOW TABLES IN silver.silver_db").show(truncate=False)
    except Exception as e:
        print(f"SHOW TABLES IN silver.silver_db error: {e}")

    for k in [
        "spark.sql.catalog.silver",
        "spark.sql.catalog.silver.type",
        "spark.sql.catalog.silver.warehouse",
        "spark.sql.catalog.gold",
        "spark.sql.catalog.gold.type",
        "spark.sql.catalog.gold.warehouse",
    ]:
        try:
            print(f"{k} = {spark.conf.get(k)}")
        except Exception:
            pass


# =======================
# Ensure gold namespace & tables
# =======================

def ensure_gold_namespace_and_tables(spark):
    logger.info("Đảm bảo namespace và bảng Gold tồn tại...")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS gold.gold_db")

    # Dimension tables
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS gold.gold_db.dim_date (
          DateKey INT,
          FullDateAlternateKey DATE,
          DayNameOfWeek STRING,
          DayNumberOfMonth INT,
          DayNumberOfYear INT,
          MonthName STRING,
          MonthNumberOfYear INT,
          CalendarQuarter INT,
          CalendarYear INT
        ) USING iceberg
        """
    )

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS gold.gold_db.dim_author (
          AuthorKey BIGINT,
          AuthorID_NK STRING,
          AuthorName STRING
        ) USING iceberg
        """
    )

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS gold.gold_db.dim_topic (
          TopicKey BIGINT,
          TopicID_NK STRING,
          TopicName STRING
        ) USING iceberg
        """
    )

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS gold.gold_db.dim_sub_topic (
          SubTopicKey BIGINT,
          SubTopicID_NK STRING,
          SubTopicName STRING,
          ParentTopicKey BIGINT,
          ParentTopicName STRING
        ) USING iceberg
        """
    )

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS gold.gold_db.dim_keyword (
          KeywordKey BIGINT,
          KeywordID_NK STRING,
          KeywordText STRING
        ) USING iceberg
        """
    )

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS gold.gold_db.dim_reference_source (
          ReferenceSourceKey BIGINT,
          ReferenceID_NK STRING,
          ReferenceText STRING
        ) USING iceberg
        """
    )

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS gold.gold_db.dim_interaction_type (
          InteractionTypeKey BIGINT,
          InteractionType STRING
        ) USING iceberg
        """
    )

    # Fact tables
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS gold.gold_db.fact_article_publication (
          PublicationDateKey INT,
          ArticlePublicationTimestamp TIMESTAMP,
          AuthorKey BIGINT,
          TopicKey BIGINT,
          SubTopicKey BIGINT,
          ArticleID_NK STRING,
          ArticleTitle STRING,
          ArticleDescription STRING,
          PublishedArticleCount INT,
          OpinionCount INT,
          WordCountInMainContent INT,
          CharacterCountInMainContent INT,
          EstimatedReadTimeMinutes DOUBLE,
          TaggedKeywordCountInArticle INT,
          ReferenceSourceCountInArticle INT
        ) USING iceberg
        PARTITIONED BY (PublicationDateKey)
        """
    )

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS gold.gold_db.fact_article_keyword (
          ArticlePublicationDateKey INT,
          ArticleID_NK STRING,
          KeywordKey BIGINT,
          AuthorKey BIGINT,
          TopicKey BIGINT,
          SubTopicKey BIGINT,
          IsKeywordTaggedToArticle INT
        ) USING iceberg
        PARTITIONED BY (ArticlePublicationDateKey)
        """
    )

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS gold.gold_db.fact_article_reference (
          ArticlePublicationDateKey INT,
          ArticleID_NK STRING,
          ReferenceSourceKey BIGINT,
          AuthorKey BIGINT,
          TopicKey BIGINT,
          SubTopicKey BIGINT,
          IsReferenceUsedInArticle INT
        ) USING iceberg
        PARTITIONED BY (ArticlePublicationDateKey)
        """
    )

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS gold.gold_db.fact_top_comment_activity (
          ArticlePublicationDateKey INT,
          CommentDateKey INT,
          ArticleID_NK STRING,
          CommentID_NK STRING,
          AuthorKey BIGINT,
          TopicKey BIGINT,
          SubTopicKey BIGINT,
          CommenterName STRING,
          IsTopComment INT,
          LikesOnTopComment INT
        ) USING iceberg
        PARTITIONED BY (ArticlePublicationDateKey)
        """
    )

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS gold.gold_db.fact_top_comment_interaction_detail (
          ArticlePublicationDateKey INT,
          InteractionDateKey INT,
          ArticleID_NK STRING,
          CommentID_NK STRING,
          InteractionTypeKey BIGINT,
          AuthorKey BIGINT,
          TopicKey BIGINT,
          SubTopicKey BIGINT,
          InteractionInstanceCount INT,
          InteractionValue INT
        ) USING iceberg
        PARTITIONED BY (ArticlePublicationDateKey)
        """
    )

    logger.info("Đã tạo namespace và bảng Gold.")


# =======================
# Dimensions
# =======================

def build_and_merge_dimensions(spark):
    logger.info("Bắt đầu xử lý dimensions...")

    # Kiểm tra bảng Silver bắt buộc
    required_tables = [
        "silver.silver_db.articles",
        "silver.silver_db.authors",
        "silver.silver_db.topics",
        "silver.silver_db.subtopics",
        "silver.silver_db.keywords",
        "silver.silver_db.references_table",
    ]
    for table in required_tables:
        if not check_table_exists(spark, table):
            logger.error(f"Bảng {table} không tồn tại!")
            raise ValueError(f"Bảng {table} không tồn tại!")

    # Cache bảng articles vì được dùng nhiều
    a_tbl = spark.table("silver.silver_db.articles").cache()
    au_tbl = spark.table("silver.silver_db.authors")
    tp_tbl = spark.table("silver.silver_db.topics")
    st_tbl = spark.table("silver.silver_db.subtopics")
    kw_tbl = spark.table("silver.silver_db.keywords")
    rf_tbl = spark.table("silver.silver_db.references_table")

    # ----- dim_date -----
    dim_date = (
        a_tbl
        .select(col("PublicationDate").cast("timestamp").alias("ts"))
        .where(col("ts").isNotNull())
        .select(
            col("ts").cast("date").alias("FullDateAlternateKey"),
            date_format(col("ts"), "EEEE").alias("DayNameOfWeek"),
            dayofmonth(col("ts")).alias("DayNumberOfMonth"),
            dayofyear(col("ts")).alias("DayNumberOfYear"),
            date_format(col("ts"), "MMMM").alias("MonthName"),
            month(col("ts")).alias("MonthNumberOfYear"),
            quarter(col("ts")).alias("CalendarQuarter"),
            year(col("ts")).alias("CalendarYear"),
            int_date_key_from_ts(col("ts")).alias("DateKey"),
        )
        .dropDuplicates(["DateKey"])
    )
    dim_date.createOrReplaceTempView("vd_dim_date")
    spark.sql(
        """
        MERGE INTO gold.gold_db.dim_date t
        USING vd_dim_date s
        ON t.DateKey = s.DateKey
        WHEN MATCHED THEN UPDATE SET
          t.FullDateAlternateKey = s.FullDateAlternateKey,
          t.DayNameOfWeek = s.DayNameOfWeek,
          t.DayNumberOfMonth = s.DayNumberOfMonth,
          t.DayNumberOfYear = s.DayNumberOfYear,
          t.MonthName = s.MonthName,
          t.MonthNumberOfYear = s.MonthNumberOfYear,
          t.CalendarQuarter = s.CalendarQuarter,
          t.CalendarYear = s.CalendarYear
        WHEN NOT MATCHED THEN INSERT (
          DateKey, FullDateAlternateKey, DayNameOfWeek, DayNumberOfMonth, DayNumberOfYear,
          MonthName, MonthNumberOfYear, CalendarQuarter, CalendarYear
        ) VALUES (
          s.DateKey, s.FullDateAlternateKey, s.DayNameOfWeek, s.DayNumberOfMonth, s.DayNumberOfYear,
          s.MonthName, s.MonthNumberOfYear, s.CalendarQuarter, s.CalendarYear
        )
        """
    )

    # ----- dim_author -----
    dim_author = (
        au_tbl
        .withColumn("AuthorKey", xxhash64(col("AuthorID")))
        .withColumnRenamed("AuthorID", "AuthorID_NK")
        .withColumnRenamed("AuthorName", "AuthorName")
        .dropDuplicates(["AuthorID_NK"])
    )
    dim_author.createOrReplaceTempView("vd_dim_author")
    spark.sql(
        """
        MERGE INTO gold.gold_db.dim_author t
        USING vd_dim_author s
        ON t.AuthorID_NK = s.AuthorID_NK
        WHEN MATCHED THEN UPDATE SET
          t.AuthorKey = s.AuthorKey,
          t.AuthorName = s.AuthorName
        WHEN NOT MATCHED THEN INSERT (AuthorKey, AuthorID_NK, AuthorName)
        VALUES (s.AuthorKey, s.AuthorID_NK, s.AuthorName)
        """
    )

    # ----- dim_topic -----
    dim_topic = (
        tp_tbl
        .withColumn("TopicKey", xxhash64(col("TopicID")))
        .withColumnRenamed("TopicID", "TopicID_NK")
        .withColumnRenamed("TopicName", "TopicName")
        .dropDuplicates(["TopicID_NK"])
    )
    dim_topic.createOrReplaceTempView("vd_dim_topic")
    spark.sql(
        """
        MERGE INTO gold.gold_db.dim_topic t
        USING vd_dim_topic s
        ON t.TopicID_NK = s.TopicID_NK
        WHEN MATCHED THEN UPDATE SET
          t.TopicKey = s.TopicKey,
          t.TopicName = s.TopicName
        WHEN NOT MATCHED THEN INSERT (TopicKey, TopicID_NK, TopicName)
        VALUES (s.TopicKey, s.TopicID_NK, s.TopicName)
        """
    )

    # ----- dim_sub_topic (join lấy ParentTopicName) -----
    dim_sub = (
        st_tbl.alias("st")
        .join(tp_tbl.alias("tp"), col("st.TopicID") == col("tp.TopicID"), "left")
        .select(
            xxhash64(col("st.SubTopicID")).alias("SubTopicKey"),
            col("st.SubTopicID").alias("SubTopicID_NK"),
            col("st.SubTopicName").alias("SubTopicName"),
            xxhash64(col("st.TopicID")).alias("ParentTopicKey"),
            col("tp.TopicName").alias("ParentTopicName"),
        )
        .dropDuplicates(["SubTopicID_NK"])
    )
    dim_sub.createOrReplaceTempView("vd_dim_sub_topic")
    spark.sql(
        """
        MERGE INTO gold.gold_db.dim_sub_topic t
        USING vd_dim_sub_topic s
        ON t.SubTopicID_NK = s.SubTopicID_NK
        WHEN MATCHED THEN UPDATE SET
          t.SubTopicKey = s.SubTopicKey,
          t.SubTopicName = s.SubTopicName,
          t.ParentTopicKey = s.ParentTopicKey,
          t.ParentTopicName = s.ParentTopicName
        WHEN NOT MATCHED THEN INSERT (SubTopicKey, SubTopicID_NK, SubTopicName, ParentTopicKey, ParentTopicName)
        VALUES (s.SubTopicKey, s.SubTopicID_NK, s.SubTopicName, s.ParentTopicKey, s.ParentTopicName)
        """
    )

    # ----- dim_keyword -----
    dim_kw = (
        kw_tbl
        .withColumn("KeywordKey", xxhash64(col("KeywordID")))
        .withColumnRenamed("KeywordID", "KeywordID_NK")
        .withColumnRenamed("KeywordText", "KeywordText")
        .dropDuplicates(["KeywordID_NK"])
    )
    dim_kw.createOrReplaceTempView("vd_dim_keyword")
    spark.sql(
        """
        MERGE INTO gold.gold_db.dim_keyword t
        USING vd_dim_keyword s
        ON t.KeywordID_NK = s.KeywordID_NK
        WHEN MATCHED THEN UPDATE SET
          t.KeywordKey = s.KeywordKey,
          t.KeywordText = s.KeywordText
        WHEN NOT MATCHED THEN INSERT (KeywordKey, KeywordID_NK, KeywordText)
        VALUES (s.KeywordKey, s.KeywordID_NK, s.KeywordText)
        """
    )

    # ----- dim_reference_source -----
    dim_ref = (
        rf_tbl
        .withColumn("ReferenceSourceKey", xxhash64(col("ReferenceID")))
        .withColumnRenamed("ReferenceID", "ReferenceID_NK")
        .withColumnRenamed("ReferenceText", "ReferenceText")
        .dropDuplicates(["ReferenceID_NK"])
    )
    dim_ref.createOrReplaceTempView("vd_dim_reference")
    spark.sql(
        """
        MERGE INTO gold.gold_db.dim_reference_source t
        USING vd_dim_reference s
        ON t.ReferenceID_NK = s.ReferenceID_NK
        WHEN MATCHED THEN UPDATE SET
          t.ReferenceSourceKey = s.ReferenceSourceKey,
          t.ReferenceText = s.ReferenceText
        WHEN NOT MATCHED THEN INSERT (ReferenceSourceKey, ReferenceID_NK, ReferenceText)
        VALUES (s.ReferenceSourceKey, s.ReferenceID_NK, s.ReferenceText)
        """
    )

    # ----- dim_interaction_type -----
    if check_table_exists(spark, "silver.silver_db.comment_interactions"):
        ci_tbl = spark.table("silver.silver_db.comment_interactions")
        dim_inter = (
            ci_tbl
            # Chuẩn hóa InteractionType: lower + trim
            .select(lower(trim(col("InteractionType"))).alias("InteractionType"))
            .where(col("InteractionType").isNotNull() & (col("InteractionType") != ""))
            .dropDuplicates(["InteractionType"])
            .withColumn("InteractionTypeKey", xxhash64(col("InteractionType")))
        )
        dim_inter.createOrReplaceTempView("vd_dim_interaction_type")
        spark.sql(
            """
            MERGE INTO gold.gold_db.dim_interaction_type t
            USING vd_dim_interaction_type s
            ON t.InteractionTypeKey = s.InteractionTypeKey
            WHEN MATCHED THEN UPDATE SET
              t.InteractionType = s.InteractionType
            WHEN NOT MATCHED THEN INSERT (InteractionTypeKey, InteractionType)
            VALUES (s.InteractionTypeKey, s.InteractionType)
            """
        )
    else:
        logger.warning("Bảng silver.silver_db.comment_interactions không tồn tại, bỏ qua dim_interaction_type.")

    a_tbl.unpersist()
    logger.info("Hoàn thành xử lý dimensions.")


# =======================
# Facts
# =======================

def build_and_merge_facts(spark):
    logger.info("Bắt đầu xử lý facts...")

    # Check bảng articles bắt buộc
    if not check_table_exists(spark, "silver.silver_db.articles"):
        raise ValueError("Thiếu bảng silver.silver_db.articles")

    a_tbl = spark.table("silver.silver_db.articles").alias("a").cache()

    if not check_table_exists(spark, "silver.silver_db.article_keywords"):
        raise ValueError("Thiếu bảng silver.silver_db.article_keywords")
    if not check_table_exists(spark, "silver.silver_db.article_references"):
        raise ValueError("Thiếu bảng silver.silver_db.article_references")

    ak_tbl = spark.table("silver.silver_db.article_keywords").alias("ak")
    ar_tbl = spark.table("silver.silver_db.article_references").alias("ar")

    # ----- fact_article_publication -----
    kw_cnt = ak_tbl.groupBy(col("ArticleID")).count().withColumnRenamed("count", "kw_cnt")
    rf_cnt = ar_tbl.groupBy(col("ArticleID")).count().withColumnRenamed("count", "ref_cnt")

    fact_pub = (
        a_tbl
        .join(kw_cnt, kw_cnt.ArticleID == col("a.ArticleID"), "left")
        .join(rf_cnt, rf_cnt.ArticleID == col("a.ArticleID"), "left")
        .withColumn("ts", col("a.PublicationDate").cast("timestamp"))
        .where(col("ts").isNotNull())
        .withColumn("PublicationDateKey", int_date_key_from_ts(col("ts")))
        .withColumn("ArticlePublicationTimestamp", col("ts"))
        .withColumn("AuthorKey", xxhash64(col("a.AuthorID")))
        .withColumn("TopicKey", xxhash64(col("a.TopicID")))
        .withColumn("SubTopicKey", xxhash64(col("a.SubTopicID")))
        .withColumn("ArticleID_NK", col("a.ArticleID"))
        .withColumn("ArticleTitle", col("a.Title"))
        .withColumn("ArticleDescription", col("a.Description"))
        .withColumn("PublishedArticleCount", lit(1))
        .withColumn("OpinionCount", coalesce(col("a.OpinionCount").cast("int"), lit(0)))
        .withColumn(
            "WordCountInMainContent",
            coalesce(size(split(regexp_replace(col("a.MainContent").cast("string"), r"\s+", " "), " ")), lit(0))
        )
        .withColumn(
            "CharacterCountInMainContent",
            coalesce(length(col("a.MainContent").cast("string")), lit(0))
        )
        .withColumn(
            "EstimatedReadTimeMinutes",
            (coalesce(size(split(regexp_replace(col("a.MainContent").cast("string"), r"\s+", " "), " ")), lit(0)).cast("double") / lit(220.0))
        )
        .withColumn("TaggedKeywordCountInArticle", coalesce(col("kw_cnt"), lit(0)))
        .withColumn("ReferenceSourceCountInArticle", coalesce(col("ref_cnt"), lit(0)))
        .select(
            "PublicationDateKey", "ArticlePublicationTimestamp", "AuthorKey", "TopicKey", "SubTopicKey",
            "ArticleID_NK", "ArticleTitle", "ArticleDescription", "PublishedArticleCount", "OpinionCount",
            "WordCountInMainContent", "CharacterCountInMainContent", "EstimatedReadTimeMinutes",
            "TaggedKeywordCountInArticle", "ReferenceSourceCountInArticle",
        )
        .dropDuplicates(["PublicationDateKey", "ArticleID_NK"])
    )
    fact_pub.createOrReplaceTempView("vd_fact_article_publication")
    spark.sql(
        """
        MERGE INTO gold.gold_db.fact_article_publication t
        USING vd_fact_article_publication s
        ON t.ArticleID_NK = s.ArticleID_NK AND t.PublicationDateKey = s.PublicationDateKey
        WHEN MATCHED THEN UPDATE SET
          t.ArticlePublicationTimestamp = s.ArticlePublicationTimestamp,
          t.AuthorKey = s.AuthorKey,
          t.TopicKey = s.TopicKey,
          t.SubTopicKey = s.SubTopicKey,
          t.ArticleTitle = s.ArticleTitle,
          t.ArticleDescription = s.ArticleDescription,
          t.PublishedArticleCount = s.PublishedArticleCount,
          t.OpinionCount = s.OpinionCount,
          t.WordCountInMainContent = s.WordCountInMainContent,
          t.CharacterCountInMainContent = s.CharacterCountInMainContent,
          t.EstimatedReadTimeMinutes = s.EstimatedReadTimeMinutes,
          t.TaggedKeywordCountInArticle = s.TaggedKeywordCountInArticle,
          t.ReferenceSourceCountInArticle = s.ReferenceSourceCountInArticle
        WHEN NOT MATCHED THEN INSERT (
          PublicationDateKey, ArticlePublicationTimestamp, AuthorKey, TopicKey, SubTopicKey,
          ArticleID_NK, ArticleTitle, ArticleDescription, PublishedArticleCount, OpinionCount,
          WordCountInMainContent, CharacterCountInMainContent, EstimatedReadTimeMinutes,
          TaggedKeywordCountInArticle, ReferenceSourceCountInArticle
        ) VALUES (
          s.PublicationDateKey, s.ArticlePublicationTimestamp, s.AuthorKey, s.TopicKey, s.SubTopicKey,
          s.ArticleID_NK, s.ArticleTitle, s.ArticleDescription, s.PublishedArticleCount, s.OpinionCount,
          s.WordCountInMainContent, s.CharacterCountInMainContent, s.EstimatedReadTimeMinutes,
          s.TaggedKeywordCountInArticle, s.ReferenceSourceCountInArticle
        )
        """
    )

    # ----- fact_article_keyword -----
    fact_kw = (
        ak_tbl.join(a_tbl, col("ak.ArticleID") == col("a.ArticleID"), "inner")
        .withColumn("ts", col("a.PublicationDate").cast("timestamp"))
        .where(col("ts").isNotNull())
        .withColumn("ArticlePublicationDateKey", int_date_key_from_ts(col("ts")))
        .withColumn("ArticleID_NK", col("a.ArticleID"))
        .withColumn("KeywordKey", xxhash64(col("ak.KeywordID")))
        .withColumn("AuthorKey", xxhash64(col("a.AuthorID")))
        .withColumn("TopicKey", xxhash64(col("a.TopicID")))
        .withColumn("SubTopicKey", xxhash64(col("a.SubTopicID")))
        .withColumn("IsKeywordTaggedToArticle", lit(1))
        .select(
            "ArticlePublicationDateKey", "ArticleID_NK", "KeywordKey", "AuthorKey", "TopicKey", "SubTopicKey", "IsKeywordTaggedToArticle"
        )
        .dropDuplicates(["ArticlePublicationDateKey", "ArticleID_NK", "KeywordKey"])
    )
    fact_kw.createOrReplaceTempView("vd_fact_article_keyword")
    spark.sql(
        """
        MERGE INTO gold.gold_db.fact_article_keyword t
        USING vd_fact_article_keyword s
        ON t.ArticlePublicationDateKey = s.ArticlePublicationDateKey
           AND t.ArticleID_NK = s.ArticleID_NK
           AND t.KeywordKey = s.KeywordKey
        WHEN MATCHED THEN UPDATE SET
          t.AuthorKey = s.AuthorKey,
          t.TopicKey = s.TopicKey,
          t.SubTopicKey = s.SubTopicKey,
          t.IsKeywordTaggedToArticle = s.IsKeywordTaggedToArticle
        WHEN NOT MATCHED THEN INSERT (
          ArticlePublicationDateKey, ArticleID_NK, KeywordKey, AuthorKey, TopicKey, SubTopicKey, IsKeywordTaggedToArticle
        ) VALUES (
          s.ArticlePublicationDateKey, s.ArticleID_NK, s.KeywordKey, s.AuthorKey, s.TopicKey, s.SubTopicKey, s.IsKeywordTaggedToArticle
        )
        """
    )

    # ----- fact_article_reference -----
    fact_ref = (
        ar_tbl.join(a_tbl, col("ar.ArticleID") == col("a.ArticleID"), "inner")
        .withColumn("ts", col("a.PublicationDate").cast("timestamp"))
        .where(col("ts").isNotNull())
        .withColumn("ArticlePublicationDateKey", int_date_key_from_ts(col("ts")))
        .withColumn("ArticleID_NK", col("a.ArticleID"))
        .withColumn("ReferenceSourceKey", xxhash64(col("ar.ReferenceID")))
        .withColumn("AuthorKey", xxhash64(col("a.AuthorID")))
        .withColumn("TopicKey", xxhash64(col("a.TopicID")))
        .withColumn("SubTopicKey", xxhash64(col("a.SubTopicID")))
        .withColumn("IsReferenceUsedInArticle", lit(1))
        .select(
            "ArticlePublicationDateKey", "ArticleID_NK", "ReferenceSourceKey", "AuthorKey", "TopicKey", "SubTopicKey", "IsReferenceUsedInArticle"
        )
        .dropDuplicates(["ArticlePublicationDateKey", "ArticleID_NK", "ReferenceSourceKey"])
    )
    fact_ref.createOrReplaceTempView("vd_fact_article_reference")
    spark.sql(
        """
        MERGE INTO gold.gold_db.fact_article_reference t
        USING vd_fact_article_reference s
        ON t.ArticlePublicationDateKey = s.ArticlePublicationDateKey
           AND t.ArticleID_NK = s.ArticleID_NK
           AND t.ReferenceSourceKey = s.ReferenceSourceKey
        WHEN MATCHED THEN UPDATE SET
          t.AuthorKey = s.AuthorKey,
          t.TopicKey = s.TopicKey,
          t.SubTopicKey = s.SubTopicKey,
          t.IsReferenceUsedInArticle = s.IsReferenceUsedInArticle
        WHEN NOT MATCHED THEN INSERT (
          ArticlePublicationDateKey, ArticleID_NK, ReferenceSourceKey, AuthorKey, TopicKey, SubTopicKey, IsReferenceUsedInArticle
        ) VALUES (
          s.ArticlePublicationDateKey, s.ArticleID_NK, s.ReferenceSourceKey, s.AuthorKey, s.TopicKey, s.SubTopicKey, s.IsReferenceUsedInArticle
        )
        """
    )

    # ----- fact_comment_activity -----
    if check_table_exists(spark, "silver.silver_db.comments"):
        c_tbl = spark.table("silver.silver_db.comments").alias("c")
        fact_cmt = (
            c_tbl.join(a_tbl, col("c.ArticleID") == col("a.ArticleID"), "inner")
            .withColumn("ts", col("a.PublicationDate").cast("timestamp"))
            .where(col("ts").isNotNull())
            .withColumn("ArticlePublicationDateKey", int_date_key_from_ts(col("ts")))
            # Hiện đang dùng ngày publish làm CommentDateKey; nếu có cột riêng về ngày comment thì đổi ở đây
            .withColumn("CommentDateKey", int_date_key_from_ts(col("ts")))
            .withColumn("ArticleID_NK", col("a.ArticleID"))
            .withColumn("CommentID_NK", col("c.CommentID"))
            .withColumn("AuthorKey", xxhash64(col("a.AuthorID")))
            .withColumn("TopicKey", xxhash64(col("a.TopicID")))
            .withColumn("SubTopicKey", xxhash64(col("a.SubTopicID")))
            .withColumn("CommenterName", col("c.CommenterName"))
            .withColumn("IsTopComment", lit(1))  #
            .withColumn("LikesOnTopComment", coalesce(col("c.TotalLikes").cast("int"), lit(0)))
            .select(
                "ArticlePublicationDateKey", "CommentDateKey", "ArticleID_NK", "CommentID_NK",
                "AuthorKey", "TopicKey", "SubTopicKey", "CommenterName", "IsTopComment", "LikesOnTopComment"
            )
            .dropDuplicates(["ArticlePublicationDateKey", "ArticleID_NK", "CommentID_NK"])
        )
        fact_cmt.createOrReplaceTempView("vd_fact_top_comment_activity")
        spark.sql(
            """
            MERGE INTO gold.gold_db.fact_top_comment_activity t
            USING vd_fact_top_comment_activity s
            ON t.ArticlePublicationDateKey = s.ArticlePublicationDateKey
               AND t.ArticleID_NK = s.ArticleID_NK
               AND t.CommentID_NK = s.CommentID_NK
            WHEN MATCHED THEN UPDATE SET
              t.CommentDateKey = s.CommentDateKey,
              t.AuthorKey = s.AuthorKey,
              t.TopicKey = s.TopicKey,
              t.SubTopicKey = s.SubTopicKey,
              t.CommenterName = s.CommenterName,
              t.IsTopComment = s.IsTopComment,
              t.LikesOnTopComment = s.LikesOnTopComment
            WHEN NOT MATCHED THEN INSERT (
              ArticlePublicationDateKey, CommentDateKey, ArticleID_NK, CommentID_NK,
              AuthorKey, TopicKey, SubTopicKey, CommenterName, IsTopComment, LikesOnTopComment
            ) VALUES (
              s.ArticlePublicationDateKey, s.CommentDateKey, s.ArticleID_NK, s.CommentID_NK,
              s.AuthorKey, s.TopicKey, s.SubTopicKey, s.CommenterName, s.IsTopComment, s.LikesOnTopComment
            )
            """
        )
    else:
        logger.warning("Bảng silver.silver_db.comments không tồn tại, bỏ qua fact_top_comment_activity.")

    # ----- fact_top_comment_interaction_detail -----
    if check_table_exists(spark, "silver.silver_db.comment_interactions") and check_table_exists(spark, "silver.silver_db.comments"):
        ci_tbl = spark.table("silver.silver_db.comment_interactions").alias("ci")
        c_tbl = spark.table("silver.silver_db.comments").alias("c")
        ci_join = (
            ci_tbl
            .join(c_tbl, col("ci.CommentID") == col("c.CommentID"), "inner")
            .join(a_tbl, col("c.ArticleID") == col("a.ArticleID"), "inner")
        )

        fact_inter = (
            ci_join
            .withColumn("ts", col("a.PublicationDate").cast("timestamp"))
            .where(col("ts").isNotNull())
            .withColumn("ArticlePublicationDateKey", int_date_key_from_ts(col("ts")))
            .withColumn("InteractionDateKey", int_date_key_from_ts(col("ts")))  # nếu có ngày interaction riêng thì thay ở đây
            .withColumn("ArticleID_NK", col("a.ArticleID"))
            .withColumn("CommentID_NK", col("c.CommentID"))
            # Thống nhất với dim_interaction_type: lower + trim + xxhash64 (BIGINT)
            .withColumn("InteractionTypeKey", xxhash64(lower(trim(col("ci.InteractionType")))))
            .withColumn("AuthorKey", xxhash64(col("a.AuthorID")))
            .withColumn("TopicKey", xxhash64(col("a.TopicID")))
            .withColumn("SubTopicKey", xxhash64(col("a.SubTopicID")))
            .withColumn("InteractionInstanceCount", lit(1))
            .withColumn("InteractionValue", coalesce(col("ci.InteractionCount").cast("int"), lit(0)))
            .select(
                "ArticlePublicationDateKey", "InteractionDateKey", "ArticleID_NK", "CommentID_NK", "InteractionTypeKey",
                "AuthorKey", "TopicKey", "SubTopicKey", "InteractionInstanceCount", "InteractionValue",
            )
            .dropDuplicates(["ArticlePublicationDateKey", "ArticleID_NK", "CommentID_NK", "InteractionTypeKey"])
        )
        fact_inter.createOrReplaceTempView("vd_fact_top_comment_interaction_detail")
        spark.sql(
            """
            MERGE INTO gold.gold_db.fact_top_comment_interaction_detail t
            USING vd_fact_top_comment_interaction_detail s
            ON t.ArticlePublicationDateKey = s.ArticlePublicationDateKey
               AND t.ArticleID_NK = s.ArticleID_NK
               AND t.CommentID_NK = s.CommentID_NK
               AND t.InteractionTypeKey = s.InteractionTypeKey
            WHEN MATCHED THEN UPDATE SET
              t.InteractionDateKey = s.InteractionDateKey,
              t.AuthorKey = s.AuthorKey,
              t.TopicKey = s.TopicKey,
              t.SubTopicKey = s.SubTopicKey,
              t.InteractionInstanceCount = s.InteractionInstanceCount,
              t.InteractionValue = s.InteractionValue
            WHEN NOT MATCHED THEN INSERT (
              ArticlePublicationDateKey, InteractionDateKey, ArticleID_NK, CommentID_NK,
              InteractionTypeKey, AuthorKey, TopicKey, SubTopicKey, InteractionInstanceCount, InteractionValue
            ) VALUES (
              s.ArticlePublicationDateKey, s.InteractionDateKey, s.ArticleID_NK, s.CommentID_NK,
              s.InteractionTypeKey, s.AuthorKey, s.TopicKey, s.SubTopicKey, s.InteractionInstanceCount, s.InteractionValue
            )
            """
        )
    else:
        logger.warning("Thiếu comments hoặc comment_interactions, bỏ qua fact_top_comment_interaction_detail.")

    a_tbl.unpersist()
    logger.info("Hoàn thành xử lý facts.")


# =======================
# Main
# =======================
if __name__ == "__main__":
    spark = None
    try:
        logger.info("Bắt đầu ETL Silver -> Gold (refactored, fixed)")
        spark = create_spark_session()
        ensure_gold_namespace_and_tables(spark)
        build_and_merge_dimensions(spark)
        build_and_merge_facts(spark)
        logger.info("Hoàn thành ETL Gold.")
    except Exception as e:
        logger.error(f"Lỗi nghiêm trọng trong ETL Gold: {str(e)}", exc_info=True)
        raise
    finally:
        if spark:
            logger.info("Đang dừng Spark")
            spark.stop()
