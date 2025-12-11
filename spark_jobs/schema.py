from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType

news_schema = StructType([
    StructField("title", StringType(), True),
    StructField("url", StringType(), True),
    StructField("author", StringType(), True),
    StructField("topic", StringType(), True),
    StructField("sub_topic", StringType(), True),
    StructField("publish_date", StringType(), True),
    StructField("description", StringType(), True),
    StructField("main_content", StringType(), True),
    StructField("keywords", ArrayType(StringType()), True),
    StructField("references", ArrayType(StringType()), True),
    StructField("comment_count", IntegerType(), True),
    StructField("top_comments", ArrayType(
        StructType([
            StructField("commenter_name", StringType(), True),
            StructField("comment_content", StringType(), True),
            StructField("total_likes", IntegerType(), True),
            StructField("interaction_details", StringType(), True)
        ])
    ), True),
    StructField("ingested_at", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("day", IntegerType(), True)
])