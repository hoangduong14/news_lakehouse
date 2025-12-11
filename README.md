pip install pyspark
pip install minio

_____GCS_BRONZE_TO_SILVER.PY______
spark-submit \
  --packages "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.5" \
  --conf spark.driver.userClassPathFirst=true \
  --conf spark.executor.userClassPathFirst=true \
  --conf spark.sql.catalog.silver=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.silver.type=hadoop \
  --conf spark.sql.catalog.silver.warehouse=gs://my-crawl-bucket-silver/warehouse/ \
  --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
  --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
  --conf spark.hadoop.google.cloud.auth.service.account.enable=true \
  --conf spark.hadoop.google.cloud.auth.service.account.json.keyfile=/Users/hoangtheduong/Downloads/mythical-bazaar-475215-i7-337ee07f878c.json \
  /Users/hoangtheduong/Documents/Projects/DA2/spark_jobs/gcs_bronze_to_silver.py

  



____MỚi nhất_
$SPARK_HOME/bin/spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.22 \
  --deploy-mode client \
  --driver-memory 4g \
  --executor-memory 3g \
  --conf spark.sql.session.timeZone=Asia/Ho_Chi_Minh \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.shuffle.partitions=200 \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.kryoserializer.buffer=64m \
  --conf spark.kryoserializer.buffer.max=256m \
  --conf spark.network.timeout=600s \
  --conf spark.executor.heartbeatInterval=60s \
  --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
  --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
  --conf spark.hadoop.google.cloud.auth.service.account.enable=true \
  --conf spark.hadoop.google.cloud.auth.service.account.json.keyfile="${GOOGLE_APPLICATION_CREDENTIALS}" \
  --conf spark.hadoop.google.cloud.fs.gs.request.retries=5 \
  --conf spark.hadoop.google.cloud.fs.gs.request.retry.interval.ms=1000 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.silver=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.silver.type=hadoop \
  --conf spark.sql.catalog.silver.warehouse="${ICEBERG_WAREHOUSE}" \
  --conf spark.sql.streaming.stopGracefullyOnShutdown=true \
  --conf spark.driver.maxResultSize=1g \
  /Users/hoangtheduong/Documents/Projects/DA2/spark_jobs/gcs_bronze_to_silver.py


_____GCS_BRONZE_TO_SILVER.PY______
$SPARK_HOME/bin/spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.22 \
  --driver-memory 4g \
  --executor-memory 2g \
  /Users/hoangtheduong/Documents/Projects/DA2/spark_jobs/gcs_bronze_to_silver.py



27/11
$SPARK_HOME/bin/spark-submit \
  --master 'local[1]' \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.22 \
  --conf "spark.driver.extraJavaOptions=-XX:TieredStopAtLevel=1" \
  --conf "spark.executor.extraJavaOptions=-XX:TieredStopAtLevel=1" \
  --conf "spark.sql.execution.arrow.pyspark.enabled=false" \
  --conf "spark.sql.parquet.enableVectorizedReader=false" \
  --conf "spark.sql.orc.enableVectorizedReader=false" \
  --conf "spark.sql.codegen.wholeStage=false" \
  --conf "spark.sql.shuffle.partitions=50" \
  --driver-memory 4g \
  --executor-memory 2g \
  /Users/hoangtheduong/Documents/Projects/DA2/spark_jobs/gcs_bronze_to_silver.py






_____GCS_SILVER_TO_GOLD.PY______
spark-submit \
  --master 'local[*]' \
  --packages "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.5" \
  --conf spark.driver.userClassPathFirst=true \
  --conf spark.executor.userClassPathFirst=true \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.silver=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.silver.type=hadoop \
  --conf spark.sql.catalog.silver.warehouse=gs://my-crawl-bucket-silver/warehouse/ \
  --conf spark.sql.catalog.gold=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.gold.type=hadoop \
  --conf spark.sql.catalog.gold.warehouse=gs://my-crawl-bucket-gold/warehouse/ \
  --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
  --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
  --conf spark.hadoop.google.cloud.auth.service.account.enable=true \
  --conf spark.hadoop.google.cloud.auth.service.account.json.keyfile=/Users/hoangtheduong/Downloads/mythical-bazaar-475215-i7-337ee07f878c.json \
  --conf spark.sql.shuffle.partitions=4 \
  --driver-memory 4g \
  --executor-memory 2g \
  /Users/hoangtheduong/Documents/Projects/DA2/spark_jobs/gcs_silver_to_gold.py





---Mới nhất
$SPARK_HOME/bin/spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.22 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.silver=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.silver.type=hadoop \
  --conf spark.sql.catalog.silver.warehouse=$SILVER_WAREHOUSE \
  --conf spark.sql.catalog.gold=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.gold.type=hadoop \
  --conf spark.sql.catalog.gold.warehouse=$GOLD_WAREHOUSE \
  --conf spark.hadoop.google.cloud.auth.service.account.enable=true \
  --conf spark.hadoop.google.cloud.auth.service.account.json.keyfile="$GOOGLE_APPLICATION_CREDENTIALS" \
  /Users/hoangtheduong/Documents/Projects/DA2/spark_jobs/gcs_silver_to_gold.py


__Mới nhất - ngày 25/11/2025: 00;14
$SPARK_HOME/bin/spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.22 \
  --driver-memory 4g \
  --executor-memory 3g \
  /Users/hoangtheduong/Documents/Projects/DA2/spark_jobs/gcs_silver_to_gold.py



27/11
$SPARK_HOME/bin/spark-submit \
  --master 'local[1]' \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.22 \
  --conf "spark.driver.extraJavaOptions=-XX:TieredStopAtLevel=1" \
  --conf "spark.executor.extraJavaOptions=-XX:TieredStopAtLevel=1" \
  --conf "spark.sql.execution.arrow.pyspark.enabled=false" \
  --conf "spark.sql.parquet.enableVectorizedReader=false" \
  --conf "spark.sql.orc.enableVectorizedReader=false" \
  --conf "spark.sql.codegen.wholeStage=false" \
  --driver-memory 4g \
  --executor-memory 4g \
  /Users/hoangtheduong/Documents/Projects/DA2/spark_jobs/gcs_silver_to_gold.py











_____GCS_GOLD_TO_BIGQUERY.PY______
  spark-submit \
  --master 'local[*]' \
  --packages \
"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,\
com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.5,\
com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.3,\
com.google.guava:guava:33.3.1-jre" \
  --conf spark.driver.userClassPathFirst=true \
  --conf spark.executor.userClassPathFirst=true \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.gold=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.gold.type=hadoop \
  --conf spark.sql.catalog.gold.warehouse=gs://my-crawl-bucket-gold/warehouse/ \
  --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
  --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
  --conf spark.hadoop.google.cloud.auth.service.account.enable=true \
  --conf spark.hadoop.google.cloud.auth.service.account.json.keyfile=/Users/hoangtheduong/Downloads/mythical-bazaar-475215-i7-337ee07f878c.json \
  --conf spark.sql.shuffle.partitions=4 \
  --driver-memory 4g \
  --executor-memory 2g \
  /Users/hoangtheduong/Documents/Projects/DA2/spark_jobs/gold_to_bigquery.py



------Mới nhất-----
$SPARK_HOME/bin/spark-submit \
  --packages \
org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,\
com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.22,\
com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.38.0 \
  --repositories https://maven-central.storage-download.googleapis.com/maven2,https://repo1.maven.org/maven2 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.gold=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.gold.type=hadoop \
  --conf spark.sql.catalog.gold.warehouse=gs://my-crawl-bucket-gold/warehouse/ \
  --conf spark.hadoop.google.cloud.auth.service.account.enable=true \
  --conf spark.hadoop.google.cloud.auth.service.account.json.keyfile="$GOOGLE_APPLICATION_CREDENTIALS" \
  --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
  --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
  --driver-memory 4g \
  --executor-memory 2g \
  /Users/hoangtheduong/Documents/Projects/DA2/spark_jobs/gold_to_bigquery.py


--MỚI NHẤT 25/11/2025
$SPARK_HOME/bin/spark-submit \
  --packages \
org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,\
com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.22,\
com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.38.0 \
  --driver-memory 4g \
  --executor-memory 2g \
  /Users/hoangtheduong/Documents/Projects/DA2/spark_jobs/gold_to_bigquery.py



27/11
$SPARK_HOME/bin/spark-submit \
  --master 'local[1]' \
  --packages \
org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,\
com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.22,\
com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.38.0 \
  --conf "spark.driver.extraJavaOptions=-XX:TieredStopAtLevel=1" \
  --conf "spark.executor.extraJavaOptions=-XX:TieredStopAtLevel=1" \
  --conf "spark.sql.execution.arrow.pyspark.enabled=false" \
  --conf "spark.sql.parquet.enableVectorizedReader=false" \
  --conf "spark.sql.orc.enableVectorizedReader=false" \
  --conf "spark.sql.codegen.wholeStage=false" \
  --driver-memory 4g \
  --executor-memory 4g \
  /Users/hoangtheduong/Documents/Projects/DA2/spark_jobs/gold_to_bigquery.py



pkill -9 python
pkill -9 chromedriver
pkill -9 "Google Chrome"


taskkill /IM chromedriver.exe /F
taskkill /IM chrome.exe /F
taskkill /IM python.exe /F

_CONSOLE SPARK
$SPARK_HOME/bin/pyspark \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.22 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.gold=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.gold.type=hadoop \
  --conf spark.sql.catalog.gold.warehouse=gs://my-crawl-bucket-gold/warehouse/ \
  --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
  --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
  --conf spark.hadoop.google.cloud.auth.service.account.enable=true \
  --conf spark.hadoop.google.cloud.auth.service.account.json.keyfile=$GOOGLE_APPLICATION_CREDENTIALS


$SPARK_HOME/bin/pyspark \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.22 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.gold=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.gold.type=hadoop \
  --conf spark.sql.catalog.gold.warehouse=gs://my-crawl-bucket-silver/warehouse/ \
  --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
  --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
  --conf spark.hadoop.google.cloud.auth.service.account.enable=true \
  --conf spark.hadoop.google.cloud.auth.service.account.json.keyfile=$GOOGLE_APPLICATION_CREDENTIALS