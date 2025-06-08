from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType

# Define schema for DNS logs
schema_dns = StructType([
    StructField("ColoCode", StringType(), True),
    StructField("QueryName", StringType(), True),
    StructField("SourceIP", StringType(), True)
])

# Define schema for Firewall logs
schema_firewall = StructType([
    StructField("BotDetectionIDs", StringType(), True),
    StructField("BotScore", StringType(), True),
    StructField("ClientIP", StringType(), True)
])

spark = SparkSession.builder.appName("KafkaSparkStreaming").getOrCreate()

# Read from Kafka
kafka_stream = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "KAFKA_DNS_TOPIC,KAFKA_FIREWALL_TOPIC") \
    .load()

parsed_stream = kafka_stream.selectExpr("CAST(value AS STRING) as data") \
    .filter(col("data").isNotNull()) \
    .withColumn("parsed_data", from_json(col("data"), schema_dns)) \
    .filter(col("parsed_data").isNotNull())

# Write to HDFS in Parquet format
parsed_stream.writeStream \
    .format("parquet") \
    .option("path", "hdfs://localhost:9000/logs") \
    .option("checkpointLocation", "hdfs://localhost:9000/checkpoints") \
    .start() \
    .awaitTermination()