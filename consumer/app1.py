from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, hour, dayofweek, to_timestamp, count
from pyspark.sql.types import *
import time
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

logger.info("Starting DNS Log Processor application")

# --- Define schema based on your JSON ---
schema = StructType([
    StructField("ColoCode", StringType(), True),
    StructField("EDNSSubnet", StringType(), True),
    StructField("EDNSSubnetLength", IntegerType(), True),
    StructField("QueryName", StringType(), True),
    StructField("QueryType", IntegerType(), True),
    StructField("ResponseCached", BooleanType(), True),
    StructField("ResponseCode", IntegerType(), True),
    StructField("SourceIP", StringType(), True),
    StructField("Timestamp", StringType(), True)
])

logger.info("Creating Spark session")
# --- Spark session ---
spark = SparkSession.builder \
    .appName("KafkaDNSLogProcessor") \
    .config("spark.sql.streaming.schemaInference", "true") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.apache.hadoop:hadoop-common:3.2.1,org.postgresql:postgresql:42.2.20") \
    .getOrCreate()

spark.sparkContext.setLogLevel("DEBUG")
logger.info("Spark session created successfully")

# --- Kafka source ---
logger.info("Connecting to Kafka at kafka:9092, subscribing to topic: dns")
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "dns") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()
logger.info("Kafka source defined")
# --- Parse and transform logs ---
logger.info("Setting up data transformation pipeline")
logs = df.selectExpr("CAST(value AS STRING) as value", "CAST(timestamp AS TIMESTAMP) as kafka_timestamp") \
    .select(from_json(col("value"), schema).alias("data"), col("kafka_timestamp")).select("data.*", "kafka_timestamp")

# Print raw Kafka messages to debug
raw_messages = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "topic", "partition", "offset", "timestamp")
raw_messages.writeStream \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 5) \
    .outputMode("append") \
    .start()

logger.info("Set up raw message debugging stream")

logs = logs.withColumn("ts", to_timestamp("Timestamp")) \
           .withColumn("HourOfDay", hour("ts")) \
           .withColumn("DayOfWeek", dayofweek("ts")) \
           .withColumn("ResponseCachedInt", col("ResponseCached").cast("int"))

# --- Debug: Print malformed records ---
malformed = logs.filter(col("ColoCode").isNull())
malformed.writeStream \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 10) \
    .start()

logger.info("Set up malformed records debugging stream")

# --- Write to PostgreSQL ---
def to_postgres(batch_df, batch_id):
    try:
        record_count = batch_df.count()
        logger.info(f"Processing batch {batch_id} with {record_count} records for PostgreSQL")
        
        if record_count > 0:
            logger.info(f"Writing batch {batch_id} to PostgreSQL")
            batch_df.write \
                .format("jdbc") \
                .option("url", "jdbc:postgresql://postgres:5432/dns_logs") \
                .option("dbtable", "dns_logs") \
                .option("user", "superset") \
                .option("password", "superset") \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
            logger.info(f"Successfully wrote batch {batch_id} to PostgreSQL")
        else:
            logger.info(f"Batch {batch_id} has no records, skipping PostgreSQL write")
    except Exception as e:
        logger.error(f"[PostgreSQL ERROR] Batch {batch_id}: {str(e)}")

# --- Write to HDFS ---
def to_hdfs(batch_df, batch_id):
    try:
        record_count = batch_df.count()
        logger.info(f"Processing batch {batch_id} with {record_count} records for HDFS")
        
        if record_count > 0:
            logger.info(f"Writing batch {batch_id} to HDFS")
            batch_df.write \
                .mode("append") \
                .parquet("hdfs://namenode:8020/dns_logs")
            logger.info(f"Successfully wrote batch {batch_id} to HDFS")
        else:
            logger.info(f"Batch {batch_id} has no records, skipping HDFS write")
    except Exception as e:
        logger.error(f"[HDFS ERROR] Batch {batch_id}: {str(e)}")

# --- Function to handle batch processing ---
def process_batch(batch_df, batch_id):
    start_time = time.time()
    logger.info(f"Starting to process batch {batch_id}")
    
    try:
        record_count = batch_df.count()
        logger.info(f"Batch {batch_id} contains {record_count} records")
        
        if record_count > 0:
            # First record for debugging
            logger.info(f"Sample record from batch {batch_id}: {batch_df.limit(1).collect()}")
            
            # Process batch
            to_postgres(batch_df, batch_id)
            to_hdfs(batch_df, batch_id)
        
        logger.info(f"Completed processing batch {batch_id} in {time.time() - start_time:.2f} seconds")
    except Exception as e:
        logger.error(f"Error processing batch {batch_id}: {str(e)}")

logger.info("Starting streaming query")

# --- Combine writing ---
query = logs.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/spark_checkpoint_dns") \
    .start()

logger.info("Streaming query started, waiting for termination")
query.awaitTermination()