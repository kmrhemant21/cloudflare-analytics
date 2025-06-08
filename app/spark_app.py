from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType

# Define schema for DNS messages
dns_schema = StructType([
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

# Create Spark Session
spark = SparkSession.builder \
    .appName("KafkaToPostgres") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.postgresql:postgresql:42.2.20") \
    .getOrCreate()

# Set log level to reduce verbosity
spark.sparkContext.setLogLevel("WARN")

# Read from Kafka topic
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "dns") \
    .option("startingOffsets", "earliest") \
    .option("kafka.group.id", "dns-postgres-consumer") \
    .load()

# Parse JSON messages
parsed_df = df.selectExpr("CAST(value AS STRING) as json_message") \
    .select(from_json("json_message", dns_schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp_parsed", to_timestamp("Timestamp"))

# For debugging - show messages in console
console_query = parsed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# Function to write micro-batches to PostgreSQL
def write_to_postgres(batch_df, batch_id):
    # Count records in batch
    count = batch_df.count()
    print(f"Processing batch {batch_id} with {count} records")
    
    if count > 0:
        # Write batch to PostgreSQL
        batch_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/dns_logs") \
            .option("dbtable", "dns_logs") \
            .option("user", "superset") \
            .option("password", "superset") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        print(f"Successfully wrote batch {batch_id} to PostgreSQL")

# Write to PostgreSQL using foreachBatch
postgres_query = parsed_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoints/postgres") \
    .start()

# Wait for queries to terminate
spark.streams.awaitAnyTermination()