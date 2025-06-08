from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

# Create Spark Session
spark = SparkSession.builder \
    .appName("KafkaToConsole") \
    .getOrCreate()

# Read from Kafka topic
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "dns") \
    .option("startingOffsets", "earliest") \
    .load()

# Extract the value as a string
messages = df.selectExpr("CAST(value AS STRING) as message")

# Write messages to the console
query = messages.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()