# cloudflare-analytics

hive-server

beeline -u jdbc:hive2://localhost:10000

CREATE EXTERNAL TABLE IF NOT EXISTS cloudflare_logs (
  log STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/logs/cloudflare/';

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 spark_streaming.py

spark-submit --master spark://0.0.0.0:7077 --name spark-pi --class org.apache.spark.examples.SparkPi  local:///opt/spark/examples/jars/spark-examples_2.12-3.4.0.jar 100


## Kafka tutorial

### Kafka cli

```sh
kafka-topics --bootstrap-server localhost:9092 --create --topic hello-world --partitions 1 --replication-factor 1
kafka-topics --bootstrap-server localhost:9092 --list
kafka-console-producer --bootstrap-server localhost:9092 --topic hello-world
kafka-console-consumer --bootstrap-server localhost:9092 --topic hello-world --from-beginning
kafka-topics --bootstrap-server localhost:9092 --delete --topic hello-world
```