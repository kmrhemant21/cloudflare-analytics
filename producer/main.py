import os
import requests
from kafka import KafkaProducer
import json
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment variables
LOKI_URL = os.environ.get("LOKI_URL")
KAFKA_BROKER = os.environ.get("KAFKA_BROKER")  # Explicit IPv4
KAFKA_DNS_TOPIC = os.environ.get("KAFKA_DNS_TOPIC")
KAFKA_FIREWALL_TOPIC = os.environ.get("KAFKA_FIREWALL_TOPIC")

def create_producer():
    """Create Kafka producer with reliable connection settings"""
    for i in range(1, 21):
        try:
            # Simplified producer configuration
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                api_version=(2, 6),  # Matches your broker version 2.6
                retries=5,
                retry_backoff_ms=1000,
                request_timeout_ms=15000,
                max_block_ms=30000
            )
            
            # Verify connection by getting metadata
            producer.partitions_for(KAFKA_DNS_TOPIC)
            producer.partitions_for(KAFKA_FIREWALL_TOPIC)
            logger.info("Successfully connected to Kafka")
            return producer
            
        except Exception as e:
            logger.warning(f"Attempt {i}/20 failed: {str(e)}")
            time.sleep(5)
    
    raise Exception("Could not connect to Kafka after 20 attempts")

def fetch_loki_logs():
    """Fetch logs from Loki with error handling"""
    try:
        params = {
            "query": '{namespace="cloudflarelogs"}',
            "limit": 1000,
            "direction": "FORWARD"
        }
        resp = requests.get(
            f"{LOKI_URL}/loki/api/v1/query_range",
            params=params,
            timeout=10
        )
        resp.raise_for_status()
        data = resp.json()
        
        for stream in data.get("data", {}).get("result", []):
            for value in stream.get("values", []):
                log_line = value[1]
                yield log_line
                
    except requests.exceptions.RequestException as e:
        logger.error(f"Loki request failed: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error fetching logs: {str(e)}")
        raise

def send_to_kafka_topic(producer, log):
    try:
        log_data = json.loads(log)
        if (
            isinstance(log_data, dict)
            and "ColoCode" in log_data
            and "QueryName" in log_data
            and "SourceIP" in log_data
        ):
            producer.send(KAFKA_DNS_TOPIC, log_data)
            logger.info(f"Sent DNS log to {KAFKA_DNS_TOPIC}")
        elif (
            isinstance(log_data, dict)
            and "BotDetectionIDs" in log_data
            and "BotScore" in log_data
            and "ClientIP" in log_data
        ):
            producer.send(KAFKA_FIREWALL_TOPIC, log_data)
            logger.info(f"Sent Firewall log to {KAFKA_FIREWALL_TOPIC}")
        else:
            logger.warning(f"Unrecognized log format: {log}")
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse log as JSON: {str(e)}")
def main():
    """Main processing loop with proper cleanup"""
    producer = None
    try:
        producer = create_producer()
        logger.info("Starting log processing...")

        while True:
            try:
                log_count = 0
                start_time = time.time()

                for log in fetch_loki_logs():
                    send_to_kafka_topic(producer, log)
                    log_count += 1
                    if log_count % 100 == 0:
                        logger.info(log)

                producer.flush()
                if log_count > 0:
                    elapsed = time.time() - start_time
                    logger.info(f"Sent {log_count} logs in {elapsed:.2f} seconds")

                time.sleep(60)

            except Exception as processing_error:
                logger.error(f"Processing error: {str(processing_error)}")
                time.sleep(10)

    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
    finally:
        if producer:
            producer.close(timeout=10)
            logger.info("Producer closed cleanly")

if __name__ == "__main__":
    main()