"""
consumer_earthquake.py

Consumes real-time earthquake data from a cloud-hosted Kafka topic.

Example JSON message:
{
    "magnitude": 2.75,
    "location": "8 km SSE of Maria Antonia, Puerto Rico",
    "latitude": 17.9155,
    "longitude": -66.8498333333333,
    "depth": 13.42,
    "time": "2025-02-02T00:24:43.990000"
}
"""

#####################################
# Import Modules
#####################################

# Standard library
import json
import sys
import time

# External modules
from kafka import KafkaConsumer
from dc_texter import send_text # send myself alerts

# Local utilities
import utils.utils_config as config
from utils.utils_producer import verify_services, is_topic_available
from utils.utils_logger import logger

#####################################
# Define Processing Function
#####################################

def process_earthquake_message(data: dict) -> None:
    """
    Process earthquake data and send an alert if it meets criteria.
    """
    try:
        magnitude = data.get("magnitude", 0)
        location = data.get("location", "Unknown location")
        time = data.get("time", "Unknown time")
        latitude = data.get("latitude", "N/A")
        longitude = data.get("longitude", "N/A")
        depth = data.get("depth", "N/A")

        # Define what makes an earthquake "interesting"
        if magnitude >= 1.5:  
            msg = (f"ALERT: Earthquake detected!\n"
                   f"Magnitude: {magnitude}\n"
                   f"Location: {location}\n"
                   f"Time: {time}\n"
                   f"Coordinates: ({latitude}, {longitude})\n"
                   f"Depth: {depth} km")
            send_text(msg)
            logger.info("Sent alert: " + msg)
    except Exception as e:
        logger.error(f"Error processing earthquake data: {e}")

#####################################
# Define Main Function
#####################################


def main() -> None:
    """
    Main function to consume messages from a Kafka topic.
    """
    logger.info("Starting Kafka Consumer...")

    logger.info("STEP 1. Read environment variables.")
    try:
        interval_secs: int = config.get_message_interval_seconds_as_int()
        topic: str = config.get_kafka_topic()
        kafka_server: str = config.get_kafka_broker_address()
    except Exception as e:
        logger.error(f"ERROR: Failed to load environment variables: {e}")
        sys.exit(1)

    logger.info(f"Using Kafka broker: {kafka_server}, Topic: {topic}")

    logger.info("STEP 2. Verify Kafka services and create consumer.")
    try:
        verify_services()
        is_topic_available(topic)

        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=kafka_server,
            auto_offset_reset="earliest",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )
        logger.info(f"Connected to Kafka broker at {kafka_server}")

    except Exception as e:
        logger.error(f"ERROR: Failed to connect to Kafka: {e}")
        sys.exit(2)

    logger.info("STEP 3. Consuming messages from Kafka...")
    try:
        for message in consumer:
            logger.info(f"Received message: {message.value}")
            print(f"Received: {message.value}")

            # Process the earthquake message
            process_earthquake_message(message.value)

            time.sleep(interval_secs)

    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user (CTRL+C).")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        if 'consumer' in locals() and consumer is not None:
            consumer.close()
            logger.info("Kafka consumer closed.")

        logger.info("Consumer shutting down.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
