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


# External modules
from dc_texter import send_text
from dc_mailer import send_mail
import confluent_kafka

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
        if magnitude >= 4.0:
            msg = (
                f"ALERT: Earthquake detected!\n"
                f"Magnitude: {magnitude}\n"
                f"Location: {location}\n"
                f"Time: {time}\n"
                f"Coordinates: ({latitude}, {longitude})\n"
                f"Depth: {depth} km"
            )
            try:
                logger.info(f"Attempting to send text alert: {msg}")
                send_text(msg)
                logger.info("SUCCESS: Sent text alert: " + msg)
            except Exception as e:
                logger.warning(f"WARNING: Text alert not sent: {e}")

            try:
                logger.info(f"Attempting to send email alert: {msg}")
                send_mail(subject="Earthquake Alert!", body=msg)
                logger.info("SUCCESS: Sent email alert: " + msg)
            except Exception as e:
                logger.warning(f"WARNING: Email alert not sent: {e}")
    except Exception as e:
        logger.error(f"ERROR: Error processing earthquake data: {e}")


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

        consumer = confluent_kafka.Consumer(
            {
                "bootstrap.servers": kafka_server,
                "group.id": "earthquake-consumer-group",
                "auto.offset.reset": "earliest",
            }
        )
        consumer.subscribe([topic])
        logger.info(f"Connected to Kafka broker at {kafka_server}")

    except Exception as e:
        logger.error(f"ERROR: Failed to connect to Kafka: {e}")
        sys.exit(2)

    logger.info("STEP 3. Consuming messages from Kafka...")
    try:

        while True:
            message = consumer.poll(timeout=interval_secs) 
            if message is None:
                continue
            if message.error():
                logger.error(f"Consumer error: {message.error()}")
                continue
            logger.info(f"Received message: {message.value}")
            process_earthquake_message(json.loads(message.value().decode("utf-8")))
 
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user (CTRL+C).")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        if "consumer" in locals() and consumer is not None:
            consumer.close()
            logger.info("Kafka consumer closed.")

        logger.info("Consumer shutting down.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
