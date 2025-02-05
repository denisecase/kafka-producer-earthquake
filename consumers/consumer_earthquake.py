"""
consumers/consumer_earthquake.py

Consume earthquake data from a Kafka topic and send alerts.
"""

import sys

import utils.utils_config as config
from utils.utils_consumer import create_kafka_consumer, consume_messages, verify_services
from utils.utils_logger import logger
from consumers.earthquake_alerts import send_earthquake_alert

def process_earthquake_message(data: dict) -> None:
    """
    Process earthquake data and send alerts as appropriate.
    """
    try:
        send_earthquake_alert(
            magnitude=data.get("magnitude", 0),
            location=data.get("location", "Unknown location"),
            event_time=data.get("time", "Unknown time"),
            latitude=data.get("latitude", "N/A"),
            longitude=data.get("longitude", "N/A"),
            depth=data.get("depth", "N/A"),
        )
    except Exception as e:
        logger.error(f"Error processing earthquake message: {e}")


def main() -> None:
    """
    Main function to consume messages from a Kafka topic.
    """
    logger.info("Starting Kafka Consumer...")

    logger.info("Reading environment variables...")
    try:
        interval_secs = config.get_message_interval_seconds_as_int()
        topic = config.get_kafka_topic()
    except Exception as e:
        logger.error(f"Failed to load environment variables: {e}")
        sys.exit(1)

    verify_services()

    logger.info("Creating Kafka consumer...")
    consumer = create_kafka_consumer(topic)

    logger.info("Consuming messages from Kafka...")
    consume_messages(consumer, process_earthquake_message, interval_secs)


if __name__ == "__main__":
    main()
