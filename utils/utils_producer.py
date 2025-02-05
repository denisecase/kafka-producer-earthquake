"""
utils/utils_producer.py - common functions used by producers.

- Ensures Kafka services are ready before sending messages.
- Creates a producer instance dynamically (from utils_connector.py).
- Ensures the topic exists before sending messages.
- Provides a testable main() entry point for debugging.
"""

import sys
import time
from .utils_logger import logger
from .utils_connector import KAFKA_TOPIC,  get_kafka_producer, create_kafka_topic, check_kafka_service_is_ready


def verify_services(retries=5, delay=5):
    """
    Ensure Kafka is running before attempting to produce messages.
    Implements a retry mechanism for better production resilience.
    """
    for attempt in range(retries):
        if check_kafka_service_is_ready():
            logger.info("Kafka service is ready.")
            return True
        logger.warning(
            f"Kafka broker not ready. Retry {attempt + 1}/{retries} in {delay}s..."
        )
        time.sleep(delay)

    logger.error("Kafka broker is not available after multiple attempts. Exiting...")
    sys.exit(2)


def create_kafka_producer():
    """
    Get a producer using the correct Kafka provider.
    Includes error handling to prevent crashes.
    """
    verify_services()

    try:
        producer = get_kafka_producer()
        return producer
    except Exception as e:
        logger.error(f"Failed to create Kafka Producer: {e}")
        sys.exit(1)  # Exit if producer cannot be created


def ensure_topic_exists(topic_name):
    """
    Ensure a Kafka topic exists before sending messages.
    Includes error handling.
    """
    verify_services()

    try:
        create_kafka_topic(topic_name)
    except Exception as e:
        logger.error(f"Failed to create Kafka topic '{topic_name}': {e}")
        sys.exit(1)  # Exit if topic creation fails


def main():
    """
    Main entry point.
    - Ensures Kafka is running.
    - Ensures topic exists.
    - Prepares producer.
    """
    verify_services()
    ensure_topic_exists(KAFKA_TOPIC)
    logger.info(f"Kafka Producer is ready to send messages to {KAFKA_TOPIC}.")


if __name__ == "__main__":
    main()
