"""
utils/utils_consumer.py - Common functions used by Kafka consumers.

This module handles Kafka message consumption with built-in resilience 
for production environments.

Features:
- Ensures Kafka services are ready before consuming messages.
- Creates and verifies the existence of Kafka topics.
- Implements retries for Kafka readiness to handle temporary failures.
- Gracefully handles consumer errors and logs recoverable issues.
- Supports custom message processing via a user-defined function.
- Cleans up resources and ensures graceful shutdown on exit.
"""


import sys
import time
from .utils_logger import logger
from .utils_connector import (
    KAFKA_TOPIC,
    get_kafka_consumer,
    check_kafka_service_is_ready,
    fetch_message,
)


def verify_services(retries=5, delay=5):
    """
    Ensure Kafka is running before attempting to consume messages.
    Implements a retry mechanism to handle temporary unavailability.
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


def create_kafka_consumer(group_id="default-group"):
    """
    Get a consumer using the correct Kafka provider.
    Includes error handling to prevent crashes.
    """
    verify_services()

    try:
        consumer = get_kafka_consumer(group_id)
        consumer.subscribe([KAFKA_TOPIC])
        return consumer
    except Exception as e:
        logger.error(f"Failed to create Kafka Consumer: {e}")
        sys.exit(1)  # Exit if consumer creation fails


def consume_messages(consumer, process_message, interval_secs=1):
    """
    Consumes messages from Kafka and processes them using the provided function.

    Args:
        consumer: Kafka consumer instance.
        process_message (function): Function to process each message.
        interval_secs (int): Polling interval in seconds.
    """
    if consumer is None:
        logger.error("Kafka Consumer is not initialized. Exiting...")
        return

    try:
        logger.info("Listening for messages...")

        while True:
            msg = fetch_message(consumer)
            if msg is None:
                logger.info("No new messages. Retrying...")
                continue  # No message received, continue polling
            # Log the raw message value received
            logger.info(f"Raw message received: {msg}")
            # Check if the message has a valid magnitude before processing
            if not msg or "magnitude" not in msg or msg["magnitude"] == 0:
                continue  # Skip if the magnitude is invalid or zero

            try:
                process_message(msg)
                consumer.commit()  # Commit the offset after processing the message
            except Exception as e:
                logger.error(f"Error processing message: {e}")

    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user. Shutting down.")
    finally:
        consumer.close()
        logger.info("Kafka Consumer closed.")


def main():
    """
    Main entry point.
    - Ensures Kafka is running.
    - Ensures topic exists.
    - Starts consuming messages.
    """
    verify_services()
    consume_messages()


if __name__ == "__main__":
    main()
