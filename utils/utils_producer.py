"""
utils_producer.py - common functions used by producers.

Producers send messages to a Kafka topic.
"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import sys
import time

# Import external packages
import confluent_kafka

# admin can ONLY  be accessed as import from confluent_kafka
from confluent_kafka.admin import AdminClient, NewTopic


# Import functions from local modules
from .utils_config import get_kafka_broker_address
from .utils_logger import logger

#####################################
# Kafka and Zookeeper Readiness Checks
#####################################


def check_kafka_service_is_ready():
    """
    Check if Kafka is ready by connecting to the broker and fetching metadata.

    Returns:
        bool: True if Kafka is ready, False otherwise.
    """
    kafka_broker = get_kafka_broker_address()

    try:
        admin_client = AdminClient({"bootstrap.servers": kafka_broker})
        # Fetch metadata to check if Kafka is up
        cluster_metadata = admin_client.list_topics(timeout=5)
        if cluster_metadata.topics:
            logger.info(
                f"Kafka is ready. Available topics: {list(cluster_metadata.topics.keys())}"
            )
            return True
        else:
            logger.warning("Kafka is running, but no topics are available.")
            return True
    except Exception as e:
        logger.error(f"Error checking Kafka: {e}")
        return False


#####################################
# Verify Zookeeper and Kafka Services
#####################################


def verify_services():
    # Verify Kafka is ready
    if not check_kafka_service_is_ready():
        logger.error(
            "Kafka broker is not ready. Please check your Kafka setup. Exiting..."
        )
        sys.exit(2)


#####################################
# Create a Kafka Producer
#####################################


def create_kafka_producer(kafka_server):
    """
    Initialize Kafka producer.
    Returns:
        Producer object or None if failed.
    """
    try:
        verify_services()
        producer = confluent_kafka.Producer({"bootstrap.servers": kafka_server})
        return producer
    except Exception as e:
        logger.warning(f"WARNING: Kafka connection failed: {e}")
        return None


#####################################
# Create a Kafka Topic
#####################################


def create_kafka_topic(topic_name, group_id=None):
    """
    Create a fresh Kafka topic with the given name.
    Args:
        topic_name (str): Name of the Kafka topic.
    """
    kafka_broker = get_kafka_broker_address()

    try:
        admin_client = AdminClient({"bootstrap.servers": kafka_broker})

        # Check if the topic exists
        topics = admin_client.list_topics(timeout=5).topics
        if topic_name in topics:
            logger.info(f"Topic '{topic_name}' exists.")
        else:
            logger.info(f"Creating '{topic_name}'.")
            new_topic = NewTopic(
                topic_name, num_partitions=1, replication_factor=1
            )
            fs = admin_client.create_topics([new_topic])
            # Ensure topic creation succeeds
            for topic, f in fs.items():
                try:
                    f.result()  # Block until topic is created or fails
                    logger.info(f"Topic '{topic}' created successfully.")
                except Exception as e:
                    logger.error(f"Failed to create topic '{topic}': {e}")
    except Exception as e:
        logger.error(f"Error managing topic '{topic_name}': {e}")
        sys.exit(1)


#####################################
# Find Out if Topic is Available
#####################################


def is_topic_available(topic) -> bool:
    """
    Ensure Kafka topic is created before producing messages.
    Retries up to 5 times if topic creation fails.
    """
    max_retries = 5
    for attempt in range(max_retries):
        try:
            create_kafka_topic(topic)
            return True
        except Exception as e:
            logger.warning(f"Retrying topic creation ({attempt+1}/{max_retries}): {e}")
            time.sleep(5)
    return False


#####################################
# Main Function for Testing
#####################################


def main():
    """
    Main entry point.
    """

    if not check_kafka_service_is_ready():
        logger.error("Kafka is not ready. Check .env file and ensure Kafka is running.")
        sys.exit(2)

    logger.info("All services are ready. Proceed with producer setup.")
    create_kafka_topic("test_topic", "default_group")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
