"""
utils/utils_connector.py
Manages Kafka connections dynamically based on the selected provider in the environment.

This module provides:
- Dynamic selection of Kafka providers (Confluent, Local, Redpanda).
- Factory functions for creating Kafka Producers and Consumers.
- Kafka topic management (creating topics if needed).
- Broker readiness checks to ensure Kafka is available.
- Error handling and logging for Kafka connection issues.
- Support for environment variable-based configuration.

Environment Variables:
- KAFKA_PROVIDER: Defines the Kafka provider (e.g., "confluent", "local", "redpanda").
- KAFKA_BROKER: Kafka broker address for connecting producers and consumers.
- KAFKA_TOPIC: Default Kafka topic used for message exchange.

Functions:
- get_kafka_producer(): Returns an appropriate Kafka Producer instance.
- get_kafka_consumer(): Returns an appropriate Kafka Consumer instance.
- check_kafka_service_is_ready(): Checks if the Kafka broker is reachable.
- create_kafka_topic(): Creates a Kafka topic if it does not already exist.
"""
import json
import os
import logging
import socket
from dotenv import load_dotenv
from confluent_kafka import Producer as ConfluentProducer, Consumer as ConfluentConsumer
from kafka import KafkaProducer as PythonProducer, KafkaConsumer as PythonConsumer
from kafka.admin import KafkaAdminClient, NewTopic

load_dotenv()

KAFKA_PROVIDER = os.getenv("KAFKA_PROVIDER", "local").lower()
KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

logger.info(f"Using Kafka Provider: {KAFKA_PROVIDER}")
logger.info(f"Kafka Broker: {KAFKA_BROKER}")


def check_kafka_service_is_ready(timeout=5):
    """
    Checks if the Kafka broker is reachable.

    Returns:
        bool: True if Kafka is reachable, False otherwise.
    """
    if not KAFKA_BROKER:
        logger.error("KAFKA_BROKER is not set. Cannot check Kafka readiness.")
        return False

    host, port = KAFKA_BROKER.split(":")
    try:
        with socket.create_connection((host, int(port)), timeout):
            logger.info("Kafka broker is reachable.")
            return True
    except (socket.timeout, ConnectionRefusedError) as e:
        logger.warning(f"Kafka broker not reachable: {e}")
        return False


def get_kafka_producer():
    """Returns the correct Kafka Producer based on the provider."""
    if not check_kafka_service_is_ready():
        logger.error("Kafka is not available. Cannot create producer.")
        return None

    try:
        if KAFKA_PROVIDER == "confluent":
            producer = ConfluentProducer({"bootstrap.servers": KAFKA_BROKER})
        elif KAFKA_PROVIDER in ["local", "redpanda"]:
            producer = PythonProducer(bootstrap_servers=KAFKA_BROKER)
        else:
            raise ValueError(f"Unsupported KAFKA_PROVIDER: {KAFKA_PROVIDER}")

        logger.info(f"Kafka Producer created for {KAFKA_PROVIDER}")
        return producer

    except Exception as e:
        logger.error(f"Failed to create Kafka Producer: {e}")
        return None


def get_kafka_consumer(group_id="default-group"):
    """Returns the correct Kafka Consumer based on the provider."""
    if not check_kafka_service_is_ready():
        logger.error("Kafka is not available. Cannot create consumer.")
        return None

    try:
        if KAFKA_PROVIDER == "confluent":
            consumer = ConfluentConsumer(
                {
                    "bootstrap.servers": KAFKA_BROKER,
                    "group.id": group_id,
                    "auto.offset.reset": "earliest",
                }
            )
            consumer.subscribe([KAFKA_TOPIC])
        elif KAFKA_PROVIDER in ["local", "redpanda"]:
            consumer = PythonConsumer(
                KAFKA_TOPIC,
                group_id=group_id,
                bootstrap_servers=KAFKA_BROKER,
                auto_offset_reset="earliest",
            )
        else:
            raise ValueError(f"Unsupported KAFKA_PROVIDER: {KAFKA_PROVIDER}")

        logger.info(f"Kafka Consumer created for {KAFKA_PROVIDER}")
        return consumer

    except Exception as e:
        logger.error(f"Failed to create Kafka Consumer: {e}")
        return None


def create_kafka_topic(topic_name, num_partitions=1, replication_factor=1):
    """
    Creates a Kafka topic if it does not already exist.

    Args:
        topic_name (str): The name of the topic to create.
        num_partitions (int): The number of partitions for the topic.
        replication_factor (int): The replication factor for the topic.

    Returns:
        bool: True if the topic was created or already exists, False on failure.
    """
    if not check_kafka_service_is_ready():
        logger.error("Kafka is not available. Cannot create topic.")
        return False

    try:
        admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)

        # Fetch existing topics
        existing_topics = admin_client.list_topics()
        if topic_name in existing_topics:
            logger.info(f"Kafka topic '{topic_name}' already exists.")
            return True

        # Create new topic
        topic = NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
        admin_client.create_topics([topic])
        logger.info(f"Kafka topic '{topic_name}' created successfully.")
        return True

    except Exception as e:
        logger.error(f"Failed to create Kafka topic '{topic_name}': {e}")
        return False
    

def check_for_errors(msg):
    """
    Check for errors in a Kafka message.

    Args:
        msg: Kafka message object.

    Returns:
        bool: True if an error was found, False otherwise.
    """
    if msg is None:
        return False

    if isinstance(msg, ConfluentConsumer) and msg.error():
        logger.warning(f"Consumer error: {msg.error()}")
        return True

    return False

def send_kafka_message(producer, topic, message):
    """
    Sends a message to Kafka using the appropriate method based on the provider.

    Args:
        producer: Kafka producer instance.
        topic (str): Kafka topic to send the message to.
        message (dict or str): The message to send.

    Returns:
        bool: True if the message was successfully sent, False otherwise.
    """
    if producer is None:
        logger.error("Kafka Producer is not initialized. Cannot send message.")
        return False

    try:
        message_str = json.dumps(message) if isinstance(message, dict) else str(message)
        
        if isinstance(producer, ConfluentProducer):
            producer.produce(topic, value=message_str.encode("utf-8"))
            producer.flush()
        elif isinstance(producer, PythonProducer):
            producer.send(topic, value=message_str.encode("utf-8"))
            producer.flush()
        else:
            raise TypeError("Unsupported producer type")

        logger.info(f"Successfully sent message to Kafka topic '{topic}': {message_str}")
        return True

    except Exception as e:
        logger.error(f"Failed to send message to Kafka topic '{topic}': {e}")
        return False



def fetch_message(consumer):
    """
    Fetches and deserializes a Kafka message based on the platform.
    Platform-specific message reading and deserialization logic.

    Args:
        consumer: Kafka consumer instance.

    Returns:
        dict: The deserialized message.
    """
    msg = None
    try:
        if KAFKA_PROVIDER == "confluent":
            msg = consumer.poll(timeout=1.0) 
        elif KAFKA_PROVIDER in ["local", "redpanda"]:
            msg = consumer.poll(timeout_ms=1000)
        else:
            logger.error(f"Unsupported Kafka provider: {KAFKA_PROVIDER}")
            return None

        if msg is None or (isinstance(msg, dict) and not msg):
            return None 
        else:
            logger.info(f"TEMP Received message: {msg}") 

        if isinstance(msg, ConfluentConsumer) and msg.error():
            logger.warning(f"Consumer error (Confluent): {msg.error()}")
            return None  
        elif isinstance(msg, PythonConsumer) and msg.error():
            logger.warning(f"Consumer error (Python Kafka): {msg.error()}")
            return None  
 
        # Extract message content from the ConsumerRecord, which is within msg
        if isinstance(msg, dict):
            for topic_partition, records in msg.items():
                for record in records:
                    message_value = record.value  

                    # If the message in byte format, decode it
                    if isinstance(message_value, bytes):
                        message_value = message_value.decode("utf-8")

                    # Deserialize JSON into Python dictionary
                    message_dict = json.loads(message_value)
                    return message_dict

        logger.warning("Message format is unexpected.")
        return None  

    except Exception as e:
        logger.error(f"Error fetching message: {e}")
        return None 
    