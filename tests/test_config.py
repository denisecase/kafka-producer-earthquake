import utils.utils_config as config

def test_kafka_topic():
    """Ensure Kafka topic loads from config."""
    topic = config.get_kafka_topic()
    assert isinstance(topic, str) and topic.strip() != "", "Kafka topic is empty or invalid!"
