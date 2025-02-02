"""
Config Utility
File: utils/utils_config.py

This script provides the configuration functions for the project. 

It centralizes the configuration management 
by loading environment variables from .env in the root project folder
 and constructing file paths using pathlib. 

If you rename any variables in .env, remember to:
- recopy .env to .env.example (and hide the secrets)
- update the corresponding function in this module.
"""

#####################################
# Imports
#####################################

# import from Python Standard Library
import os
import pathlib

# import from external packages
from dotenv import load_dotenv

# import from local modules
from .utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################


def get_zookeeper_address() -> str:
    """Fetch ZOOKEEPER_ADDRESS from environment or use default."""
    address = os.getenv("ZOOKEEPER_ADDRESS", "127.0.0.1:2181")
    logger.info(f"ZOOKEEPER_ADDRESS: {address}")
    return address


def get_kafka_broker_address() -> str:
    """Fetch KAFKA_BROKER_ADDRESS from environment or use default."""
    address = os.getenv("KAFKA_BROKER_ADDRESS", "127.0.0.1:9092")
    logger.info(f"KAFKA_BROKER_ADDRESS: {address}")
    return address


def get_kafka_topic() -> str:
    """Fetch KAFKA_TOPIC from environment or use default."""
    topic = os.getenv("KAFKA_TOPIC", "earthquake-events")
    logger.info(f"KAFKA_TOPIC: {topic}")
    return topic


def get_message_interval_seconds_as_int() -> int:
    """Fetch MESSAGE_INTERVAL_SECONDS from environment or use default."""
    interval = int(os.getenv("MESSAGE_INTERVAL_SECONDS", 5))
    logger.info(f"MESSAGE_INTERVAL_SECONDS: {interval}")
    return interval


def get_base_data_path() -> pathlib.Path:
    """Fetch BASE_DATA_DIR from environment or use default."""
    project_root = pathlib.Path(__file__).parent.parent
    data_dir = project_root / os.getenv("BASE_DATA_DIR", "data")
    logger.info(f"BASE_DATA_DIR: {data_dir}")
    return data_dir


def get_live_data_path() -> pathlib.Path:
    """Fetch LIVE_DATA_FILE_NAME from environment or use default."""
    live_data_path = get_base_data_path() / os.getenv(
        "LIVE_DATA_FILE_NAME", "earthquake_live.json"
    )
    logger.info(f"LIVE_DATA_PATH: {live_data_path}")
    return live_data_path


def get_source_data_url() -> str:
    """Fetch USGS_API_URL from environment or use default."""
    url = os.getenv("USGS_API_URL", "https://earthquake.usgs.gov/fdsnws/event/1/query")
    logger.info(f"USGS_API_URL: {url}")
    return url


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    # Test the configuration functions
    logger.info("Testing configuration.")
    try:
        get_zookeeper_address()
        get_kafka_broker_address()
        get_kafka_topic()
        get_message_interval_seconds_as_int()
        get_base_data_path()
        get_live_data_path()
        get_source_data_url()
        logger.info("SUCCESS: Configuration function tests complete.")

    except Exception as e:
        logger.error(f"ERROR: Configuration function test failed: {e}")
