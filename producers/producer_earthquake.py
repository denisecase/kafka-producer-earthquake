"""
producer_earthquake.py

Fetch real-time earthquake data from USGS API and stream it to a file and Kafka topic.

Example JSON message:
{
    "magnitude": 4.8,
    "location": "California",
    "latitude": 36.7783,
    "longitude": -119.4179,
    "depth": 10.5,
    "time": "2024-02-01T12:34:56Z"
}

"""

#####################################
# Import Modules
#####################################

# Standard library
import json
import os
import pathlib
import random
import sys
import time
from datetime import datetime
import requests

# External modules
from kafka import KafkaProducer

# Local utilities
import utils.utils_config as config
from utils.utils_producer import verify_services, create_kafka_topic
from utils.utils_logger import logger

#####################################
# Define Function to Fetch Earthquake Data
#####################################


USGS_API_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"

def fetch_earthquake_data():
    """
    Fetch real-time earthquake data from the USGS API.
    Returns a list of earthquake events.
    """
    params = {
        "format": "geojson",
        "limit": 5,  # Fetch last 5 events
        "minmagnitude": 2.5,  # Filter out very minor quakes
        "orderby": "time",
    }

    try:
        response = requests.get(USGS_API_URL, params=params)
        response.raise_for_status()  # Raise an error for HTTP issues
        data = response.json()

        earthquakes = []
        for feature in data["features"]:
            properties = feature["properties"]
            geometry = feature["geometry"]
            
            earthquake = {
                "magnitude": properties.get("mag"),
                "location": properties.get("place"),
                "latitude": geometry["coordinates"][1],
                "longitude": geometry["coordinates"][0],
                "depth": geometry["coordinates"][2],
                "time": datetime.utcfromtimestamp(properties["time"] / 1000).isoformat(),
            }
            earthquakes.append(earthquake)

        return earthquakes

    except Exception as e:
        logger.error(f"ERROR: Failed to fetch earthquake data: {e}")
        return []


#####################################
# Define Main Function
#####################################

def main() -> None:
    logger.info("Starting Producer to run continuously.")

    logger.info("STEP 1. Read environment variables.")
    try:
        interval_secs: int = config.get_message_interval_seconds_as_int()
        topic: str = config.get_kafka_topic()
        kafka_server: str = config.get_kafka_broker_address()
        live_data_path: pathlib.Path = config.get_live_data_path()
    except Exception as e:
        logger.error(f"ERROR: Failed to load environment variables: {e}")
        sys.exit(1)

    logger.info("STEP 2. Delete the live data file if exists to start fresh.")
    try:
        if live_data_path.exists():
            live_data_path.unlink()
            logger.info("Deleted existing live data file.")
        logger.info("STEP 3. Build the path folders to the live data file if needed.")
        os.makedirs(live_data_path.parent, exist_ok=True)
    except Exception as e:
        logger.error(f"ERROR: Failed to reset live data file: {e}")
        sys.exit(2)

    logger.info("STEP 4. Create a Kafka producer and topic.")
    producer = None
    try:
        verify_services()
        producer = KafkaProducer(
            bootstrap_servers=kafka_server,
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        )
        create_kafka_topic(topic)
        logger.info(f"Kafka producer connected to {kafka_server}")
    except Exception as e:
        logger.warning(f"WARNING: Kafka connection failed: {e}")
        producer = None

    logger.info("STEP 5. Stream earthquake data.")
    try:
        for message in fetch_earthquake_data():
            logger.info(message)

            with live_data_path.open("a") as f:
                f.write(json.dumps(message) + "\n")
                logger.info(f"Wrote message to file: {message}")

            # Send to Kafka 
            if producer:
                producer.send(topic, value=message)
                logger.info(f"Sent message to Kafka topic '{topic}': {message}")

            time.sleep(interval_secs)

    except KeyboardInterrupt:
        logger.warning("WARNING: Producer interrupted by user.")
    except Exception as e:
        logger.error(f"ERROR: Unexpected error: {e}")
    finally:
        if producer:
            producer.close()
            logger.info("Kafka producer closed.")
        logger.info("FINALLY: Producer shutting down.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
