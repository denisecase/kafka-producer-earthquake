"""
producer_earthquake.py

Fetch real-time earthquake data from USGS API and stream it to a file and Kafka topic.

Example JSON message:
{
    "magnitude": 2.75,
    "location": "8 km SSE of Maria Antonia, Puerto Rico",
    "latitude": 17.9155,
    "longitude": -66.8498333333333,
    "depth": 13.42,
    "time": "2025-02-02T00:24:43.990000",
}
"""

#####################################
# Import Modules
#####################################

# Standard library
from datetime import datetime, timezone
import json
import os
import pathlib
import requests
import sys
import time

# Local utilities
import utils.utils_config as config
from utils.utils_producer import create_kafka_producer, is_topic_available
from utils.utils_logger import logger

#####################################
# INITIALIZE
#####################################

start_time = time.time()
timeout = 20 * 60  # 20 minutes in seconds

USGS_API_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"

#####################################
# Define Function to Fetch Earthquake Data
#####################################


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
        url: str = config.get_source_data_url() or USGS_API_URL

        response = requests.get(url, params=params)

        # Raise an error for HTTP issues
        response.raise_for_status()

        # Parse JSON response into a dictionary
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
                "time": datetime.fromtimestamp(
                    properties["time"] / 1000, tz=timezone.utc
                ).isoformat(),
            }
            earthquakes.append(earthquake)

        return earthquakes

    except Exception as e:
        logger.error(f"ERROR: Failed to fetch earthquake data: {e}")
        return []



#####################################
# Stream Earthquake Data
#####################################


def stream_earthquake_data(producer, topic, live_data_path, interval_secs):
    """
    Continuously fetch and send earthquake data to Kafka topic.
    Runs for a set timeout (20 minutes).
    """
    message_count = 0  # Track messages for periodic flushing

    while time.time() - start_time < timeout:
        earthquakes = fetch_earthquake_data()
        for message in earthquakes:
            logger.info(message)

            with live_data_path.open("a") as f:
                f.write(json.dumps(message) + "\n")
                logger.info(f"Wrote message to file: {message}")

            if producer:
                producer.produce(topic, value=json.dumps(message).encode("utf-8"))
                message_count += 1

            # Flush producer every 10 messages for efficiency
            if producer and message_count % 10 == 0:
                producer.flush()

        time.sleep(interval_secs)

    logger.info("Producer finished execution. Waiting before shutdown.")
    time.sleep(10)  # Allow logs to settle
    if producer:
        producer.flush()
    logger.info("FINALLY: Producer shutting down.")


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
        logger.info("Build the path folders to the live data file if needed.")
        os.makedirs(live_data_path.parent, exist_ok=True)
    except Exception as e:
        logger.error(f"ERROR: Failed to reset live data file: {e}")
        sys.exit(2)

    logger.info("STEP 3. Initialize Kafka producer.")
    producer = create_kafka_producer(kafka_server)

    if not producer:
        logger.error("ERROR: Kafka producer could not be initialized. Exiting.")
        sys.exit(3)

    logger.info("STEP 4. Ensure Kafka topic exists.")
    if not is_topic_available(topic):
        logger.error("ERROR: Kafka topic could not be created. Exiting.")
        sys.exit(4)

    logger.info("STEP 5. Start streaming earthquake data.")
    try:
        stream_earthquake_data(producer, topic, live_data_path, interval_secs)
    except KeyboardInterrupt:
        logger.warning("WARNING: Producer interrupted by user.")
    except Exception as e:
        logger.error(f"ERROR: Unexpected error: {e}")
    finally:
        logger.info("Producer shutting down.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
