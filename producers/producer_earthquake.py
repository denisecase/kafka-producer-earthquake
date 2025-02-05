"""
producers/producer_earthquake.py

Fetch real-time earthquake data from the USGS API and stream it to Kafka.

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

# Standard library
from datetime import datetime, timezone
import json
import pathlib
import requests
import sys
import time

# Local utilities
from utils.utils_logger import logger
from utils.utils_connector import KAFKA_TOPIC,send_kafka_message
from utils.utils_producer import (
    create_kafka_producer,
    ensure_topic_exists,
    verify_services,
)
import utils.utils_config as config

# Initialize
start_time = time.time()
timeout = 20 * 60  # 20 minutes

USGS_API_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"


def fetch_earthquake_data():
    """Fetch real-time earthquake data from USGS API."""
    params = {
        "format": "geojson",
        "limit": 5,  # Fetch last 5 events
        "minmagnitude": 2.5,
        "orderby": "time",
    }

    try:
        url = config.get_source_data_url() or USGS_API_URL
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        logger.info(f"Raw earthquake data: {data}")  # Log raw data to inspect


         # Return earthquakes where magnitude is greater than 0
        return [
            {
                "magnitude": feature["properties"].get("mag", 0) if feature["properties"].get("mag") else 0,
                "location": feature["properties"].get("place", "Unknown location"),
                "latitude": feature["geometry"]["coordinates"][1],
                "longitude": feature["geometry"]["coordinates"][0],
                "depth": feature["geometry"]["coordinates"][2],
                "time": datetime.fromtimestamp(
                    feature["properties"]["time"] / 1000, tz=timezone.utc
                ).isoformat(),
            }
            for feature in data["features"]
            if feature["properties"].get("mag", 0) > 0  
        ]

    except requests.RequestException as e:
        logger.error(f"HTTP error while fetching earthquake data: {e}")
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse earthquake data response: {e}")
    except Exception as e:
        logger.error(f"Unexpected error occurred: {e}")

    return []


def stream_earthquake_data(producer, topic, live_data_path, interval_secs):
    """
    Fetch and send earthquake data to Kafka.
    Runs for 20 minutes.
    """
    while time.time() - start_time < timeout:
        earthquakes = fetch_earthquake_data()
        for message in earthquakes:
            logger.info(message)

            # Write to local file
            try:
                with live_data_path.open("a") as f:
                    f.write(json.dumps(message) + "\n")
                    logger.info(f"Wrote message to file: {message}")
            except OSError as e:
                logger.error(f"Failed to write message to file: {e}")
                continue

            # Send to Kafka with retry logic
            for attempt in range(3):  # Retry up to 3 times
                if send_kafka_message(producer, topic, message):
                    break  
                logger.warning(f"Retry {attempt + 1}/3 - Kafka send failed.")
                time.sleep(1)
    

        time.sleep(interval_secs)

    logger.info("Producer finished execution. Shutting down.")


def main():
    """Main function to start the producer."""
    logger.info("Starting Kafka Producer.")

    verify_services()

    try:
        interval_secs = config.get_message_interval_seconds_as_int()
        live_data_path = pathlib.Path(config.get_live_data_path())
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        sys.exit(1)

    try:
        if live_data_path.exists():
            live_data_path.unlink()
            logger.info("Deleted existing live data file.")
        live_data_path.parent.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        logger.error(f"Failed to reset live data file: {e}")
        sys.exit(2)

    producer = create_kafka_producer()
    if not producer:
        logger.error("Failed to initialize Kafka producer. Exiting.")
        sys.exit(3)

    ensure_topic_exists(KAFKA_TOPIC)

    try:
        stream_earthquake_data(producer, KAFKA_TOPIC, live_data_path, interval_secs)
    except KeyboardInterrupt:
        logger.warning("Producer interrupted.")
    except Exception as e:
        logger.error(f"Unexpected error occurred: {e}")
    finally:
        producer.flush()
        logger.info("Producer shutdown complete.")


if __name__ == "__main__":
    main()
