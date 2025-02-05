"""
consumers/earthquake_alerts.py - Handles sending earthquake alerts via email and text.

Features:
- Sends text alerts via `dc_texter`.
- Sends email alerts via `dc_mailer`.
- Includes error handling to prevent crashes.
- Logs all alert activity for monitoring.
"""

from dc_texter import send_text
from dc_mailer import send_mail
from utils.utils_logger import logger


def send_earthquake_alert(magnitude, location, event_time, latitude, longitude, depth):
    """
    Sends an earthquake alert via text and email if it meets the alert threshold.

    Args:
        magnitude (float): The earthquake magnitude.
        location (str): The location of the earthquake.
        event_time (str): The time of the earthquake.
        latitude (float): The latitude of the earthquake.
        longitude (float): The longitude of the earthquake.
        depth (float): The depth of the earthquake in km.
    """
    if magnitude < 4.0:
        logger.info(f"Earthquake detected (Mag {magnitude}) but below alert threshold.")
        return  

    msg = (
        f"EARTHQUAKE ALERT\n"
        f"Magnitude: {magnitude}\n"
        f"Location: {location}\n"
        f"Time: {event_time}\n"
        f"Coordinates: ({latitude}, {longitude})\n"
        f"Depth: {depth} km"
    )

    try:
        logger.info(f"Sending text alert: {msg}")
        send_text(msg)
        logger.info("Text alert sent successfully.")
    except Exception as e:
        logger.warning(f"Failed to send text alert: {e}")

    try:
        logger.info(f"Sending email alert: {msg}")
        send_mail(subject="Earthquake Alert!", body=msg)
        logger.info("Email alert sent successfully.")
    except Exception as e:
        logger.warning(f"Failed to send email alert: {e}")
