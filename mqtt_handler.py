import json
import logging
from typing import Optional

import paho.mqtt.client as mqtt

# Configure logging
logging.basicConfig(level=logging.INFO)


class MQTTHandler:
    """Handles all MQTT-related operations."""

    def __init__(self, broker: str, port: int, topic_prefix: str):
        self.broker = broker
        self.port = port
        self.topic_prefix = topic_prefix
        self.client = mqtt.Client()

        # Attach callbacks
        self.client.on_connect = self.on_connect
        self.client.on_publish = self.on_publish

        try:
            self.client.connect(self.broker, self.port, 60)
            self.client.loop_start()  # Starts the MQTT loop in a background thread
            logging.info(f"Connected to MQTT Broker at {self.broker}:{self.port}")
        except Exception as e:
            logging.error(f"Failed to connect to MQTT Broker: {e}")

    def on_connect(self, client, userdata, flags, rc):
        """Handles MQTT connection event."""
        if rc == 0:
            logging.info("MQTT Connected successfully.")
        else:
            logging.error(f"MQTT Connection failed with code {rc}")

    def on_publish(self, client, userdata, mid):
        """Handles message publishing acknowledgment."""
        logging.info(f"Message {mid} successfully published.")

    def publish_message(self, bike_id: str, message: dict) -> Optional[int]:
        """Publishes a message to a specific bike topic."""
        topic = f"{self.topic_prefix}{bike_id}"
        json_message = json.dumps(message)  # Convert dict to JSON string
        if not self.client.is_connected():
            logging.error("MQTT client is not connected. Attempting to reconnect...")
            try:
                self.client.reconnect()
            except Exception as e:
                logging.error(f"Reconnect failed: {e}")
                return None

        result, mid = self.client.publish(topic, json_message)
        if result == mqtt.MQTT_ERR_SUCCESS:
            logging.info(f"Message sent to {topic}: {json_message}")
            return mid
        else:
            logging.error("Failed to publish MQTT message")
            return None
