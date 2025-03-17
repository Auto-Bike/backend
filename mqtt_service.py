import json
import logging

import redis
from mqtt_handler import MQTTHandler

# Configure logging
logging.basicConfig(level=logging.INFO)


class RedisSubscriber:
    """Handles Redis Pub/Sub and forwards messages to MQTT."""

    def __init__(
        self,
        redis_host: str,
        redis_port: int,
        redis_channel: str,
        mqtt_handler: MQTTHandler,
    ):
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_channel = redis_channel
        self.mqtt_handler = mqtt_handler

        try:
            self.redis_client = redis.Redis(
                host=self.redis_host, port=self.redis_port, db=0
            )
            self.pubsub = self.redis_client.pubsub()
            self.pubsub.subscribe(self.redis_channel)
            logging.info(f"Subscribed to Redis channel: {self.redis_channel}")
        except Exception as e:
            logging.error(f"Failed to connect to Redis: {e}")
            raise

    def listen_for_messages(self):
        """Listens to Redis and forwards messages to MQTT."""
        logging.info("Listening for messages on Redis...")

        try:
            for message in self.pubsub.listen():
                if message["type"] == "message":
                    try:
                        data = json.loads(message["data"])
                        topic = data.get("topic")
                        payload = data.get("payload")
                        print(topic, payload)
                        if topic and payload:
                            self.mqtt_handler.publish_message(topic, payload)
                            logging.info(f"Published to MQTT: {topic} -> {payload}")
                        else:
                            logging.warning(f"Invalid message format: {data}")
                    except json.JSONDecodeError:
                        logging.error(
                            f"Failed to decode JSON from Redis: {message['data']}"
                        )
        except Exception as e:
            logging.error(f"Error in Redis subscriber: {e}")


if __name__ == "__main__":
    # Configuration
    REDIS_HOST = "localhost"
    REDIS_PORT = 6379
    REDIS_CHANNEL = "mqtt_channel"

    MQTT_BROKER = "3.15.51.67"
    MQTT_PORT = 1883
    MQTT_TOPIC_PREFIX = "bike/"  # Prefix for bike commands

    # Initialize MQTT handler
    mqtt_handler = MQTTHandler(MQTT_BROKER, MQTT_PORT, MQTT_TOPIC_PREFIX)

    # Start Redis Subscriber
    redis_subscriber = RedisSubscriber(
        REDIS_HOST, REDIS_PORT, REDIS_CHANNEL, mqtt_handler
    )
    redis_subscriber.listen_for_messages()
