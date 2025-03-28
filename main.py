import asyncio
import json
from abc import ABC, abstractmethod

import redis
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

import config

# --- Interfaces for Abstraction (Interface Segregation & Dependency Inversion) ---


class IMessagePublisher(ABC):
    @abstractmethod
    def publish(self, channel: str, message: str) -> int:
        pass


class IDataStore(ABC):
    @abstractmethod
    def get(self, key: str):
        pass

    @abstractmethod
    def set(self, key: str, value, ex=None):
        pass


# --- Concrete Implementations using Redis (Single Responsibility) ---


class RedisPublisher(IMessagePublisher):
    def __init__(self, client: redis.Redis):
        self.client = client

    def publish(self, channel: str, message: str) -> int:
        return self.client.publish(channel, message)


class RedisDataStore(IDataStore):
    def __init__(self, client: redis.Redis):
        self.client = client

    def get(self, key: str):
        return self.client.get(key)

    def set(self, key: str, value, ex=None):
        self.client.set(key, value, ex=ex)


# --- Pydantic Models for Request Validation (Separation of Concerns) ---


class BikeCommand(BaseModel):
    command: str
    speed: int = None
    angle: int = None


class GPSData(BaseModel):
    bike_id: str
    latitude: float
    longitude: float
    timestamp: float


class NavigateCommand(BaseModel):
    start: dict  # e.g., {"lat": 43.65, "lon": -79.38}
    destination: dict


# --- Bike Service with Business Logic (Single Responsibility) ---


class BikeService:
    def __init__(self, data_store: IDataStore, publisher: IMessagePublisher):
        self.data_store = data_store
        self.publisher = publisher

    async def get_latest_gps(self, bike_id: str):
        """Retrieve the latest GPS data for a bike."""
        redis_key = f"gps:{bike_id}"
        gps_data = self.data_store.get(redis_key)
        if gps_data:
            return json.loads(gps_data)
        else:
            raise HTTPException(
                status_code=404, detail="No GPS data found for this bike"
            )

    async def test_bike_connection(self, bike_id: str):
        """Send a connection request and wait for the bike’s acknowledgment."""
        message = {"topic": bike_id, "payload": {"command": "connect"}}
        self.publisher.publish("mqtt_channel", json.dumps(message))

        redis_key = f"ack:{bike_id}"
        self.data_store.set(redis_key, "waiting", ex=10)

        for _ in range(10):  # Retry for 10 seconds
            ack_status = self.data_store.get(redis_key)
            if ack_status == "acknowledged":
                return {
                    "status": "success",
                    "message": f"Bike {bike_id} responded successfully",
                }
            await asyncio.sleep(1)

        return {"status": "failed", "message": f"Bike {bike_id} did not respond"}

    def handle_bike_response(self, bike_id: str):
        """Handle acknowledgment received from the bike."""
        redis_key = f"ack:{bike_id}"
        self.data_store.set(redis_key, "acknowledged", ex=30)
        return {
            "status": "success",
            "message": f"Bike {bike_id} acknowledged connection",
        }

    def send_command(self, bike_id: str, command: BikeCommand):
        """Validate and send a command to the bike."""
        valid_commands = {"forward", "backward", "left", "right", "stop", "center"}
        if command.command not in valid_commands:
            raise HTTPException(
                status_code=400,
                detail="Invalid command. Use 'forward', 'backward', 'left', 'right', 'center', or 'stop'.",
            )

        message = {
            "topic": bike_id,
            "payload": {
                "command": command.command,
                "speed": command.speed,
                "turning_angle": command.angle,
            },
        }
        published_count = self.publisher.publish("mqtt_channel", json.dumps(message))
        if published_count > 0:
            return {
                "status": "sent",
                "message": f"Command '{command.command}' sent to Bike {bike_id}",
                "redis_receivers": published_count,
            }
        else:
            raise HTTPException(
                status_code=500, detail="No MQTT subscribers received the message."
            )

    def send_navigation(self, bike_id: str, nav: NavigateCommand):
        message = {
            "topic": bike_id,
            "payload": {
                "command": "navigate",
                "start": nav.start,
                "destination": nav.destination,
            },
        }
        published_count = self.publisher.publish("mqtt_channel", json.dumps(message))
        if published_count > 0:
            return {
                "status": "sent",
                "message": f"Navigation route sent to Bike {bike_id}",
                "redis_receivers": published_count,
            }
        else:
            raise HTTPException(
                status_code=500, detail="No MQTT subscribers received the message."
            )


# --- FastAPI Application Setup with Dependency Injection (Dependency Inversion) ---

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize Redis client and inject dependencies into service classes.
redis_client = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
data_store = RedisDataStore(redis_client)
publisher = RedisPublisher(redis_client)
bike_service = BikeService(data_store, publisher)

# --- API Endpoints ---


@app.get("/latest-gps/{bike_id}")
async def get_latest_gps(bike_id: str):
    return await bike_service.get_latest_gps(bike_id)


@app.get("/test-bike-connection/{bike_id}")
async def test_bike_connection(bike_id: str):
    return await bike_service.test_bike_connection(bike_id)


@app.post("/bike-response")
async def bike_response(bike_id: str):
    return bike_service.handle_bike_response(bike_id)


@app.post("/send-command")
async def send_command(command: BikeCommand):
    # Here we use the bike id from config. You might extend this endpoint to accept a bike_id.
    return bike_service.send_command(config.BIKE_ID, command)


@app.post("/send-navigation")
async def send_navigation(nav: NavigateCommand):
    return bike_service.send_navigation(config.BIKE_ID, nav)


@app.get("/")
def read_root():
    return {"Hello": "World"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
