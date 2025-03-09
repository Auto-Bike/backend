from fastapi import FastAPI,HTTPException, Depends
from pydantic import BaseModel
from mqtt_handler import MQTTHandler
from fastapi.middleware.cors import CORSMiddleware
from connection_tracker import connection_tracker, ConnectionTracker
import config
import redis
import json
import asyncio

app = FastAPI()
redis_client = redis.Redis(host="localhost", port=6379, db=0,decode_responses=True)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins (change to frontend domain in production)
    allow_credentials=True,
    allow_methods=["*"],  # Allow all methods (POST, GET, OPTIONS, etc.)
    allow_headers=["*"],  # Allow all headers
)

# Initialize MQTT handler
# mqtt_handler = MQTTHandler(config.MQTT_BROKER, config.MQTT_PORT, config.MQTT_TOPIC_PREFIX)

class BikeCommand(BaseModel):
    command: str
    speed: int = 50  # Default speed is 50%

@app.get("/test-bike-connection/{bike_id}")
async def request_bike(bike_id: str):
    """Sends an MQTT message and tracks acknowledgment in Redis."""
    message = {"topic": f"{bike_id}", "payload": {"command": "connect"}}
    redis_client.publish("mqtt_channel", json.dumps(message))  # Send to Redis queue

    # Create event tracker in Redis
    redis_key = f"ack:{bike_id}"
    redis_client.set(redis_key, "waiting", ex=10)  # Set expiration (10s)

    # Wait for bike acknowledgment
    for _ in range(10):  # Retry for 10 seconds
        ack_status = redis_client.get(redis_key)
        if ack_status == "acknowledged":
            return {"status": "success", "message": f"Bike {bike_id} responded successfully"}
        await asyncio.sleep(1)  # Wait before retrying

    return {"status": "failed", "message": f"Bike {bike_id} did not respond"}

    
@app.post("/bike-response")
async def bike_response(bike_id: str):
    """Receives an acknowledgment from the bike and updates Redis."""
    redis_key = f"ack:{bike_id}"
    redis_client.set(redis_key, "acknowledged", ex=30)  # Store acknowledgment

    return {"status": "success", "message": f"Bike {bike_id} acknowledged connection"}

@app.post("/send-command")
async def send_command(command: BikeCommand):
    """
    Sends an MQTT command to the specified bike.
    Example JSON request:
    {
        "command": "forward"
    }
    """
    valid_commands = {"forward", "backward", "left", "right", "stop"}
    
    if command.command not in valid_commands:
        raise HTTPException(status_code=400, detail="Invalid command. Use 'forward', 'backward', 'left', 'right', or 'stop'.")

    message = {
        "topic": f"{config.BIKE_ID}", 
        "payload": {
            "command": command.command,
            "speed": command.speed,
        }
    }
    published_count = redis_client.publish("mqtt_channel", json.dumps(message))  # Send to Redis queue

    # mid = mqtt_handler.publish_message(config.BIKE_ID, message)

    if published_count > 0:  # At least one subscriber received the message
        return {
            "status": "sent",
            "message": f"Command '{command.command}' sent to Bike {config.BIKE_ID}",
            "redis_receivers": published_count  # Add Redis return value
        }
    else:
        raise HTTPException(status_code=500, detail="No MQTT subscribers received the message.")



@app.get("/")
def read_root():
    return {"Hello": "World"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
