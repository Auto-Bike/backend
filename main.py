from fastapi import FastAPI,HTTPException, Depends
from pydantic import BaseModel
from mqtt_handler import MQTTHandler
from fastapi.middleware.cors import CORSMiddleware
from connection_tracker import connection_tracker, ConnectionTracker
import config

app = FastAPI()


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins (change to frontend domain in production)
    allow_credentials=True,
    allow_methods=["*"],  # Allow all methods (POST, GET, OPTIONS, etc.)
    allow_headers=["*"],  # Allow all headers
)

# Initialize MQTT handler
mqtt_handler = MQTTHandler(config.MQTT_BROKER, config.MQTT_PORT, config.MQTT_TOPIC_PREFIX)

class BikeCommand(BaseModel):
    command: str
    speed: int = 50  # Default speed is 50%

@app.get("/test-bike-connection/{bike_id}")
async def request_bike(bike_id: str,  tracker: ConnectionTracker = Depends(lambda: connection_tracker)):
    """Sends an MQTT message to wake up the bike."""
    mid = mqtt_handler.publish_message(bike_id, "connect")

    if mid is None:
        return {"status": "failed", "message": "Failed to publish MQTT message"}

    # Track response and wait for confirmation from the bike
    tracker.create_event(bike_id)
    response = await tracker.wait_for_response(bike_id)

    return response
    
@app.post("/bike-response")
async def bike_response(bike_id: str, tracker: ConnectionTracker = Depends(lambda: connection_tracker)):
    """
    Receives an HTTP response from the bike after it gets the MQTT "connect" message.
    """
    if tracker.set_response(bike_id):
        return {"status": "success", "message": f"Bike {bike_id} acknowledged connection"}

    return {"status": "error", "message": "Unexpected response from bike"}

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
        "command": command.command,
        "speed": command.speed,
    }

    mid = mqtt_handler.publish_message(config.BIKE_ID, message)
    
    if mid is not None:
        return {"status": "sent", "message": f"Command '{command.command}' sent to Bike {config.BIKE_ID}", "mid": mid}
    else:
        raise HTTPException(status_code=500, detail="Failed to publish MQTT message.")



@app.get("/")
def read_root():
    return {"Hello": "World"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
