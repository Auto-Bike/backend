import asyncio
from typing import Dict


class ConnectionTracker:
    """Tracks bike connection requests and responses."""

    def __init__(self):
        self.pending_responses: Dict[str, asyncio.Event] = {}

    def create_event(self, bike_id: str):
        """Creates a new event to track the bike response."""
        self.pending_responses[bike_id] = asyncio.Event()

    async def wait_for_response(self, bike_id: str, timeout: int = 5):
        """Waits for the bike's response within the timeout period."""
        try:
            #
            await asyncio.wait_for(
                self.pending_responses[bike_id].wait(), timeout=timeout
            )
            return {
                "status": "success",
                "message": f"Bike {bike_id} responded successfully",
            }
        except asyncio.TimeoutError:
            return {
                "status": "failed",
                "message": f"Bike {bike_id} did not respond within {timeout} seconds",
            }
        finally:
            self.pending_responses.pop(bike_id, None)  # Cleanup

    def set_response(self, bike_id: str):
        """Marks the bike response as received."""
        if bike_id in self.pending_responses:
            self.pending_responses[bike_id].set()
            return True
        return False


# Create a shared instance of ConnectionTracker
connection_tracker = ConnectionTracker()
