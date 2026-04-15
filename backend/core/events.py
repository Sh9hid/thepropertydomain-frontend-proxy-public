from typing import List
import datetime
from fastapi import WebSocket

class ConnectionManager:
    """
    Manages real-time WebSocket connections for the 'War Room' dashboard.
    Allows for instantaneous broadcasting of scraper and lead events.
    """
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.log_buffer: List[dict] = [] # Stores last 100 logs for replay

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        # Replay buffered logs
        for log in reversed(self.log_buffer):
            await websocket.send_json(log)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        """
        Sends a JSON payload to all connected clients.
        Used for real-time notifications of new leads, report completion, etc.
        """
        if message.get("type") == "SYSTEM_LOG":
            self.log_buffer.insert(0, message)
            self.log_buffer = self.log_buffer[:100]

        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception:
                # Handle stale connections
                pass

    async def broadcast_log(self, message: str, level: str = "INFO", category: str = "SYSTEM"):
        """
        Broadcasts a log message to the frontend pulse.
        """
        await self.broadcast({
            "type": "SYSTEM_LOG",
            "data": {
                "message": message,
                "level": level,
                "category": category,
                "timestamp": datetime.datetime.now().isoformat()
            }
        })

    async def broadcast_ticker_event(self, event: dict):
        """
        Push a real-time signal event to the ticker bar.
        event dict: { id, type, source, address, suburb, owner_name, heat_score,
                      lead_id, icon, color, headline, detected_at }
        """
        await self.broadcast({"type": "TICKER_EVENT", "data": event})

# Singleton instance for project-wide use
event_manager = ConnectionManager()
