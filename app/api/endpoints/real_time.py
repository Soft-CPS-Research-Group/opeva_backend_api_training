from fastapi import APIRouter, WebSocket, Query
from app.controllers import websocket_controller

router = APIRouter()

@router.websocket("/ws/data")
async def websocket_endpoint(
    websocket: WebSocket,
    exchange_name: str = Query(...)
):
    await websocket_controller.connect_client(websocket, exchange_name)