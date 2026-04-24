import asyncio
from fastapi import WebSocket, WebSocketDisconnect
from app.services import real_time_data_service

# Dicionário global para gerir clientes por exchange
_clients: dict[str, list[WebSocket]] = {}


async def connect_client(websocket: WebSocket, exchange_name: str):
    await websocket.accept()

    # Liga ao RabbitMQ via Service
    await real_time_data_service.connect_to_rabbit(exchange_name, _on_message)

    if exchange_name not in _clients:
        _clients[exchange_name] = []
    _clients[exchange_name].append(websocket)

    try:
        while True:
            await websocket.receive_text()  # lança WebSocketDisconnect quando cliente fecha
    except (WebSocketDisconnect, Exception):
        await _disconnect_client(exchange_name, websocket)


async def _disconnect_client(exchange_name: str, websocket: WebSocket):
    if exchange_name in _clients:
        _clients[exchange_name].remove(websocket)
        if not _clients[exchange_name]:
            del _clients[exchange_name]
            await real_time_data_service.unsubscribe(exchange_name)

    if not _clients:
        await real_time_data_service.disconnect_rabbitmq()


# Callback que o RabbitMQ chama quando chega uma mensagem.
async def _on_message(exchange_name: str, raw_data: str):
    clients = _clients.get(exchange_name, [])
    if not clients:
        return

    message = real_time_data_service.process(raw_data)

    dead = []
    for client in clients:
        try:
            await client.send_json(message)
        except Exception:
            dead.append(client)  # ← regista clientes mortos

    for client in dead:
        clients.remove(client)  # limpa depois do loop