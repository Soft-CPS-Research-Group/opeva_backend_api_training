import json
from typing import Callable
from app.rabbitMQ_adapter import RabbitMQAdapter

adapter = RabbitMQAdapter()

async def connect_to_rabbit(exchange_name: str, callback: Callable):
    await adapter.connect(exchange_name, callback)

async def unsubscribe(exchange_name: str):
    await adapter.unsubscribe(exchange_name)

async def disconnect_rabbitmq():
    await adapter.disconnect()

def process(raw: str) -> dict:
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {"raw": raw}