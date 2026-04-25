import aio_pika
from typing import Callable
from aio_pika.abc import AbstractRobustQueue

class RabbitMQAdapter:
    def __init__(self):
        self.url = "amqp://frontendapp:frontendappmq@softcps.dei.isep.ipp.pt:5672/"
        self._connection = None
        self._channel = None
        self._queues: dict[str, AbstractRobustQueue] = {}  # exchange_name -> queue

    async def connect(self, exchange_name: str, callback: Callable):
        # se fila já existe para esta exchange, não faz nada
        if exchange_name in self._queues:
            return

        # só conecta se não houver ligação
        if not self._connection or self._connection.is_closed:
            self._connection = await aio_pika.connect_robust(self.url)

        if not self._channel or self._channel.is_closed:
            self._channel = await self._connection.channel()

        # get exchange que já existe no broker
        exchange_obj = await self._channel.get_exchange(exchange_name)

        # cria fila temporária para esta exchange
        queue = await self._channel.declare_queue(
            exchange_name,
            durable=False,
            auto_delete=True
        )

        # bind à exchange fanout — sem routing key
        await queue.bind(exchange=exchange_obj)

        # guarda referência
        self._queues[exchange_name] = queue

        # arranca consumer
        async def on_message(message: aio_pika.IncomingMessage):
            async with message.process():
                await callback(exchange_name, message.body.decode())

        await queue.consume(on_message)

    async def unsubscribe(self, exchange_name: str):
        if exchange_name not in self._queues:
            return

        queue = self._queues.pop(exchange_name)
        await queue.delete()  # auto_delete trata do unbind

    async def disconnect(self):
        if self._channel and not self._channel.is_closed:
            await self._channel.close()
        if self._connection and not self._connection.is_closed:
            await self._connection.close()
        self._connection = None
        self._channel = None