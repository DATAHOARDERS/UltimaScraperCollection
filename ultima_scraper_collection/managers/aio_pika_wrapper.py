from typing import Any

import aio_pika
import ujson


class AioPikaWrapper:
    def __init__(self, host: str = "localhost"):
        self.amqp_url = f"amqp://{host}/"
        self.connection = None
        self.channel = None

    def get_connection(self):
        assert self.connection is not None
        return self.connection

    def get_channel(self):
        assert self.channel is not None
        return self.channel

    async def connect(self, prefetch_count: int = 0):
        if self.connection is not None:
            return
        self.connection = await aio_pika.connect_robust(self.amqp_url)
        self.channel = await self.connection.channel()
        await self.channel.set_qos(prefetch_count=prefetch_count)

    async def declare_queue(self, queue_name: str, durable: bool = True):
        if self.channel is None:
            await self.connect()
        assert self.channel is not None
        return await self.channel.declare_queue(
            queue_name, durable=durable, arguments={"x-message-deduplication": True}
        )

    async def publish_message(
        self,
        queue_name: str,
        message: dict[str, Any],
        durable: bool = True,
        priority: int = 0,
    ):
        if self.channel is None:
            await self.connect()
        await self.declare_queue(queue_name, durable)
        assert self.channel is not None

        message_id = message.get(
            "id", "default_id"
        )  # Ensure you have a unique ID for deduplication
        headers = {"x-deduplication-header": message_id}
        try:
            await self.channel.default_exchange.publish(
                aio_pika.Message(
                    body=ujson.dumps(message).encode(),
                    delivery_mode=(
                        aio_pika.DeliveryMode.PERSISTENT
                        if durable
                        else aio_pika.DeliveryMode.NOT_PERSISTENT
                    ),
                    headers=headers,
                    priority=priority,
                ),
                routing_key=queue_name,
            )
            print(f"Message published to {queue_name}")
            return True
        except aio_pika.exceptions.DeliveryError as e:
            print(f"Error publishing message: {e}")
            return False

    async def close(self):
        if self.connection:
            await self.connection.close()
