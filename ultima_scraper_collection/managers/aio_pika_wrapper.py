from typing import Any

import aio_pika
import ujson


def create_notification(
    category: str,
    site_name: str,
    item: Any,
):
    json_message = {
        "site_name": site_name,
        "category": category,
        "performer_id": item.id,
        "username": item.username,
    }
    message = {"id": item.id, "data": json_message}
    return message


def create_message(
    site_name: str, item: Any, mandatory_jobs: dict[str, dict[str, list[str]]]
):
    json_message = {
        "site_name": site_name,
        "performer_id": item.id,
        "username": item.username,
        "mandatory_jobs": mandatory_jobs,
    }
    message = {"id": item.id, "data": json_message}
    return message


class AioPikaWrapper:
    def __init__(self, host: str = "localhost", heartbeat: int | None = None):
        heartbeat_param = f"?heartbeat={heartbeat}" if heartbeat is not None else ""
        self.amqp_url = f"amqp://{host}/{heartbeat_param}"
        self.connection = None
        self.channel = None

    def get_connection(self):
        assert self.connection is not None
        return self.connection

    def get_channel(self):
        assert self.channel is not None
        return self.channel

    async def connect(self, prefetch_count: int = 0):
        if self.connection and not self.connection.is_closed:
            return
        self.connection = await aio_pika.connect_robust(self.amqp_url)
        self.channel = await self.connection.channel()
        await self.channel.set_qos(prefetch_count=prefetch_count)

    async def disconnect(self):
        if self.connection:
            await self.connection.close()

    async def declare_queue(self, queue_name: str, durable: bool = True):
        if not self.channel or self.channel.is_closed:
            await self.connect()
        assert self.channel is not None
        return await self.channel.declare_queue(
            queue_name,
            durable=durable,
            arguments={"x-message-deduplication": True, "x-max-priority": 10},
        )

    async def publish_message(
        self,
        queue_name: str,
        message: dict[str, Any],
        durable: bool = True,
        priority: int | None = None,
        print_error: bool = True,
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
            if print_error:
                print(f"Error publishing message: {e}")
            return False

    async def publish_notification(self, message: dict[str, Any]):
        await self.publish_message("telegram_notifications", message)
        await self.publish_message("discord_notifications", message)

    async def push_back_message(
        self,
        queue_name: str,
        message: aio_pika.abc.AbstractIncomingMessage,
        priority: int | None = None,
    ):
        await message.reject()
        task = ujson.loads(message.body.decode())
        await self.publish_message(queue_name, task, priority=priority)

    async def close(self):
        if self.connection:
            await self.connection.close()
