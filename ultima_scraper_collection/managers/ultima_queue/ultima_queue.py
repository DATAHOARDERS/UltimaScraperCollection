from typing import Any, Callable, Coroutine

import aio_pika
import orjson


class MandatoryJob:
    def __init__(
        self,
        job_name: str,
        success_queue_name: str | None = None,
        failure_queue_name: str | None = None,
    ) -> None:
        self.job_name = job_name
        self.success_queue_name = success_queue_name
        self.failure_queue_name = failure_queue_name

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_name": self.job_name,
            "success_queue_name": self.success_queue_name,
            "failure_queue_name": self.failure_queue_name,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "MandatoryJob":
        return cls(**data)


class StandardData:
    def __init__(
        self,
        site_name: str,
        performer_id: int,
        performer_username: str,
        category: str = "",
        mandatory_jobs: list[MandatoryJob] = [],
        extra_data: dict[str, Any] = {},
    ) -> None:
        self.site_name = site_name
        self.performer_id = performer_id
        self.performer_username = performer_username
        self.mandatory_jobs = mandatory_jobs
        self.category = category
        self.extra_data = extra_data

    def to_dict(self) -> dict[str, Any]:
        return {
            "site_name": self.site_name,
            "performer_id": self.performer_id,
            "performer_username": self.performer_username,
            "mandatory_jobs": [x.to_dict() for x in self.mandatory_jobs],
            "category": self.category,
            "extra_data": self.extra_data,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "StandardData":
        mandatory_jobs = [
            MandatoryJob.from_dict(x) for x in data.get("mandatory_jobs", [])
        ]
        data.pop("mandatory_jobs", None)
        return cls(**data, mandatory_jobs=mandatory_jobs)

    @classmethod
    def from_legacy(cls, data: dict[str, Any]) -> "StandardData":
        temp_data = data["data"]
        return cls(
            site_name=temp_data["site_name"],
            performer_id=temp_data["performer_id"],
            performer_username=temp_data["username"],
        )

    def create_mandatory_job(
        self,
        job_name: str,
        success_queue_name: str | None = None,
        failure_queue_name: str | None = None,
    ) -> MandatoryJob:
        # Check if a job with the same job_name already exists
        for job in self.mandatory_jobs:
            if job.job_name == job_name:
                return job  # Return the existing job if found

        # If no match is found, create and add the new job
        mandatory_job = MandatoryJob(job_name, success_queue_name, failure_queue_name)
        self.mandatory_jobs.append(mandatory_job)
        return mandatory_job


class UltimaQueue:
    def __init__(self, host: str = "localhost") -> None:
        self.amqp_url = f"amqp://{host}"
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

    async def consume_messages(
        self,
        queue_name: str,
        callback: Callable[[Any, Any], Coroutine[Any, Any, None]],
        *args: Any,
        prefetch_count: int = 0,
    ):
        """Consume messages from a specified queue."""
        if not self.channel or self.channel.is_closed:
            await self.connect(prefetch_count=prefetch_count)
        queue = await self.declare_queue(queue_name)

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                try:
                    async with message.process(requeue=True, ignore_processed=True):
                        await callback(message, *args)
                        # await message.ack()
                except Exception as _e:
                    continue
        await self.disconnect()

    async def publish_message(
        self,
        queue_name: str,
        message: dict[str, Any],
        durable: bool = True,
        priority: int | None = None,
        unique_id: int | str | None = None,
        headers: dict[str, Any] = {},
        print_error: bool = True,
        surpress: bool = False,
    ):
        if self.channel is None:
            await self.connect()
        await self.declare_queue(queue_name, durable)
        assert self.channel is not None

        if unique_id is None:
            unique_id = message.get("id")
        if unique_id:
            # unique_id = str(unique_id)
            if not headers:
                headers = {"x-deduplication-header": unique_id}
        try:
            await self.channel.default_exchange.publish(
                aio_pika.Message(
                    body=orjson.dumps(message, option=orjson.OPT_NON_STR_KEYS),
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
            if not surpress:
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
        task = orjson.loads(message.body.decode())
        await self.publish_message(
            queue_name, task, priority=priority, headers=message.headers
        )

    async def close(self):
        if self.connection:
            await self.connection.close()
