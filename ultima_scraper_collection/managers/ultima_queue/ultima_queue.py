import asyncio
import os
import socket
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, AsyncIterator, Callable, Coroutine

import orjson
from redis.asyncio import Redis


def create_notification(
    category: str,
    site_name: str,
    item: Any,
) -> dict[str, Any]:
    """Create a notification message for performers/users"""
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
) -> dict[str, Any]:
    """Create a job message for performers/users"""
    json_message = {
        "site_name": site_name,
        "performer_id": item.id,
        "username": item.username,
        "mandatory_jobs": mandatory_jobs,
    }
    message = {"id": item.id, "data": json_message}
    return message


class QueueBackend(Enum):
    REDIS = "redis"


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
        data = data.copy()  # Don't modify the original
        data.pop("queue", None)  # Remove queue field if present
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


# Create a message-like object for compatibility
class RedisMessage:
    def __init__(self, body_data: dict[str, Any], msg_id: str):
        self.body = orjson.dumps(body_data)
        self.headers = body_data.get("_headers", {})
        self._msg_id = msg_id
        self._data = body_data

    async def ack(self):
        pass  # Handled automatically after successful processing

    async def reject(self):
        pass  # Could implement retry logic here

    def get_data(self) -> dict[str, Any]:
        return self._data


class QueueBackendInterface(ABC):
    """Abstract interface for queue backends"""

    @abstractmethod
    async def publish_message(
        self,
        queue_name: str,
        message: dict[str, Any],
        durable: bool = True,
        priority: int | None = None,
        unique_id: int | str | None = None,
        headers: dict[str, Any] = {},
        print_error: bool = True,
        suppress: bool = False,
    ) -> bool:
        pass

    @abstractmethod
    async def consume_messages(
        self,
        queue_name: str,
        callback: Callable[[Any, Any], Coroutine[Any, Any, None]],
        *args: Any,
        prefetch_count: int = 0,
    ) -> None:
        pass


class RedisQueueBackend(QueueBackendInterface):
    """Redis Streams implementation of queue backend"""

    def __init__(
        self,
        redis_url: str | None = None,
        stream_prefix: str = "ultima",
        group_name: str = "processors",
        maxlen: int = 10000,
    ):
        self.redis_url = redis_url or os.getenv("REDIS_URL", "redis://localhost:6379/0")
        self.stream_prefix = stream_prefix
        self.group_name = group_name
        self.maxlen = maxlen
        self._redis: Redis | None = None
        self._consumer_name = f"{socket.gethostname()}-{os.getpid()}"

    def _get_stream_name(self, queue_name: str) -> str:
        """Generate a proper stream name from queue name"""
        return f"{self.stream_prefix}:{queue_name}"

    async def connect(self) -> None:
        if self._redis is None:
            self._redis = Redis.from_url(self.redis_url, decode_responses=False)
            # Note: We don't create groups here since we don't know which queues will be used

    async def close(self) -> None:
        if self._redis is not None:
            await self._redis.close()
            self._redis = None

    async def publish_message(
        self,
        queue_name: str,
        message: dict[str, Any],
        print_error: bool = True,
        suppress: bool = False,
    ) -> bool:
        try:
            await self.connect()
            assert self._redis is not None

            # Use the proper stream name
            stream_name = self._get_stream_name(queue_name)

            if not suppress:
                print(f"Message published to {stream_name}")
            return True

        except Exception as e:
            if print_error:
                print(f"Error publishing message to {queue_name}: {e}")
            return False

    async def consume_messages(
        self,
        queue_name: str,
        callback: Callable[[Any, Any], Coroutine[Any, Any, None]],
        *args: Any,
        prefetch_count: int = 0,
    ) -> None:
        await self.connect()
        assert self._redis is not None

        stream_name = self._get_stream_name(queue_name)

        # Ensure consumer group exists for this stream
        try:
            await self._redis.xgroup_create(
                stream_name, self.group_name, id="$", mkstream=True
            )
        except Exception as e:
            if "BUSYGROUP" not in str(e):
                raise

        semaphore = asyncio.Semaphore(prefetch_count or 1)

        async def _process_message(msg_id: str, data: dict[str, Any]):
            async with semaphore:
                try:

                    message = RedisMessage(data, msg_id)
                    await callback(message, *args)

                    # Acknowledge and delete message on success
                    await self._redis.xack(stream_name, self.group_name, msg_id)
                    await self._redis.xdel(stream_name, msg_id)

                except Exception as e:
                    print(f"Error processing message {msg_id}: {e}")
                    # Leave in PEL for inspection

        # Main consumer loop
        while True:
            try:
                resp = await self._redis.xreadgroup(
                    self.group_name,
                    self._consumer_name,
                    streams={stream_name: ">"},
                    count=10,
                    block=5000,
                )

                if not resp:
                    continue

                for _stream, messages in resp:
                    for msg_id, fields in messages:
                        body = fields.get(b"body")
                        try:
                            data = orjson.loads(body) if body is not None else {}
                        except Exception:
                            data = {}

                        msg_id_str = (
                            msg_id.decode() if isinstance(msg_id, bytes) else msg_id
                        )
                        asyncio.create_task(_process_message(msg_id_str, data))

            except Exception as e:
                print(f"Error in consumer loop for {stream_name}: {e}")
                await asyncio.sleep(1)


class UltimaQueue:
    """Universal queue system using Redis backend"""

    def __init__(
        self,
        backend: QueueBackend = QueueBackend.REDIS,
        redis_url: str | None = None,
        stream_prefix: str = "ultima",
        group_name: str = "processors",
    ):
        self.backend_type = backend
        self._backend: QueueBackendInterface

        if backend == QueueBackend.REDIS:
            self._backend = RedisQueueBackend(redis_url, stream_prefix, group_name)
        else:
            raise ValueError(f"Unsupported backend: {backend}")

    async def publish_message(
        self,
        queue_name: str,
        message: dict[str, Any],
        durable: bool = True,
        priority: int | None = None,
        unique_id: int | str | None = None,
        headers: dict[str, Any] = {},
        print_error: bool = True,
        suppress: bool = False,
    ) -> bool:
        return await self._backend.publish_message(
            queue_name=queue_name,
            message=message,
            durable=durable,
            priority=priority,
            unique_id=unique_id,
            headers=headers,
            print_error=print_error,
            suppress=suppress,
        )

    async def consume_messages(
        self,
        queue_name: str,
        callback: Callable[[Any, Any], Coroutine[Any, Any, None]],
        *args: Any,
        prefetch_count: int = 0,
    ) -> None:
        await self._backend.consume_messages(
            queue_name, callback, *args, prefetch_count=prefetch_count
        )

    async def publish_notification(self, message: dict[str, Any]) -> None:
        """Convenience method for publishing notifications"""
        await self.publish_message("telegram_notifications", message)
        await self.publish_message("discord_notifications", message)

    async def publish_job(self, job_data: dict[str, Any]) -> bool:
        """Publish a job to the jobs queue"""
        return await self.publish_message("jobs", job_data)

    async def publish_scrape_job(self, job_data: dict[str, Any]) -> bool:
        """Publish a scrape job"""
        return await self.publish_message("scrape_jobs", job_data)

    async def publish_download_job(self, job_data: dict[str, Any]) -> bool:
        """Publish a download job"""
        return await self.publish_message("download_jobs", job_data)

    async def consume_jobs(
        self, callback: Callable[[Any, Any], Coroutine[Any, Any, None]], *args: Any
    ) -> None:
        """Consume jobs from the jobs queue"""
        await self.consume_messages("jobs", callback, *args)

    async def consume_scrape_jobs(
        self, callback: Callable[[Any, Any], Coroutine[Any, Any, None]], *args: Any
    ) -> None:
        """Consume scrape jobs"""
        await self.consume_messages("scrape_jobs", callback, *args)

    async def consume_download_jobs(
        self, callback: Callable[[Any, Any], Coroutine[Any, Any, None]], *args: Any
    ) -> None:
        """Consume download jobs"""
        await self.consume_messages("download_jobs", callback, *args)

    async def consume_notifications(
        self,
        notification_type: str,
        callback: Callable[[Any, Any], Coroutine[Any, Any, None]],
        *args: Any,
    ) -> None:
        """Consume notifications (telegram_notifications, discord_notifications, etc.)"""
        await self.consume_messages(
            f"{notification_type}_notifications", callback, *args
        )


# Backward compatibility exports
__all__ = [
    "UltimaQueue",
    "StandardData",
    "MandatoryJob",
    "QueueBackend",
    "create_notification",
    "create_message",
]
