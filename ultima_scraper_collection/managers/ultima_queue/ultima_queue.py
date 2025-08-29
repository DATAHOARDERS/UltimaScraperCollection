import asyncio
import os
import socket
import uuid
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, AsyncIterator, Callable, Coroutine, cast

import orjson
from redis.asyncio import Redis


def create_notification(
    category: str,
    site_name: str,
    item: Any,
) -> dict[str, Any]:
    """Create a notification message for performers/users"""
    json_message: dict[str, Any] = {
        "site_name": site_name,
        "category": category,
        "performer_id": item.id,
        "username": item.username,
    }
    message: dict[str, Any] = {"id": item.id, "data": json_message}
    return message


def create_message(
    site_name: str, item: Any, mandatory_jobs: dict[str, dict[str, list[str]]]
) -> dict[str, Any]:
    """Create a job message for performers/users"""
    json_message: dict[str, Any] = {
        "site_name": site_name,
        "performer_id": item.id,
        "username": item.username,
        "mandatory_jobs": mandatory_jobs,
    }
    message: dict[str, Any] = {"id": item.id, "data": json_message}
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
        consumer_name: str | None = None,
    ) -> None:
        pass

    @abstractmethod
    async def process_history_and_reclaim(
        self,
        queue_name: str,
        *,
        prefetch_count: int = 0,
        consumer_names: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Collect reclaimable messages for the given queue and consumers.

        Returns a list of message payload dicts: {'id': id, 'data': {...}}
        The backend should remove the original entries (ack+del) to avoid
        duplicate delivery; callers will re-publish or distribute the
        returned payloads to consumers.
        """
        pass

    @abstractmethod
    async def list_queue_jobs(
        self, queue_name: str, limit: int = 100
    ) -> list[dict[str, Any]]:
        """Return a list of entries for the given queue with status information.

        Each item in the returned list will be a dict: {"id": <id>, "data": {...}, "status": "queued"|"pending"}
        """
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
        # Connection and stream configuration
        self.redis_url = redis_url or os.getenv("REDIS_URL", "redis://localhost:6379/0")
        self.stream_prefix = stream_prefix
        self.group_name = group_name
        self.maxlen = maxlen
        # Client and consumer identity
        self._redis: Any | None = None
        self._consumer_name = (
            f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:6]}"
        )
        print(f"DEBUG: Created consumer {self._consumer_name}")
        # Reclaim settings (XAUTOCLAIM) to retry stale pending entries
        # Can be tuned via env vars: ULTIMA_REDIS_RECLAIM_MIN_IDLE_MS, ULTIMA_REDIS_RECLAIM_BATCH
        try:
            self._reclaim_min_idle_ms = int(
                os.getenv("ULTIMA_REDIS_RECLAIM_MIN_IDLE_MS", "30000")
            )
        except Exception:
            self._reclaim_min_idle_ms = 30000
        try:
            self._reclaim_batch = int(os.getenv("ULTIMA_REDIS_RECLAIM_BATCH", "100"))
        except Exception:
            self._reclaim_batch = 100

    async def process_history_and_reclaim(
        self,
        queue_name: str,
        *,
        prefetch_count: int = 0,
        consumer_names: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Collect history entries and reclaim pending messages, returning a list of payloads.

        This implementation will ACK+DEL reclaimed entries (to avoid duplicates)
        and return their payloads for the caller (manager) to re-publish or
        distribute to workers.
        """
        await self.connect()
        assert self._redis is not None

        reclaimed: list[dict[str, Any]] = []
        stream_name = self._get_stream_name(queue_name)
        consumers_list = consumer_names or [self._consumer_name]

        # Deliver any never-consumed historical entries for the first consumer
        try:
            first_consumer = consumers_list[0]
            while True:
                history_resp = await self._redis.xreadgroup(
                    self.group_name,
                    first_consumer,
                    streams={stream_name: "0"},
                    count=max(1, prefetch_count or 1),
                )
                if not history_resp:
                    break
                made_progress = False
                for _stream, messages in history_resp:
                    for msg_id, fields in messages:
                        fields_map = cast(dict[bytes, bytes], fields)
                        body_bytes = fields_map.get(b"body")
                        if body_bytes is not None:
                            decoded_payload: Any = orjson.loads(body_bytes)
                            if isinstance(decoded_payload, dict):
                                data: dict[str, Any] = cast(
                                    dict[str, Any], decoded_payload
                                )
                            else:
                                data = {"body": decoded_payload}
                        else:
                            data = {}
                        msg_id_str = (
                            msg_id.decode()
                            if isinstance(msg_id, (bytes, bytearray))
                            else msg_id
                        )
                        reclaimed.append({"id": msg_id_str, "data": data})
                        try:
                            await self._redis.xack(
                                stream_name, self.group_name, msg_id_str
                            )
                            await self._redis.xdel(stream_name, msg_id_str)
                            made_progress = True
                        except Exception as e:
                            print(f"Error removing history message {msg_id_str}: {e}")
                if not made_progress:
                    break
        except Exception as e:
            print(f"Error delivering history for {stream_name}: {e}")

        # Reclaim pending entries using XAUTOCLAIM when available, falling back
        # to XPENDING/XCLAIM for older servers.
        for local_consumer in consumers_list:
            try:
                start_id: str | bytes = "0-0"
                while True:
                    try:
                        xac_ret = await self._redis.xautoclaim(
                            stream_name,
                            self.group_name,
                            local_consumer,
                            self._reclaim_min_idle_ms,
                            start_id,
                            count=self._reclaim_batch,
                            justid=False,
                        )
                    except Exception:
                        raise

                    # redis-py may return (next_id, messages) or (next_id, messages, deleted)
                    if isinstance(xac_ret, (list, tuple)):
                        ln = len(xac_ret)  # type: ignore[arg-type]
                        if ln == 2:
                            next_id_any, claimed_any = cast(tuple[Any, Any], xac_ret)
                        elif ln == 3:
                            next_id_any, claimed_any, _deleted = cast(
                                tuple[Any, Any, Any], xac_ret
                            )
                        else:
                            raise ValueError(
                                "Unexpected XAUTOCLAIM return format with length "
                                + str(ln)
                            )
                        next_id = cast(str | bytes, next_id_any)
                        claimed_list = cast(
                            list[tuple[bytes | str, dict[bytes, bytes]]],
                            claimed_any,
                        )
                    else:
                        raise ValueError(
                            f"Unexpected XAUTOCLAIM return type: {type(xac_ret)}"
                        )

                    if not claimed_list:
                        break

                    for msg_id_raw, fields in claimed_list:
                        fields_map = fields
                        body_bytes = fields_map.get(b"body")
                        if body_bytes is not None:
                            decoded_payload2: Any = orjson.loads(body_bytes)
                            if isinstance(decoded_payload2, dict):
                                data = cast(dict[str, Any], decoded_payload2)
                            else:
                                data = {"body": decoded_payload2}
                        else:
                            data = {}

                        msg_id_str = (
                            msg_id_raw.decode()
                            if isinstance(msg_id_raw, (bytes, bytearray))
                            else str(msg_id_raw)
                        )

                        reclaimed.append({"id": msg_id_str, "data": data})
                        try:
                            await self._redis.xack(
                                stream_name, self.group_name, msg_id_str
                            )
                            await self._redis.xdel(stream_name, msg_id_str)
                        except Exception as e:
                            print(f"Error removing claimed message {msg_id_str}: {e}")

                    # Advance start_id; when server says '0-0' there are no further entries
                    if next_id in ("0-0", b"0-0"):
                        break
                    start_id = next_id
            except Exception as e:
                print(
                    f"INFO: XAUTOCLAIM not available or failed for {stream_name}: {e}. Falling back to XPENDING/XCLAIM."
                )

                try:
                    while True:
                        try:
                            pending = await self._redis.execute_command(
                                b"XPENDING",
                                stream_name,
                                self.group_name,
                                b"IDLE",
                                str(self._reclaim_min_idle_ms).encode(),
                                b"-",
                                b"+",
                                str(self._reclaim_batch).encode(),
                            )
                        except Exception as _xp_err:
                            print(
                                f"WARN: XPENDING fallback failed for {stream_name}: {_xp_err}"
                            )
                            break

                        if not pending:
                            break

                        ids: list[bytes] = []
                        pending_rows = cast(list[list[Any] | tuple[Any, ...]], pending)
                        for row in pending_rows:
                            if not row:
                                continue
                            mid_val: Any = row[0]
                            mid_str: str = (
                                mid_val.decode()
                                if isinstance(mid_val, (bytes, bytearray))
                                else str(mid_val)
                            )
                            ids.append(mid_str.encode())

                        if not ids:
                            break

                        try:
                            claimed_entries = await self._redis.execute_command(
                                b"XCLAIM",
                                stream_name,
                                self.group_name,
                                local_consumer,
                                str(self._reclaim_min_idle_ms).encode(),
                                *ids,
                            )
                        except Exception as _xc_err:
                            print(
                                f"WARN: XCLAIM fallback failed for {stream_name}: {_xc_err}"
                            )
                            break

                        if not claimed_entries:
                            continue

                        entries_list = cast(
                            list[tuple[bytes | str, dict[bytes, bytes]]],
                            claimed_entries,
                        )
                        for msg_id_raw, fields_map2 in entries_list:
                            msg_id_str: str = (
                                msg_id_raw.decode()
                                if isinstance(msg_id_raw, (bytes, bytearray))
                                else str(msg_id_raw)
                            )

                            body_bytes2 = fields_map2.get(b"body")
                            if body_bytes2 is not None:
                                decoded_payload3: Any = orjson.loads(body_bytes2)
                                if isinstance(decoded_payload3, dict):
                                    data = cast(dict[str, Any], decoded_payload3)
                                else:
                                    data = {"body": decoded_payload3}
                            else:
                                data = {}

                            reclaimed.append({"id": msg_id_str, "data": data})
                            try:
                                await self._redis.xack(
                                    stream_name, self.group_name, msg_id_str
                                )
                                await self._redis.xdel(stream_name, msg_id_str)
                            except Exception as _proc_err:
                                print(
                                    f"Error removing claimed (fallback) message {msg_id_str}: {_proc_err}"
                                )
                except Exception as _fallback_err:
                    print(
                        f"Error during XPENDING/XCLAIM fallback for {stream_name}: {_fallback_err}"
                    )

        return reclaimed

    async def list_queue_jobs(
        self, queue_name: str, limit: int = 100
    ) -> list[dict[str, Any]]:
        """List entries in the stream and mark them as 'pending' if present in the consumer group's PEL.

        This performs an XRANGE to fetch recent entries and an XPENDING to collect pending ids
        belonging to this consumer group. Entries returned will include the original body
        decoded and a status field ('queued' or 'pending').
        """
        await self.connect()
        assert self._redis is not None

        stream_name = self._get_stream_name(queue_name)
        pending_ids: set[str] = set()

        # Try to collect pending IDs for the group. Use XPENDING <stream> <group> - + COUNT
        try:
            pending = await self._redis.execute_command(
                "XPENDING", stream_name, self.group_name, "-", "+", str(limit)
            )
            if pending:
                for row in pending:
                    if not row:
                        continue
                    mid = row[0]
                    mid_str = (
                        mid.decode()
                        if isinstance(mid, (bytes, bytearray))
                        else str(mid)
                    )
                    pending_ids.add(mid_str)
        except Exception:
            # If XPENDING fails (older servers, permissions), just proceed without pending info
            pending_ids = set()

        # Fetch entries from the stream
        try:
            entries = await self._redis.xrange(stream_name, "-", "+", count=limit)
        except Exception:
            entries = []

        results: list[dict[str, Any]] = []
        for msg_id, fields in entries:
            fields_map = fields
            body_bytes = None
            if isinstance(fields_map, dict):
                # keys are bytes when decode_responses=False
                body_bytes = fields_map.get(b"body") or fields_map.get("body")

            if body_bytes is not None:
                try:
                    decoded = orjson.loads(body_bytes)
                except Exception:
                    decoded = {"body": body_bytes}
                if isinstance(decoded, dict):
                    data = decoded
                else:
                    data = {"body": decoded}
            else:
                data = {}

            msg_id_str = (
                msg_id.decode()
                if isinstance(msg_id, (bytes, bytearray))
                else str(msg_id)
            )
            status = "pending" if msg_id_str in pending_ids else "queued"
            results.append({"id": msg_id_str, "data": data, "status": status})

        return results

    def _get_stream_name(self, queue_name: str) -> str:
        """Generate a proper stream name from queue name"""
        return f"{self.stream_prefix}:{queue_name}"

    async def connect(self) -> None:
        if self._redis is None:
            self._redis = Redis.from_url(self.redis_url, decode_responses=False)  # type: ignore
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
            # Write message into Redis Stream under field 'body'
            await self._redis.xadd(
                stream_name,
                {b"body": orjson.dumps(message)},
                maxlen=self.maxlen,
                approximate=True,
            )

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
        consumer_name: str | None = None,
    ) -> None:
        await self.connect()
        assert self._redis is not None

        stream_name = self._get_stream_name(queue_name)
        # Resolve the consumer identity to use for this consume loop
        local_consumer = consumer_name or self._consumer_name

        # Ensure consumer group exists for this stream
        try:
            await self._redis.xgroup_create(
                stream_name, self.group_name, id="0", mkstream=True
            )
        except Exception as e:
            if "BUSYGROUP" not in str(e):
                raise
            # Optionally backfill an existing group to deliver historical entries
            if os.getenv("ULTIMA_REDIS_GROUP_BACKFILL", "0") in {"1", "true", "True"}:
                try:
                    await self._redis.xgroup_setid(stream_name, self.group_name, id="0")
                    print(
                        f"DEBUG: Backfilled group '{self.group_name}' to 0 for {stream_name}"
                    )
                except Exception as _e:
                    print(
                        f"WARN: Failed to set group id to 0 for {stream_name}/{self.group_name}: {_e}"
                    )

        # # Deliver history and reclaim pending entries (extracted helper)
        # await self.process_history_and_reclaim(
        #     queue_name,
        #     callback,
        #     *args,
        #     prefetch_count=prefetch_count,
        #     consumer_name=local_consumer,
        # )

        # Main consumer loop: read new messages and process them inline
        read_count = max(1, prefetch_count or 1)
        while True:
            try:
                resp = await self._redis.xreadgroup(
                    self.group_name,
                    local_consumer,
                    streams={stream_name: ">"},
                    count=read_count,
                    block=5000,
                )

                if not resp:
                    continue

                for _stream, messages in resp:
                    for msg_id, fields in messages:
                        fields_map3 = cast(dict[bytes, bytes], fields)
                        body_bytes3 = fields_map3.get(b"body")
                        if body_bytes3 is not None:
                            decoded_payload4: Any = orjson.loads(body_bytes3)
                            if isinstance(decoded_payload4, dict):
                                data = cast(dict[str, Any], decoded_payload4)
                            else:
                                data = {"body": decoded_payload4}
                        else:
                            data = {}

                        msg_id_str = (
                            msg_id.decode()
                            if isinstance(msg_id, (bytes, bytearray))
                            else str(msg_id)
                        )

                        try:
                            message = RedisMessage(data, msg_id_str)
                            await callback(message, *args)
                            await self._redis.xack(
                                stream_name, self.group_name, msg_id_str
                            )
                            await self._redis.xdel(stream_name, msg_id_str)
                        except Exception as e:
                            print(f"Error processing message {msg_id_str}: {e}")
                            # Leave in PEL for retry/inspection

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
        self.consumers: list[UltimaConsumer] = []
        self.backend_type = backend
        self._backend: QueueBackendInterface

        if backend == QueueBackend.REDIS:
            # Allow overriding group name via env to match existing deployments
            env_group = os.getenv("ULTIMA_REDIS_GROUP")
            effective_group = env_group or group_name
            self._backend = RedisQueueBackend(redis_url, stream_prefix, effective_group)
        else:
            raise ValueError(f"Unsupported backend: {backend}")

    async def publish_message(
        self,
        queue_name: str,
        message: dict[str, Any],
        print_error: bool = True,
        suppress: bool = False,
    ) -> bool:
        return await self._backend.publish_message(
            queue_name=queue_name,
            message=message,
            print_error=print_error,
            suppress=suppress,
        )

    # Consume APIs are provided by `UltimaConsumer` (created via `create_consumer`).
    # The manager keeps publishing helpers only.

    def create_consumer(self, name: str) -> "UltimaConsumer":
        """Create a named consumer bound to this queue manager."""
        consumer = UltimaConsumer(self, name)
        self.consumers.append(consumer)
        return consumer

    async def consume_with_name(
        self,
        queue_name: str,
        callback: Callable[[Any, Any], Coroutine[Any, Any, None]],
        *args: Any,
        prefetch_count: int = 0,
        consumer_name: str | None = None,
    ) -> None:
        """Public delegator used by UltimaConsumer to invoke backend consume with a specific consumer name."""
        await self._backend.consume_messages(
            queue_name,
            callback,
            *args,
            prefetch_count=prefetch_count,
            consumer_name=consumer_name,
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

    async def process_history_and_reclaim(
        self,
        consumers: list["UltimaConsumer"],
        queue_name: str,
        prefetch_count: int = 0,
    ) -> None:
        """Delegate reclaim/history work to the configured backend implementation.

        The manager-level helper accepts an `UltimaConsumer` instance so callers
        can pass a specific consumer to own the reclaimed work. The real
        implementation lives on the backend (e.g. RedisQueueBackend).
        """
        # Delegate to the backend implementation. RedisQueueBackend exposes
        # `process_history_and_reclaim`. Use a cast for static typing clarity.
        backend_impl = cast(RedisQueueBackend, self._backend)
        consumer_names = [c.consumer_name for c in consumers]
        reclaimed = await backend_impl.process_history_and_reclaim(
            queue_name, prefetch_count=prefetch_count, consumer_names=consumer_names
        )

        # Simple distribution: round-robin assign reclaimed payloads back to consumers
        if reclaimed:
            idx = 0
            num_consumers = len(consumers) or 1
            for item in reclaimed:
                # Each item has {'id': id, 'data': {...}}
                target_consumer = consumers[idx % num_consumers]
                # Re-publish message to the same queue so consumers will pick it up.
                # We attach a header to indicate the intended consumer (optional).
                payload = (
                    item["data"].copy()
                    if isinstance(item.get("data"), dict)
                    else {"body": item.get("data")}
                )
                headers_dict = (
                    payload.get("_headers")
                    if isinstance(payload.get("_headers"), dict)
                    else {}
                )
                headers_dict["target_consumer"] = target_consumer.consumer_name
                payload["_headers"] = headers_dict
                await self.publish_message(queue_name, payload)
                idx += 1
        return None

    # Note: consume helpers removed from manager. Use an `UltimaConsumer`:
    # consumer = queue.create_consumer("name")
    # await consumer.consume_messages(...)


class UltimaConsumer:
    """Lightweight consumer wrapper with a fixed consumer name bound to a queue manager."""

    def __init__(self, manager: "UltimaQueue", consumer_name: str) -> None:
        self.manager = manager
        self.consumer_name = consumer_name

    async def consume_messages(
        self,
        queue_name: str,
        callback: Callable[[Any, Any], Coroutine[Any, Any, None]],
        *args: Any,
        prefetch_count: int = 0,
    ) -> None:
        # Delegate to the manager via public delegator, binding this consumer's name.
        # Calculate a numeric worker_id from the consumer name (e.g. 'worker-3' -> 3)
        try:
            worker_id = int(self.consumer_name.split("-")[1])
        except Exception:
            worker_id = 0

        # Prepend worker_id so the callback receives it as args[0]
        new_args = (worker_id,)
        if args:
            new_args = new_args + tuple(args)

        await self.manager.consume_with_name(
            queue_name,
            callback,
            *new_args,
            prefetch_count=prefetch_count,
            consumer_name=self.consumer_name,
        )

    async def consume_jobs(
        self,
        callback: Callable[[Any, Any], Coroutine[Any, Any, None]],
        *args: Any,
        prefetch_count: int = 0,
    ) -> None:
        await self.consume_messages(
            "jobs", callback, *args, prefetch_count=prefetch_count
        )

    async def consume_scrape_jobs(
        self,
        callback: Callable[[Any, Any], Coroutine[Any, Any, None]],
        *args: Any,
        prefetch_count: int = 0,
    ) -> None:
        await self.consume_messages(
            "scrape_jobs", callback, *args, prefetch_count=prefetch_count
        )

    async def consume_download_jobs(
        self,
        callback: Callable[[Any, Any], Coroutine[Any, Any, None]],
        *args: Any,
        prefetch_count: int = 0,
    ) -> None:
        await self.consume_messages(
            "download_jobs", callback, *args, prefetch_count=prefetch_count
        )

    async def consume_notifications(
        self,
        notification_type: str,
        callback: Callable[[Any, Any], Coroutine[Any, Any, None]],
        *args: Any,
        prefetch_count: int = 0,
    ) -> None:
        await self.consume_messages(
            f"{notification_type}_notifications",
            callback,
            *args,
            prefetch_count=prefetch_count,
        )


# Backward compatibility exports
__all__ = [
    "UltimaQueue",
    "StandardData",
    "MandatoryJob",
    "QueueBackend",
    "create_notification",
    "create_message",
    "UltimaConsumer",
]
