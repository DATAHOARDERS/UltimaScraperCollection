import asyncio
import hashlib
import logging
import os
import socket
import uuid
from enum import Enum
from typing import Any, AsyncIterator, Callable, Coroutine, Optional, cast

import orjson
from pydantic import BaseModel
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


class StandardData(BaseModel):
    class OptionData(BaseModel):
        class MandatoryJob(BaseModel):
            job_name: str
            success_queue_name: str | None = None
            failure_queue_name: str | None = None

        mandatory_jobs: list[MandatoryJob] = []
        ppv_only: bool = False
        extra_data: dict[str, Any] = {}

        def create_mandatory_job(
            self,
            job_name: str,
            success_queue_name: str | None = None,
            failure_queue_name: str | None = None,
        ) -> "StandardData.OptionData.MandatoryJob":
            # Check if a job with the same job_name already exists
            for job in self.mandatory_jobs:
                if job.job_name == job_name:
                    return job  # Return the existing job if found

            # If no match is found, create and add the new job
            mandatory_job = self.MandatoryJob(
                job_name=job_name,
                success_queue_name=success_queue_name,
                failure_queue_name=failure_queue_name,
            )
            self.mandatory_jobs.append(mandatory_job)
            return mandatory_job

    site_name: str
    user_id: int
    username: str
    category: str = ""
    options: OptionData = OptionData()
    host_id: int | None = None
    skippable: bool = False
    active: bool | None = None


# Create a message-like object for compatibility
class RedisMessage:
    def __init__(
        self,
        backend: "RedisQueueBackend",
        stream_name: str,
        body_data: dict[str, Any],
        msg_id: str,
    ):
        self._backend = backend
        self._stream_name = stream_name
        self.body = orjson.dumps(body_data)
        self.headers = body_data.get("_headers", {})
        self._msg_id = msg_id
        self._data = body_data

    async def ack(self):
        """Acknowledge and delete this message from the stream."""
        try:
            await self._backend._ack_and_del(self._stream_name, self._msg_id)
            # Best-effort: remove dedupe key if message carried a fingerprint
            try:
                fp = (
                    self._data.get("_fingerprint")
                    if isinstance(self._data, dict)
                    else None
                )
                await self._backend._remove_dedupe_if_matches(
                    self._stream_name, fp, self._msg_id
                )
            except Exception:
                pass
        except Exception:
            # Best-effort; swallow errors to avoid crashing handler
            pass

    async def reject(self):
        """Placeholder for rejecting/requeuing the message."""
        # Could be expanded to implement dead-lettering or retries
        pass

    def get_data(self) -> dict[str, Any]:
        return self._data


class RedisQueueBackend:
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
        # Module-level logger
        self._logger = logging.getLogger(__name__)
        self._logger.debug("Created consumer %s", self._consumer_name)
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
        # Dedup TTL for published jobs (read once)
        try:
            self._dedupe_ttl = int(os.getenv("ULTIMA_JOB_DEDUPE_TTL_SECONDS", "86400"))
        except Exception:
            self._dedupe_ttl = 86400

        # Cached SHA for the publish Lua script (lazy-loaded)
        self._publish_lua_sha: Optional[str] = None

    # --- small helpers to reduce duplication ---
    def _to_msg_id_str(self, msg_id: Any) -> str:
        return (
            msg_id.decode() if isinstance(msg_id, (bytes, bytearray)) else str(msg_id)
        )

    def _decode_body(self, fields: dict[Any, Any]) -> dict[str, Any]:
        # Accept either bytes keys or str keys
        body = None
        if isinstance(fields, dict):
            body = fields.get(b"body") or fields.get("body")

        if body is None:
            return {}

        try:
            decoded = orjson.loads(body)
        except Exception:
            return {"body": body}

        if isinstance(decoded, dict):
            return decoded
        return {"body": decoded}

    async def _ack_and_del(self, stream_name: str, msg_id_str: str) -> None:
        assert self._redis is not None
        try:
            await self._redis.xack(stream_name, self.group_name, msg_id_str)
        except Exception as e:
            self._logger.debug("xack failed for %s: %s", msg_id_str, e)
        try:
            await self._redis.xdel(stream_name, msg_id_str)
        except Exception as e:
            self._logger.debug("xdel failed for %s: %s", msg_id_str, e)

    async def _remove_dedupe_if_matches(
        self, stream_name: str, fingerprint: str | None, msg_id_str: str
    ) -> None:
        """Atomically delete the dedupe key if it equals msg_id_str.

        Uses a tiny Lua script to check GET(key) == msg_id and DEL atomically.
        """
        if not fingerprint:
            return
        assert self._redis is not None
        dedupe_key = f"{stream_name}:dedupe:{fingerprint}"
        lua = (
            "if redis.call('GET', KEYS[1]) == ARGV[1] then\n"
            "  return redis.call('DEL', KEYS[1])\n"
            "else\n"
            "  return 0\n"
            "end\n"
        )
        try:
            await self._redis.eval(lua, 1, dedupe_key, msg_id_str)
        except Exception:
            # best-effort cleanup; don't raise
            pass

    async def _load_publish_lua(self) -> None:
        """Load (or ensure loaded) the Lua script used to perform atomic dedupe+XADD.

        Script behaviour:
          - SET dedupe_key NX EX ttl
          - if set succeeded -> XADD stream MAXLEN ~ maxlen body <message>
          - return the XADD id on success, or 0 on duplicate
        """
        assert self._redis is not None
        if self._publish_lua_sha:
            return

        lua = (
            "local dedupe_key = KEYS[1]\n"
            "local stream = KEYS[2]\n"
            "local maxlen = tonumber(ARGV[1])\n"
            "local body = ARGV[2]\n"
            "local ttl = tonumber(ARGV[3])\n"
            "-- If key exists, check if it's still pointing at a live entry; if so, treat as duplicate\n"
            "local existing = redis.call('GET', dedupe_key)\n"
            "if existing then\n"
            "  local r = redis.call('XRANGE', stream, existing, existing)\n"
            "  if r and (#r > 0) then\n"
            "    return 0\n"
            "  end\n"
            "  -- Stale key: remove and continue to republish\n"
            "  redis.call('DEL', dedupe_key)\n"
            "end\n"
            "-- Reserve publication atomically\n"
            "local reserved = redis.call('SET', dedupe_key, 'PENDING', 'NX', 'EX', ttl)\n"
            "if not reserved then\n"
            "  return 0\n"
            "end\n"
            "local id = redis.call('XADD', stream, 'MAXLEN', '~', maxlen, 'body', body)\n"
            "redis.call('SET', dedupe_key, id, 'EX', ttl)\n"
            "return id\n"
        )
        try:
            sha = await self._redis.script_load(lua)
            if isinstance(sha, (bytes, bytearray)):
                sha = sha.decode()
            self._publish_lua_sha = sha
        except Exception as e:
            self._logger.debug("Failed to load publish Lua script: %s", e)

    async def process_history_and_reclaim(
        self,
        queue_name: str,
        *,
        prefetch_count: int = 0,
        consumer_names: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Collect history entries and reclaim pending messages, returning a list of payloads.

        This implementation will ACK+DEL reclaimed entries (to avoid duplicates)
        """
        await self.connect()
        assert self._redis is not None

        reclaimed: list[dict[str, Any]] = []
        stream_name = self._get_stream_name(queue_name)
        consumers_list = consumer_names or [self._consumer_name]

        # First deliver any never-consumed historical entries for the first consumer
        try:
            first_consumer = consumers_list[0]
            reclaimed.extend(
                await self._deliver_history(stream_name, first_consumer, prefetch_count)
            )
        except Exception as e:
            self._logger.warning("Error delivering history for %s: %s", stream_name, e)

        # For each consumer, try XAUTOCLAIM reclaim and fall back to XPENDING/XCLAIM
        for local_consumer in consumers_list:
            try:
                reclaimed.extend(
                    await self._reclaim_with_xautoclaim(
                        stream_name, local_consumer, prefetch_count
                    )
                )
            except Exception as e:
                self._logger.info(
                    "XAUTOCLAIM not available or failed for %s: %s. Falling back to XPENDING/XCLAIM.",
                    stream_name,
                    e,
                )

                try:
                    reclaimed.extend(
                        await self._reclaim_with_xpending_fallback(
                            stream_name, local_consumer
                        )
                    )
                except Exception as _fallback_err:
                    self._logger.warning(
                        "Error during XPENDING/XCLAIM fallback for %s: %s",
                        stream_name,
                        _fallback_err,
                    )

        return reclaimed

    async def _deliver_history(
        self, stream_name: str, consumer: str, prefetch_count: int
    ) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        assert self._redis is not None
        while True:
            history_resp = await self._redis.xreadgroup(
                self.group_name,
                consumer,
                streams={stream_name: "0"},
                count=max(1, prefetch_count or 1),
            )
            if not history_resp:
                break
            msgs_to_ack: list[str] = []
            for _stream, messages in history_resp:
                for msg_id, fields in messages:
                    data = self._decode_body(cast(dict[bytes, bytes], fields))
                    msg_id_str = self._to_msg_id_str(msg_id)
                    results.append({"id": msg_id_str, "data": data})
                    msgs_to_ack.append(msg_id_str)

            # Acknowledge and delete in a single pipeline for efficiency
            if msgs_to_ack:
                pipe = self._redis.pipeline()
                for mid in msgs_to_ack:
                    pipe.xack(stream_name, self.group_name, mid)
                    pipe.xdel(stream_name, mid)
                try:
                    await pipe.execute()
                except Exception as e:
                    self._logger.warning("Failed pipeline ack/del for history: %s", e)
                    # best-effort; continue
                else:
                    # Best-effort: remove dedupe keys for acknowledged entries
                    for mid in msgs_to_ack:
                        try:
                            entries = await self._redis.xrange(
                                stream_name, mid, mid, count=1
                            )
                            if entries:
                                _id, fields = entries[0]
                                body = self._decode_body(
                                    cast(dict[bytes, bytes], fields)
                                )
                                fp = (
                                    body.get("_fingerprint")
                                    if isinstance(body, dict)
                                    else None
                                )
                                await self._remove_dedupe_if_matches(
                                    stream_name, fp, mid
                                )
                        except Exception:
                            # Ignore cleanup errors
                            pass
        return results

    async def _reclaim_with_xautoclaim(
        self, stream_name: str, local_consumer: str, prefetch: int
    ) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        assert self._redis is not None
        start_id: str | bytes = "0-0"
        while True:
            xac_ret = await self._redis.xautoclaim(
                stream_name,
                self.group_name,
                local_consumer,
                self._reclaim_min_idle_ms,
                start_id,
                count=self._reclaim_batch,
                justid=False,
            )

            # redis-py may return (next_id, messages) or (next_id, messages, deleted)
            if isinstance(xac_ret, (list, tuple)):
                ln = len(xac_ret)
                if ln == 2:
                    next_id_any, claimed_any = cast(tuple[Any, Any], xac_ret)
                elif ln >= 3:
                    next_id_any, claimed_any = cast(tuple[Any, Any, Any], xac_ret)[:2]
                else:
                    raise ValueError("Unexpected XAUTOCLAIM return format")
                next_id = cast(str | bytes, next_id_any)
                claimed_list = cast(
                    list[tuple[bytes | str, dict[bytes, bytes]]], claimed_any
                )
            else:
                raise ValueError(f"Unexpected XAUTOCLAIM return type: {type(xac_ret)}")

            if not claimed_list:
                break

            msgs_to_ack: list[str] = []
            for msg_id_raw, fields in claimed_list:
                data = self._decode_body(cast(dict[bytes, bytes], fields))
                msg_id_str = self._to_msg_id_str(msg_id_raw)
                results.append({"id": msg_id_str, "data": data})
                msgs_to_ack.append(msg_id_str)

            # Ack/del in pipeline
            pipe = self._redis.pipeline()
            for mid in msgs_to_ack:
                pipe.xack(stream_name, self.group_name, mid)
                pipe.xdel(stream_name, mid)
            try:
                await pipe.execute()
            except Exception as e:
                self._logger.warning("Failed pipeline ack/del for xautoclaim: %s", e)

            if next_id in ("0-0", b"0-0"):
                break
            start_id = next_id

        return results

    async def _reclaim_with_xpending_fallback(
        self, stream_name: str, local_consumer: str
    ) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        assert self._redis is not None
        while True:
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

            claimed_entries = await self._redis.execute_command(
                b"XCLAIM",
                stream_name,
                self.group_name,
                local_consumer,
                str(self._reclaim_min_idle_ms).encode(),
                *ids,
            )

            if not claimed_entries:
                continue

            entries_list = cast(
                list[tuple[bytes | str, dict[bytes, bytes]]], claimed_entries
            )
            msgs_to_ack: list[str] = []
            for msg_id_raw, fields_map2 in entries_list:
                msg_id_str: str = (
                    msg_id_raw.decode()
                    if isinstance(msg_id_raw, (bytes, bytearray))
                    else str(msg_id_raw)
                )
                data = self._decode_body(fields_map2)
                results.append({"id": msg_id_str, "data": data})
                msgs_to_ack.append(msg_id_str)

            # Ack/del in pipeline
            pipe = self._redis.pipeline()
            for mid in msgs_to_ack:
                pipe.xack(stream_name, self.group_name, mid)
                pipe.xdel(stream_name, mid)
            try:
                await pipe.execute()
            except Exception as _proc_err:
                self._logger.warning(
                    "Error removing claimed (fallback) messages for %s: %s",
                    stream_name,
                    _proc_err,
                )

        return results

    async def __aenter__(self) -> "RedisQueueBackend":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
        await self.close()

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
            data = self._decode_body(cast(dict[bytes, bytes], fields))
            msg_id_str = self._to_msg_id_str(msg_id)
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

    def _get_fingerprint_source(
        self, queue_name: str, message: dict[str, Any]
    ) -> bytes:
        """Build a stable, minimal bytes source for fingerprinting.

        By default this builds a compact representation containing only the
        core job-identifying fields so that incidental metadata (server ids,
        priority, ephemeral extra_data) doesn't prevent deduplication.

        Set the environment variable ULTIMA_DEDUPE_STRICT=1 to fall back to a
        strict mode that fingerprints the entire message object instead.
        """
        strict = os.getenv("ULTIMA_DEDUPE_STRICT", "0").lower() in (
            "1",
            "true",
            "yes",
        )

        # If strict requested or non-dict message, use full message serialization
        if strict or not isinstance(message, dict):
            try:
                src = orjson.dumps({"queue": queue_name, "message": message})
            except Exception:
                src = str((queue_name, message)).encode()
            return src if isinstance(src, (bytes, bytearray)) else src.encode()

        # Otherwise build a compact canonical payload consisting of the
        # identifying fields only. This reduces false negatives for similar
        # jobs that differ only in metadata.
        core: dict[str, Any] = {}
        # Normalized user id: accept either 'user_id' or 'performer_id'
        effective_user_id: Any = None
        try:
            if isinstance(message, dict):
                effective_user_id = (
                    message.get("user_id")
                    if message.get("user_id") is not None
                    else message.get("performer_id")
                )
        except Exception:
            effective_user_id = None

        # Only include the explicitly requested identifying fields
        if "site_name" in message:
            core["site_name"] = message.get("site_name")
        if effective_user_id is not None:
            core["user_id"] = effective_user_id
        if "category" in message:
            core["category"] = message.get("category")

        try:
            src = orjson.dumps({"queue": queue_name, "core": core})
        except Exception:
            src = str((queue_name, core)).encode()
        return src if isinstance(src, (bytes, bytearray)) else src.encode()

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

            # Deduplication TTL (env override allowed)
            try:
                dedupe_ttl = int(os.getenv("ULTIMA_JOB_DEDUPE_TTL_SECONDS", "86400"))
            except Exception:
                dedupe_ttl = 86400

            # Lock TTL used to protect the small window while we XADD and set the
            # dedupe key to the final msg_id. Keep this short to avoid long locks
            # if a publisher dies between steps.
            try:
                lock_ttl = int(os.getenv("ULTIMA_DEDUPE_LOCK_TTL_SECONDS", "30"))
            except Exception:
                lock_ttl = 30

            # Build fingerprint source (may be compacted by _get_fingerprint_source)
            fp_source = self._get_fingerprint_source(queue_name, message)
            fingerprint = hashlib.sha1(fp_source).hexdigest()
            dedupe_key = f"{stream_name}:dedupe:{fingerprint}"

            # Prepare the message body bytes to send to Redis/XADD. Include the
            # fingerprint so consumers can clean up the dedupe key when they
            # remove the message.
            if isinstance(message, dict):
                payload_obj = message.copy()
            else:
                payload_obj = {"body": message}
            payload_obj["_fingerprint"] = fingerprint
            try:
                message_bytes = orjson.dumps(payload_obj)
            except Exception:
                message_bytes = str(payload_obj).encode()

            # Try the atomic Lua path first (SET NX EX + XADD). If Lua isn't
            # available or errors occur, fall back to the previous SET+XADD logic.
            try:
                await self._load_publish_lua()
                if self._publish_lua_sha:
                    try:
                        res = await self._redis.evalsha(
                            self._publish_lua_sha,
                            2,
                            dedupe_key,
                            stream_name,
                            str(self.maxlen),
                            message_bytes,
                            str(dedupe_ttl),
                        )
                    except Exception as e:
                        self._logger.debug(
                            "Lua EVALSHA failed for %s: %s", stream_name, e
                        )
                        res = None

                    if res == 0 or res == b"0":
                        if not suppress:
                            self._logger.info(
                                "Duplicate message detected for %s; skipping publish.",
                                stream_name,
                            )
                        return True
                    if res:
                        if not suppress:
                            self._logger.info(
                                "Message published to %s (lua)", stream_name
                            )
                        return True
            except Exception as _lua_err:
                self._logger.debug(
                    "Lua path unavailable for %s: %s; falling back.",
                    stream_name,
                    _lua_err,
                )

            # Fallback: implement stale key logic like the Lua script
            # Check if dedupe key exists and if so, whether it points to a live message
            try:
                existing_msg_id = await self._redis.get(dedupe_key)
                if existing_msg_id:
                    existing_msg_id_str = (
                        existing_msg_id.decode()
                        if isinstance(existing_msg_id, bytes)
                        else str(existing_msg_id)
                    )

                    # If it's still "PENDING" or "LOCK", it's a stale lock from a failed publish
                    if existing_msg_id_str in ("PENDING", "LOCK"):
                        await self._redis.delete(dedupe_key)
                        if not suppress:
                            self._logger.debug(
                                "Removed stale lock (%s) for %s",
                                existing_msg_id_str,
                                stream_name,
                            )
                    else:
                        # Check if the referenced message still exists in the stream
                        try:
                            entries = await self._redis.xrange(
                                stream_name,
                                existing_msg_id_str,
                                existing_msg_id_str,
                                count=1,
                            )
                            if entries and len(entries) > 0:
                                # Message still exists in stream, this is a true duplicate
                                if not suppress:
                                    self._logger.info(
                                        "Duplicate message detected for %s; skipping publish.",
                                        stream_name,
                                    )
                                return True
                            else:
                                # Stale key: message was deleted, remove the key and continue
                                await self._redis.delete(dedupe_key)
                                if not suppress:
                                    self._logger.debug(
                                        "Removed stale dedupe key for %s",
                                        stream_name,
                                    )
                        except Exception:
                            # If we can't check, assume stale and delete key
                            await self._redis.delete(dedupe_key)
            except Exception:
                # If we can't get/check the key, continue with publish
                pass

            # Try to reserve the dedupe key atomically
            try:
                lock_ok = await self._redis.set(
                    dedupe_key, "LOCK", nx=True, ex=lock_ttl
                )
            except Exception:
                lock_ok = True

            if not lock_ok:
                if not suppress:
                    self._logger.info(
                        "Duplicate message detected for %s; skipping publish (race condition).",
                        stream_name,
                    )
                return True

            # Perform XADD and then set the dedupe_key to the real msg id
            try:
                msg_id = await self._redis.xadd(
                    stream_name,
                    {b"body": message_bytes},
                    maxlen=self.maxlen,
                    approximate=True,
                )
                # Ensure dedupe_key points to the msg id
                try:
                    await self._redis.set(dedupe_key, msg_id, ex=dedupe_ttl)
                except Exception:
                    # If setting the key fails, it's not fatal; the lock will
                    # expire and duplicates may occur rarely
                    pass
            except Exception:
                # On unexpected redis errors, fail-open and return False
                if print_error:
                    self._logger.exception(
                        "Error publishing message to %s", stream_name
                    )
                return False

            if not suppress:
                self._logger.info("Message published to %s", stream_name)
            return True

        except Exception as e:
            if print_error:
                self._logger.exception(
                    "Error publishing message to %s: %s", queue_name, e
                )
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
            # Force backfill of existing group to deliver historical entries
            try:
                await self._redis.xgroup_setid(stream_name, self.group_name, id="0")
                self._logger.debug(
                    "Backfilled group '%s' to 0 for %s",
                    self.group_name,
                    stream_name,
                )
            except Exception as _e:
                self._logger.warning(
                    "Failed to set group id to 0 for %s/%s: %s",
                    stream_name,
                    self.group_name,
                    _e,
                )

        # Do not drain full history on startup — prefer fresh/new messages.
        # Only reclaim stale pending entries via XAUTOCLAIM so new messages are not blocked.
        read_count = max(1, prefetch_count or 1)

        # Reclaim stale pending messages from other consumers (XAUTOCLAIM)
        try:
            reclaimed = await self._reclaim_with_xautoclaim(
                stream_name, local_consumer, read_count
            )
        except Exception as e:
            self._logger.info(
                "XAUTOCLAIM not available or failed for %s: %s", stream_name, e
            )
            reclaimed = []

        if reclaimed:
            for item in reclaimed:
                try:
                    mid_any = item.get("id")
                    if mid_any is None:
                        continue
                    mid = mid_any if isinstance(mid_any, str) else str(mid_any)
                    pdata = cast(dict[str, Any], item.get("data") or {})

                    # Respect an optional target_consumer header: if the message
                    # was intended for another consumer, re-publish it so the
                    # intended consumer can pick it up later (or skip processing).
                    headers = pdata.get("_headers") if isinstance(pdata, dict) else None
                    target = None
                    if isinstance(headers, dict):
                        target = headers.get("target_consumer")

                    if target and target != local_consumer:
                        # Re-publish so the intended consumer may receive it; do not ACK here.
                        await self._redis.xadd(
                            stream_name, {b"body": orjson.dumps(pdata)}
                        )
                        # Remove the reclaimed entry to avoid duplicates
                        try:
                            await self._redis.xack(stream_name, self.group_name, mid)
                            await self._redis.xdel(stream_name, mid)
                        except Exception:
                            pass
                        continue

                    message = RedisMessage(self, stream_name, pdata, mid)
                    await callback(message, *args)
                    # Note: reclaimed entries were previously acked; leave to handler to ack
                    fp = pdata.get("_fingerprint")
                    await self._remove_dedupe_if_matches(stream_name, fp, mid)
                except Exception as e:
                    self._logger.exception(
                        "Error processing reclaimed message %s: %s", item.get("id"), e
                    )

        # Main consumer loop: read new messages and process them inline
        read_count = max(1, prefetch_count or 1)
        reclaim_counter = 0  # Periodic reclaim check
        while True:
            try:
                # Every 10 iterations, check for additional stale pending messages
                # and also check for any messages that might have been skipped
                if reclaim_counter % 10 == 0:
                    # First, try XAUTOCLAIM for stale pending entries
                    try:
                        additional_reclaimed = await self._reclaim_with_xautoclaim(
                            stream_name, local_consumer, read_count
                        )
                        if additional_reclaimed:
                            for item in additional_reclaimed:
                                try:
                                    mid_any = item.get("id")
                                    if mid_any is None:
                                        continue
                                    mid = (
                                        mid_any
                                        if isinstance(mid_any, str)
                                        else str(mid_any)
                                    )
                                    pdata = cast(dict[str, Any], item.get("data") or {})

                                    # Check target consumer
                                    headers = (
                                        pdata.get("_headers")
                                        if isinstance(pdata, dict)
                                        else None
                                    )
                                    target = None
                                    if isinstance(headers, dict):
                                        target = headers.get("target_consumer")

                                    if target and target != local_consumer:
                                        # Re-publish for intended consumer
                                        await self._redis.xadd(
                                            stream_name, {b"body": orjson.dumps(pdata)}
                                        )
                                        try:
                                            await self._redis.xack(
                                                stream_name, self.group_name, mid
                                            )
                                            await self._redis.xdel(stream_name, mid)
                                        except Exception:
                                            pass
                                        continue

                                    message = RedisMessage(
                                        self, stream_name, pdata, mid
                                    )
                                    await callback(message, *args)
                                    fp = pdata.get("_fingerprint")
                                    await self._remove_dedupe_if_matches(
                                        stream_name, fp, mid
                                    )
                                except Exception as e:
                                    self._logger.exception(
                                        "Error processing additional reclaimed message %s: %s",
                                        item.get("id"),
                                        e,
                                    )
                    except Exception as e:
                        self._logger.debug("Additional reclaim check failed: %s", e)

                    # Second, check for any messages that might have been missed by reading from "0"
                    try:
                        missed_resp = await self._redis.xreadgroup(
                            self.group_name,
                            local_consumer,
                            streams={stream_name: "0"},
                            count=read_count,
                        )
                        if missed_resp:
                            for _stream, messages in missed_resp:
                                for msg_id, fields in messages:
                                    fields_map = cast(dict[bytes, bytes], fields)
                                    body_bytes = fields_map.get(b"body")
                                    if body_bytes is not None:
                                        try:
                                            decoded_payload: Any = orjson.loads(
                                                body_bytes
                                            )
                                        except Exception:
                                            decoded_payload = None
                                        if isinstance(decoded_payload, dict):
                                            data = cast(dict[str, Any], decoded_payload)
                                        else:
                                            data = {"body": decoded_payload}
                                    else:
                                        data = {}

                                    msg_id_str = (
                                        msg_id.decode()
                                        if isinstance(msg_id, (bytes, bytearray))
                                        else str(msg_id)
                                    )

                                    try:
                                        # Check target consumer
                                        headers = (
                                            data.get("_headers")
                                            if isinstance(data, dict)
                                            else None
                                        )
                                        target = None
                                        if isinstance(headers, dict):
                                            target = headers.get("target_consumer")

                                        if target and target != local_consumer:
                                            # Re-publish for intended consumer
                                            await self._redis.xadd(
                                                stream_name,
                                                {b"body": orjson.dumps(data)},
                                            )
                                            try:
                                                await self._redis.xack(
                                                    stream_name,
                                                    self.group_name,
                                                    msg_id_str,
                                                )
                                                await self._redis.xdel(
                                                    stream_name, msg_id_str
                                                )
                                            except Exception:
                                                pass
                                            continue

                                        message = RedisMessage(
                                            self, stream_name, data, msg_id_str
                                        )
                                        await callback(message, *args)
                                        await self._redis.xack(
                                            stream_name, self.group_name, msg_id_str
                                        )
                                        await self._redis.xdel(stream_name, msg_id_str)
                                        fp = (
                                            data.get("_fingerprint")
                                            if isinstance(data, dict)
                                            else None
                                        )
                                        await self._remove_dedupe_if_matches(
                                            stream_name, fp, msg_id_str
                                        )
                                    except Exception as e:
                                        self._logger.exception(
                                            "Error processing missed message %s: %s",
                                            msg_id_str,
                                            e,
                                        )
                    except Exception as e:
                        self._logger.debug("Missed message check failed: %s", e)

                reclaim_counter += 1

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
                            # If a message carries a target_consumer header, ensure only
                            # the intended consumer processes it. If this consumer is
                            # not the target, re-publish the message and remove the
                            # current pending entry to allow the intended consumer to
                            # receive it later.
                            headers = (
                                data.get("_headers") if isinstance(data, dict) else None
                            )
                            target = None
                            if isinstance(headers, dict):
                                target = headers.get("target_consumer")

                            if target and target != local_consumer:
                                # Re-publish the payload so it becomes available for the
                                # intended consumer. Keep headers intact.
                                try:
                                    await self._redis.xadd(
                                        stream_name, {b"body": orjson.dumps(data)}
                                    )
                                except Exception:
                                    # best-effort re-publish; continue to ack/del original
                                    pass

                                # Remove the current reclaimed/pending entry so other
                                # consumers won't keep seeing it.
                                try:
                                    await self._redis.xack(
                                        stream_name, self.group_name, msg_id_str
                                    )
                                    await self._redis.xdel(stream_name, msg_id_str)
                                except Exception:
                                    pass
                                # Skip processing on this consumer
                                continue

                            message = RedisMessage(self, stream_name, data, msg_id_str)
                            await callback(message, *args)
                            # Handler should call message.ack() to acknowledge; do not auto-ack here.
                        except Exception as e:
                            self._logger.exception(
                                "Error processing message %s: %s", msg_id_str, e
                            )
                            # Leave in PEL for retry/inspection

            except Exception as e:
                self._logger.exception(
                    "Error in consumer loop for %s: %s", stream_name, e
                )
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
        if backend != QueueBackend.REDIS:
            raise ValueError(f"Unsupported backend: {backend}")

        # Allow overriding group name via env to match existing deployments
        env_group = os.getenv("ULTIMA_REDIS_GROUP")
        effective_group = env_group or group_name
        # Instantiate Redis-only backend (project targets Redis exclusively)
        self._backend: RedisQueueBackend = RedisQueueBackend(
            redis_url, stream_prefix, effective_group
        )

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
        # `process_history_and_reclaim`.
        backend_impl = self._backend
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
                headers_obj = payload.get("_headers")
                headers_dict: dict[str, str] = (
                    headers_obj.copy() if isinstance(headers_obj, dict) else {}
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
        new_args = ()
        if "worker-" in self.consumer_name:
            worker_id = int(self.consumer_name.split("-")[1])
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
    "QueueBackend",
    "create_notification",
    "create_message",
    "UltimaConsumer",
]
