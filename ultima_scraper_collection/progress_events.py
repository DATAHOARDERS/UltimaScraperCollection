import os
from typing import Any

import orjson
from redis.asyncio import Redis


class ProgressEvents:
    def __init__(
        self, redis_url: str | None = None, stream: str = "jobs:events"
    ) -> None:
        self.redis_url = redis_url or os.getenv("REDIS_URL", "redis://localhost:6379/0")
        self.stream = stream
        self._redis: Redis | None = None

    async def connect(self) -> Redis:
        if self._redis is None:
            self._redis = Redis.from_url(self.redis_url, decode_responses=False)
        return self._redis

    async def emit(self, event: dict[str, Any]) -> None:
        r = await self.connect()
        await r.xadd(
            self.stream, {b"event": orjson.dumps(event)}, maxlen=5000, approximate=True
        )

    async def close(self) -> None:
        if self._redis is not None:
            await self._redis.close()
            self._redis = None
