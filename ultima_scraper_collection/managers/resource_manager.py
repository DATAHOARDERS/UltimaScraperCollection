import asyncio
import os
from typing import Any, Optional

import psutil


class ResourceManager:
    def __init__(
        self,
        max_concurrent_connections: int = 100,
        min_concurrent_connections: int = 10,
        min_buffer_size_mb: int = 4,
        max_buffer_size_mb: int = 256,
        ram_scale_min_pct: float = 20.0,  # Below this RAM usage = max concurrency
        ram_scale_max_pct: float = 90.0,  # Above this RAM usage = min concurrency
        monitor_interval: float = 2.0,  # seconds between checks
    ):
        self.max_conns = max_concurrent_connections
        self.min_conns = min_concurrent_connections

        self.min_buffer_mb = min_buffer_size_mb
        self.max_buffer_mb = max_buffer_size_mb

        self.ram_min_pct = ram_scale_min_pct
        self.ram_max_pct = ram_scale_max_pct

        self.monitor_interval = monitor_interval

        self._current_conns = self.max_conns
        self._current_buffer_mb = self.max_buffer_mb

        self._running = False
        self._task: Optional[asyncio.Task[Any]] = None

    def _scale_value(self, value_pct, min_value, max_value):
        """Linearly scale a value based on RAM usage percent."""
        if value_pct <= self.ram_min_pct:
            return max_value
        elif value_pct >= self.ram_max_pct:
            return min_value
        else:
            scale = (max_value - min_value) * (
                1
                - (
                    (value_pct - self.ram_min_pct)
                    / (self.ram_max_pct - self.ram_min_pct)
                )
            )
            return int(min_value + scale)

    async def _monitor_loop(self):
        while self._running:
            try:
                ram_used_pct = psutil.virtual_memory().percent

                new_conns = self._scale_value(
                    ram_used_pct, self.min_conns, self.max_conns
                )
                new_buffer = self._scale_value(
                    ram_used_pct, self.min_buffer_mb, self.max_buffer_mb
                )

                self._current_conns = new_conns
                self._current_buffer_mb = new_buffer

                # Optional debug print
                # print(f"[ResourceManager] RAM: {ram_used_pct:.1f}%, Conns: {new_conns}, Buffer: {new_buffer}MB")

            except Exception as e:
                print(f"[ResourceManager] Monitor error: {e}")

            await asyncio.sleep(self.monitor_interval)

    def start(self):
        if not self._running:
            self._running = True
            self._task = asyncio.create_task(self._monitor_loop())

    def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()

    def get_current_concurrency(self) -> int:
        return self._current_conns

    def get_current_buffer_size(self) -> int:
        return self._current_buffer_mb * 1024 * 1024


async def resource_test():
    res_mgr = ResourceManager()
    res_mgr.start()

    try:
        while True:
            print(
                f"Allowed concurrency: {res_mgr.get_current_concurrency()}, "
                f"Buffer size: {res_mgr.get_current_buffer_size() / (1024*1024):.1f} MB"
            )
            await asyncio.sleep(5)
    except KeyboardInterrupt:
        pass
    finally:
        res_mgr.stop()
