from typing import Any, Callable

from celery import Celery, Task


class CustomTask(Task):
    abstract = True  # Prevent direct instantiation

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        print(f"Task {task_id} failed with exception: {exc}. Retrying...")
        self.retry(exc=exc, args=args)  # Retry on failure


class UltimaQueue:
    def __init__(self, name: str = "proj") -> None:
        self.celery = Celery(
            name,
            broker="amqp://guest:guest@localhost:5672/",
        )
        self.celery.conf.broker_connection_retry_on_startup = True
