import asyncio
import json
import os
import queue
import threading

from typing import Any

import nats
from nats.aio.client import Client as NATS


NATS_URL = os.getenv("NATS_URL", "nats://127.0.0.1:4222")
SUBJECT = os.getenv("NATS_SUBJECT", "market.tick.demo")


class NatsProducer:
    def __init__(self, nats_url: str = NATS_URL, subject: str = SUBJECT) -> None:
        self.nats_url = nats_url
        self.subject = subject
        self.nc: NATS | None = None

    async def connect(self) -> None:
        if self.nc is None or self.nc.is_closed:
            self.nc = await nats.connect(self.nats_url)

    async def push(self, data: dict[str, Any], flush: bool = False) -> None:
        await self.connect()
        assert self.nc is not None

        payload = json.dumps(data, ensure_ascii=False).encode("utf-8")
        await self.nc.publish(self.subject, payload)
        if flush:
            await self.flush()

    async def flush(self) -> None:
        await self.connect()
        assert self.nc is not None

        await self.nc.flush()

    async def close(self) -> None:
        if self.nc is not None and not self.nc.is_closed:
            await self.nc.drain()


class NatsThreadedProducer:
    def __init__(
        self,
        nats_url: str = NATS_URL,
        subject: str = SUBJECT,
        batch_size: int = 500,
        flush_interval: float = 0.05,
        max_queue_size: int = 100_000,
    ) -> None:
        self.nats_url = nats_url
        self.subject = subject
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.queue: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=max_queue_size)
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None

    def start(self) -> None:
        if self.thread is not None and self.thread.is_alive():
            return

        self.stop_event.clear()
        self.thread = threading.Thread(target=self._run, name="nats-producer", daemon=True)
        self.thread.start()

    def push(self, data: dict[str, Any], block: bool = False, timeout: float = 0.001) -> bool:
        try:
            self.queue.put(data, block=block, timeout=timeout)
            return True
        except queue.Full:
            return False

    def close(self, timeout: float = 5.0) -> None:
        self.stop_event.set()
        if self.thread is not None:
            self.thread.join(timeout=timeout)

    def _run(self) -> None:
        asyncio.run(self._worker())

    async def _worker(self) -> None:
        producer = NatsProducer(self.nats_url, self.subject)
        try:
            while not self.stop_event.is_set() or not self.queue.empty():
                sent = 0

                while sent < self.batch_size:
                    try:
                        data = self.queue.get_nowait()
                    except queue.Empty:
                        break

                    await producer.push(data, flush=False)
                    self.queue.task_done()
                    sent += 1

                if sent:
                    await producer.flush()
                else:
                    await asyncio.sleep(self.flush_interval)
        finally:
            await producer.close()
