import asyncio
import json
import queue
import threading

from typing import Any

import nats
from nats.aio.client import Client as NATS

from credentials import NATS_AM_SUBJECT, NATS_PRODUCER_URL
from data.base.base_producer import BaseProducer, Payload


class _AsyncNatsProducer:
    """NATS 异步发送实现，仅供本模块内部使用。"""

    def __init__(self, nats_url: str, nats_subject: str) -> None:
        self.nats_url = nats_url
        self.nats_subject = nats_subject
        self.nc: NATS | None = None

    async def connect(self) -> None:
        if self.nc is None or self.nc.is_closed:
            self.nc = await nats.connect(self.nats_url)

    def is_connected(self) -> bool:
        return self.nc is not None and not self.nc.is_closed

    async def push(self, destination: str, payload: Payload, flush: bool = False) -> None:
        await self.connect()
        assert self.nc is not None

        encoded_payload = self._encode_body(payload)
        await self.nc.publish(destination, encoded_payload)
        if flush:
            await self.flush()

    async def flush(self) -> None:
        await self.connect()
        assert self.nc is not None

        await self.nc.flush()

    async def close(self) -> None:
        if self.nc is not None and not self.nc.is_closed:
            await self.nc.drain()
            self.nc = None

    @staticmethod
    def _encode_body(body: Payload) -> bytes:
        if isinstance(body, bytes):
            return body
        if isinstance(body, str):
            return body.encode("utf-8")
        return json.dumps(body, ensure_ascii=False).encode("utf-8")


class NatsProducer(BaseProducer):
    """同步 NATS 生产者，内部通过后台线程运行 asyncio 事件循环。"""

    def __init__(
        self,
        nats_url: str = NATS_PRODUCER_URL,
        nats_subject: str = NATS_AM_SUBJECT,
        batch_size: int = 1,
        flush_interval: float = 0.05,
        max_queue_size: int = 10_000,
    ) -> None:
        self._worker = NatsThreadedProducer(
            nats_url=nats_url,
            nats_subject=nats_subject,
            batch_size=batch_size,
            flush_interval=flush_interval,
            max_queue_size=max_queue_size,
        )

    @property
    def nats_url(self) -> str:
        return self._worker.nats_url

    @property
    def nats_subject(self) -> str:
        return self._worker.nats_subject

    def connect(self) -> None:
        self._worker.start()

    def is_connected(self) -> bool:
        thread = self._worker.thread
        return thread is not None and thread.is_alive()

    def push(self, destination: str, payload: Payload) -> None:
        if not self.is_connected():
            self.connect()
        if not self._worker.push(payload, destination=destination, block=True, timeout=5.0):
            raise RuntimeError("NATS producer queue is full")

    def close(self) -> None:
        self._worker.close()


class NatsThreadedProducer:
    """高吞吐批量 NATS 生产者，内部维护独立线程与 asyncio 事件循环。"""

    def __init__(
        self,
        nats_url: str = NATS_PRODUCER_URL,
        nats_subject: str = NATS_AM_SUBJECT,
        batch_size: int = 500,
        flush_interval: float = 0.05,
        max_queue_size: int = 100_000,
    ) -> None:
        self.nats_url = nats_url
        self.nats_subject = nats_subject
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.queue: queue.Queue[tuple[str, Payload]] = queue.Queue(maxsize=max_queue_size)
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None

    def start(self) -> None:
        if self.thread is not None and self.thread.is_alive():
            return

        self.stop_event.clear()
        self.thread = threading.Thread(target=self._run, name="nats-producer", daemon=True)
        self.thread.start()

    def push(
        self,
        data: Payload,
        destination: str | None = None,
        block: bool = False,
        timeout: float = 0.001,
    ) -> bool:
        try:
            self.queue.put((destination or self.nats_subject, data), block=block, timeout=timeout)
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
        producer = _AsyncNatsProducer(self.nats_url, self.nats_subject)
        try:
            while not self.stop_event.is_set() or not self.queue.empty():
                sent = 0

                while sent < self.batch_size:
                    try:
                        destination, data = self.queue.get_nowait()
                    except queue.Empty:
                        break

                    await producer.push(destination, data, flush=False)
                    self.queue.task_done()
                    sent += 1

                if sent:
                    await producer.flush()
                else:
                    await asyncio.sleep(self.flush_interval)
        finally:
            await producer.close()
