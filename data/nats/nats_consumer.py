import asyncio
import json
import threading
from collections.abc import Callable
from typing import Any

import nats
from nats.aio.client import Client as NATS
from nats.aio.msg import Msg

from credentials import NATS_AM_SUBJECT, NATS_CONSUMER_URL
from data.base.base_consumer import BaseConsumer, MessageHandler, Payload

MessageCallback = Callable[[dict[str, Any] | str], None]


class NatsConsumer(BaseConsumer):
    """同步 NATS 消费者，内部通过后台线程运行 asyncio 事件循环。"""

    def __init__(
        self,
        nats_url: str = NATS_CONSUMER_URL,
        nats_subject: str = NATS_AM_SUBJECT,
    ) -> None:
        self.nats_url = nats_url
        self.nats_subject = nats_subject
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._ready = threading.Event()
        self._nc: NATS | None = None
        self._connected = False

    def connect(self) -> None:
        self._run(self._connect_async())

    def is_connected(self) -> bool:
        return self._connected

    def listen(self, destination: str, handler: MessageHandler) -> None:
        self.connect()
        loop = self._ensure_loop()
        future = asyncio.run_coroutine_threadsafe(
            self._listen_async(destination, handler),
            loop,
        )
        try:
            future.result()
        except KeyboardInterrupt:
            future.cancel()
            raise
        finally:
            self.close()

    def run(self, callback: MessageCallback) -> None:
        def adapted_handler(message: Payload) -> None:
            callback(message)

        self.listen(self.nats_subject, adapted_handler)

    def close(self) -> None:
        if self._loop is not None and self._connected:
            try:
                asyncio.run_coroutine_threadsafe(self._close_async(), self._loop).result(timeout=5.0)
            except Exception:
                pass

        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._loop.stop)

        thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=5.0)

        self._loop = None
        self._thread = None
        self._ready.clear()
        self._nc = None
        self._connected = False

    def _ensure_loop(self) -> asyncio.AbstractEventLoop:
        if self._loop is not None and self._thread is not None and self._thread.is_alive():
            return self._loop

        self._ready.clear()
        self._thread = threading.Thread(target=self._run_loop, name="nats-consumer", daemon=True)
        self._thread.start()
        self._ready.wait()
        assert self._loop is not None
        return self._loop

    def _run_loop(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        self._ready.set()
        loop.run_forever()

    def _run(self, coro: Any) -> Any:
        loop = self._ensure_loop()
        return asyncio.run_coroutine_threadsafe(coro, loop).result()

    async def _connect_async(self) -> None:
        if self._nc is None or self._nc.is_closed:
            self._nc = await nats.connect(self.nats_url)
        self._connected = True

    async def _listen_async(self, destination: str, handler: MessageHandler) -> None:
        await self._connect_async()
        assert self._nc is not None

        async def handle_message(msg: Msg) -> None:
            payload = self._decode_payload(msg)
            handler(payload)

        await self._nc.subscribe(destination, cb=handle_message)
        print(f"subscribed to {destination} on {self.nats_url}")

        try:
            while True:
                await asyncio.sleep(3600)
        finally:
            await self._close_async()

    async def _close_async(self) -> None:
        if self._nc is not None and not self._nc.is_closed:
            await self._nc.drain()
        self._nc = None
        self._connected = False

    @staticmethod
    def _decode_payload(msg: Msg) -> dict[str, Any] | str:
        text = msg.data.decode("utf-8")
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return text
