import asyncio
import json
from collections.abc import Awaitable, Callable
from typing import Any

import nats
from nats.aio.client import Client as NATS
from nats.aio.msg import Msg

from credentials import NATS_CONSUMER_SUBJECT, NATS_CONSUMER_URL

MessageCallback = Callable[[dict[str, Any] | str], None | Awaitable[None]]


class NatsConsumer:
    def __init__(
        self,
        nats_url: str = NATS_CONSUMER_URL,
        nats_subject: str = NATS_CONSUMER_SUBJECT,
    ) -> None:
        self.nats_url = nats_url
        self.nats_subject = nats_subject
        self.nc: NATS | None = None

    async def connect(self) -> None:
        if self.nc is None or self.nc.is_closed:
            self.nc = await nats.connect(self.nats_url)

    async def run(self, callback: MessageCallback) -> None:
        await self.connect()
        assert self.nc is not None

        async def handle_message(msg: Msg) -> None:
            payload = self._decode_payload(msg)
            result = callback(payload)
            if result is not None:
                await result

        await self.nc.subscribe(self.nats_subject, cb=handle_message)
        print(f"subscribed to {self.nats_subject} on {self.nats_url}")

        try:
            while True:
                await asyncio.sleep(1)
        finally:
            await self.close()

    async def close(self) -> None:
        if self.nc is not None and not self.nc.is_closed:
            await self.nc.drain()

    @staticmethod
    def _decode_payload(msg: Msg) -> dict[str, Any] | str:
        text = msg.data.decode("utf-8")
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return text
