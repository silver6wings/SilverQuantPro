"""
消费方订阅 NATS K 线。

- subscribe_kline(code_list, callback)：code_list 非空时过滤；[] 表示接收全部。
- interval > 0：按周期批处理；interval == 0：每条消息尽快 dispatch（仍串行 callback）。
- 批内为 KlineDispatchBatch.bars 列表；(code, timestamp) 重复则覆盖。
- callback 执行期间新 bar 写入缓冲，结束后自动 drain。
"""
import asyncio
import json
import logging
import threading
from collections.abc import Callable
from typing import Any

import nats
from nats.aio.client import Client as NATS
from nats.aio.msg import Msg

from credentials import NATS_AM_KLINE_SUBJECT, NATS_CONSUMER_URL
from data.kline.kline_entity import KlineBarDispatch, KlineDispatchBatch, is_kline_bar
from framework.time_util import now_ms

logger = logging.getLogger(__name__)

KlineCallback = Callable[[KlineDispatchBatch], None]

_SUB_OK = 0
_SUB_FAIL = -1

BarKey = tuple[str, int]


class AmazingKlineNatsConsumer:
    def __init__(
        self,
        nats_url: str = NATS_CONSUMER_URL,
        nats_subject: str = NATS_AM_KLINE_SUBJECT,
        interval: float = 1.0,
    ) -> None:
        self.nats_url = nats_url
        self.nats_subject = nats_subject
        self.interval = interval
        self.bar_count = 0

        self._lock = threading.Lock()
        self._callback: KlineCallback | None = None
        self._code_list: frozenset[str] = frozenset()
        self._pending: dict[BarKey, KlineBarDispatch] = {}
        self._callback_running = False

        self._runner_thread: threading.Thread | None = None
        self._nc: NATS | None = None
        self._dispatch_wakeup: asyncio.Event | None = None

    def subscribe_kline(self, code_list: list[str], callback: KlineCallback) -> int:
        with self._lock:
            if self._callback is not None:
                return _SUB_FAIL
            self._callback = callback
            self._code_list = frozenset(code_list)
            self._pending = {}
            self.bar_count = 0
            need_start = self._runner_thread is None or not self._runner_thread.is_alive()

        if need_start:
            self._runner_thread = threading.Thread(
                target=self._run_loop,
                name="nats-kline-consumer",
                daemon=True,
            )
            self._runner_thread.start()

        logger.info("kline subscription started, codes=%d, interval=%s", len(code_list), self.interval)
        return _SUB_OK

    def unsubscribe_kline(self) -> int:
        with self._lock:
            if self._callback is None:
                return _SUB_FAIL
            self._callback = None
            self._code_list = frozenset()
            self._pending = {}

        logger.info("kline subscription stopped, bar_count=%d", self.bar_count)
        return _SUB_OK

    def update_code_list(self, code_list: list[str]) -> None:
        with self._lock:
            self._code_list = frozenset(code_list)

    def wait(self) -> None:
        thread = self._runner_thread
        if thread is not None and thread.is_alive():
            thread.join()

    def _run_loop(self) -> None:
        asyncio.run(self._run_async())

    async def _run_async(self) -> None:
        self._dispatch_wakeup = asyncio.Event()
        self._nc = await nats.connect(self.nats_url)
        await self._nc.subscribe(self.nats_subject, cb=self.on_message)

        logger.info("subscribed to %s on %s", self.nats_subject, self.nats_url)

        try:
            if self.interval <= 0:
                await self._immediate_dispatch_loop()
            else:
                await self._interval_dispatch_loop()
        finally:
            await self._nc.drain()
            self._nc = None
            self._dispatch_wakeup = None

    async def on_message(self, msg: Msg) -> None:
        try:
            data = json.loads(msg.data)
        except json.JSONDecodeError:
            return

        code, bar = self._extract_code_and_bar(data)
        if not code or bar is None:
            return

        receive_ms = now_ms()
        bar_key = (code, int(bar["timestamp"]))

        with self._lock:
            if self._callback is None:
                return
            if self._code_list and code not in self._code_list:
                return
            self._pending[bar_key] = KlineBarDispatch(code=code, bar=bar, received_ms=receive_ms)
            self.bar_count += 1

        if self.interval <= 0 and self._dispatch_wakeup is not None:
            self._dispatch_wakeup.set()

    async def _interval_dispatch_loop(self) -> None:
        while True:
            await asyncio.sleep(self.interval)
            with self._lock:
                subscribed = self._callback is not None
            if subscribed:
                await self._dispatch_pending()

    async def _immediate_dispatch_loop(self) -> None:
        assert self._dispatch_wakeup is not None
        while True:
            await self._dispatch_wakeup.wait()
            self._dispatch_wakeup.clear()
            with self._lock:
                subscribed = self._callback is not None
            if subscribed:
                await self._dispatch_pending()

    async def _dispatch_pending(self) -> None:
        while True:
            if not await self._dispatch_once():
                return

    async def _dispatch_once(self) -> bool:
        if self._callback_running:
            return False

        with self._lock:
            if self._callback is None or not self._pending:
                return False
            callback = self._callback
            if self.interval <= 0:
                key = next(iter(self._pending))
                batch = KlineDispatchBatch(bars=[self._pending.pop(key)])
            else:
                pending, self._pending = self._pending, {}
                batch = KlineDispatchBatch(bars=list(pending.values()))
        self._callback_running = True
        try:
            await asyncio.to_thread(callback, batch)
        finally:
            self._callback_running = False

        with self._lock:
            has_more = bool(self._pending) and self._callback is not None

        if has_more:
            if self.interval <= 0 and self._dispatch_wakeup is not None:
                self._dispatch_wakeup.set()
            return True
        return False

    @staticmethod
    def _extract_code_and_bar(data: dict[str, Any]) -> tuple[str | None, dict[str, Any] | None]:
        if len(data) != 1:
            return None, None
        code, bar = next(iter(data.items()))
        if is_kline_bar(bar):
            return str(code), bar
        return None, None
