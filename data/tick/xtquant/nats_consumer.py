"""
消费方订阅 NATS 行情，API 对齐 QMT xtdata 的 subscribe_whole_quote / unsubscribe_quote。
quote 字段为统一 tick 格式（见 data.tick.tick_quote，adapter 见 data.tick.amazing / xtquant）。

消费逻辑说明
------------

1. 单订阅
   - 同一时刻只允许一个 subscribe_whole_quote。
   - 再次 subscribe 前须先 unsubscribe_quote。

2. 数据接收与缓存
   - NATS 每条消息可为多 code 的 {code: quote, ...}（producer 按包发送）。
   - 收到后按 code 写入 _quotes；同 code 新 tick 覆盖旧值。
   - code_list 非空时只接收列表内的 code；传空列表 [] 表示接收全部 code。

3. callback 触发与去重
   - 每个 interval 周期结束时尝试 dispatch 一次（默认 1 秒一次）。
   - dispatch 时将 quotes 整体交给 callback，并立刻 _quotes = {} 换新容器。
   - callback 执行期间新 tick 写入新 dict，互不干扰。

4. 空 dict 不回调
   - 若某个 interval 内没有收到匹配的 tick，跳过 callback，不会传入空 dict。

5. callback 串行
   - 上一轮 callback 未完成时跳过本轮 dispatch。

用法示例
--------
    consumer = XtquantNatsConsumer(interval=1.0)

    def callback_sub_whole(quotes: dict) -> None:
        ...

    if consumer.subscribe_whole_quote(["000001.SZ"], callback_sub_whole) != 0:
        raise RuntimeError("subscribe failed")
    try:
        consumer.wait()
    finally:
        consumer.unsubscribe_quote()
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

from credentials import NATS_CONSUMER_URL, NATS_XT_SUBJECT
from data.tick.tick_quote import is_tick_quote

logger = logging.getLogger(__name__)

_DEFAULT_DISPATCH_INTERVAL = 1.0

Quotes = dict[str, dict[str, Any]]
QuoteCallback = Callable[[Quotes], None]

_SUB_OK = 0
_SUB_FAIL = -1


class XtquantNatsConsumer:
    def __init__(
        self,
        nats_url: str = NATS_CONSUMER_URL,
        nats_subject: str = NATS_XT_SUBJECT,
        interval: float = _DEFAULT_DISPATCH_INTERVAL,
    ) -> None:
        self.nats_url = nats_url
        self.nats_subject = nats_subject
        self.interval = interval
        self.quote_count = 0

        self._lock = threading.Lock()
        self._callback: QuoteCallback | None = None
        self._code_list: frozenset[str] = frozenset()
        self._quotes: Quotes = {}
        self._callback_running = False

        self._runner_thread: threading.Thread | None = None
        self._nc: NATS | None = None

    def subscribe_whole_quote(self, code_list: list[str], callback: QuoteCallback) -> int:
        with self._lock:
            if self._callback is not None:
                return _SUB_FAIL
            self._callback = callback
            self._code_list = frozenset(code_list)
            self._quotes = {}
            self.quote_count = 0
            need_start = self._runner_thread is None or not self._runner_thread.is_alive()

        if need_start:
            self._runner_thread = threading.Thread(
                target=self._run_loop,
                name="xtquant-nats-consumer",
                daemon=True,
            )
            self._runner_thread.start()

        logger.info("quote subscription started, codes=%d, interval=%s", len(code_list), self.interval)
        return _SUB_OK

    def unsubscribe_quote(self) -> int:
        with self._lock:
            if self._callback is None:
                return _SUB_FAIL
            self._callback = None
            self._code_list = frozenset()
            self._quotes = {}

        logger.info("quote subscription stopped, quote_count=%d", self.quote_count)
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
        self._nc = await nats.connect(self.nats_url)
        await self._nc.subscribe(self.nats_subject, cb=self.on_message)

        logger.info("subscribed to %s on %s", self.nats_subject, self.nats_url)

        try:
            await self._dispatch_loop()
        finally:
            await self._nc.drain()
            self._nc = None

    async def on_message(self, msg: Msg) -> None:
        try:
            data = json.loads(msg.data)
        except json.JSONDecodeError:
            return

        parsed = self._parse_quotes(data)
        if not parsed:
            return

        with self._lock:
            if self._callback is None:
                return
            if self._code_list:
                for code, quote in parsed.items():
                    if code in self._code_list:
                        self._quotes[code] = quote
                        self.quote_count += 1
            else:
                self._quotes.update(parsed)
                self.quote_count += len(parsed)

    async def _dispatch_loop(self) -> None:
        while True:
            await asyncio.sleep(self.interval)

            with self._lock:
                subscribed = self._callback is not None

            if subscribed:
                await self._dispatch()

    async def _dispatch(self) -> None:
        if self._callback_running:
            return

        with self._lock:
            if self._callback is None or not self._quotes:
                return
            quotes, self._quotes = self._quotes, {}
            callback = self._callback

        self._callback_running = True
        try:
            await asyncio.to_thread(callback, quotes)
        finally:
            self._callback_running = False

    @staticmethod
    def _parse_quotes(data: Any) -> Quotes | None:
        if not isinstance(data, dict):
            return None

        quotes: Quotes = {}
        for code, quote in data.items():
            if isinstance(code, str) and is_tick_quote(quote):
                quotes[code] = quote

        return quotes or None
