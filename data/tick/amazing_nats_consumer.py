"""
消费方订阅 NATS 行情，API 对齐 QMT xtdata 的 subscribe_whole_quote / unsubscribe_quote。

消费逻辑说明
------------

1. 单订阅
   - 同一时刻只允许一个 subscribe_whole_quote，避免多个 code_list / quotes dict 互相串数据。
   - 再次 subscribe 前须先 unsubscribe_quote。

2. 数据接收与缓存
   - NATS 每收到一条消息（格式 {"000001.SZ": {quote...}}），按 code 写入 quotes dict。
   - 同一 code 的新 tick 会覆盖旧值，始终只保留最新一条。
   - code_list 非空时只接收列表内的 code；传空列表 [] 表示接收全部 code。

3. callback 触发与去重
   - 每个 interval 周期结束时尝试 dispatch 一次。
   - dispatch 时将 quotes 整体交给 callback，并立刻 quotes = {} 换新容器。
   - callback 执行期间新 tick 写入新 dict，互不干扰。

4. 空 dict 不回调
   - 若某个 interval 内没有收到匹配的 tick，跳过 callback，不会传入空 dict。

5. callback 串行
   - dispatch 会 await callback 结束后再进入下一轮 sleep；callback 慢会推迟下一轮。

6. 并发与线程安全
   - subscribe_whole_quote 内部启动 NATS 消费循环，无需外部调用 run。
   - unsubscribe_quote 仅取消业务订阅，可在任意线程调用。
   - quotes 的写入与 swap 由 asyncio.Lock 保护，避免 tick 丢失。

用法示例
--------
    consumer = AmazingNatsConsumer(interval=1.0)

    def callback_sub_whole(quotes: dict) -> None:
        ...

    seq = consumer.subscribe_whole_quote(["000001.SZ"], callback_sub_whole)
    try:
        consumer.wait()  # 或自行阻塞主线程
    finally:
        consumer.unsubscribe_quote(seq)
"""
import asyncio
import json
import logging
import os
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import nats
from nats.aio.client import Client as NATS
from nats.aio.msg import Msg


logger = logging.getLogger(__name__)

NATS_URL = os.getenv("NATS_URL", "nats://127.0.0.1:4222")
NATS_SUBJECT = os.getenv("NATS_SUBJECT", "market.tick.amazing")

_DEFAULT_DISPATCH_INTERVAL = 1.0

Quotes = dict[str, dict[str, Any]]
QuoteCallback = Callable[[Quotes], None]


@dataclass
class QuoteSubscription:
    sequence: int
    code_list: frozenset[str]
    callback: QuoteCallback
    quotes: Quotes = field(default_factory=dict)
    callback_running: bool = False
    skipped_count: int = 0
    quotes_lock: asyncio.Lock = field(default_factory=asyncio.Lock)


class AmazingNatsConsumer:
    def __init__(
        self,
        nats_url: str = NATS_URL,
        subject: str = NATS_SUBJECT,
        interval: float = _DEFAULT_DISPATCH_INTERVAL,
    ) -> None:
        self.nats_url = nats_url
        self.subject = subject
        self.interval = interval
        self._next_sequence = 0
        self._subscription: QuoteSubscription | None = None
        self._lock = threading.Lock()
        self._runner_thread: threading.Thread | None = None
        self._nc: NATS | None = None
        self.total_count = 0
        self.total_bytes = 0
        self.window_count = 0
        self.window_bytes = 0
        self.decode_error_count = 0
        self.missing_code_count = 0

    def subscribe_whole_quote(self, code_list: list[str], callback: QuoteCallback) -> int:
        with self._lock:
            if self._subscription is not None:
                raise RuntimeError("only one subscription allowed, unsubscribe first")
            self._next_sequence += 1
            sequence = self._next_sequence
            self._subscription = QuoteSubscription(
                sequence=sequence,
                code_list=frozenset(code_list),
                callback=callback,
            )
            need_start = self._runner_thread is None or not self._runner_thread.is_alive()

        if need_start:
            self._runner_thread = threading.Thread(
                target=self._run_loop,
                name="nats-consumer",
                daemon=True,
            )
            self._runner_thread.start()

        logger.info("quote subscription started, sequence=%d, codes=%d", sequence, len(code_list))
        return sequence

    def unsubscribe_quote(self, sub_sequence: int) -> None:
        with self._lock:
            if self._subscription is None or self._subscription.sequence != sub_sequence:
                return
            self._subscription = None

        logger.info("quote subscription stopped, sequence=%d", sub_sequence)

    def wait(self) -> None:
        thread = self._runner_thread
        if thread is not None and thread.is_alive():
            thread.join()

    def _run_loop(self) -> None:
        asyncio.run(self._run_async())

    async def _run_async(self) -> None:
        self._nc = await nats.connect(self.nats_url)
        await self._nc.subscribe(self.subject, cb=self.on_message)

        logger.info("subscribed to %s on %s", self.subject, self.nats_url)

        try:
            await self._dispatch_loop()
        finally:
            await self._nc.drain()
            self._nc = None

    async def on_message(self, msg: Msg) -> None:
        message_size = len(msg.data)
        self.total_count += 1
        self.total_bytes += message_size
        self.window_count += 1
        self.window_bytes += message_size

        try:
            data = json.loads(msg.data)
        except json.JSONDecodeError:
            self.decode_error_count += 1
            return

        code, quote = self._extract_code_and_quote(data)
        if not code or quote is None:
            self.missing_code_count += 1
            return

        with self._lock:
            sub = self._subscription
        if sub is None:
            return
        if sub.code_list and code not in sub.code_list:
            return

        async with sub.quotes_lock:
            sub.quotes[code] = quote

    async def _dispatch_loop(self) -> None:
        last_time = time.perf_counter()

        while True:
            await asyncio.sleep(self.interval)

            with self._lock:
                sub = self._subscription

            now = time.perf_counter()
            elapsed = now - last_time
            last_time = now
            current_time = time.localtime()

            count = self.window_count
            size_bytes = self.window_bytes
            self.window_count = 0
            self.window_bytes = 0

            msg_per_sec = count / elapsed if elapsed > 0 else 0
            mb_per_sec = size_bytes / elapsed / 1024 / 1024 if elapsed > 0 else 0
            avg_bytes = size_bytes / count if count else 0
            pending_codes = len(sub.quotes) if sub is not None else 0
            skipped = sub.skipped_count if sub is not None else 0

            logger.info(
                "%s rate=%.0f msg/s, window_count=%d, window_size=%.2f MB, "
                "throughput=%.2f MB/s, avg_size=%.0f B, total_count=%d, "
                "active=%s, pending_codes=%d, decode_errors=%d, missing_code=%d, "
                "skipped=%d, total_size=%.2f MB",
                time.strftime("%Y-%m-%d %H:%M:%S", current_time),
                msg_per_sec,
                count,
                size_bytes / 1024 / 1024,
                mb_per_sec,
                avg_bytes,
                self.total_count,
                sub is not None,
                pending_codes,
                self.decode_error_count,
                self.missing_code_count,
                skipped,
                self.total_bytes / 1024 / 1024,
            )

            if sub is not None:
                with self._lock:
                    if self._subscription is sub:
                        await self._dispatch_subscription(sub)

    async def _dispatch_subscription(self, sub: QuoteSubscription) -> None:
        if sub.callback_running:
            sub.skipped_count += 1
            return

        async with sub.quotes_lock:
            if not sub.quotes:
                return
            quotes = sub.quotes
            sub.quotes = {}

        sub.callback_running = True
        try:
            await asyncio.to_thread(sub.callback, quotes)
        finally:
            sub.callback_running = False

    @staticmethod
    def _extract_code_and_quote(data: dict[str, Any]) -> tuple[str | None, dict[str, Any] | None]:
        if len(data) == 1:
            code, quote = next(iter(data.items()))
            if isinstance(quote, dict) and "time" in quote:
                return str(code), quote

        return None, None


