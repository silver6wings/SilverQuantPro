"""
消费方订阅 NATS 行情，API 对齐 QMT xtdata 的 subscribe_whole_quote / unsubscribe_quote。

消费逻辑说明
------------

1. 单订阅
   - 同一时刻只允许一个 subscribe_whole_quote。
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
   - 上一轮 callback 未完成时跳过本轮 dispatch。

用法示例
--------
    consumer = AmazingNatsConsumer(interval=1.0)

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
import os
import threading
from collections.abc import Callable
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

_SUB_OK = 0
_SUB_FAIL = -1


class AmazingNatsConsumer:
    """NATS 行情消费端。

    线程模型（两个线程协作）：
    - 主线程：调用 subscribe / unsubscribe / wait，读写订阅配置。
    - runner 线程（daemon）：内部跑一个 asyncio 事件循环，负责收 NATS 消息、
      定时 dispatch、在线程池里执行用户 callback。

    _lock 的作用：
    - 保护 _callback、_code_list、_quotes 等共享状态，避免主线程与 runner 线程
      同时读写导致数据错乱。
    - 持锁时间很短（只做判断和 dict 读写），且不在持锁期间 await，避免卡死事件循环。
    """

    def __init__(
        self,
        nats_url: str = NATS_URL,
        subject: str = NATS_SUBJECT,
        interval: float = _DEFAULT_DISPATCH_INTERVAL,
    ) -> None:
        """初始化配置。此时还没有启动 runner 线程，也不会连接 NATS。"""
        self.nats_url = nats_url
        self.subject = subject
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
        """注册行情回调。由主线程调用。

        成功返回 0，已有订阅时返回 -1。
        首次订阅会启动后台 runner 线程；之后重复订阅只更新内存状态，不重复建线程。
        """
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
                name="nats-consumer",
                daemon=True,
            )
            self._runner_thread.start()

        logger.info("quote subscription started, codes=%d", len(code_list))
        return _SUB_OK

    def unsubscribe_quote(self) -> int:
        """取消订阅。由主线程调用。

        成功返回 0，本来就没订阅时返回 -1。
        只清空订阅状态和缓存，runner 线程和 NATS 连接会继续存活（方便再次订阅）。
        """
        with self._lock:
            if self._callback is None:
                return _SUB_FAIL
            self._callback = None
            self._code_list = frozenset()
            self._quotes = {}

        logger.info("quote subscription stopped, quote_count=%d", self.quote_count)
        return _SUB_OK

    def wait(self) -> None:
        """阻塞主线程，直到 runner 线程结束。

        正常运行时 runner 是 while True 不会退出，所以通常靠 KeyboardInterrupt 打断。
        """
        thread = self._runner_thread
        if thread is not None and thread.is_alive():
            thread.join()

    def _run_loop(self) -> None:
        """runner 线程入口。在独立线程里创建并运行 asyncio 事件循环。"""
        asyncio.run(self._run_async())

    async def _run_async(self) -> None:
        """在 runner 线程的 asyncio 事件循环中运行。

        连接 NATS、注册 on_message，然后进入定时 dispatch 循环。
        退出时 drain 连接，释放 NATS 资源。
        """
        self._nc = await nats.connect(self.nats_url)
        await self._nc.subscribe(self.subject, cb=self.on_message)

        logger.info("subscribed to %s on %s", self.subject, self.nats_url)

        try:
            await self._dispatch_loop()
        finally:
            await self._nc.drain()
            self._nc = None

    async def on_message(self, msg: Msg) -> None:
        """NATS 消息回调，在 runner 线程的事件循环里异步触发。

        解析 JSON 后，短暂持 _lock 写入 _quotes。
        不在锁内做 await，避免阻塞事件循环。
        """
        try:
            data = json.loads(msg.data)
            # logger.debug("nats recv %s", next(iter(data)) if isinstance(data, dict) and data else data)
        except json.JSONDecodeError:
            return

        code, quote = self._extract_code_and_quote(data)
        if not code or quote is None:
            return

        # logger.debug(code)
        with self._lock:
            if self._callback is None:
                return
            if self._code_list and code not in self._code_list:
                return
            self._quotes[code] = quote
            self.quote_count += 1

    async def _dispatch_loop(self) -> None:
        """定时调度循环，在 runner 线程的事件循环里运行。

        每 interval 秒醒来一次，若当前有订阅则尝试 dispatch。
        sleep 期间事件循环可继续处理 on_message，不会耽误收消息。
        """
        while True:
            await asyncio.sleep(self.interval)

            with self._lock:
                subscribed = self._callback is not None

            if subscribed:
                await self._dispatch()

    async def _dispatch(self) -> None:
        """把缓存的 quotes 交给用户 callback。

        1. 若上一轮 callback 还在跑（_callback_running），本轮直接跳过。
        2. 短暂持 _lock，把 _quotes swap 出来并清空，然后释放锁。
        3. 用 asyncio.to_thread 在线程池执行 callback，不阻塞事件循环收消息。

        注意：用户 callback 必须尽快返回，否则会拖慢后续 dispatch。
        """
        if self._callback_running:
            return

        with self._lock:
            if self._callback is None or not self._quotes:
                return
            quotes = self._quotes
            self._quotes = {}
            callback = self._callback

        self._callback_running = True
        try:
            await asyncio.to_thread(callback, quotes)
        finally:
            self._callback_running = False

    @staticmethod
    def _extract_code_and_quote(data: dict[str, Any]) -> tuple[str | None, dict[str, Any] | None]:
        """从 JSON dict 提取 code 和 quote。纯函数，不涉及线程。"""
        if len(data) == 1:
            code, quote = next(iter(data.items()))
            if isinstance(quote, dict) and "time" in quote:
                return str(code), quote

        return None, None
