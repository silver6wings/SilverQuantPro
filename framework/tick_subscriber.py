"""
通用 tick 行情订阅调度器。

整合 AM_NATS / AM_DIRECT / XT_NATS / XT_DIRECT 四路后端，提供：
- on_quotes(hour, minute, second, quotes) 回调，quotes 为 {code: TickQuoteDict}
- record 时将 quote 拍平存入 today_ticks，列下标见 tick_quote.TickCol
- 可选 tick_manager：15:05 自动落盘 parquet

用法示例
--------
    from framework.tick_subscriber import ConsumerType, TickSubscriber

    def on_quotes(hour: int, minute: int, second: int, quotes: dict) -> None:
        print(hour, minute, second, len(quotes))

    sub = TickSubscriber(
        consumer_type=ConsumerType.AM_NATS,
        code_list=["000001.SZ"],
        on_quotes=on_quotes,
    )
    sub.run()

"""
from __future__ import annotations

import logging
import signal
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from typing import Any, Protocol

from data.tick.tick_quote import (
    TickPayload,
    copy_tick_rows,
    local_ms_from_hms,
    parse_hms,
    quotes_to_store_payload,
)
from framework.tick_manager import (
    DEFAULT_TICK_HISTORY_DIR,
    DEFAULT_TICK_HISTORY_RETENTION_DAYS,
    TickManager,
)

logger = logging.getLogger(__name__)

# -----------------------
# 类型别名与模块常量
# -----------------------

RawQuotes = dict[str, Any]
Quotes = TickPayload
BackendQuoteCallback = Callable[[RawQuotes], None]
QuoteCallback = Callable[[int, int, int, Quotes], None]
QuoteWindows = list[list[str]] | tuple[tuple[str, str], ...] | None  # quote_window 入参类型

_SUB_OK = 0          # subscribe / unsubscribe 成功
_SUB_FAIL = -1       # subscribe / unsubscribe 失败

_DEFAULT_LOG_NAME = "tick_subscriber"         # run() 控制台日志前缀
_DEFAULT_DISPATCH_INTERVAL = 1.0              # 后端 batch 间隔 & 主循环 sleep（秒）

DEFAULT_QUOTE_WINDOWS: tuple[tuple[str, str], ...] = (  # 默认订阅窗口；None 或空则不订阅
    ("09:14:30", "11:30:30"),
    ("12:59:30", "15:00:30"),
)

_CLEAR_TICK_TODAY_BEFORE = "09:15"              # 此时间前清空前一日 today_ticks 内存


# -----------------------
# 消费端类型与时间窗口
# -----------------------

class ConsumerType(str, Enum):
    AM_NATS = "am_nats"
    AM_DIRECT = "am_direct"
    XT_NATS = "xt_nats"
    XT_DIRECT = "xt_direct"


def _seconds(text: str) -> int:
    hour, minute, second = parse_hms(text)
    return hour * 3600 + minute * 60 + second


def _minutes_of_day(now: datetime) -> int:
    return now.hour * 60 + now.minute


def _before_clear_tick_today(now: datetime) -> bool:
    hour, minute, _ = parse_hms(_CLEAR_TICK_TODAY_BEFORE)
    return _minutes_of_day(now) < hour * 60 + minute


def _has_quote_window(windows: QuoteWindows) -> bool:
    return bool(windows)


def in_quote_windows(now: datetime, windows: QuoteWindows = None) -> bool:
    current = now.hour * 3600 + now.minute * 60 + now.second
    return any(_seconds(start) <= current < _seconds(stop) for start, stop in windows or DEFAULT_QUOTE_WINDOWS)


# -----------------------
# 行情后端：Protocol / 批量缓冲 / 直连 / 工厂
# -----------------------

class QuoteBackend(Protocol):
    def subscribe_whole_quote(self, code_list: list[str], callback: BackendQuoteCallback) -> int:
        """成功返回 0，失败返回 -1。"""

    def unsubscribe_quote(self) -> int:
        """成功返回 0，失败返回 -1。"""


class _QuoteBatchBuffer:
    """按 interval 聚合 tick quote（TickQuoteDict）后回调；空 batch 不触发。"""

    def __init__(self, interval: float, label: str) -> None:
        self.interval = interval
        self._label = label
        self._lock = threading.Lock()
        self._callback: BackendQuoteCallback | None = None
        self._quotes: RawQuotes = {}
        self._callback_running = False
        self._running = False
        self._thread: threading.Thread | None = None

    def start(self, callback: BackendQuoteCallback) -> bool:
        with self._lock:
            if self._callback is not None:
                return False
            self._callback = callback
            self._quotes = {}
        if not self._running:
            self._running = True
            self._thread = threading.Thread(
                target=self._loop,
                name=f"{self._label}-dispatch",
                daemon=True,
            )
            self._thread.start()
        return True

    def stop(self) -> bool:
        with self._lock:
            if self._callback is None:
                return False
            self._callback = None
            self._quotes = {}
        self._running = False
        thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=self.interval + 1.0)
        return True

    def push(self, payload: RawQuotes) -> None:
        with self._lock:
            if self._callback is None:
                return
            self._quotes.update(payload)

    def _loop(self) -> None:
        while self._running:
            time.sleep(self.interval)
            if self._callback_running:
                continue
            with self._lock:
                if self._callback is None or not self._quotes:
                    continue
                quotes, self._quotes = self._quotes, {}
                callback = self._callback
            self._callback_running = True
            try:
                callback(quotes)
            except Exception:
                logger.exception("%s callback failed", self._label)
            finally:
                self._callback_running = False


class AmazingDirectQuoteBackend:
    """AmazingData 直连后端，包装 delegate.amazing_delegate.AmazingSubscriber。

    限制：Amazing SDK 的 SubscribeData.run() 无法在同进程内干净地 stop 后再 restart；
    stop 仅禁用 callback，底层订阅线程仍阻塞在 run()。因此不适合 TickSubscriber 的
    「午休 unsub / 下午再 sub」这类同进程多次启停；更适合进程级「开一次、关一次」，
    或 quote_window 覆盖全天且不做中途 unsub 的场景。
    """

    def __init__(self, interval: float = _DEFAULT_DISPATCH_INTERVAL) -> None:
        self._buffer = _QuoteBatchBuffer(interval, "am-direct")
        self._subscriber: Any = None

    def subscribe_whole_quote(self, code_list: list[str], callback: BackendQuoteCallback) -> int:
        from delegate.amazing_delegate import AmazingSubscriber

        if not self._buffer.start(callback):
            return _SUB_FAIL
        self._subscriber = AmazingSubscriber()
        self._subscriber.set_sub_code_list(code_list)
        self._subscriber.start_sub(self._buffer.push)
        logger.info(
            "amazing direct subscribed %d codes, interval=%s",
            len(code_list),
            self._buffer.interval,
        )
        return _SUB_OK

    def unsubscribe_quote(self) -> int:
        if not self._buffer.stop():
            return _SUB_FAIL
        if self._subscriber is not None:
            self._subscriber.stop_sub()
        logger.info("amazing direct unsubscribed (callback stopped; SDK thread may still run)")
        return _SUB_OK


class XtdataQuoteBackend:
    """xtdata 直连后端：归一化为统一 tick quote，按 interval 批量回调。"""

    def __init__(self, interval: float = _DEFAULT_DISPATCH_INTERVAL) -> None:
        self._buffer = _QuoteBatchBuffer(interval, "xt-direct")
        self._seq: int | None = None
        from xtquant import xtdata

        xtdata.enable_hello = False
        self._xtdata = xtdata

    def subscribe_whole_quote(self, code_list: list[str], callback: BackendQuoteCallback) -> int:
        if self._seq is not None:
            return _SUB_FAIL
        if not self._buffer.start(callback):
            return _SUB_FAIL
        seq = self._xtdata.subscribe_whole_quote(code_list, callback=self._on_raw)
        if seq < 0:
            self._buffer.stop()
            return _SUB_FAIL
        self._seq = seq
        logger.info(
            "xtdata subscribed %d codes, seq=%s, interval=%s",
            len(code_list),
            seq,
            self._buffer.interval,
        )
        return _SUB_OK

    def unsubscribe_quote(self) -> int:
        if self._seq is None:
            return _SUB_FAIL
        self._xtdata.unsubscribe_quote(self._seq)
        self._seq = None
        self._buffer.stop()
        logger.info("xtdata unsubscribed")
        return _SUB_OK

    def _on_raw(self, raw: dict[str, Any]) -> None:
        from data.tick.xtquant.tick_adapter import quotes_to_tick_payload

        payload = quotes_to_tick_payload(raw)
        if payload:
            self._buffer.push(payload)


def _build_backend(consumer_type: ConsumerType, dispatch_interval: float) -> QuoteBackend:
    if consumer_type is ConsumerType.AM_NATS:
        from data.tick.amazing.nats_consumer import AmazingNatsConsumer

        return AmazingNatsConsumer(interval=dispatch_interval)

    if consumer_type is ConsumerType.AM_DIRECT:
        return AmazingDirectQuoteBackend(interval=dispatch_interval)

    if consumer_type is ConsumerType.XT_NATS:
        from data.tick.xtquant.nats_consumer import XtquantNatsConsumer

        return XtquantNatsConsumer(interval=dispatch_interval)

    if consumer_type is ConsumerType.XT_DIRECT:
        return XtdataQuoteBackend(interval=dispatch_interval)

    raise ValueError(f"unknown consumer_type: {consumer_type!r}")


# -----------------------
# TickSubscriber 主类
# -----------------------

@dataclass
class TickSubscriber:
    """通用 tick 行情订阅调度器。"""

    # --- 必填 ---
    consumer_type: ConsumerType                          # AM_NATS / AM_DIRECT / XT_NATS / XT_DIRECT
    code_list: list[str]                                 # 订阅 code；NATS 传 [] 接收 producer 全量
    on_quotes: QuoteCallback                             # (hour, minute, second, {code: TickQuoteDict})

    # --- 可选 ---
    dispatch_interval: float = _DEFAULT_DISPATCH_INTERVAL  # 后端 batch 间隔 & 主循环 tick 间隔（秒）
    quote_window: QuoteWindows = DEFAULT_QUOTE_WINDOWS
    record_tick_today: bool = False                        # 内存缓存当日 on_quotes
    save_tick_history: bool = False                        # 15:05 自动落盘（须有 record 数据）
    tick_manager: TickManager | None = None                # tick 数据总入口；save_tick_history=True 且为 None 时自动创建
    tick_history_dir: str = DEFAULT_TICK_HISTORY_DIR       # 自动创建 TickManager 时的 parquet 根目录
    tick_history_retention_days: int = DEFAULT_TICK_HISTORY_RETENTION_DAYS

    _backend: QuoteBackend = field(init=False, repr=False)
    _subscribed: bool = field(default=False, init=False, repr=False)
    _running: bool = field(default=True, init=False, repr=False)
    _stop_requested: bool = field(default=False, init=False, repr=False)
    _tick_today: dict[str, list[list[Any]]] = field(default_factory=dict, init=False, repr=False)
    _tick_today_date: date | None = field(default=None, init=False, repr=False)
    _tick_today_lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)
    _tick_manager: TickManager | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        self._backend = _build_backend(self.consumer_type, self.dispatch_interval)
        if self.tick_manager is not None:
            self._tick_manager = self.tick_manager
        elif self.save_tick_history:
            self._tick_manager = TickManager(
                root=self.tick_history_dir,
                retention_days=self.tick_history_retention_days,
            )

    @property
    def is_subscribed(self) -> bool:
        return self._subscribed

    def update_code_list(self, code_list: list[str]) -> None:
        """更新订阅 code 列表。NATS 后端热更新过滤；XT_DIRECT 会 unsub/resub；AM_DIRECT 仅缓存待下次 sub。"""
        self.code_list = list(code_list)
        if not self._subscribed:
            return
        update = getattr(self._backend, "update_code_list", None)
        if update is not None:
            update(self.code_list)
        elif self.consumer_type is ConsumerType.XT_DIRECT:
            self._do_unsubscribe("code_list change")
            self._do_subscribe("code_list change")

    # -----------------------
    # 生命周期：启动 / 停止
    # -----------------------

    def run(self) -> None:
        signal.signal(signal.SIGINT, self._on_signal)
        signal.signal(signal.SIGTERM, self._on_signal)

        self._log(
            f"ready; consumer={self.consumer_type.value}, "
            f"codes={len(self.code_list)}, "
            f"window={self._format_window()}, "
            f"interval={self.dispatch_interval}s, "
            f"record_tick_today={self.record_tick_today}, "
            f"save_tick_history={self._tick_manager is not None}"
        )

        self._maybe_subscribe_on_startup()

        try:
            while self._running:
                self._tick(datetime.now())
                time.sleep(self.dispatch_interval)
        finally:
            self.shutdown()

    def shutdown(self) -> None:
        if self._stop_requested:
            return
        self._stop_requested = True
        self._running = False
        self._log("shutting down")
        if self._subscribed:
            self._do_unsubscribe("shutdown")
        self._log("exit")

    def _on_signal(self, signum: int, _frame: object) -> None:
        self._log(f"received signal {signum}")
        self._running = False

    # -----------------------
    # 盘中 tick：today 内存
    # -----------------------

    def today_ticks(self, code: str) -> list[list[Any]]:
        """返回指定 code 当日 tick 行（拍平数值 list，列下标见 TickCol）。"""
        with self._tick_today_lock:
            rows = self._tick_today.get(code)
            if not rows:
                return []
            return copy_tick_rows(rows)

    def clear_tick_today(self) -> None:
        with self._tick_today_lock:
            self._tick_today.clear()
            self._tick_today_date = None

    def _maybe_clear_tick_today(self, now: datetime, today: date) -> None:
        if not self.record_tick_today:
            return
        if not _before_clear_tick_today(now):
            return
        with self._tick_today_lock:
            if self._tick_today_date is not None and self._tick_today_date < today:
                self._tick_today.clear()
                self._tick_today_date = None
                self._log("tick today cleared (before 09:15, previous day)")

    def _record_quotes(self, quotes: Quotes, now: datetime) -> None:
        if not quotes:
            return
        today = now.date()
        local_ms = local_ms_from_hms(now.hour, now.minute, now.second, today=today)
        rows_by_code = quotes_to_store_payload(quotes, local_ms)
        if not rows_by_code:
            return
        with self._tick_today_lock:
            if (
                _before_clear_tick_today(now)
                and self._tick_today_date is not None
                and self._tick_today_date < today
            ):
                self._tick_today.clear()
            self._tick_today_date = today
            for code, row in rows_by_code.items():
                self._tick_today.setdefault(code, []).append(row)

    def _snapshot_tick_today(self) -> dict[str, list[list[Any]]]:
        with self._tick_today_lock:
            return {code: list(rows) for code, rows in self._tick_today.items() if rows}

    def _maybe_auto_save_ticks(self, now: datetime, today: date) -> None:
        if self._tick_manager is None:
            return
        self._tick_manager.maybe_auto_save(now, today, self._snapshot_tick_today(), log=self._log)

    def _quotes_callback(self, now: datetime, quotes: Quotes) -> None:
        self.on_quotes(now.hour, now.minute, now.second, quotes)
        if self.record_tick_today:
            self._record_quotes(quotes, now)

    # -----------------------
    # 订阅 / 退订
    # -----------------------

    def _maybe_subscribe_on_startup(self) -> None:
        if not _has_quote_window(self.quote_window):
            return
        now = datetime.now()
        if not in_quote_windows(now, self.quote_window):
            return
        if not self._subscribed:
            self._log("within quote window on startup, subscribing")
            self._do_subscribe("startup")

    def _do_subscribe(self, reason: str) -> None:
        def backend_callback(raw_quotes: RawQuotes) -> None:
            if not raw_quotes:
                return
            now = datetime.now()
            if self.record_tick_today:
                self._quotes_callback(now, raw_quotes)
            else:
                self.on_quotes(now.hour, now.minute, now.second, raw_quotes)

        result = self._backend.subscribe_whole_quote(self.code_list, backend_callback)
        if result != _SUB_OK:
            self._log(f"subscribe failed ({reason})")
            return
        self._subscribed = True
        self._log(f"subscribed {len(self.code_list)} codes ({reason})")

    def _do_unsubscribe(self, reason: str) -> None:
        result = self._backend.unsubscribe_quote()
        if result != _SUB_OK:
            self._log(f"unsubscribe failed ({reason})")
            self._subscribed = False
            return
        self._subscribed = False
        self._log(f"unsubscribed ({reason})")

    # -----------------------
    # 定时调度主循环
    # -----------------------

    def _tick(self, now: datetime) -> None:
        today = now.date()
        self._maybe_clear_tick_today(now, today)
        self._maybe_auto_save_ticks(now, today)
        self._tick_window(now)

    def _tick_window(self, now: datetime) -> None:
        if not _has_quote_window(self.quote_window):
            return

        if in_quote_windows(now, self.quote_window):
            if not self._subscribed:
                self._do_subscribe("quote window")
        elif self._subscribed:
            self._do_unsubscribe("outside quote window")

    # -----------------------
    # 内部工具
    # -----------------------

    def _log(self, message: str) -> None:
        stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{_DEFAULT_LOG_NAME} {stamp}] {message}", flush=True)
        logger.info("[%s] %s", _DEFAULT_LOG_NAME, message)

    def _format_window(self) -> str:
        if not _has_quote_window(self.quote_window):
            return "none"
        return ", ".join(f"[{start}, {stop})" for start, stop in self.quote_window)
