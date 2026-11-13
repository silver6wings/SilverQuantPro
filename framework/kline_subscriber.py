"""
Amazing K 线 NATS 订阅调度器。

- 在 trading_session_windows 内 subscribe_kline，窗外自动 unsubscribe
- on_klines(hour, minute, second, batch) 交付 KlineDispatchBatch
- record 时按 (period, timestamp) 覆盖当日 bar，列下标见 kline_store.KlineCol
- 仅 AM_NATS

用法
----
    from framework.kline_subscriber import KlineSubscriber

    def on_klines(h, m, s, batch):
        ...

    KlineSubscriber(code_list=["000001.SZ"], on_klines=on_klines).run()
"""
from __future__ import annotations

import logging
import signal
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Protocol

from data.kline.kline_entity import KlineDispatchBatch
from data.kline.kline_store import BarKey, bar_to_store_row, rows_from_bucket, upsert_bar
from data.tick.tick_quote import local_ms_from_hms, parse_hms
from framework.kline_manager import (
    DEFAULT_KLINE_HISTORY_DIR,
    DEFAULT_KLINE_HISTORY_RETENTION_DAYS,
    KlineManager,
)
from framework.trading_session import (
    DEFAULT_TRADING_SESSION_WINDOWS,
    TradingSessionWindows,
    has_trading_session_windows,
    in_trading_session,
)

logger = logging.getLogger(__name__)

KlineCallback = Callable[[int, int, int, KlineDispatchBatch], None]

_SUB_OK = 0
_SUB_FAIL = -1

_DEFAULT_LOG_NAME = "kline_subscriber"
_DEFAULT_DISPATCH_INTERVAL = 1.0
_CLEAR_KLINE_TODAY_BEFORE = "09:14"  # 此时间前清空前一日 today kline 内存


class KlineBackend(Protocol):
    def subscribe_kline(self, code_list: list[str], callback: Callable[[KlineDispatchBatch], None]) -> int:
        ...

    def unsubscribe_kline(self) -> int:
        ...

    def update_code_list(self, code_list: list[str]) -> None:
        ...


def _minutes_of_day(now: datetime) -> int:
    return now.hour * 60 + now.minute


def _before_clear_kline_today(now: datetime) -> bool:
    hour, minute, _ = parse_hms(_CLEAR_KLINE_TODAY_BEFORE)
    return _minutes_of_day(now) < hour * 60 + minute


@dataclass
class KlineSubscriber:
    code_list: list[str]
    on_klines: KlineCallback
    dispatch_interval: float = _DEFAULT_DISPATCH_INTERVAL
    trading_session_windows: TradingSessionWindows = DEFAULT_TRADING_SESSION_WINDOWS
    record_kline_today: bool = False
    save_kline_history: bool = False
    kline_manager: KlineManager | None = None
    kline_history_dir: str = DEFAULT_KLINE_HISTORY_DIR
    kline_history_retention_days: int = DEFAULT_KLINE_HISTORY_RETENTION_DAYS

    _backend: KlineBackend = field(init=False, repr=False)
    _subscribed: bool = field(default=False, init=False, repr=False)
    _running: bool = field(default=True, init=False, repr=False)
    _stop_requested: bool = field(default=False, init=False, repr=False)
    _kline_today: dict[str, dict[BarKey, list[Any]]] = field(default_factory=dict, init=False, repr=False)
    _kline_today_date: date | None = field(default=None, init=False, repr=False)
    _kline_today_lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)
    _kline_manager: KlineManager | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        from data.kline.amazing.nats_consumer import AmazingKlineNatsConsumer

        self._backend = AmazingKlineNatsConsumer(interval=self.dispatch_interval)
        if self.kline_manager is not None:
            self._kline_manager = self.kline_manager
        elif self.save_kline_history:
            self._kline_manager = KlineManager(
                root=self.kline_history_dir,
                retention_days=self.kline_history_retention_days,
            )

    @property
    def is_subscribed(self) -> bool:
        return self._subscribed

    def update_code_list(self, code_list: list[str]) -> None:
        self.code_list = list(code_list)
        if self._subscribed:
            self._backend.update_code_list(self.code_list)

    def run(self) -> None:
        signal.signal(signal.SIGINT, self._on_signal)
        signal.signal(signal.SIGTERM, self._on_signal)

        self._log(
            f"ready; consumer=am_nats, codes={len(self.code_list)}, "
            f"session={self._format_session()}, interval={self.dispatch_interval}s, "
            f"record_kline_today={self.record_kline_today}, "
            f"save_kline_history={self._kline_manager is not None}"
        )
        self._maybe_subscribe_on_startup()

        try:
            while self._running:
                self._poll_loop(datetime.now())
                time.sleep(self.dispatch_interval if self.dispatch_interval > 0 else 0.2)
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

    def today_klines(self, code: str) -> list[list[Any]]:
        """返回指定 code 当日 K 线行（拍平数值 list，列下标见 KlineCol）。"""
        with self._kline_today_lock:
            bucket = self._kline_today.get(code)
            if not bucket:
                return []
            return rows_from_bucket(bucket)

    def clear_kline_today(self) -> None:
        with self._kline_today_lock:
            self._kline_today.clear()
            self._kline_today_date = None

    def _maybe_clear_kline_today(self, now: datetime, today: date) -> None:
        if not self.record_kline_today:
            return
        if not _before_clear_kline_today(now):
            return
        with self._kline_today_lock:
            if self._kline_today_date is not None and self._kline_today_date < today:
                self._kline_today.clear()
                self._kline_today_date = None
                self._log("kline today cleared (before 09:14, previous day)")

    def _record_bars(self, batch: KlineDispatchBatch, now: datetime) -> None:
        if not batch.bars:
            return
        today = now.date()
        fallback_ms = local_ms_from_hms(now.hour, now.minute, now.second, today=today)
        with self._kline_today_lock:
            if (
                _before_clear_kline_today(now)
                and self._kline_today_date is not None
                and self._kline_today_date < today
            ):
                self._kline_today.clear()
            self._kline_today_date = today
            for item in batch.bars:
                local_ms = item.received_ms or fallback_ms
                row = bar_to_store_row(item.bar, local_ms)
                upsert_bar(self._kline_today.setdefault(item.code, {}), row)

    def _snapshot_kline_today(self) -> dict[str, list[list[Any]]]:
        with self._kline_today_lock:
            return {
                code: rows_from_bucket(bucket)
                for code, bucket in self._kline_today.items()
                if bucket
            }

    def _maybe_auto_save_klines(self, now: datetime, today: date) -> None:
        if self._kline_manager is None:
            return
        self._kline_manager.maybe_auto_save(now, today, self._snapshot_kline_today(), log=self._log)

    def _dispatch_batch(self, batch: KlineDispatchBatch) -> None:
        if not batch.bars:
            return
        now = datetime.now()
        self.on_klines(now.hour, now.minute, now.second, batch)
        if self.record_kline_today:
            self._record_bars(batch, now)

    def _maybe_subscribe_on_startup(self) -> None:
        if not has_trading_session_windows(self.trading_session_windows):
            return
        now = datetime.now()
        if not in_trading_session(now, self.trading_session_windows):
            return
        if not self._subscribed:
            self._log("within trading session on startup, subscribing")
            self._do_subscribe("startup")

    def _do_subscribe(self, reason: str) -> None:
        def backend_callback(batch: KlineDispatchBatch) -> None:
            self._dispatch_batch(batch)

        result = self._backend.subscribe_kline(self.code_list, backend_callback)
        if result != _SUB_OK:
            self._log(f"subscribe failed ({reason})")
            return
        self._subscribed = True
        self._log(f"subscribed {len(self.code_list)} codes ({reason})")

    def _do_unsubscribe(self, reason: str) -> None:
        result = self._backend.unsubscribe_kline()
        if result != _SUB_OK:
            self._log(f"unsubscribe failed ({reason})")
            self._subscribed = False
            return
        self._subscribed = False
        self._log(f"unsubscribed ({reason})")

    def _poll_loop(self, now: datetime) -> None:
        today = now.date()
        self._maybe_clear_kline_today(now, today)
        self._maybe_auto_save_klines(now, today)
        self._poll_trading_session(now)

    def _poll_trading_session(self, now: datetime) -> None:
        if not has_trading_session_windows(self.trading_session_windows):
            return
        if in_trading_session(now, self.trading_session_windows):
            if not self._subscribed:
                self._do_subscribe("trading session")
        elif self._subscribed:
            self._do_unsubscribe("outside trading session")

    def _log(self, message: str) -> None:
        stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{_DEFAULT_LOG_NAME} {stamp}] {message}", flush=True)
        logger.info("[%s] %s", _DEFAULT_LOG_NAME, message)

    def _format_session(self) -> str:
        if not has_trading_session_windows(self.trading_session_windows):
            return "none"
        return ", ".join(f"[{start}, {stop})" for start, stop in self.trading_session_windows)
