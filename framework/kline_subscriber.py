"""
Amazing K 线 NATS 订阅调度器。

- 在 trading_session_windows 内 subscribe_kline，窗外自动 unsubscribe
- on_klines(hour, minute, second, batch) 交付 KlineDispatchBatch
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
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Protocol

from data.kline.kline_entity import KlineDispatchBatch
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


class KlineBackend(Protocol):
    def subscribe_kline(self, code_list: list[str], callback: Callable[[KlineDispatchBatch], None]) -> int:
        ...

    def unsubscribe_kline(self) -> int:
        ...

    def update_code_list(self, code_list: list[str]) -> None:
        ...


@dataclass
class KlineSubscriber:
    code_list: list[str]
    on_klines: KlineCallback
    dispatch_interval: float = _DEFAULT_DISPATCH_INTERVAL
    trading_session_windows: TradingSessionWindows = DEFAULT_TRADING_SESSION_WINDOWS

    _backend: KlineBackend = field(init=False, repr=False)
    _subscribed: bool = field(default=False, init=False, repr=False)
    _running: bool = field(default=True, init=False, repr=False)
    _stop_requested: bool = field(default=False, init=False, repr=False)

    def __post_init__(self) -> None:
        from data.kline.amazing.nats_consumer import AmazingKlineNatsConsumer

        self._backend = AmazingKlineNatsConsumer(interval=self.dispatch_interval)

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
            f"session={self._format_session()}, interval={self.dispatch_interval}s"
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

    def _dispatch_batch(self, batch: KlineDispatchBatch) -> None:
        if not batch.bars:
            return
        now = datetime.now()
        self.on_klines(now.hour, now.minute, now.second, batch)

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
