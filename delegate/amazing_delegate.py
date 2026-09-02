"""
AmazingData 会话与行情订阅封装。

文档: https://cloud.chinastock.com.cn/p/DSG36jYQx2IY_Y8CIAA

AmazingDelegate：单例，进程级 login / logout，静态查码表。
AmazingSubscriber：独立实例，在后台线程运行 SubscribeData，将 snapshot 转为统一 tick quote 后回调。

stop_sub 仅停止 callback，不 logout；订阅线程可能仍阻塞在 sub_data.run()，
适合「每日启停进程」而非同进程内反复 start/stop。
"""
from __future__ import annotations

import logging
import threading
from typing import Any, Callable

import AmazingData as ad

from data.tick.amazing.tick_adapter import Snapshot, snapshot_to_tick_payload
from data.tick.tick_quote import TickPayload
from tools.utils_remote_am import AmazingSecurityType, am_login, am_logout, get_am_data

logger = logging.getLogger(__name__)

QuoteCallback = Callable[[TickPayload], None]


class AmazingDelegate:
    _instance: AmazingDelegate | None = None
    _lock = threading.Lock()
    _initialized = False

    def __new__(cls) -> AmazingDelegate:
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        if AmazingDelegate._initialized:
            return
        with AmazingDelegate._lock:
            if AmazingDelegate._initialized:
                return
            am_login()
            self.amazing_data = get_am_data()
            AmazingDelegate._initialized = True

    def login(self) -> None:
        try:
            am_login()
        except Exception:
            pass

    def logout(self) -> None:
        try:
            am_logout()
        except Exception:
            pass

    def __del__(self) -> None:
        if AmazingDelegate._instance is not self:
            return
        self.logout()

    @classmethod
    def get_codes(cls, security_type: str) -> list[str]:
        code_list = cls().amazing_data.get_code_list(security_type=security_type)
        return list(code_list)

    @classmethod
    def get_hs_stock_codes(cls) -> list[str]:
        return cls.get_codes(AmazingSecurityType.HS_STOCK)

    @classmethod
    def get_hs_index_codes(cls) -> list[str]:
        return cls.get_codes(AmazingSecurityType.HS_INDEX)

    @classmethod
    def get_hs_etf_codes(cls) -> list[str]:
        return cls.get_codes(AmazingSecurityType.HS_ETF)


class AmazingSubscriber:
    def __init__(self) -> None:
        self.subscribed_code_list = ["000001.SZ"]
        self._thread: threading.Thread | None = None

    def set_sub_code_list(self, code_list: list[str]) -> None:
        self.subscribed_code_list = list(code_list)

    def start_sub(self, callback: QuoteCallback) -> None:
        if self._thread is not None and self._thread.is_alive():
            print("AmazingSubscriber is already running")
            return

        AmazingDelegate()
        self._thread = threading.Thread(
            target=self._run_subscribe,
            args=(callback,),
            name="amazing-subscriber",
            daemon=True,
        )
        self._thread.start()

    def stop_sub(self, timeout: float = 5.0) -> None:
        """停止 callback；不 logout。订阅线程或仍卡在 run() 直至进程退出。"""
        thread = self._thread
        if thread is None:
            return

        self._thread = None
        if thread.is_alive():
            thread.join(timeout=timeout)

    def wait(self, timeout: float | None = None) -> None:
        thread = self._thread
        if thread is None:
            return
        thread.join(timeout=timeout)

    def _run_subscribe(self, callback: QuoteCallback) -> None:
        sub_data = ad.SubscribeData()

        @sub_data.register(code_list=self.subscribed_code_list, period=ad.constant.Period.snapshot.value)
        def onSnapshot(data: Snapshot, period: Any) -> None:
            if self._thread is None:
                return
            try:
                payload = snapshot_to_tick_payload(data)
            except Exception:
                code = getattr(data, "code", type(data).__name__)
                logger.critical(
                    "failed to convert snapshot to tick quote: code=%s",
                    code,
                    exc_info=True,
                )
                return
            try:
                callback(payload)
            except Exception:
                code = getattr(data, "code", type(data).__name__)
                logger.exception("snapshot callback failed: code=%s", code)

        try:
            sub_data.run()
        finally:
            if self._thread is threading.current_thread():
                self._thread = None
