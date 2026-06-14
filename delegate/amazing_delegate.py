"""
Amazing Documents
https://cloud.chinastock.com.cn/p/DSG36jYQx2IY_Y8CIAA
"""
import logging
import threading
from typing import Any, Callable

import AmazingData as ad
from delegate.amazing_snapshot import Quote, Snapshot, snapshot_to_qmt_quote
from tools.utils_remote_am import AmazingSecurityType, am_login, am_logout, get_am_data

logger = logging.getLogger(__name__)

QuoteCallback = Callable[[Quote], None]


class AmazingDelegate:
    _instance: "AmazingDelegate | None" = None
    _lock = threading.Lock()
    _initialized = False

    def __new__(cls) -> "AmazingDelegate":
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
            self.subscribed_code_list = ["000001.SZ"]
            self._thread: threading.Thread | None = None
            AmazingDelegate._initialized = True

    def __del__(self) -> None:
        if AmazingDelegate._instance is not self:
            return
        try:
            am_logout()
        except Exception:
            pass

    # ======== subscribe ticks ========

    def set_sub_code_list(self, code_list: list[str]) -> None:
        self.subscribed_code_list = list(code_list)

    def start_sub(self, callback: QuoteCallback) -> None:
        if self._thread is not None and self._thread.is_alive():
            print("AmazingDelegate is already running")
            return

        self._thread = threading.Thread(
            target=self._run_subscribe,
            args=(callback,),
            name="amazing-delegate",
            daemon=True,
        )
        self._thread.start()

    def stop_sub(self, timeout: float = 5.0) -> None:
        thread = self._thread
        if thread is None:
            return

        self._thread = None
        try:
            am_logout()
        except Exception:
            pass

        if thread.is_alive():
            thread.join(timeout=timeout)

        try:
            am_login()
        except Exception:
            pass

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
            # data convert to qmt quote
            try:
                quote = snapshot_to_qmt_quote(data)
            except Exception:
                code = getattr(data, "code", type(data).__name__)
                logger.critical(
                    "failed to convert snapshot to qmt quote: code=%s",
                    code,
                    exc_info=True,
                )
                return
            # quote callback
            try:
                callback(quote)
            except Exception:
                code = getattr(data, "code", type(data).__name__)
                logger.exception("snapshot callback failed: code=%s", code)

        try:
            sub_data.run()
        finally:
            if self._thread is threading.current_thread():
                self._thread = None

    # ======== random get codes ========

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
