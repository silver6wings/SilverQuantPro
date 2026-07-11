"""
AmazingData 行情订阅：在独立线程中运行 SubscribeData，将 snapshot 转为统一 tick quote 后回调。

会话由 AmazingDelegate 单例管理：进程启动时 login，进程退出时 logout。
stop_sub 仅停止 callback，不调用 logout；订阅线程可能仍阻塞在 sub_data.run()，
适合「每日启停进程」而非同进程内反复 start/stop。
"""
import logging
import threading
from typing import Any, Callable

import AmazingData as ad
from delegate.amazing_delegate import AmazingDelegate
from data.tick.amazing.tick_adapter import Snapshot, snapshot_to_tick_payload
from data.tick.tick_quote import TickPayload

logger = logging.getLogger(__name__)

QuoteCallback = Callable[[TickPayload], None]


class AmazingSubscriber:
    def __init__(self, delegate: AmazingDelegate | None = None) -> None:
        self._delegate = delegate or AmazingDelegate()
        self.subscribed_code_list = ["000001.SZ"]
        self._thread: threading.Thread | None = None

    def set_sub_code_list(self, code_list: list[str]) -> None:
        self.subscribed_code_list = list(code_list)

    def start_sub(self, callback: QuoteCallback) -> None:
        if self._thread is not None and self._thread.is_alive():
            print("AmazingSubscriber is already running")
            return

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
