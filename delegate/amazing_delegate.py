"""
Amazing Documents
https://cloud.chinastock.com.cn/p/DSG36jYQx2IY_Y8CIAA
"""
import threading
from typing import Any, Callable

import AmazingData as ad
from tools.utils_remote_am import AmazingSecurityType, am_login, am_logout, get_am_data

Quote = dict[str, Any]
QuoteCallback = Callable[[Quote], None]

_LOT_SIZE = 100
Snapshot = ad.constant.Snapshot | ad.constant.SnapshotIndex


def _to_lots(volume: int | float | None) -> int:
    if not volume:
        return 0
    return int(volume) // _LOT_SIZE


def _snapshot_to_qmt_quote(data: Snapshot) -> Quote:
    """Convert AmazingData snapshot to QMT xtdata quote format: {code: quote}."""
    code = data.code
    trade_time = data.trade_time
    volume = int(data.volume or 0)
    trading_phase_code = getattr(data, "trading_phase_code", None)

    quote: dict[str, Any] = {
        "time": int(trade_time.timestamp() * 1000),
        "lastPrice": data.last,
        "open": data.open,
        "high": data.high,
        "low": data.low,
        "lastClose": data.pre_close,
        "amount": data.amount,
        "volume": volume // _LOT_SIZE,
        "pvolume": volume,
        "askPrice": [
            getattr(data, "ask_price1", 0.0),
            getattr(data, "ask_price2", 0.0),
            getattr(data, "ask_price3", 0.0),
            getattr(data, "ask_price4", 0.0),
            getattr(data, "ask_price5", 0.0),
        ],
        "bidPrice": [
            getattr(data, "bid_price1", 0.0),
            getattr(data, "bid_price2", 0.0),
            getattr(data, "bid_price3", 0.0),
            getattr(data, "bid_price4", 0.0),
            getattr(data, "bid_price5", 0.0),
        ],
        "askVol": [
            _to_lots(getattr(data, "ask_volume1", 0)),
            _to_lots(getattr(data, "ask_volume2", 0)),
            _to_lots(getattr(data, "ask_volume3", 0)),
            _to_lots(getattr(data, "ask_volume4", 0)),
            _to_lots(getattr(data, "ask_volume5", 0)),
        ],
        "bidVol": [
            _to_lots(getattr(data, "bid_volume1", 0)),
            _to_lots(getattr(data, "bid_volume2", 0)),
            _to_lots(getattr(data, "bid_volume3", 0)),
            _to_lots(getattr(data, "bid_volume4", 0)),
            _to_lots(getattr(data, "bid_volume5", 0)),
        ],
        "transactionNum": data.num_trades,
        "stockStatus": trading_phase_code,
        "openInt": 0,
        "lastSettlementPrice": 0.0,
        "settlementPrice": 0.0,
        "pe": 0.0,
        "volRatio": 0.0,
        "speed1Min": 0.0,
        "speed5Min": 0.0,
    }
    return {code: quote}


class AmazingDelegate:
    def __init__(self) -> None:
        am_login()
        self.code_list = ["000001.SZ"]
        self._thread: threading.Thread | None = None

    def __del__(self) -> None:
        try:
            am_logout()
        except Exception:
            pass

    def set_code_list(self, code_list: list[str]) -> None:
        self.code_list = list(code_list)

    @staticmethod
    def get_all_stock_codes() -> list[str]:
        amd = get_am_data()
        code_list = amd.get_code_list(security_type=AmazingSecurityType.EXTRA_STOCK_A_SH_SZ)
        return list(code_list)

    def start_sub(self, callback: QuoteCallback) -> None:
        if self._thread is not None and self._thread.is_alive():
            print("AmazingDelegate is already running")
            return

        self._thread = threading.Thread(
            target=self._run_stocks,
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

    def _run_stocks(self, callback: QuoteCallback) -> None:
        sub_data = ad.SubscribeData()

        @sub_data.register(code_list=self.code_list, period=ad.constant.Period.snapshot.value)
        def onSnapshot(data: Snapshot, period: Any) -> None:
            if self._thread is None:
                return
            callback(_snapshot_to_qmt_quote(data))

        try:
            sub_data.run()
        finally:
            if self._thread is threading.current_thread():
                self._thread = None
