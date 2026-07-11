"""QMT xtdata 数据访问封装。"""
import logging
import threading
from typing import Any, Callable

import pandas as pd

from tools.utils_remote_xt import XtDividendType

logger = logging.getLogger(__name__)

_DAILY_FIELDS = ("time", "open", "high", "low", "close", "volume", "amount")
_DEFAULT_DOWNLOAD_TIMEOUT = 60.0


def _run_with_timeout(func: Callable[..., Any], args: tuple[Any, ...], timeout: float) -> Any:
    """兜底：xtdata 下载接口无原生 timeout，社区常用 threading 包一层。

    注意：超时后底层 C++ 调用仍在 daemon 线程里跑，无法强杀，只是主流程不再等。
    """
    result: list[Any] = [None]
    error: list[BaseException | None] = [None]

    def _wrapper() -> None:
        try:
            result[0] = func(*args)
        except BaseException as exc:
            error[0] = exc

    thread = threading.Thread(target=_wrapper, daemon=True)
    thread.start()
    thread.join(timeout)
    if thread.is_alive():
        raise TimeoutError(f"xtdata call timed out after {timeout}s")
    if error[0] is not None:
        raise error[0]
    return result[0]


class XtdataDelegate:
    @staticmethod
    def ensure_sector_data() -> None:
        """同步板块分类数据。

        勿用 xtdata.download_sector_data()：新版内部走 download_history_data2，
        QMT 客户端与 xtquant 版本不匹配时回调不触发，会在 while 循环里假死。
        """
        from xtquant import xtdata

        try:
            if xtdata.get_sector_list():
                return
        except Exception:
            pass

        client = xtdata.get_client()
        client.down_all_sector_data()

    @classmethod
    def get_code_list(cls, sectors: list[str]) -> list[str]:
        from xtquant import xtdata

        cls.ensure_sector_data()

        code_list: list[str] = []
        seen: set[str] = set()
        for sector in sectors:
            codes = xtdata.get_stock_list_in_sector(sector)
            if not codes:
                print(
                    f"warning: sector '{sector}' returned 0 codes, "
                    f"check name via xtdata.get_sector_list()"
                )
            print(f"{sector}: {len(codes)}")
            for code in codes:
                if code not in seen:
                    seen.add(code)
                    code_list.append(code)
        return code_list

    @staticmethod
    def _read_code_daily(
        code: str,
        start_time: str,
        end_time: str,
        dividend_type: str,
    ) -> pd.DataFrame:
        from xtquant import xtdata

        raw = xtdata.get_market_data_ex(
            field_list=list(_DAILY_FIELDS),
            stock_list=[code],
            period="1d",
            start_time=start_time,
            end_time=end_time,
            count=-1,
            dividend_type=dividend_type,
            fill_data=False,
        )
        df = raw.get(code) if isinstance(raw, dict) else None
        if df is None:
            return pd.DataFrame(columns=["code", *_DAILY_FIELDS])
        return XtdataDelegate._normalize_daily_frame(code, df)

    @staticmethod
    def _daily_history_covers_range(
        df: pd.DataFrame,
        start_time: str,
        end_time: str,
    ) -> bool:
        if df.empty or "time" not in df.columns:
            return False
        if not start_time and not end_time:
            return True

        times = pd.to_numeric(df["time"], errors="coerce").dropna()
        if times.empty:
            return False

        min_time = int(times.min())
        max_time = int(times.max())
        if start_time:
            start_key = int(start_time)
            if min_time > start_key:
                return False
        if end_time:
            end_key = int(end_time)
            if max_time < end_key:
                return False
        return True

    @staticmethod
    def _download_code_daily(code: str, start_time: str, end_time: str) -> None:
        """下载单只股票日线到本地缓存（未复权原始数据）。

        社区建议：优先 incrementally=True 从本地末条往后补，比全量快且不易堵。
        start_time 为空时走增量；指定 start_time 时按区间补数据。
        """
        from xtquant import xtdata

        if start_time:
            xtdata.download_history_data(
                stock_code=code,
                period="1d",
                start_time=start_time,
                end_time=end_time,
            )
            return

        xtdata.download_history_data(
            stock_code=code,
            period="1d",
            start_time="",
            end_time=end_time,
            incrementally=True,
        )

    @staticmethod
    def _normalize_daily_frame(code: str, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame(columns=["code", *_DAILY_FIELDS])

        out = df.copy()
        if "time" not in out.columns:
            out = out.reset_index(names="time")
        out.insert(0, "code", code)

        keep = ["code", *[field for field in _DAILY_FIELDS if field in out.columns]]
        return out[keep].sort_values("time").reset_index(drop=True)

    @classmethod
    def get_code_daily_history(
        cls,
        code: str,
        start_time: str = "",
        end_time: str = "",
        dividend_type: str = XtDividendType.FRONT,
        download: bool = True,
        download_timeout: float = _DEFAULT_DOWNLOAD_TIMEOUT,
    ) -> pd.DataFrame:
        """获取单只股票日线；默认前复权。

        流程（社区推荐「先读后下」）：
        1. get_market_data_ex 读本地缓存（快，不走下载）
        2. 区间不完整时再 download_history_data 增量补数据
        3. 下载仍可能卡住，保留 timeout 兜底

        start_time / end_time 格式：YYYYMMDD，空字符串表示不限制。
        """
        df = cls._read_code_daily(code, start_time, end_time, dividend_type)
        if not download or cls._daily_history_covers_range(df, start_time, end_time):
            return df

        try:
            _run_with_timeout(
                cls._download_code_daily,
                (code, start_time, end_time),
                download_timeout,
            )
        except TimeoutError:
            logger.warning(
                "daily download timed out for %s (%s-%s), returning cached data if any",
                code,
                start_time or "*",
                end_time or "*",
            )
            return df

        return cls._read_code_daily(code, start_time, end_time, dividend_type)


if __name__ == "__main__":
    code = "600000.SH"
    start_time = "20260101"
    end_time = "20260628"
    df = XtdataDelegate.get_code_daily_history(
        code,
        start_time=start_time,
        end_time=end_time,
        dividend_type=XtDividendType.FRONT,
    )
    print(df)
