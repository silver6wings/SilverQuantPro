"""
统一 K 线格式，供各数据源 adapter 构建后推入消息队列。

字段（单 code 的 kline dict）：
- timestamp: 13 位毫秒时间戳（bar 起始时刻）
- period: bar 周期，秒（如 180、600、7200；见 KlinePeriodName）
- open, high, low, close, volume, amount

volume 单位与 tick 一致：A 股现货等为「手」（Amazing adapter 由股换算）。

producer 热路径 build_kline_bar 直出 dict。
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

Numeric = float | int | None

KlineBarDict = dict[str, Any]
KlinePayload = dict[str, KlineBarDict]


class KlinePeriodName(str, Enum):
    """订阅/配置使用的规范周期名（min3～min120，与 AmazingData Period 一致）。"""

    MIN1 = "min1"
    MIN3 = "min3"
    MIN5 = "min5"
    MIN10 = "min10"
    MIN15 = "min15"
    MIN30 = "min30"
    MIN60 = "min60"
    MIN120 = "min120"


DEFAULT_KLINE_PERIOD = KlinePeriodName.MIN1

_PERIOD_NAME_TO_SECONDS: dict[str, int] = {
    KlinePeriodName.MIN1.value: 60,
    KlinePeriodName.MIN3.value: 180,
    KlinePeriodName.MIN5.value: 300,
    KlinePeriodName.MIN10.value: 600,
    KlinePeriodName.MIN15.value: 900,
    KlinePeriodName.MIN30.value: 1800,
    KlinePeriodName.MIN60.value: 3600,
    KlinePeriodName.MIN120.value: 7200,
    "1min": 60,
    "m1": 60,
    "3min": 180,
    "m3": 180,
    "5min": 300,
    "m5": 300,
    "10min": 600,
    "m10": 600,
    "15min": 900,
    "m15": 900,
    "30min": 1800,
    "m30": 1800,
    "60min": 3600,
    "m60": 3600,
    "120min": 7200,
    "m120": 7200,
    "1h": 3600,
    "hour1": 3600,
    "2h": 7200,
}

_PRICE_DECIMALS = 4
_AMOUNT_DECIMALS = 2

_REQUIRED_KLINE_BAR_FIELDS = frozenset(
    {
        "timestamp",
        "period",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
    }
)


def _norm_price(value: Numeric) -> float:
    if value is None:
        return 0.0
    number = float(value)
    if number == 0.0:
        return 0.0
    return round(number, _PRICE_DECIMALS)


def _norm_amount(value: Numeric) -> float:
    if value is None:
        return 0.0
    number = float(value)
    if number == 0.0:
        return 0.0
    return round(number, _AMOUNT_DECIMALS)


def _norm_volume(value: Numeric) -> int:
    if value is None:
        return 0
    return int(value)


def build_kline_bar(
    *,
    timestamp: int,
    period: int,
    open: float,
    high: float,
    low: float,
    close: float,
    volume: int,
    amount: float,
) -> KlineBarDict:
    return {
        "timestamp": int(timestamp),
        "period": int(period),
        "open": _norm_price(open),
        "high": _norm_price(high),
        "low": _norm_price(low),
        "close": _norm_price(close),
        "volume": _norm_volume(volume),
        "amount": _norm_amount(amount),
    }


def is_kline_bar(data: Any) -> bool:
    if not isinstance(data, dict):
        return False
    if not _REQUIRED_KLINE_BAR_FIELDS.issubset(data.keys()):
        return False
    if "lastPrice" in data:
        return False
    return True


@dataclass(frozen=True, slots=True)
class KlineBarDispatch:
    code: str
    bar: KlineBarDict
    received_ms: int


@dataclass(frozen=True, slots=True)
class KlineDispatchBatch:
    bars: list[KlineBarDispatch]


def resolve_period_seconds(period_name: str | KlinePeriodName) -> int:
    """配置名 → period 秒数。规范名为 KlinePeriodName；另兼容 1min / 5min 等别名。"""
    if isinstance(period_name, KlinePeriodName):
        key = period_name.value
    else:
        key = period_name.strip().lower().replace(" ", "")
    seconds = _PERIOD_NAME_TO_SECONDS.get(key)
    if seconds is None:
        raise ValueError(f"unsupported kline period: {period_name}")
    return seconds
