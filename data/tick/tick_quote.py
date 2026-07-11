"""
统一 tick quote 格式定义，供各数据源 adapter 构建后推入消息队列。

本模块只定义 payload 契约，与传输层无关（NATS / MQTT / Redis 等均复用同一格式）。

字段（单 code 的 quote dict）：
- timestamp: 13 位毫秒时间戳
- lastClose, open, high, low, lastPrice, volume, amount
- askPrice1..5, bidPrice1..5, askVol1..5, bidVol1..5
- extra: 可选扩展字段（线上可省略）

数值精度（写入 dict 前统一归一化，避免 1.00000000009 类浮点尾差）：
- 价格类字段：4 位小数（覆盖 A 股/期货/期权常见报价精度）
- amount：2 位小数
- volume / 盘口量：整数

TickQuote 为 frozen + slots 的 dataclass，仅在解析/需要点号访问时使用；
producer 热路径 build_tick_quote 直出 dict，不经过中间对象。

Consumer 性能：callback 默认收到 TickQuoteDict，计算热路径直接用 quote["lastPrice"] 等；
需要点号访问时再 TickQuote.from_dict(quote)，每个 code 调一次即可，勿重复解析。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

# 单 code 的 quote body（传输层无关），例如 {"timestamp": ..., "lastPrice": ..., ...}
TickQuoteDict = dict[str, Any]

# from_dict 等路径传入的标量：adapter 给 float/int，JSON 解析后也可能缺失为 None
Numeric = float | int | None

# 一条消息的 payload：code → quote。与 NATS / MQTT / Redis 等传输无关，只描述 body 结构。
# amazing 常见 1 个 code（{"000001.SZ": {...}}）；
# xtquant 常见多 code 打包（{"000001.SZ": {...}, "600000.SH": {...}, ...}）
TickPayload = dict[str, TickQuoteDict]

_PRICE_DECIMALS = 4
_AMOUNT_DECIMALS = 2

_TICK_QUOTE_FIELD_NAMES = frozenset(
    {
        "timestamp",
        "lastClose",
        "open",
        "high",
        "low",
        "lastPrice",
        "volume",
        "amount",
        "askPrice1",
        "askPrice2",
        "askPrice3",
        "askPrice4",
        "askPrice5",
        "bidPrice1",
        "bidPrice2",
        "bidPrice3",
        "bidPrice4",
        "bidPrice5",
        "askVol1",
        "askVol2",
        "askVol3",
        "askVol4",
        "askVol5",
        "bidVol1",
        "bidVol2",
        "bidVol3",
        "bidVol4",
        "bidVol5",
        "extra",
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


def _parse_extra(data: dict[str, Any]) -> dict[str, Any] | None:
    extra = data.get("extra")
    if extra is None:
        unknown = {key: value for key, value in data.items() if key not in _TICK_QUOTE_FIELD_NAMES}
        return dict(unknown) if unknown else None
    if isinstance(extra, dict):
        return dict(extra)
    return None


def _build_tick_quote_dict(
    *,
    timestamp: int,
    last_close: Numeric,
    open: Numeric,
    high: Numeric,
    low: Numeric,
    last_price: Numeric,
    volume: Numeric,
    amount: Numeric,
    ask_price1: Numeric = 0.0,
    ask_price2: Numeric = 0.0,
    ask_price3: Numeric = 0.0,
    ask_price4: Numeric = 0.0,
    ask_price5: Numeric = 0.0,
    bid_price1: Numeric = 0.0,
    bid_price2: Numeric = 0.0,
    bid_price3: Numeric = 0.0,
    bid_price4: Numeric = 0.0,
    bid_price5: Numeric = 0.0,
    ask_vol1: Numeric = 0,
    ask_vol2: Numeric = 0,
    ask_vol3: Numeric = 0,
    ask_vol4: Numeric = 0,
    ask_vol5: Numeric = 0,
    bid_vol1: Numeric = 0,
    bid_vol2: Numeric = 0,
    bid_vol3: Numeric = 0,
    bid_vol4: Numeric = 0,
    bid_vol5: Numeric = 0,
    extra: dict[str, Any] | None = None,
) -> TickQuoteDict:
    payload: TickQuoteDict = {
        "timestamp": int(timestamp),
        "lastClose": _norm_price(last_close),
        "open": _norm_price(open),
        "high": _norm_price(high),
        "low": _norm_price(low),
        "lastPrice": _norm_price(last_price),
        "volume": _norm_volume(volume),
        "amount": _norm_amount(amount),
        "askPrice1": _norm_price(ask_price1),
        "askPrice2": _norm_price(ask_price2),
        "askPrice3": _norm_price(ask_price3),
        "askPrice4": _norm_price(ask_price4),
        "askPrice5": _norm_price(ask_price5),
        "bidPrice1": _norm_price(bid_price1),
        "bidPrice2": _norm_price(bid_price2),
        "bidPrice3": _norm_price(bid_price3),
        "bidPrice4": _norm_price(bid_price4),
        "bidPrice5": _norm_price(bid_price5),
        "askVol1": _norm_volume(ask_vol1),
        "askVol2": _norm_volume(ask_vol2),
        "askVol3": _norm_volume(ask_vol3),
        "askVol4": _norm_volume(ask_vol4),
        "askVol5": _norm_volume(ask_vol5),
        "bidVol1": _norm_volume(bid_vol1),
        "bidVol2": _norm_volume(bid_vol2),
        "bidVol3": _norm_volume(bid_vol3),
        "bidVol4": _norm_volume(bid_vol4),
        "bidVol5": _norm_volume(bid_vol5),
    }
    if extra:
        payload["extra"] = dict(extra)
    return payload


@dataclass(frozen=True, slots=True)
class TickQuote:
    timestamp: int
    lastClose: float
    open: float
    high: float
    low: float
    lastPrice: float
    volume: int
    amount: float
    askPrice1: float = 0.0
    askPrice2: float = 0.0
    askPrice3: float = 0.0
    askPrice4: float = 0.0
    askPrice5: float = 0.0
    bidPrice1: float = 0.0
    bidPrice2: float = 0.0
    bidPrice3: float = 0.0
    bidPrice4: float = 0.0
    bidPrice5: float = 0.0
    askVol1: int = 0
    askVol2: int = 0
    askVol3: int = 0
    askVol4: int = 0
    askVol5: int = 0
    bidVol1: int = 0
    bidVol2: int = 0
    bidVol3: int = 0
    bidVol4: int = 0
    bidVol5: int = 0
    extra: dict[str, Any] | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> TickQuote:
        if "timestamp" not in data:
            raise ValueError("tick quote missing required field: timestamp")

        normalized = _build_tick_quote_dict(
            timestamp=data["timestamp"],
            last_close=data.get("lastClose"),
            open=data.get("open"),
            high=data.get("high"),
            low=data.get("low"),
            last_price=data.get("lastPrice"),
            volume=data.get("volume"),
            amount=data.get("amount"),
            ask_price1=data.get("askPrice1"),
            ask_price2=data.get("askPrice2"),
            ask_price3=data.get("askPrice3"),
            ask_price4=data.get("askPrice4"),
            ask_price5=data.get("askPrice5"),
            bid_price1=data.get("bidPrice1"),
            bid_price2=data.get("bidPrice2"),
            bid_price3=data.get("bidPrice3"),
            bid_price4=data.get("bidPrice4"),
            bid_price5=data.get("bidPrice5"),
            ask_vol1=data.get("askVol1"),
            ask_vol2=data.get("askVol2"),
            ask_vol3=data.get("askVol3"),
            ask_vol4=data.get("askVol4"),
            ask_vol5=data.get("askVol5"),
            bid_vol1=data.get("bidVol1"),
            bid_vol2=data.get("bidVol2"),
            bid_vol3=data.get("bidVol3"),
            bid_vol4=data.get("bidVol4"),
            bid_vol5=data.get("bidVol5"),
            extra=_parse_extra(data),
        )
        return cls(
            **{key: normalized[key] for key in _TICK_QUOTE_FIELD_NAMES if key != "extra"},
            extra=normalized.get("extra"),
        )

    def to_dict(self) -> TickQuoteDict:
        return _build_tick_quote_dict(
            timestamp=self.timestamp,
            last_close=self.lastClose,
            open=self.open,
            high=self.high,
            low=self.low,
            last_price=self.lastPrice,
            volume=self.volume,
            amount=self.amount,
            ask_price1=self.askPrice1,
            ask_price2=self.askPrice2,
            ask_price3=self.askPrice3,
            ask_price4=self.askPrice4,
            ask_price5=self.askPrice5,
            bid_price1=self.bidPrice1,
            bid_price2=self.bidPrice2,
            bid_price3=self.bidPrice3,
            bid_price4=self.bidPrice4,
            bid_price5=self.bidPrice5,
            ask_vol1=self.askVol1,
            ask_vol2=self.askVol2,
            ask_vol3=self.askVol3,
            ask_vol4=self.askVol4,
            ask_vol5=self.askVol5,
            bid_vol1=self.bidVol1,
            bid_vol2=self.bidVol2,
            bid_vol3=self.bidVol3,
            bid_vol4=self.bidVol4,
            bid_vol5=self.bidVol5,
            extra=self.extra,
        )


def build_tick_quote(
    *,
    timestamp: int,
    last_close: float,
    open: float,
    high: float,
    low: float,
    last_price: float,
    volume: int,
    amount: float,
    ask_price1: float = 0.0,
    ask_price2: float = 0.0,
    ask_price3: float = 0.0,
    ask_price4: float = 0.0,
    ask_price5: float = 0.0,
    bid_price1: float = 0.0,
    bid_price2: float = 0.0,
    bid_price3: float = 0.0,
    bid_price4: float = 0.0,
    bid_price5: float = 0.0,
    ask_vol1: int = 0,
    ask_vol2: int = 0,
    ask_vol3: int = 0,
    ask_vol4: int = 0,
    ask_vol5: int = 0,
    bid_vol1: int = 0,
    bid_vol2: int = 0,
    bid_vol3: int = 0,
    bid_vol4: int = 0,
    bid_vol5: int = 0,
    extra: dict[str, Any] | None = None,
) -> TickQuoteDict:
    return _build_tick_quote_dict(
        timestamp=timestamp,
        last_close=last_close,
        open=open,
        high=high,
        low=low,
        last_price=last_price,
        volume=volume,
        amount=amount,
        ask_price1=ask_price1,
        ask_price2=ask_price2,
        ask_price3=ask_price3,
        ask_price4=ask_price4,
        ask_price5=ask_price5,
        bid_price1=bid_price1,
        bid_price2=bid_price2,
        bid_price3=bid_price3,
        bid_price4=bid_price4,
        bid_price5=bid_price5,
        ask_vol1=ask_vol1,
        ask_vol2=ask_vol2,
        ask_vol3=ask_vol3,
        ask_vol4=ask_vol4,
        ask_vol5=ask_vol5,
        bid_vol1=bid_vol1,
        bid_vol2=bid_vol2,
        bid_vol3=bid_vol3,
        bid_vol4=bid_vol4,
        bid_vol5=bid_vol5,
        extra=extra,
    )


def is_tick_quote(data: Any) -> bool:
    return isinstance(data, dict) and "timestamp" in data
