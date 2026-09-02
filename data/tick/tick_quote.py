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

TickSubscriber：on_quotes(hour, minute, second, quotes) 交付 TickQuoteDict batch；
record 时拍平为 today_ticks 行，列下标见 TickCol（TIMESTAMP=int ms）；
parquet 落盘 local/time → HH:MM:SS 字符串。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from enum import IntEnum
from typing import Any

import pandas as pd

# ---------------------------------------------------------------------------
# 类型别名
# ---------------------------------------------------------------------------

TickQuoteDict = dict[str, Any]
Numeric = float | int | None
TickPayload = dict[str, TickQuoteDict]

TickStoreRow = list[Any]
TickStorePayload = dict[str, list[TickStoreRow]]

# ---------------------------------------------------------------------------
# TickQuoteDict 字段集（传输 / on_quotes）
# ---------------------------------------------------------------------------

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

# ---------------------------------------------------------------------------
# today_ticks / parquet 拍平列（local + 数值字段，无 extra）
# ---------------------------------------------------------------------------

class TickCol(IntEnum):
    """拍平 list 列下标。"""

    LOCAL = 0
    TIMESTAMP = 1
    LAST_CLOSE = 2
    OPEN = 3
    HIGH = 4
    LOW = 5
    LAST_PRICE = 6
    VOLUME = 7
    AMOUNT = 8
    ASK_PRICE_1 = 9
    ASK_PRICE_2 = 10
    ASK_PRICE_3 = 11
    ASK_PRICE_4 = 12
    ASK_PRICE_5 = 13
    BID_PRICE_1 = 14
    BID_PRICE_2 = 15
    BID_PRICE_3 = 16
    BID_PRICE_4 = 17
    BID_PRICE_5 = 18
    ASK_VOL_1 = 19
    ASK_VOL_2 = 20
    ASK_VOL_3 = 21
    ASK_VOL_4 = 22
    ASK_VOL_5 = 23
    BID_VOL_1 = 24
    BID_VOL_2 = 25
    BID_VOL_3 = 26
    BID_VOL_4 = 27
    BID_VOL_5 = 28


TICK_STORE_COLUMNS: tuple[str, ...] = (
    "local",
    "time",          # 对应 TickCol.TIMESTAMP；落盘为 HH:MM:SS
    "lastClose",
    "open",
    "high",
    "low",
    "lastPrice",
    "volume",
    "amount",
    *(f"askPrice{i}" for i in range(1, 6)),
    *(f"askVol{i}" for i in range(1, 6)),
    *(f"bidPrice{i}" for i in range(1, 6)),
    *(f"bidVol{i}" for i in range(1, 6)),
)

TICK_STORE_TIME_COLUMNS: tuple[str, ...] = ("local", "time")
_TICK_STORE_TIME_INDEXES: tuple[int, int] = (TickCol.LOCAL, TickCol.TIMESTAMP)

_BOOK_LEVEL_FIELDS: tuple[str, ...] = tuple(
    field
    for index in range(1, 6)
    for field in (
        f"askPrice{index}",
        f"askVol{index}",
        f"bidPrice{index}",
        f"bidVol{index}",
    )
)

# ---------------------------------------------------------------------------
# TickQuoteDict 构建
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# 时间工具
# ---------------------------------------------------------------------------


def now_ms() -> int:
    return int(datetime.now().timestamp() * 1000)


def local_ms_from_hms(
    hour: int,
    minute: int,
    second: int,
    *,
    today: date | None = None,
) -> int:
    today = today or date.today()
    return int(datetime(today.year, today.month, today.day, hour, minute, second).timestamp() * 1000)


def parse_hms(text: str) -> tuple[int, int, int]:
    parts = text.split(":")
    if len(parts) == 2:
        return int(parts[0]), int(parts[1]), 0
    return int(parts[0]), int(parts[1]), int(parts[2])


def clock_matches(text: str, now: datetime) -> bool:
    hour, minute, _ = parse_hms(text)
    return now.hour == hour and now.minute == minute


def timestamp_ms_to_hms(timestamp_ms: int) -> str:
    """13 位毫秒时间戳 → HH:MM:SS（parquet 落盘，不含日期）。"""
    return datetime.fromtimestamp(int(timestamp_ms) / 1000).strftime("%H:%M:%S")


def hms_to_timestamp_ms(day: int, hms: str) -> int:
    """HH:MM:SS + 交易日 day(YYYYMMDD) → 13 位毫秒时间戳（parquet 读回）。"""
    hour, minute, second = parse_hms(str(hms).strip())
    year, month, d = day // 10000, (day // 100) % 100, day % 100
    return int(datetime(year, month, d, hour, minute, second).timestamp() * 1000)


# ---------------------------------------------------------------------------
# TickQuoteDict → 拍平 store 行
# ---------------------------------------------------------------------------


def resolve_tick_quote(quote: Any) -> TickQuoteDict | None:
    """TickQuoteDict 原样返回；xtdata 原始 dict 归一化。"""
    if not isinstance(quote, dict):
        return None
    if is_tick_quote(quote):
        return quote
    from data.tick.xtquant.tick_adapter import quote_to_tick_payload

    return quote_to_tick_payload(quote)


def quote_to_store_row(quote: TickQuoteDict, local_ms: int) -> TickStoreRow:
    """TickQuoteDict → today_ticks 拍平数值行（无 extra）。"""
    row: TickStoreRow = [
        local_ms,
        int(quote.get("timestamp", 0)),
        quote.get("lastClose", 0),
        quote.get("open", 0),
        quote.get("high", 0),
        quote.get("low", 0),
        quote.get("lastPrice", 0),
        quote.get("volume", 0),
        quote.get("amount", 0),
    ]
    row.extend(quote.get(field, 0) for field in _BOOK_LEVEL_FIELDS)
    return row


def quotes_to_store_payload(
    quotes: dict[str, Any],
    local_ms: int,
) -> TickStorePayload:
    """{code: quote} → {code: 拍平数值行}，供 today_ticks / parquet 使用。"""
    payload: TickStorePayload = {}
    for code, quote in quotes.items():
        tick = resolve_tick_quote(quote)
        if tick is not None:
            payload[str(code)] = quote_to_store_row(tick, local_ms)
    return payload


def copy_tick_rows(rows: list[list[Any]]) -> list[list[Any]]:
    return [row.copy() if isinstance(row, list) else row for row in rows]


# ---------------------------------------------------------------------------
# store 行 ↔ parquet
# ---------------------------------------------------------------------------


def rows_to_dataframe(rows: list[list[Any]]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=list(TICK_STORE_COLUMNS))


def rows_to_parquet_dataframe(rows: list[list[Any]]) -> pd.DataFrame:
    frame = rows_to_dataframe(rows)
    for column in TICK_STORE_TIME_COLUMNS:
        values = frame[column].to_numpy(dtype="int64", copy=False)
        frame[column] = [timestamp_ms_to_hms(int(value)) for value in values]
    return frame


def parquet_dataframe_to_rows(frame: pd.DataFrame, day: int) -> list[list[Any]]:
    ordered = frame.reindex(columns=list(TICK_STORE_COLUMNS))
    year, month, d = day // 10000, (day // 100) % 100, day % 100
    base_ts = int(datetime(year, month, d).timestamp())
    for column in TICK_STORE_TIME_COLUMNS:
        parts = ordered[column].astype(str).str.split(":", expand=True)
        hours = parts[0].astype(int)
        minutes = parts[1].astype(int)
        seconds = parts[2].astype(int) if parts.shape[1] > 2 else 0
        ordered[column] = (base_ts + hours * 3600 + minutes * 60 + seconds) * 1000
    rows = ordered.values.tolist()
    for row in rows:
        for index in _TICK_STORE_TIME_INDEXES:
            row[index] = int(row[index])
    return rows
