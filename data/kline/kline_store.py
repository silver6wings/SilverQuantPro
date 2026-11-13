"""K 线内存行与 parquet 列。同一 (period, timestamp) 只保留最后一笔。"""
from __future__ import annotations

from enum import IntEnum
from typing import Any

import pandas as pd

from data.kline.kline_entity import KlineBarDict
from data.tick.tick_quote import hms_to_timestamp_ms, timestamp_ms_to_hms

KlineStoreRow = list[Any]
BarKey = tuple[int, int]  # (period_seconds, timestamp_ms)


class KlineCol(IntEnum):
    """拍平 list 列下标。"""

    LOCAL = 0
    TIMESTAMP = 1
    PERIOD = 2
    OPEN = 3
    HIGH = 4
    LOW = 5
    CLOSE = 6
    VOLUME = 7
    AMOUNT = 8


KLINE_STORE_COLUMNS: tuple[str, ...] = (
    "local",
    "time",
    "period",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
)

KLINE_STORE_TIME_COLUMNS: tuple[str, ...] = ("local", "time")
_KLINE_STORE_INT_INDEXES: tuple[int, ...] = (
    KlineCol.LOCAL,
    KlineCol.TIMESTAMP,
    KlineCol.PERIOD,
    KlineCol.VOLUME,
)


def bar_key(period: int, timestamp_ms: int) -> BarKey:
    return int(period), int(timestamp_ms)


def bar_to_store_row(bar: KlineBarDict, local_ms: int) -> KlineStoreRow:
    return [
        int(local_ms),
        int(bar.get("timestamp", 0)),
        int(bar.get("period", 0)),
        bar.get("open", 0),
        bar.get("high", 0),
        bar.get("low", 0),
        bar.get("close", 0),
        bar.get("volume", 0),
        bar.get("amount", 0),
    ]


def upsert_bar(bucket: dict[BarKey, KlineStoreRow], row: KlineStoreRow) -> None:
    bucket[bar_key(row[KlineCol.PERIOD], row[KlineCol.TIMESTAMP])] = row


def rows_from_bucket(bucket: dict[BarKey, KlineStoreRow]) -> list[KlineStoreRow]:
    ordered = sorted(
        bucket.values(),
        key=lambda row: (int(row[KlineCol.TIMESTAMP]), int(row[KlineCol.PERIOD])),
    )
    return [row.copy() for row in ordered]


def copy_kline_rows(rows: list[KlineStoreRow]) -> list[KlineStoreRow]:
    return [row.copy() for row in rows]


def rows_to_parquet_dataframe(rows: list[KlineStoreRow]) -> pd.DataFrame:
    frame = pd.DataFrame(rows, columns=list(KLINE_STORE_COLUMNS))
    for column in KLINE_STORE_TIME_COLUMNS:
        values = frame[column].to_numpy(dtype="int64", copy=False)
        frame[column] = [timestamp_ms_to_hms(int(value)) for value in values]
    return frame


def parquet_dataframe_to_rows(frame: pd.DataFrame, day: int) -> list[KlineStoreRow]:
    ordered = frame.reindex(columns=list(KLINE_STORE_COLUMNS))
    for column in KLINE_STORE_TIME_COLUMNS:
        ordered[column] = [
            hms_to_timestamp_ms(day, value) for value in ordered[column].astype(str)
        ]
    rows = ordered.values.tolist()
    for row in rows:
        for index in _KLINE_STORE_INT_INDEXES:
            row[index] = int(row[index])
    return rows
