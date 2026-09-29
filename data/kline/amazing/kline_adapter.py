"""AmazingData Kline → 统一 kline 格式。"""

from __future__ import annotations

import AmazingData as ad

from data.kline.kline_entity import KlinePayload, build_kline_bar

Kline = ad.constant.Kline

# A 股 K 线成交量：SDK 为股，统一格式为手（与 tick amazing snapshot 一致）
_LOT_SIZE = 100


def _vol_lots(volume: int | float | None) -> int:
    return int(volume or 0) // _LOT_SIZE


def kline_to_payload(data: Kline, period_seconds: int) -> KlinePayload:
    """Convert AmazingData Kline to {code: kline_bar}."""
    if not data.code or data.kline_time is None:
        raise ValueError("kline missing code or kline_time")

    return {
        data.code: build_kline_bar(
            timestamp=int(data.kline_time.timestamp() * 1000),
            period=period_seconds,
            open=data.open,
            high=data.high,
            low=data.low,
            close=data.close,
            volume=_vol_lots(data.volume),
            amount=data.amount,
        )
    }
