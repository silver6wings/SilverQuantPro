"""
Amazing kline_time → epoch ms（交易所本地 Asia/Shanghai 钟面）。

SubscribeData 与 query_kline 共用本模块，避免 pandas naive .timestamp() 按 UTC 解释。
"""

from __future__ import annotations

import pandas as pd

_CN_TZ = "Asia/Shanghai"


def amazing_kline_time_to_naive_local(value) -> pd.Timestamp:
    """
    返回交易所本地钟面（无时区）。

    - query_kline：naive 的 datetime64 钟面已是 CST，勿用裸 .timestamp()（pandas 会当 UTC）。
    - 若带 tz：先转到 Asia/Shanghai 再去 tz。
    """
    ts = pd.Timestamp(value)
    if ts.tz is not None:
        return ts.tz_convert(_CN_TZ).tz_localize(None)
    return ts


def amazing_kline_time_to_epoch_ms(value) -> int:
    """bar 起点 epoch 毫秒，供与 NATS 相同的 bar_end_datetime_str 链路使用。"""
    ts = pd.Timestamp(value)
    if ts.tz is not None:
        aware = ts.tz_convert(_CN_TZ)
    else:
        aware = ts.tz_localize(_CN_TZ)
    return int(aware.timestamp() * 1000)
