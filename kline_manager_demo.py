"""
KlineSubscriber demo：消费 Amazing K 线 NATS 流。

前置条件
--------
- NATS 已启动，kline_data_scheduler_am.py 在推 K 线（或手动跑 demo_kline_am_producer.py）

用法
----
    PYTHONPATH=. python kline_manager_demo.py

on_klines(hour, minute, second, batch)：batch.bars 为 KlineBarDispatch 列表。
"""
from __future__ import annotations

import datetime
import logging
from pathlib import Path

from data.kline.kline_entity import KlineDispatchBatch
from framework.kline_subscriber import KlineSubscriber
from framework.trading_session import AM_KLINE_TRADING_SESSION_WINDOWS

_LOG_PATH = Path("_cache/kline_manager_demo.log")

CODE_LIST = [
    "000001.SZ",
    "300001.SZ",
    "600000.SH",
    "688001.SH",
    "920000.BJ",
    "000001.SH",
    "399001.SZ",
    "159159.SZ",
    "510510.SH",
    "123268.SZ",
    "113066.SH",
]

DISPATCH_INTERVAL = 1.0
# 每批回调最多打印几根 bar；<=0 表示打印本批全部
PRINT_BAR_LIMIT = 5

print("订阅:", len(CODE_LIST), CODE_LIST)


def setup_logging() -> None:
    _LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(_LOG_PATH, encoding="utf-8"),
        ],
        force=True,
    )


def on_klines(hour: int, minute: int, second: int, batch: KlineDispatchBatch) -> None:
    print(
        datetime.datetime.now(),
        f"{hour:02d}:{minute:02d}:{second:02d}",
        f"bars={len(batch.bars)}",
    )
    limit = PRINT_BAR_LIMIT if PRINT_BAR_LIMIT > 0 else len(batch.bars)
    for bar in batch.bars[:limit]:
        print(f"  {bar.code}", bar.bar)
    if limit < len(batch.bars):
        print(f"  ... and {len(batch.bars) - limit} more")


def main() -> None:
    setup_logging()

    sub = KlineSubscriber(
        code_list=list(CODE_LIST),
        on_klines=on_klines,
        dispatch_interval=DISPATCH_INTERVAL,
        trading_session_windows=AM_KLINE_TRADING_SESSION_WINDOWS,
    )
    sub.run()


if __name__ == "__main__":
    main()
