"""
TickManager + TickSubscriber demo：通过 CONSUMER_TYPE 切换行情数据源。

前置条件
--------
- am_nats / xt_nats：NATS 已启动，对应 scheduler + producer 在推行情
  - am_nats  -> tick_data_scheduler_am.py  -> job_tick_amazing.py
  - xt_nats  -> tick_data_scheduler_xt.py  -> job_tick_xtquant.py
- am_direct：AmazingData 已配置 credentials，直连 SDK（无需 NATS）
- xt_direct：本机 miniQMT 已登录，行情服务可用

用法
----
    PYTHONPATH=. python tick_manager_demo.py

修改 CONSUMER_TYPE 即可切换数据源；code_list 控制 callback 里包含哪些 code（NATS 在 consumer 侧过滤）。
on_quotes(hour, minute, second, quotes)，quotes 为 {code: TickQuoteDict}。
"""
from __future__ import annotations

import datetime
import logging
from pathlib import Path

from framework.tick_manager import TickManager
from framework.tick_subscriber import (
    ConsumerType,
    TickSubscriber,
)

# ---------------------------------------------------------------------------
# 数据源配置：改这一行即可
# ---------------------------------------------------------------------------
CONSUMER_TYPE = ConsumerType.AM_NATS
_LOG_PATH = Path("_cache/tick_manager_demo.log")

CODE_LIST = [
    "000001.SZ",    # stock
    "300001.SZ",    # stock
    "600000.SH",    # stock
    "688001.SH",    # stock
    "920000.BJ",    # stock
    "000001.SH",    # index
    "399001.SZ",    # index
    "159159.SZ",    # etf
    "510510.SH",    # etf
    "123268.SZ",    # kzz
    "113066.SH",    # kzz
]


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


def on_quotes(hour: int, minute: int, second: int, quotes: dict) -> None:
    sample_codes = list(quotes.keys())[:5]
    print(
        datetime.datetime.now(),
        f"{hour:02d}:{minute:02d}:{second:02d}",
        f"quotes={len(quotes)}",
        f"sample={sample_codes}",
    )
    for code, quote in quotes.items():
        print(code, quote)


def main() -> None:
    setup_logging()

    tick_manager = TickManager()

    sub = TickSubscriber(
        consumer_type=CONSUMER_TYPE,
        code_list=list(CODE_LIST),
        on_quotes=on_quotes,
        dispatch_interval=1.0,
        record_tick_today=True,     # 内存缓存 → today_ticks(code)
        save_tick_history=True,     # 15:05 落盘；须同时开启 record_tick_today
        tick_manager=tick_manager,
    )
    sub.run()


if __name__ == "__main__":
    main()
