"""
miniQMT xtdata 消费端 demo（原生 subscribe_whole_quote）。

前置条件
--------
- 本机已安装并登录 miniQMT，行情服务可用。

用法
----
    PYTHONPATH=. python demo/demo_tick_xt_consumer.py
"""
import datetime
import logging
import time
from pathlib import Path

from xtquant import xtdata

xtdata.enable_hello = False

_LOG_PATH = Path("_cache/demo_xt_consumer.log")

DEMO_CODE_LIST = [
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


def callback_sub_whole(quotes: dict) -> None:
    sample_codes = list(quotes.keys())[:5]
    print(datetime.datetime.now(), f"quotes={len(quotes)}", f"sample={sample_codes}")


def main() -> None:
    setup_logging()
    logger = logging.getLogger(__name__)

    seq = xtdata.subscribe_whole_quote(DEMO_CODE_LIST, callback=callback_sub_whole)
    if seq < 0:
        raise RuntimeError("subscribe failed")

    logger.info("subscribed %d codes, seq=%s", len(DEMO_CODE_LIST), seq)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        xtdata.unsubscribe_quote(seq)
        logger.info("unsubscribed seq=%s", seq)


if __name__ == "__main__":
    main()
