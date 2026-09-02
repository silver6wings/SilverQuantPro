"""
Xtquant NATS 消费端 demo。

前置条件
--------
- NATS 已启动，subject 与 credentials 中 NATS_XT_SUBJECT 一致。
- demo_tick_xt_producer.py（或 job_tick_xtquant.py）正在推送行情。

用法
----
    PYTHONPATH=. python demo/demo_tick_xt_consumer.py
"""
import datetime
import logging
from pathlib import Path

from data.tick.xtquant.nats_consumer import XtquantNatsConsumer

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

    consumer = XtquantNatsConsumer(interval=1.0)
    if consumer.subscribe_whole_quote(DEMO_CODE_LIST, callback=callback_sub_whole) != 0:
        raise RuntimeError("subscribe failed")

    logger.info("subscribed %d codes via NATS", len(DEMO_CODE_LIST))
    try:
        consumer.wait()
    except KeyboardInterrupt:
        pass
    finally:
        consumer.unsubscribe_quote()
        logger.info("unsubscribed, quote_count=%d", consumer.quote_count)


if __name__ == "__main__":
    main()
