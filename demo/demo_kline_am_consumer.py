"""
Amazing K 线 NATS 消费端 demo。

环境变量 KLINE_CONSUMER_INTERVAL：秒，默认 1.0；设为 0 表示每条消息尽快回调。
"""
import logging
from pathlib import Path

from data.kline.kline_entity import KlineDispatchBatch
from data.kline.amazing.nats_consumer import AmazingKlineNatsConsumer

_LOG_PATH = Path("_cache/demo_kline_consumer.log")


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


def main() -> None:
    import os

    setup_logging()
    interval = 1.0

    code_list = [
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

    consumer = AmazingKlineNatsConsumer(interval=interval)
    if consumer.subscribe_kline(code_list, callback=callback_on_kline) != 0:
        raise RuntimeError("subscribe failed")
    try:
        consumer.wait()
    except KeyboardInterrupt:
        pass
    finally:
        consumer.unsubscribe_kline()


def callback_on_kline(batch: KlineDispatchBatch) -> None:
    import datetime

    print(datetime.datetime.now(), f"bars: {len(batch.bars)}")
    for bar in batch.bars[:5]:
        print(bar.code, bar.bar)
    if len(batch.bars) > 5:
        print(f"... and {len(batch.bars) - 5} more")


if __name__ == "__main__":
    main()
