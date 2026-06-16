"""
Demo：调用 data/amazing_job.py 推送全市场 tick。

用法
----
    PYTHONPATH=. python demo/demo_tick_amazing_producer.py
"""
import logging
from pathlib import Path

from data.amazing_job import run

_LOG_PATH = Path("_cache/demo_producer.log")


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
    setup_logging()
    run()


if __name__ == "__main__":
    main()
