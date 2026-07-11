"""
miniQMT xtdata 生产端 demo（原生 subscribe_whole_quote，默认订阅上证/深证 A 股）。

前置条件
--------
- 本机已安装并登录 miniQMT，行情服务可用。

用法
----
    PYTHONPATH=. python demo/demo_tick_xt_producer.py
"""
import logging
import time
from pathlib import Path

from delegate.xtdata_delegate import XtdataDelegate
from tools.utils_remote_xt import XtSectorType
from xtquant import xtdata

xtdata.enable_hello = False

_LOG_PATH = Path("_cache/demo_xt_producer.log")

_DEFAULT_SECTORS = (
    XtSectorType.SZ_STOCK,
    XtSectorType.SH_STOCK,
)

_tick_count = 0


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


def callback_on_tick(quotes: dict) -> None:
    global _tick_count
    logger = logging.getLogger(__name__)
    _tick_count += len(quotes)
    for code, quote in quotes.items():
        logger.debug("%s %s", code, quote)


def main() -> None:
    setup_logging()
    logger = logging.getLogger(__name__)

    code_list = XtdataDelegate.get_code_list(list(_DEFAULT_SECTORS))
    logger.info(
        "loaded %d codes from %s",
        len(code_list),
        ", ".join(_DEFAULT_SECTORS),
    )
    print(len(code_list), code_list[:10], "...")

    seq = xtdata.subscribe_whole_quote(code_list, callback=callback_on_tick)
    if seq < 0:
        raise RuntimeError("subscribe failed")

    logger.info("subscribed, seq=%s", seq)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        xtdata.unsubscribe_quote(seq)
        logger.info("unsubscribed seq=%s, tick_count=%d", seq, _tick_count)


if __name__ == "__main__":
    main()
