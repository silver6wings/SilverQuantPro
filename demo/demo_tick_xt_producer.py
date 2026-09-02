"""
Xtquant NATS 生产端 demo。

前置条件
--------
- 本机已安装并登录 miniQMT，行情服务可用。
- NATS 已启动（如 `PYTHONPATH=. python data/job_nats_service.py`）。

用法
----
    PYTHONPATH=. python demo/demo_tick_xt_producer.py

在 main() 里改 CODE_LIST 或 SECTORS 即可自定义订阅范围。
"""
import logging
from pathlib import Path

from data.tick.xtquant.nats_producer import XtquantNatsProducer
from delegate.xtdata_delegate import XtdataDelegate
from tools.utils_remote_xt import XtSectorType

_LOG_PATH = Path("_cache/demo_xt_producer.log")

# 直接指定 code 列表；非空时优先使用，忽略 SECTORS
CODE_LIST: list[str] = []

# CODE_LIST 为空时，按板块拉取 code
SECTORS: list[str] = [
    XtSectorType.SZ_STOCK,
    XtSectorType.SH_STOCK,
    # XtSectorType.HS_INDEX,
    # XtSectorType.HS_ETF,
    # XtSectorType.HS_KZZ,
]


def setup_logging() -> None:
    _LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(_LOG_PATH, encoding="utf-8"),
        ],
        force=True,
    )


def _load_code_list() -> list[str]:
    if CODE_LIST:
        return list(CODE_LIST)

    code_list = XtdataDelegate.get_code_list(SECTORS)
    print(f"Total codes={len(code_list)}", code_list[:10], "...")
    return code_list


def main() -> None:
    setup_logging()
    logger = logging.getLogger(__name__)

    code_list = _load_code_list()
    logger.info("subscribing %d codes from %s", len(code_list), SECTORS if not CODE_LIST else "CODE_LIST")

    producer = XtquantNatsProducer()
    producer.set_code_list(code_list)
    producer.run()


if __name__ == "__main__":
    main()
