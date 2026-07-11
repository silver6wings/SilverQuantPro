"""
Amazing NATS 生产端 demo。

用法
----
    PYTHONPATH=. python demo/demo_tick_am_producer.py

在 main() 里改 CODE_LIST 或 SECURITY_TYPES 即可自定义订阅范围。
"""
import logging
from pathlib import Path

from data.tick.amazing.nats_producer import AmazingNatsProducer
from delegate.amazing_delegate import AmazingDelegate
from tools.utils_remote_am import AmazingSecurityType

_LOG_PATH = Path("_cache/demo_producer.log")

# 直接指定 code 列表；非空时优先使用，忽略 SECURITY_TYPES
CODE_LIST: list[str] = [
    "000001.SZ",
    "600000.SH",
]

# CODE_LIST 为空时，按板块类型拉取 code
SECURITY_TYPES: list[str] = [
    AmazingSecurityType.HS_STOCK,
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


def _load_code_list() -> list[str]:
    if CODE_LIST:
        return list(CODE_LIST)

    code_list: list[str] = []
    seen: set[str] = set()
    for security_type in SECURITY_TYPES:
        codes = AmazingDelegate.get_codes(security_type)
        print(f"{security_type}: {len(codes)}")
        for code in codes:
            if code not in seen:
                seen.add(code)
                code_list.append(code)
    print(f"Total codes={len(code_list)}")
    return code_list


def main() -> None:
    setup_logging()
    logger = logging.getLogger(__name__)

    code_list = _load_code_list()
    logger.info("subscribing %d codes", len(code_list))

    producer = AmazingNatsProducer()
    producer.set_code_list(code_list)
    producer.run()


if __name__ == "__main__":
    main()
