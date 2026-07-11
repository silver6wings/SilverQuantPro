import logging
import os
from pathlib import Path

from delegate.amazing_delegate import AmazingDelegate
from data.tick.amazing.nats_producer import AmazingNatsProducer
from tools.utils_remote_am import AmazingSecurityType

_DEFAULT_SECURITY_TYPES = AmazingSecurityType.HS_STOCK

_LOG_PATH = Path("_cache/am_producer.log")


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


def _load_security_types() -> list[str]:
    raw = os.getenv("AM_SUBSCRIBE_SECURITY_TYPES", _DEFAULT_SECURITY_TYPES)
    return [security_type.strip() for security_type in raw.split(",") if security_type.strip()]


def run() -> None:
    security_types = _load_security_types()
    code_list: list[str] = []
    seen: set[str] = set()
    for security_type in security_types:
        codes = AmazingDelegate.get_codes(security_type)
        print(f"{security_type}: {len(codes)}")
        for code in codes:
            if code not in seen:
                seen.add(code)
                code_list.append(code)
    print(f"Total codes={len(code_list)}")

    producer = AmazingNatsProducer()
    producer.set_code_list(code_list)
    producer.run()


def main() -> None:
    setup_logging()
    run()


if __name__ == "__main__":
    main()
