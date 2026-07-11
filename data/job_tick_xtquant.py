import logging
import os
from pathlib import Path

from delegate.xtdata_delegate import XtdataDelegate
from data.tick.xtquant.nats_producer import XtquantNatsProducer
from tools.utils_remote_xt import XtSectorType, parse_xt_sectors

_DEFAULT_SECTORS = (
    XtSectorType.SZ_STOCK,
    XtSectorType.SH_STOCK,
)

_LOG_PATH = Path("_cache/xt_producer.log")


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


def _load_sectors() -> list[str]:
    raw = os.getenv("XT_SUBSCRIBE_SECTORS", ",".join(_DEFAULT_SECTORS))
    return parse_xt_sectors(raw, _DEFAULT_SECTORS)


def run() -> None:
    sectors = _load_sectors()
    code_list = XtdataDelegate.get_code_list(sectors)
    print(f"Total codes={len(code_list)}")

    producer = XtquantNatsProducer()
    producer.set_code_list(code_list)
    producer.run()


def main() -> None:
    setup_logging()
    run()


if __name__ == "__main__":
    main()
