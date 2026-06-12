import logging
from pathlib import Path

from delegate.amazing_delegate import AmazingDelegate, AmazingSecurityType
from data.tick.amazing_nats_producer import AmazingNatsProducer

_LOG_LEVEL = logging.INFO
_LOG_PATH = Path("_cache/demo_producer.log")


def setup_logging() -> None:
    _LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=_LOG_LEVEL,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(_LOG_PATH, encoding="utf-8"),
        ],
        force=True,
    )


def main() -> None:
    setup_logging()

    delegate = AmazingDelegate()
    code_list = delegate.get_codes(AmazingSecurityType.HSA_STOCK)
    print(len(code_list), code_list)

    producer = AmazingNatsProducer()
    producer.set_code_list(code_list)
    producer.run()


if __name__ == "__main__":
    main()
