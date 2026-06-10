import logging
from pathlib import Path

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

    producer = AmazingNatsProducer()
    code_list = ["000001.SZ", "000002.SZ"]

    producer.set_code_list(code_list)
    producer.run()


if __name__ == "__main__":
    main()
